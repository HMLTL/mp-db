///# Heap Storage Engine — Implementation Plan

## Context

The project currently only **parses** SQL via Apache Calcite but does not execute it. We need to add a heap file storage engine so that CREATE TABLE, INSERT, SELECT, DELETE, and DROP TABLE actually work. The storage layer is in-memory (byte arrays simulating disk pages) but designed for future swap to file-backed I/O.

---

## Package Structure

```
com.mpdb.catalog/    — type system + table metadata
com.mpdb.storage/    — page-based heap storage (no SQL/Calcite dependency)
com.mpdb.executor/   — bridges Calcite AST → storage operations
com.mpdb.repl/       — existing REPL (modify SqlQueryProcessor)
```

---

## Phase 1: Catalog — Type System & Table Metadata

| File | Type | Role |
|---|---|---|
| `catalog/ColumnType.java` | enum | `INT(4)`, `VARCHAR(var)`, `BOOLEAN(1)` with fixedSize + isFixedLength |
| `catalog/ColumnDefinition.java` | record | `(name, type, maxLength)` |
| `catalog/TableSchema.java` | class | Holds table name + `List<ColumnDefinition>`, column lookup by name/index |
| `catalog/Catalog.java` | @Component | `ConcurrentHashMap<String, TableSchema>` — createTable/dropTable/getTable |

**Tests:** `CatalogTest`, `TableSchemaTest`

---

## Phase 2: Tuple & Serialization

| File | Type | Role |
|---|---|---|
| `storage/Tuple.java` | class | `Object[] values` + `TableSchema` reference |
| `storage/TupleSerializer.java` | class | Serialize/deserialize Tuple ↔ `byte[]` |

**Serialization format per tuple:**
```
[4B total size] [per column: INT=4B | BOOLEAN=1B | VARCHAR=4B length + N bytes UTF-8]
```

NULLs not supported in Phase 1 (can add null bitmap later).

**Tests:** `TupleSerializerTest` — round-trip for all types, edge cases

---

## Phase 3: Slotted Page

| File | Type | Role |
|---|---|---|
| `storage/SlottedPage.java` | class | 4KB page with slotted layout |
| `storage/TupleId.java` | record | `(pageIndex, slotIndex)` |

**Page layout (4096 bytes):**
```
[Header 8B: 2B slotCount | 2B freeSpacePtr | 4B pageId]
[Slot directory → grows forward: 4B per slot (2B offset + 2B length)]
   ... free space ...
[Tuple data ← grows backward from end of page]
```

Deleted slots: offset = `0xFFFF` sentinel. No compaction in Phase 1.

**Tests:** `SlottedPageTest` — insert/retrieve/delete, fill to capacity, free space math

---

## Phase 4: Heap File & Storage Engine

| File | Type | Role |
|---|---|---|
| `storage/HeapFile.java` | class | `List<SlottedPage>` per table — insert, scan, filtered scan, delete |
| `storage/StorageEngine.java` | @Component | `Map<String, HeapFile>` — create/drop/get heap files |

`HeapFile` methods: `insertTuple`, `scanAll`, `scanWithFilter(Predicate<Tuple>)`, `scanAllWithIds`, `scanWithFilterAndIds`, `deleteTuple(TupleId)`, `getTuple(TupleId)`

**Tests:** `HeapFileTest` (multi-page, scan after delete), `StorageEngineTest`

---

## Phase 5: SQL Executor — AST → Storage

| File | Type | Role |
|---|---|---|
| `executor/StatementHandler.java` | interface | `String handle(SqlNode node)` |
| `executor/ResultFormatter.java` | utility | Formats `List<Tuple>` → ASCII table |
| `executor/PredicateBuilder.java` | @Component | Converts WHERE `SqlNode` → `Predicate<Tuple>` |
| `executor/CreateTableHandler.java` | @Component | `SqlCreateTable` → catalog + storage |
| `executor/InsertHandler.java` | @Component | `SqlInsert` → Tuple → HeapFile |
| `executor/SelectHandler.java` | @Component | `SqlSelect` → scan → formatted output |
| `executor/DeleteHandler.java` | @Component | `SqlDelete` → scan with ids → delete |
| `executor/DropTableHandler.java` | @Component | `SqlDropTable` → remove from catalog + storage |
| `executor/SqlExecutor.java` | @Component | Dispatches `SqlNode.getKind()` → handler |

**Calcite AST mapping:**
- `CREATE TABLE` → `SqlCreateTable` (calcite-server DDL) → `name`, `columnList`
- `INSERT INTO` → `SqlInsert` → `getTargetTable()`, `getSource()` (VALUES rows)
- `SELECT` → `SqlSelect` → `getFrom()`, `getWhere()`, `getSelectList()`
- `DELETE` → `SqlDelete` → `getTargetTable()`, `getCondition()`
- `DROP TABLE` → `SqlDropTable` → `name`

**Predicate operators:** `=`, `<`, `>`, `<=`, `>=`, `!=` for INT/VARCHAR/BOOLEAN

**Tests:** one test class per handler + `PredicateBuilderTest`, `ResultFormatterTest`, `SqlExecutorTest`

---

## Phase 6: Integration — Wire Into Existing REPL

**Modify** `SqlQueryProcessor.java`:
- Add `SqlExecutor` as constructor dependency
- After successful parse + optional AST debug, call `sqlExecutor.execute(result.ast())` instead of returning "not yet implemented"
- Wrap in try/catch, return `"Execution Error: " + e.getMessage()` on failure

**Update** `SqlQueryProcessorTest.java`:
- Add `@Mock SqlExecutor` and verify `execute()` is called with AST

---

## Key Design Decisions

1. **No NULLs in Phase 1** — simplifies tuple format; null bitmap can be added later
2. **No page compaction on delete** — slots marked with `0xFFFF` sentinel; space not reclaimed
3. **VARCHAR uses length-prefix encoding** — 4B length + N bytes UTF-8
4. **`Predicate<Tuple>` for WHERE** — keeps storage layer SQL-free
5. **`ConcurrentHashMap`** for Catalog/StorageEngine — cheap future-proofing
6. **Max row size ~4080 bytes** — single tuple cannot span pages

---

## File Manifest

**32 new files** (19 main + 13 test), **2 modified files** (SqlQueryProcessor + its test)

---

## Verification

1. `./gradlew test` — all existing + new tests pass
2. `./gradlew bootRun` then in REPL:
   ```sql
   CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN);
   INSERT INTO users VALUES (1, 'Alice', true);
   INSERT INTO users VALUES (2, 'Bob', false);
   SELECT * FROM users;
   SELECT * FROM users WHERE id = 1;
   DELETE FROM users WHERE active = false;
   SELECT * FROM users;
   DROP TABLE users;
   ```
