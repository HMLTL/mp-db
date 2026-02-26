# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
./gradlew clean build

# Run the REPL
./gradlew bootRun

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests com.mpdb.repl.CommandProcessorTest

# Run a single test method
./gradlew test --tests "com.mpdb.repl.CommandProcessorTest.methodName"
```

Test reports are generated at `build/reports/tests/test/index.html`.

## Architecture

CLI REPL application (prompt: `mp-db>`) that parses SQL via Apache Calcite and executes against an in-memory + disk-backed storage engine.

### Request Flow

```
User Input → ReplRunner (JLine LineReader)
               ↓
         CommandProcessor
               ↓
    starts with ":"?
       ↓            ↓
     YES            NO
       ↓            ↓
ColonCommandProcessor   SqlQueryProcessor
       ↓                      ↓
    DbState             CalciteQueryParser
  (toggle flags)        (parse → SqlNode AST)
                              ↓
                         SqlExecutor
                         (dispatch by SqlKind)
                              ↓
                    Statement Handlers
              (Create/Insert/Select/Update/Delete/Drop)
                              ↓
                  Catalog + StorageEngine + HeapFile
```

### Key Packages

| Package | Role |
|---|---|
| `com.mpdb.repl` | REPL loop, command routing, Calcite parser |
| `com.mpdb.executor` | SQL execution handlers, predicate builder, result formatter |
| `com.mpdb.catalog` | Table schemas, column types, catalog persistence |
| `com.mpdb.storage` | SlottedPage, HeapFile, TupleSerializer, DiskPageManager, FreeSpaceMap |

### Key Classes

| Class | Role |
|---|---|
| `ReplRunner` | REPL loop using JLine LineReader with command history |
| `CommandProcessor` | Routes `:*` to `ColonCommandProcessor`, SQL to `SqlQueryProcessor` |
| `SqlQueryProcessor` | Parses SQL via Calcite, delegates to `SqlExecutor` |
| `SqlExecutor` | Dispatches `SqlNode` by kind to statement handlers |
| `CreateTableHandler` | Maps Calcite DDL AST → Catalog + StorageEngine |
| `InsertHandler` | Extracts VALUES → Tuple → HeapFile insert |
| `SelectHandler` | Scan with optional WHERE predicate → ResultFormatter |
| `UpdateHandler` | Scan matching rows → delete old + insert new tuple |
| `DeleteHandler` | Scan with predicate → delete matching tuples |
| `Catalog` | `ConcurrentHashMap<String, TableSchema>`, persisted to `catalog.meta` |
| `StorageEngine` | Manages `HeapFile` per table, disk-backed via `DiskPageManager` |
| `HeapFile` | `List<SlottedPage>` + `FreeSpaceMap`, insert/scan/delete operations |
| `SlottedPage` | 4096B page: 8B header, slot directory, backward-growing tuple area |
| `DiskPageManager` | `RandomAccessFile`-backed page I/O with fsync |
| `FreeSpaceMap` | Tracks free bytes per page to avoid O(n) insert scans |
| `TupleSerializer` | Serialize/deserialize Tuple ↔ byte[] (length-prefix for VARCHAR/TEXT) |
| `CatalogPersistence` | Reads/writes `catalog.meta` (table schemas) |

### Conventions

- **Quit signaling**: `ColonCommandProcessor.handleQuit()` returns literal `"EXIT"`, checked by `ReplRunner`
- **Calcite DDL**: `CalciteQueryParser` uses `SqlDdlParserImpl.FACTORY` for CREATE/DROP/ALTER
- **Update strategy**: delete-then-reinsert (handles variable-length size changes)
- **Persistence**: catalog saved on every CREATE/DROP; pages fsynced on every write; `@PreDestroy` closes files
- **Data directory**: configurable via `app.data-dir` (default `./data`), stores `catalog.meta` + `<TABLE>.dat` + `.mpdb_history`
- **State extension**: add field to `DbState`, add `ColonCommand` enum entry, handle in `ColonCommandProcessor`
- **New SQL statement**: create `XxxHandler implements StatementHandler`, add case in `SqlExecutor.execute()`

### Tech Stack

- Java 17, Spring Boot 3.4, Gradle
- Apache Calcite 1.37.0 (SQL parsing + DDL via `calcite-server`)
- JLine 3.25.1 (REPL with command history and line editing)
- Lombok (`@Getter`/`@Setter` on `DbState`)
- JUnit 5 + Mockito for tests
