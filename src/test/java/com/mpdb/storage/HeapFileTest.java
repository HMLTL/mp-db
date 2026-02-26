package com.mpdb.storage;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.ColumnType;
import com.mpdb.catalog.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HeapFileTest {

    private TableSchema schema;
    private HeapFile heapFile;

    @BeforeEach
    void setUp() {
        schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50),
                new ColumnDefinition("active", ColumnType.BOOLEAN)
        ));
        heapFile = new HeapFile(schema);
    }

    @Test
    void insertAndScanAll_shouldReturnAllTuples() {
        heapFile.insertTuple(new Tuple(schema, new Object[]{1, "Alice", true}));
        heapFile.insertTuple(new Tuple(schema, new Object[]{2, "Bob", false}));

        List<Tuple> results = heapFile.scanAll();
        assertEquals(2, results.size());
    }

    @Test
    void insertAndGetTuple_shouldReturnCorrectData() {
        TupleId id = heapFile.insertTuple(new Tuple(schema, new Object[]{1, "Alice", true}));

        Tuple tuple = heapFile.getTuple(id);
        assertNotNull(tuple);
        assertEquals(1, tuple.getValue(0));
        assertEquals("Alice", tuple.getValue(1));
        assertEquals(true, tuple.getValue(2));
    }

    @Test
    void scanWithFilter_shouldReturnMatchingTuples() {
        heapFile.insertTuple(new Tuple(schema, new Object[]{1, "Alice", true}));
        heapFile.insertTuple(new Tuple(schema, new Object[]{2, "Bob", false}));
        heapFile.insertTuple(new Tuple(schema, new Object[]{3, "Charlie", true}));

        List<Tuple> active = heapFile.scanWithFilter(t -> (Boolean) t.getValue(2));
        assertEquals(2, active.size());
    }

    @Test
    void deleteTuple_shouldRemoveFromScan() {
        TupleId id1 = heapFile.insertTuple(new Tuple(schema, new Object[]{1, "Alice", true}));
        heapFile.insertTuple(new Tuple(schema, new Object[]{2, "Bob", false}));

        assertTrue(heapFile.deleteTuple(id1));
        List<Tuple> results = heapFile.scanAll();
        assertEquals(1, results.size());
        assertEquals("Bob", results.get(0).getValue(1));
    }

    @Test
    void deleteTuple_invalidId_shouldReturnFalse() {
        assertFalse(heapFile.deleteTuple(new TupleId(999, 0)));
    }

    @Test
    void scanAllWithIds_shouldReturnTupleIds() {
        heapFile.insertTuple(new Tuple(schema, new Object[]{1, "Alice", true}));
        heapFile.insertTuple(new Tuple(schema, new Object[]{2, "Bob", false}));

        List<Map.Entry<TupleId, Tuple>> results = heapFile.scanAllWithIds();
        assertEquals(2, results.size());
        assertNotNull(results.get(0).getKey());
        assertNotNull(results.get(0).getValue());
    }

    @Test
    void scanWithFilterAndIds_shouldReturnFilteredResults() {
        heapFile.insertTuple(new Tuple(schema, new Object[]{1, "Alice", true}));
        heapFile.insertTuple(new Tuple(schema, new Object[]{2, "Bob", false}));

        List<Map.Entry<TupleId, Tuple>> results = heapFile.scanWithFilterAndIds(
                t -> (Integer) t.getValue(0) == 1
        );
        assertEquals(1, results.size());
        assertEquals("Alice", results.get(0).getValue().getValue(1));
    }

    @Test
    void multiPage_shouldSpanMultiplePages() {
        // Insert enough tuples to require multiple pages
        // Each tuple is ~60 bytes (4B size + 4B int + 4B strlen + ~5B name + 1B bool)
        // Page is 4096 bytes, so ~60 tuples per page
        for (int i = 0; i < 200; i++) {
            heapFile.insertTuple(new Tuple(schema, new Object[]{i, "Name" + i, i % 2 == 0}));
        }

        assertTrue(heapFile.getPageCount() > 1);
        assertEquals(200, heapFile.scanAll().size());
    }

    @Test
    void scanAfterDelete_shouldExcludeDeletedTuples() {
        for (int i = 0; i < 10; i++) {
            heapFile.insertTuple(new Tuple(schema, new Object[]{i, "Name" + i, true}));
        }

        // Delete even-numbered tuples
        List<Map.Entry<TupleId, Tuple>> all = heapFile.scanAllWithIds();
        for (Map.Entry<TupleId, Tuple> entry : all) {
            if ((Integer) entry.getValue().getValue(0) % 2 == 0) {
                heapFile.deleteTuple(entry.getKey());
            }
        }

        List<Tuple> remaining = heapFile.scanAll();
        assertEquals(5, remaining.size());
        for (Tuple t : remaining) {
            assertTrue((Integer) t.getValue(0) % 2 != 0);
        }
    }
}
