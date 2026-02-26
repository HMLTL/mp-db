package com.mpdb.storage;

import com.mpdb.catalog.Catalog;
import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.ColumnType;
import com.mpdb.catalog.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StorageEngineTest {

    private StorageEngine engine;
    private Catalog catalog;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        catalog = new Catalog(tempDir.toString());
        catalog.init();
        engine = new StorageEngine(tempDir.toString(), catalog);
    }

    @Test
    void createHeapFile_shouldReturnHeapFile() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));

        HeapFile heapFile = engine.createHeapFile(schema);
        assertNotNull(heapFile);
        assertTrue(engine.heapFileExists("users"));
    }

    @Test
    void createHeapFile_duplicate_shouldThrow() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        engine.createHeapFile(schema);

        assertThrows(IllegalStateException.class, () -> engine.createHeapFile(schema));
    }

    @Test
    void getHeapFile_shouldReturnExistingFile() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        engine.createHeapFile(schema);

        assertNotNull(engine.getHeapFile("users"));
        assertNotNull(engine.getHeapFile("USERS"));
    }

    @Test
    void getHeapFile_nonExistent_shouldReturnNull() {
        assertNull(engine.getHeapFile("nonexistent"));
    }

    @Test
    void dropHeapFile_shouldRemoveFile() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        engine.createHeapFile(schema);
        engine.dropHeapFile("users");

        assertFalse(engine.heapFileExists("users"));
        assertNull(engine.getHeapFile("users"));
    }

    @Test
    void dropHeapFile_nonExistent_shouldThrow() {
        assertThrows(IllegalStateException.class, () -> engine.dropHeapFile("nonexistent"));
    }

    @Test
    void persistence_shouldSurviveRestart() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50)
        ));
        catalog.createTable(schema);
        HeapFile heapFile = engine.createHeapFile(schema);
        heapFile.insertTuple(new Tuple(schema, new Object[]{1, "Alice"}));
        heapFile.insertTuple(new Tuple(schema, new Object[]{2, "Bob"}));

        // Simulate restart: reload catalog and storage engine
        Catalog reloadedCatalog = new Catalog(tempDir.toString());
        reloadedCatalog.init();
        StorageEngine reloadedEngine = new StorageEngine(tempDir.toString(), reloadedCatalog);
        reloadedEngine.init();

        assertTrue(reloadedEngine.heapFileExists("users"));
        HeapFile reloadedHeap = reloadedEngine.getHeapFile("users");
        List<Tuple> tuples = reloadedHeap.scanAll();
        assertEquals(2, tuples.size());
        assertEquals(1, tuples.get(0).getValue(0));
        assertEquals("Alice", tuples.get(0).getValue(1));
        assertEquals(2, tuples.get(1).getValue(0));
        assertEquals("Bob", tuples.get(1).getValue(1));
    }
}
