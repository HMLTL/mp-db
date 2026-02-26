package com.mpdb.storage;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.ColumnType;
import com.mpdb.catalog.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StorageEngineTest {

    private StorageEngine engine;

    @BeforeEach
    void setUp() {
        engine = new StorageEngine();
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
}
