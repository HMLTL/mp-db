package com.mpdb.catalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CatalogTest {

    private Catalog catalog;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        catalog = new Catalog(tempDir.toString());
        catalog.init();
    }

    @Test
    void createTable_shouldStoreSchema() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50)
        ));

        catalog.createTable(schema);

        assertTrue(catalog.tableExists("users"));
        assertNotNull(catalog.getTable("users"));
        assertEquals("users", catalog.getTable("users").getTableName());
    }

    @Test
    void createTable_shouldBeCaseInsensitive() {
        TableSchema schema = new TableSchema("Users", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        catalog.createTable(schema);

        assertTrue(catalog.tableExists("USERS"));
        assertTrue(catalog.tableExists("users"));
        assertNotNull(catalog.getTable("users"));
    }

    @Test
    void createTable_duplicateShouldThrow() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        catalog.createTable(schema);

        assertThrows(IllegalStateException.class, () -> catalog.createTable(schema));
    }

    @Test
    void dropTable_shouldRemoveSchema() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        catalog.createTable(schema);
        catalog.dropTable("users");

        assertFalse(catalog.tableExists("users"));
        assertNull(catalog.getTable("users"));
    }

    @Test
    void dropTable_nonExistentShouldThrow() {
        assertThrows(IllegalStateException.class, () -> catalog.dropTable("nonexistent"));
    }

    @Test
    void getTable_nonExistentShouldReturnNull() {
        assertNull(catalog.getTable("nonexistent"));
    }

    @Test
    void persistence_shouldSurviveReload() {
        TableSchema schema = new TableSchema("products", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 100),
                new ColumnDefinition("price", ColumnType.FLOAT),
                new ColumnDefinition("active", ColumnType.BOOLEAN)
        ));
        catalog.createTable(schema);

        // Create a new catalog pointing to the same directory
        Catalog reloaded = new Catalog(tempDir.toString());
        reloaded.init();

        assertTrue(reloaded.tableExists("products"));
        TableSchema loaded = reloaded.getTable("products");
        assertNotNull(loaded);
        assertEquals("products", loaded.getTableName());
        assertEquals(4, loaded.getColumnCount());
        assertEquals("id", loaded.getColumn(0).name());
        assertEquals(ColumnType.INT, loaded.getColumn(0).type());
        assertEquals("name", loaded.getColumn(1).name());
        assertEquals(ColumnType.VARCHAR, loaded.getColumn(1).type());
        assertEquals(100, loaded.getColumn(1).maxLength());
        assertEquals("price", loaded.getColumn(2).name());
        assertEquals(ColumnType.FLOAT, loaded.getColumn(2).type());
        assertEquals("active", loaded.getColumn(3).name());
        assertEquals(ColumnType.BOOLEAN, loaded.getColumn(3).type());
    }
}
