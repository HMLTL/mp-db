package com.mpdb.catalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CatalogTest {

    private Catalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new Catalog();
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
}
