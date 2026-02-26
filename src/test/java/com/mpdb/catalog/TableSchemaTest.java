package com.mpdb.catalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableSchemaTest {

    @Test
    void shouldReturnTableName() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        assertEquals("users", schema.getTableName());
    }

    @Test
    void shouldReturnColumnCount() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50),
                new ColumnDefinition("active", ColumnType.BOOLEAN)
        ));
        assertEquals(3, schema.getColumnCount());
    }

    @Test
    void shouldGetColumnByIndex() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50)
        ));
        assertEquals("id", schema.getColumn(0).name());
        assertEquals("name", schema.getColumn(1).name());
    }

    @Test
    void shouldGetColumnByName_caseInsensitive() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("Id", ColumnType.INT)
        ));
        assertNotNull(schema.getColumn("id"));
        assertNotNull(schema.getColumn("ID"));
        assertNotNull(schema.getColumn("Id"));
    }

    @Test
    void shouldReturnNullForUnknownColumn() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        assertNull(schema.getColumn("nonexistent"));
        assertEquals(-1, schema.getColumnIndex("nonexistent"));
    }

    @Test
    void shouldReturnColumnIndex() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50)
        ));
        assertEquals(0, schema.getColumnIndex("id"));
        assertEquals(1, schema.getColumnIndex("name"));
    }
}
