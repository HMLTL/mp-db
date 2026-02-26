package com.mpdb.executor;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.ColumnType;
import com.mpdb.catalog.TableSchema;
import com.mpdb.storage.Tuple;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResultFormatterTest {

    @Test
    void emptyResults_shouldReturnZeroRows() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        String result = ResultFormatter.format(List.of(), schema);
        assertEquals("(0 rows)", result);
    }

    @Test
    void singleRow_shouldFormatAsTable() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50)
        ));
        List<Tuple> tuples = List.of(new Tuple(schema, new Object[]{1, "Alice"}));

        String result = ResultFormatter.format(tuples, schema);
        assertTrue(result.contains("id"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("1"));
        assertTrue(result.contains("Alice"));
        assertTrue(result.contains("(1 row)"));
    }

    @Test
    void multipleRows_shouldShowRowCount() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        List<Tuple> tuples = List.of(
                new Tuple(schema, new Object[]{1}),
                new Tuple(schema, new Object[]{2}),
                new Tuple(schema, new Object[]{3})
        );

        String result = ResultFormatter.format(tuples, schema);
        assertTrue(result.contains("(3 rows)"));
    }

    @Test
    void shouldAlignColumns() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50)
        ));
        List<Tuple> tuples = List.of(
                new Tuple(schema, new Object[]{1, "Alice"}),
                new Tuple(schema, new Object[]{2, "Bob"})
        );

        String result = ResultFormatter.format(tuples, schema);
        // Should contain separator line
        assertTrue(result.contains("-+-"));
    }
}
