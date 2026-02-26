package com.mpdb.executor;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.ColumnType;
import com.mpdb.catalog.TableSchema;
import com.mpdb.repl.CalciteQueryParser;
import com.mpdb.storage.Tuple;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class PredicateBuilderTest {

    private PredicateBuilder predicateBuilder;
    private CalciteQueryParser parser;
    private TableSchema schema;

    @BeforeEach
    void setUp() {
        predicateBuilder = new PredicateBuilder();
        parser = new CalciteQueryParser();
        schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50),
                new ColumnDefinition("active", ColumnType.BOOLEAN)
        ));
    }

    private SqlNode parseWhere(String sql) throws Exception {
        SqlNode node = parser.parse(sql);
        return ((SqlSelect) node).getWhere();
    }

    @Test
    void equals_int() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE id = 1");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertTrue(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
        assertFalse(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
    }

    @Test
    void equals_varchar() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE name = 'Alice'");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertTrue(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
        assertFalse(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
    }

    @Test
    void greaterThan_int() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE id > 1");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertFalse(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
        assertTrue(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
    }

    @Test
    void lessThan_int() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE id < 2");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertTrue(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
        assertFalse(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
    }

    @Test
    void notEquals_int() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE id <> 1");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertFalse(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
        assertTrue(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
    }

    @Test
    void nullWhere_shouldReturnAlwaysTrue() {
        Predicate<Tuple> pred = predicateBuilder.build(null, schema);
        assertTrue(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
    }

    @Test
    void andOperator() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE id = 1 AND active = true");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertTrue(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
        assertFalse(pred.test(new Tuple(schema, new Object[]{1, "Alice", false})));
        assertFalse(pred.test(new Tuple(schema, new Object[]{2, "Alice", true})));
    }

    @Test
    void orOperator() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE id = 1 OR id = 2");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertTrue(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
        assertTrue(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
        assertFalse(pred.test(new Tuple(schema, new Object[]{3, "Charlie", true})));
    }

    @Test
    void equals_boolean() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE active = false");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertFalse(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
        assertTrue(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
    }
}
