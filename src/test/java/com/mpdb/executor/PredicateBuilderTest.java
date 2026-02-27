package com.mpdb.executor;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.ColumnType;
import com.mpdb.catalog.TableSchema;
import com.mpdb.repl.CalciteQueryParser;
import com.mpdb.storage.Tuple;
import org.apache.calcite.sql.SqlJoin;
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
    void greaterThan_float() throws Exception {
        TableSchema floatSchema = new TableSchema("products", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("price", ColumnType.FLOAT)
        ));
        CalciteQueryParser p = new CalciteQueryParser();
        SqlNode where = ((SqlSelect) p.parse("SELECT * FROM products WHERE price > 10.0")).getWhere();
        Predicate<Tuple> pred = predicateBuilder.build(where, floatSchema);

        assertTrue(pred.test(new Tuple(floatSchema, new Object[]{1, 24.5f})));
        assertFalse(pred.test(new Tuple(floatSchema, new Object[]{2, 9.99f})));
    }

    @Test
    void equals_float() throws Exception {
        TableSchema floatSchema = new TableSchema("products", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("price", ColumnType.FLOAT)
        ));
        CalciteQueryParser p = new CalciteQueryParser();
        SqlNode where = ((SqlSelect) p.parse("SELECT * FROM products WHERE price = 9.99")).getWhere();
        Predicate<Tuple> pred = predicateBuilder.build(where, floatSchema);

        assertTrue(pred.test(new Tuple(floatSchema, new Object[]{1, 9.99f})));
        assertFalse(pred.test(new Tuple(floatSchema, new Object[]{2, 24.5f})));
    }

    @Test
    void isNull_matchesNullValue() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE name IS NULL");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertTrue(pred.test(new Tuple(schema, new Object[]{1, null, true})));
        assertFalse(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
    }

    @Test
    void isNotNull_matchesNonNullValue() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE name IS NOT NULL");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertFalse(pred.test(new Tuple(schema, new Object[]{1, null, true})));
        assertTrue(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
    }

    @Test
    void comparisonWithNullValue_returnsFalse() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE name = 'Alice'");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        // NULL compared with anything returns false (SQL three-valued logic)
        assertFalse(pred.test(new Tuple(schema, new Object[]{1, null, true})));
    }

    @Test
    void columnColumnComparison_withMergedSchema() throws Exception {
        TableSchema left = new TableSchema("A", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50)
        ));
        TableSchema right = new TableSchema("B", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("a_id", ColumnType.INT)
        ));
        TableSchema merged = TableSchema.merge("A", left, "B", right);

        // Parse: SELECT * FROM A INNER JOIN B ON A.id = B.a_id
        SqlNode node = parser.parse("SELECT * FROM A INNER JOIN B ON A.id = B.a_id");
        SqlSelect select = (SqlSelect) node;
        SqlJoin join = (SqlJoin) select.getFrom();
        SqlNode condition = join.getCondition();

        Predicate<Tuple> pred = predicateBuilder.build(condition, merged);

        // A.id=1, A.name='Alice', B.id=1, B.a_id=1 -> match
        assertTrue(pred.test(new Tuple(merged, new Object[]{1, "Alice", 1, 1})));
        // A.id=1, A.name='Alice', B.id=2, B.a_id=2 -> no match
        assertFalse(pred.test(new Tuple(merged, new Object[]{1, "Alice", 2, 2})));
    }

    @Test
    void qualifiedColumnName_resolution() throws Exception {
        TableSchema left = new TableSchema("A", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        TableSchema right = new TableSchema("B", List.of(
                new ColumnDefinition("val", ColumnType.VARCHAR, 50)
        ));
        TableSchema merged = TableSchema.merge("A", left, "B", right);

        // A.ID should resolve to index 0
        assertEquals(0, merged.getColumnIndex("A.ID"));
        // B.VAL should resolve to index 1
        assertEquals(1, merged.getColumnIndex("B.VAL"));
        // Unambiguous bare name should resolve via suffix match
        assertEquals(0, merged.getColumnIndex("ID"));
        assertEquals(1, merged.getColumnIndex("VAL"));
    }

    @Test
    void equals_boolean() throws Exception {
        SqlNode where = parseWhere("SELECT * FROM users WHERE active = false");
        Predicate<Tuple> pred = predicateBuilder.build(where, schema);

        assertFalse(pred.test(new Tuple(schema, new Object[]{1, "Alice", true})));
        assertTrue(pred.test(new Tuple(schema, new Object[]{2, "Bob", false})));
    }
}
