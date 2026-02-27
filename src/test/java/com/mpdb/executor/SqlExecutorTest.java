package com.mpdb.executor;

import com.mpdb.catalog.Catalog;
import com.mpdb.repl.CalciteQueryParser;
import com.mpdb.storage.StorageEngine;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SqlExecutorTest {

    private SqlExecutor executor;
    private CalciteQueryParser parser;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        Catalog catalog = new Catalog(tempDir.toString());
        catalog.init();
        StorageEngine storageEngine = new StorageEngine(tempDir.toString(), catalog);
        PredicateBuilder predicateBuilder = new PredicateBuilder();

        executor = new SqlExecutor(
                new CreateTableHandler(catalog, storageEngine),
                new InsertHandler(catalog, storageEngine),
                new SelectHandler(catalog, storageEngine, predicateBuilder),
                new DeleteHandler(catalog, storageEngine, predicateBuilder),
                new UpdateHandler(catalog, storageEngine, predicateBuilder),
                new DropTableHandler(catalog, storageEngine)
        );
        parser = new CalciteQueryParser();
    }

    private String execute(String sql) throws Exception {
        SqlNode node = parser.parse(sql);
        return executor.execute(node);
    }

    @Test
    void createTable() throws Exception {
        String result = execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        assertTrue(result.toLowerCase().contains("table"));
        assertTrue(result.toLowerCase().contains("created"));
    }

    @Test
    void insertIntoTable() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        String result = execute("INSERT INTO users VALUES (1, 'Alice', true)");
        assertTrue(result.contains("Inserted 1 row."));
    }

    @Test
    void selectFromTable() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50))");
        execute("INSERT INTO users VALUES (1, 'Alice')");
        execute("INSERT INTO users VALUES (2, 'Bob')");

        String result = execute("SELECT * FROM users");
        assertTrue(result.contains("Alice"));
        assertTrue(result.contains("Bob"));
        assertTrue(result.contains("(2 rows)"));
    }

    @Test
    void selectWithWhere() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50))");
        execute("INSERT INTO users VALUES (1, 'Alice')");
        execute("INSERT INTO users VALUES (2, 'Bob')");

        String result = execute("SELECT * FROM users WHERE id = 1");
        assertTrue(result.contains("Alice"));
        assertFalse(result.contains("Bob"));
        assertTrue(result.contains("(1 row)"));
    }

    @Test
    void deleteFromTable() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        execute("INSERT INTO users VALUES (1, 'Alice', true)");
        execute("INSERT INTO users VALUES (2, 'Bob', false)");

        String result = execute("DELETE FROM users WHERE active = false");
        assertTrue(result.contains("Deleted 1 row."));

        String selectResult = execute("SELECT * FROM users");
        assertTrue(selectResult.contains("Alice"));
        assertFalse(selectResult.contains("Bob"));
    }

    @Test
    void dropTable() throws Exception {
        execute("CREATE TABLE users (id INT)");
        String result = execute("DROP TABLE users");
        assertTrue(result.toLowerCase().contains("dropped"));
    }

    @Test
    void dropNonExistentTable_shouldThrow() {
        assertThrows(Exception.class, () -> execute("DROP TABLE nonexistent"));
    }

    @Test
    void insertIntoNonExistentTable_shouldThrow() {
        assertThrows(Exception.class, () -> execute("INSERT INTO nonexistent VALUES (1)"));
    }

    @Test
    void selectFromNonExistentTable_shouldThrow() {
        assertThrows(Exception.class, () -> execute("SELECT * FROM nonexistent"));
    }

    @Test
    void fullWorkflow() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        execute("INSERT INTO users VALUES (1, 'Alice', true)");
        execute("INSERT INTO users VALUES (2, 'Bob', false)");
        execute("INSERT INTO users VALUES (3, 'Charlie', true)");

        String allResult = execute("SELECT * FROM users");
        assertTrue(allResult.contains("(3 rows)"));

        String filteredResult = execute("SELECT * FROM users WHERE active = true");
        assertTrue(filteredResult.contains("(2 rows)"));

        execute("DELETE FROM users WHERE active = false");

        String afterDelete = execute("SELECT * FROM users");
        assertTrue(afterDelete.contains("(2 rows)"));
        assertFalse(afterDelete.contains("Bob"));

        execute("DROP TABLE users");
    }

    @Test
    void createTable_withFloatAndText() throws Exception {
        String result = execute("CREATE TABLE products (id INT, name VARCHAR(100), price FLOAT, description TEXT, active BOOLEAN)");
        assertTrue(result.contains("created"));
    }

    @Test
    void insertAndSelect_floatAndText() throws Exception {
        execute("CREATE TABLE products (id INT, name VARCHAR(100), price FLOAT, description TEXT, active BOOLEAN)");
        execute("INSERT INTO products VALUES (1, 'Widget', 9.99, 'A fine widget', true)");
        execute("INSERT INTO products VALUES (2, 'Gadget', 24.5, 'An even finer gadget', false)");

        String result = execute("SELECT * FROM products");
        assertTrue(result.contains("Widget"));
        assertTrue(result.contains("Gadget"));
        assertTrue(result.contains("9.99"));
        assertTrue(result.contains("24.5"));
        assertTrue(result.contains("A fine widget"));
        assertTrue(result.contains("(2 rows)"));
    }

    @Test
    void selectWithWhere_float() throws Exception {
        execute("CREATE TABLE products (id INT, price FLOAT)");
        execute("INSERT INTO products VALUES (1, 9.99)");
        execute("INSERT INTO products VALUES (2, 24.5)");

        String result = execute("SELECT * FROM products WHERE price > 10.0");
        assertTrue(result.contains("24.5"));
        assertFalse(result.contains("9.99"));
        assertTrue(result.contains("(1 row)"));
    }

    @Test
    void deleteWithFloat_andText() throws Exception {
        execute("CREATE TABLE products (id INT, price FLOAT, description TEXT, active BOOLEAN)");
        execute("INSERT INTO products VALUES (1, 9.99, 'A fine widget', true)");
        execute("INSERT INTO products VALUES (2, 24.5, 'An even finer gadget', false)");

        execute("DELETE FROM products WHERE active = false");

        String result = execute("SELECT * FROM products");
        assertTrue(result.contains("9.99"));
        assertFalse(result.contains("24.5"));
        assertTrue(result.contains("(1 row)"));
    }

    @Test
    void updateWithWhere() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        execute("INSERT INTO users VALUES (1, 'Alice', true)");
        execute("INSERT INTO users VALUES (2, 'Bob', false)");

        String result = execute("UPDATE users SET name = 'Robert' WHERE id = 2");
        assertTrue(result.contains("Updated 1 row."));

        String selectResult = execute("SELECT * FROM users WHERE id = 2");
        assertTrue(selectResult.contains("Robert"));
        assertFalse(selectResult.contains("Bob"));
    }

    @Test
    void updateAllRows() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        execute("INSERT INTO users VALUES (1, 'Alice', true)");
        execute("INSERT INTO users VALUES (2, 'Bob', false)");

        String result = execute("UPDATE users SET active = true");
        assertTrue(result.contains("Updated 2 rows."));

        String selectResult = execute("SELECT * FROM users WHERE active = false");
        assertTrue(selectResult.contains("(0 rows)"));
    }

    @Test
    void updateNonExistentTable_shouldThrow() {
        assertThrows(Exception.class, () -> execute("UPDATE nonexistent SET name = 'x' WHERE id = 1"));
    }

    @Test
    void insertWithNull() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        String result = execute("INSERT INTO users VALUES (1, NULL, true)");
        assertTrue(result.contains("Inserted 1 row."));

        String selectResult = execute("SELECT * FROM users");
        assertTrue(selectResult.contains("NULL"));
        assertTrue(selectResult.contains("(1 row)"));
    }

    @Test
    void selectWhereIsNull() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        execute("INSERT INTO users VALUES (1, NULL, true)");
        execute("INSERT INTO users VALUES (2, 'Bob', false)");

        String result = execute("SELECT * FROM users WHERE name IS NULL");
        assertTrue(result.contains("(1 row)"));
        assertFalse(result.contains("Bob"));

        String result2 = execute("SELECT * FROM users WHERE name IS NOT NULL");
        assertTrue(result2.contains("Bob"));
        assertTrue(result2.contains("(1 row)"));
    }

    @Test
    void updateSetToNull() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        execute("INSERT INTO users VALUES (1, 'Alice', true)");

        execute("UPDATE users SET name = NULL WHERE id = 1");

        String result = execute("SELECT * FROM users");
        assertTrue(result.contains("NULL"));
        assertFalse(result.contains("Alice"));
    }

    @Test
    void updateWhereIsNull() throws Exception {
        execute("CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)");
        execute("INSERT INTO users VALUES (1, NULL, true)");
        execute("INSERT INTO users VALUES (2, 'Bob', false)");

        execute("UPDATE users SET name = 'Unknown' WHERE name IS NULL");

        String result = execute("SELECT * FROM users WHERE id = 1");
        assertTrue(result.contains("Unknown"));
    }

    // --- JOIN tests ---

    @Test
    void innerJoin_basic() throws Exception {
        execute("CREATE TABLE a (id INT, name VARCHAR(50))");
        execute("CREATE TABLE b (id INT, a_id INT, val VARCHAR(50))");
        execute("INSERT INTO a VALUES (1, 'Alice')");
        execute("INSERT INTO a VALUES (2, 'Bob')");
        execute("INSERT INTO b VALUES (1, 1, 'X')");
        execute("INSERT INTO b VALUES (2, 2, 'Y')");
        execute("INSERT INTO b VALUES (3, 99, 'Z')");

        String result = execute("SELECT * FROM a INNER JOIN b ON a.id = b.a_id");
        assertTrue(result.contains("Alice"));
        assertTrue(result.contains("Bob"));
        assertTrue(result.contains("X"));
        assertTrue(result.contains("Y"));
        assertFalse(result.contains("Z"));
        assertTrue(result.contains("(2 rows)"));
    }

    @Test
    void leftJoin_basic() throws Exception {
        execute("CREATE TABLE a (id INT, name VARCHAR(50))");
        execute("CREATE TABLE b (id INT, a_id INT, val VARCHAR(50))");
        execute("INSERT INTO a VALUES (1, 'Alice')");
        execute("INSERT INTO a VALUES (2, 'Bob')");
        execute("INSERT INTO a VALUES (3, 'Charlie')");
        execute("INSERT INTO b VALUES (1, 1, 'X')");

        String result = execute("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id");
        assertTrue(result.contains("Alice"));
        assertTrue(result.contains("X"));
        assertTrue(result.contains("Bob"));
        assertTrue(result.contains("Charlie"));
        assertTrue(result.contains("NULL"));
        assertTrue(result.contains("(3 rows)"));
    }

    @Test
    void joinWithWhere() throws Exception {
        execute("CREATE TABLE a (id INT, name VARCHAR(50))");
        execute("CREATE TABLE b (id INT, a_id INT, val VARCHAR(50))");
        execute("INSERT INTO a VALUES (1, 'Alice')");
        execute("INSERT INTO a VALUES (2, 'Bob')");
        execute("INSERT INTO b VALUES (1, 1, 'X')");
        execute("INSERT INTO b VALUES (2, 2, 'Y')");

        String result = execute("SELECT * FROM a INNER JOIN b ON a.id = b.a_id WHERE a.name = 'Alice'");
        assertTrue(result.contains("Alice"));
        assertTrue(result.contains("X"));
        assertFalse(result.contains("Bob"));
        assertTrue(result.contains("(1 row)"));
    }

    // --- Subquery tests ---

    @Test
    void subqueryInFrom() throws Exception {
        execute("CREATE TABLE t (id INT, name VARCHAR(50))");
        execute("INSERT INTO t VALUES (1, 'Alice')");
        execute("INSERT INTO t VALUES (2, 'Bob')");

        String result = execute("SELECT * FROM (SELECT * FROM t) AS sub");
        assertTrue(result.contains("Alice"));
        assertTrue(result.contains("Bob"));
        assertTrue(result.contains("(2 rows)"));
    }

    @Test
    void whereInSubquery() throws Exception {
        execute("CREATE TABLE t1 (id INT, name VARCHAR(50))");
        execute("CREATE TABLE t2 (id INT, t1_id INT)");
        execute("INSERT INTO t1 VALUES (1, 'Alice')");
        execute("INSERT INTO t1 VALUES (2, 'Bob')");
        execute("INSERT INTO t1 VALUES (3, 'Charlie')");
        execute("INSERT INTO t2 VALUES (1, 1)");
        execute("INSERT INTO t2 VALUES (2, 3)");

        String result = execute("SELECT * FROM t1 WHERE id IN (SELECT t1_id FROM t2)");
        assertTrue(result.contains("Alice"));
        assertFalse(result.contains("Bob"));
        assertTrue(result.contains("Charlie"));
        assertTrue(result.contains("(2 rows)"));
    }

    // --- Column projection tests ---

    @Test
    void columnProjection() throws Exception {
        execute("CREATE TABLE t (id INT, name VARCHAR(50), active BOOLEAN)");
        execute("INSERT INTO t VALUES (1, 'Alice', true)");
        execute("INSERT INTO t VALUES (2, 'Bob', false)");

        String result = execute("SELECT id, name FROM t");
        assertTrue(result.contains("Alice"));
        assertTrue(result.contains("Bob"));
        assertFalse(result.contains("ACTIVE"));
        assertTrue(result.contains("(2 rows)"));
    }

    @Test
    void ambiguousColumn_shouldThrow() throws Exception {
        execute("CREATE TABLE a (id INT, name VARCHAR(50))");
        execute("CREATE TABLE b (id INT, val VARCHAR(50))");
        execute("INSERT INTO a VALUES (1, 'Alice')");
        execute("INSERT INTO b VALUES (1, 'X')");

        // Both tables have 'id', bare reference should be ambiguous in JOIN context
        assertThrows(Exception.class, () ->
                execute("SELECT * FROM a INNER JOIN b ON id = id"));
    }

    @Test
    void unsupportedStatement_shouldThrow() throws Exception {
        // MERGE is parsed by Calcite but not supported by our executor
        assertThrows(UnsupportedOperationException.class, () -> {
            SqlNode node = parser.parse("EXPLAIN PLAN FOR SELECT * FROM users");
            executor.execute(node);
        });
    }
}
