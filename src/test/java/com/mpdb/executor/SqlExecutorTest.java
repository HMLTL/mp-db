package com.mpdb.executor;

import com.mpdb.catalog.Catalog;
import com.mpdb.repl.CalciteQueryParser;
import com.mpdb.storage.StorageEngine;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlExecutorTest {

    private SqlExecutor executor;
    private CalciteQueryParser parser;

    @BeforeEach
    void setUp() {
        Catalog catalog = new Catalog();
        StorageEngine storageEngine = new StorageEngine();
        PredicateBuilder predicateBuilder = new PredicateBuilder();

        executor = new SqlExecutor(
                new CreateTableHandler(catalog, storageEngine),
                new InsertHandler(catalog, storageEngine),
                new SelectHandler(catalog, storageEngine, predicateBuilder),
                new DeleteHandler(catalog, storageEngine, predicateBuilder),
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
    void unsupportedStatement_shouldThrow() throws Exception {
        // UPDATE is parsed by Calcite but not supported by our executor
        SqlNode node = parser.parse("UPDATE users SET name = 'x' WHERE id = 1");
        assertThrows(UnsupportedOperationException.class, () -> executor.execute(node));
    }
}
