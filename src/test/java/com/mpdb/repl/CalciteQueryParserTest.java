package com.mpdb.repl;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CalciteQueryParser Tests")
class CalciteQueryParserTest {

    private CalciteQueryParser parser;

    @BeforeEach
    void setUp() {
        parser = new CalciteQueryParser();
    }

    @Test
    @DisplayName("Should successfully parse valid SELECT query")
    void shouldParseValidSelectQuery() {
        String sql = "SELECT * FROM users";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertTrue(result.isValid(), "Query should be valid");
        assertNotNull(result.ast(), "AST should not be null");
        assertNull(result.errorMessage(), "Error message should be null");
        assertEquals("SELECT", result.getSqlKind(), "SQL kind should be SELECT");
    }

    @Test
    @DisplayName("Should successfully parse SELECT with WHERE clause")
    void shouldParseSelectWithWhere() {
        String sql = "SELECT name, age FROM users WHERE id = 1";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertTrue(result.isValid());
        assertNotNull(result.ast());
        assertEquals("SELECT", result.getSqlKind());
    }

    @Test
    @DisplayName("Should successfully parse INSERT query")
    void shouldParseInsertQuery() {
        String sql = "INSERT INTO users (name, age) VALUES ('John', 25)";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertTrue(result.isValid());
        assertNotNull(result.ast());
        assertEquals("INSERT", result.getSqlKind());
    }

    @Test
    @DisplayName("Should successfully parse UPDATE query")
    void shouldParseUpdateQuery() {
        String sql = "UPDATE users SET age = 26 WHERE id = 1";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertTrue(result.isValid());
        assertNotNull(result.ast());
        assertEquals("UPDATE", result.getSqlKind());
    }

    @Test
    @DisplayName("Should successfully parse DELETE query")
    void shouldParseDeleteQuery() {
        String sql = "DELETE FROM users WHERE id = 1";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertTrue(result.isValid());
        assertNotNull(result.ast());
        assertEquals("DELETE", result.getSqlKind());
    }


    @Test
    @DisplayName("Should return error for invalid SQL syntax")
    void shouldReturnErrorForInvalidSyntax() {
        String sql = "SELECT * FORM users"; // FORM instead of FROM
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertFalse(result.isValid(), "Query should be invalid");
        assertNull(result.ast(), "AST should be null for invalid query");
        assertNotNull(result.errorMessage(), "Error message should not be null");
        assertTrue(result.errorMessage().contains("Syntax error"), "Error message should mention syntax error");
    }

    @Test
    @DisplayName("Should return error for incomplete query")
    void shouldReturnErrorForIncompleteQuery() {
        String sql = "SELECT * FROM";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertFalse(result.isValid());
        assertNull(result.ast());
        assertNotNull(result.errorMessage());
    }

    @Test
    @DisplayName("Should handle empty string")
    void shouldHandleEmptyString() {
        String sql = "";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertFalse(result.isValid());
        assertNull(result.ast());
        assertNotNull(result.errorMessage());
    }

    @Test
    @DisplayName("Should parse case-insensitive SQL")
    void shouldParseCaseInsensitiveSql() {
        String sql = "select * from USERS where ID = 1";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertTrue(result.isValid());
        assertNotNull(result.ast());
        assertEquals("SELECT", result.getSqlKind());
    }

    @Test
    @DisplayName("Should parse complex JOIN query")
    void shouldParseComplexJoinQuery() {
        String sql = "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertTrue(result.isValid());
        assertNotNull(result.ast());
        assertEquals("SELECT", result.getSqlKind());
    }

    @Test
    @DisplayName("Should parse query with subquery")
    void shouldParseQueryWithSubquery() {
        String sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertTrue(result.isValid());
        assertNotNull(result.ast());
        assertEquals("SELECT", result.getSqlKind());
    }

    @Test
    @DisplayName("Should parse query and throw SqlParseException directly")
    void shouldThrowSqlParseExceptionDirectly() {
        String sql = "INVALID SQL QUERY";
        assertThrows(SqlParseException.class, () -> parser.parse(sql));
    }

    @Test
    @DisplayName("Should return AST string for valid query")
    void shouldReturnAstStringForValidQuery() {
        String sql = "SELECT name FROM users";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertTrue(result.isValid());
        String astString = result.getAstString();
        assertNotNull(astString);
        assertFalse(astString.isEmpty(), "AST string should not be empty");
        assertTrue(astString.toUpperCase().contains("SELECT") || astString.contains("name"),
                   "AST should contain SELECT or name");
    }

    @Test
    @DisplayName("Should return N/A for AST string when query is invalid")
    void shouldReturnNAForInvalidQueryAst() {
        String sql = "INVALID";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        assertFalse(result.isValid());
        assertEquals("N/A", result.getAstString());
        assertEquals("N/A", result.getSqlKind());
    }
}

