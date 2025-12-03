package com.example.mydb.repl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("SqlQueryProcessor Tests")
class SqlQueryProcessorTest {

    private SqlQueryProcessor processor;

    @Mock
    private CalciteQueryParser queryParser;

    @Mock
    private DbState dbState;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        processor = new SqlQueryProcessor(queryParser, dbState);
    }

    @Test
    @DisplayName("Should return error message for invalid SQL")
    void shouldReturnErrorForInvalidSql() {
        String sql = "SELECT * FORM users";
        CalciteQueryParser.ParseResult invalidResult = new CalciteQueryParser.ParseResult(
                false, null, "Syntax error at line 1, column 11: Encountered \"FORM\"", sql
        );
        when(queryParser.parseAndValidate(sql)).thenReturn(invalidResult);

        String result = processor.process(sql);

        assertTrue(result.contains("❌ SQL Parse Error"));
        assertTrue(result.contains("Syntax error"));
    }

    @Test
    @DisplayName("Should return empty response when debug mode is off")
    void shouldReturnEmptyResponseWhenDebugOff() {
        String sql = "SELECT * FROM users";
        CalciteQueryParser.ParseResult validResult = mock(CalciteQueryParser.ParseResult.class);
        when(validResult.isValid()).thenReturn(true);
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(false);

        String result = processor.process(sql);

        assertEquals("", result);
        verify(dbState).isDebugAstMode();
    }

    @Test
    @DisplayName("Should return AST when debug mode is on")
    void shouldReturnAstWhenDebugOn() {
        String sql = "SELECT * FROM users";
        CalciteQueryParser.ParseResult validResult = mock(CalciteQueryParser.ParseResult.class);
        when(validResult.isValid()).thenReturn(true);
        when(validResult.getSqlKind()).thenReturn("SELECT");
        when(validResult.getAstString()).thenReturn("SELECT * FROM `users`");
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(true);

        String result = processor.process(sql);

        assertTrue(result.contains("Query Type: SELECT"));
        assertTrue(result.contains("AST:"));
        assertTrue(result.contains("Query execution is not yet implemented"));
    }

    @Test
    @DisplayName("Should format AST output with proper indentation")
    void shouldFormatAstOutput() {
        String sql = "SELECT name, age FROM users WHERE id = 1";
        CalciteQueryParser.ParseResult validResult = mock(CalciteQueryParser.ParseResult.class);
        when(validResult.isValid()).thenReturn(true);
        when(validResult.getSqlKind()).thenReturn("SELECT");
        when(validResult.getAstString()).thenReturn("SELECT `name`, `age` FROM `users` WHERE (`id` = 1)");
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(true);

        String result = processor.process(sql);

        assertNotNull(result);
        assertTrue(result.contains("SELECT"));
        assertTrue(result.contains("AST:"));
    }

    @Test
    @DisplayName("Should handle INSERT query")
    void shouldHandleInsertQuery() {
        String sql = "INSERT INTO users (name) VALUES ('John')";
        CalciteQueryParser.ParseResult validResult = mock(CalciteQueryParser.ParseResult.class);
        when(validResult.isValid()).thenReturn(true);
        when(validResult.getSqlKind()).thenReturn("INSERT");
        when(validResult.getAstString()).thenReturn("INSERT INTO `users`");
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(true);

        String result = processor.process(sql);

        assertTrue(result.contains("Query Type: INSERT"));
    }

    @Test
    @DisplayName("Should handle UPDATE query")
    void shouldHandleUpdateQuery() {
        String sql = "UPDATE users SET age = 26 WHERE id = 1";
        CalciteQueryParser.ParseResult validResult = mock(CalciteQueryParser.ParseResult.class);
        when(validResult.isValid()).thenReturn(true);
        when(validResult.getSqlKind()).thenReturn("UPDATE");
        when(validResult.getAstString()).thenReturn("UPDATE `users`");
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(true);

        String result = processor.process(sql);

        assertTrue(result.contains("Query Type: UPDATE"));
    }

    @Test
    @DisplayName("Should handle DELETE query")
    void shouldHandleDeleteQuery() {
        String sql = "DELETE FROM users WHERE id = 1";
        CalciteQueryParser.ParseResult validResult = mock(CalciteQueryParser.ParseResult.class);
        when(validResult.isValid()).thenReturn(true);
        when(validResult.getSqlKind()).thenReturn("DELETE");
        when(validResult.getAstString()).thenReturn("DELETE FROM `users`");
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(true);

        String result = processor.process(sql);

        assertTrue(result.contains("Query Type: DELETE"));
    }


    @Test
    @DisplayName("Should verify parser is called with correct SQL")
    void shouldVerifyParserCalledWithCorrectSql() {
        String sql = "SELECT * FROM users";
        CalciteQueryParser.ParseResult validResult = mock(CalciteQueryParser.ParseResult.class);
        when(validResult.isValid()).thenReturn(true);
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(false);

        processor.process(sql);

        verify(queryParser).parseAndValidate(sql);
    }

    @Test
    @DisplayName("Should verify debug state is checked")
    void shouldVerifyDebugStateIsChecked() {
        String sql = "SELECT * FROM users";
        CalciteQueryParser.ParseResult validResult = mock(CalciteQueryParser.ParseResult.class);
        when(validResult.isValid()).thenReturn(true);
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(false);

        processor.process(sql);

        verify(dbState).isDebugAstMode();
    }

    @Test
    @DisplayName("Should not call getSqlKind when debug mode is off")
    void shouldNotCallGetSqlKindWhenDebugOff() {
        String sql = "SELECT * FROM users";
        CalciteQueryParser.ParseResult validResult = mock(CalciteQueryParser.ParseResult.class);
        when(validResult.isValid()).thenReturn(true);
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(false);

        processor.process(sql);

        verify(validResult, never()).getSqlKind();
        verify(validResult, never()).getAstString();
    }
}

