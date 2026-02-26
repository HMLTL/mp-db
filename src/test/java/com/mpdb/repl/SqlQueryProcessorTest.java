package com.mpdb.repl;

import com.mpdb.executor.SqlExecutor;
import org.apache.calcite.sql.SqlNode;
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

    @Mock
    private SqlExecutor sqlExecutor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        processor = new SqlQueryProcessor(queryParser, dbState, sqlExecutor);
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

        assertTrue(result.contains("SQL Parse Error"));
        assertTrue(result.contains("Syntax error"));
    }

    @Test
    @DisplayName("Should verify parser is called with correct SQL")
    void shouldVerifyParserCalledWithCorrectSql() {
        String sql = "SELECT * FROM users";
        SqlNode mockAst = mock(SqlNode.class);
        CalciteQueryParser.ParseResult validResult = new CalciteQueryParser.ParseResult(
                true, mockAst, null, sql
        );
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(false);
        when(sqlExecutor.execute(mockAst)).thenReturn("result");

        processor.process(sql);

        verify(queryParser).parseAndValidate(sql);
    }

    @Test
    @DisplayName("Should call executor with parsed AST")
    void shouldCallExecutorWithAst() {
        String sql = "SELECT * FROM users";
        SqlNode mockAst = mock(SqlNode.class);
        CalciteQueryParser.ParseResult validResult = new CalciteQueryParser.ParseResult(
                true, mockAst, null, sql
        );
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(false);
        when(sqlExecutor.execute(mockAst)).thenReturn("query result");

        String result = processor.process(sql);

        verify(sqlExecutor).execute(mockAst);
        assertEquals("query result", result);
    }

    @Test
    @DisplayName("Should return execution error on exception")
    void shouldReturnExecutionErrorOnException() {
        String sql = "SELECT * FROM users";
        SqlNode mockAst = mock(SqlNode.class);
        CalciteQueryParser.ParseResult validResult = new CalciteQueryParser.ParseResult(
                true, mockAst, null, sql
        );
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(false);
        when(sqlExecutor.execute(mockAst)).thenThrow(new RuntimeException("table not found"));

        String result = processor.process(sql);

        assertTrue(result.contains("Execution Error"));
        assertTrue(result.contains("table not found"));
    }

    @Test
    @DisplayName("Should verify debug state is checked")
    void shouldVerifyDebugStateIsChecked() {
        String sql = "SELECT * FROM users";
        SqlNode mockAst = mock(SqlNode.class);
        CalciteQueryParser.ParseResult validResult = new CalciteQueryParser.ParseResult(
                true, mockAst, null, sql
        );
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(false);
        when(sqlExecutor.execute(mockAst)).thenReturn("result");

        processor.process(sql);

        verify(dbState).isDebugAstMode();
    }

    @Test
    @DisplayName("Should not call getSqlKind when debug mode is off")
    void shouldNotCallGetSqlKindWhenDebugOff() {
        String sql = "SELECT * FROM users";
        SqlNode mockAst = mock(SqlNode.class);
        CalciteQueryParser.ParseResult validResult = spy(new CalciteQueryParser.ParseResult(
                true, mockAst, null, sql
        ));
        when(queryParser.parseAndValidate(sql)).thenReturn(validResult);
        when(dbState.isDebugAstMode()).thenReturn(false);
        when(sqlExecutor.execute(mockAst)).thenReturn("result");

        processor.process(sql);

        verify(validResult, never()).getSqlKind();
        verify(validResult, never()).getAstString();
    }
}
