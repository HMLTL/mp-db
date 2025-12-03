package com.mpdb.repl;

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

