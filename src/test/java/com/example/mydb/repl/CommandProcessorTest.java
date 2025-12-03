package com.example.mydb.repl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("CommandProcessor Tests")
class CommandProcessorTest {

    private CommandProcessor commandProcessor;

    @Mock
    private ColonCommandProcessor colonCommandProcessor;

    @Mock
    private SqlQueryProcessor sqlQueryProcessor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        commandProcessor = new CommandProcessor(colonCommandProcessor, sqlQueryProcessor);
    }

    @Test
    @DisplayName("Should route colon commands to ColonCommandProcessor")
    void shouldRouteColonCommandsToColonProcessor() {
        String command = ":help";
        when(colonCommandProcessor.process(command)).thenReturn("Help text");

        String result = commandProcessor.process(command);

        verify(colonCommandProcessor).process(command);
        verify(sqlQueryProcessor, never()).process(anyString());
        assertEquals("Help text", result);
    }

    @Test
    @DisplayName("Should route :quit to ColonCommandProcessor")
    void shouldRouteQuitToColonProcessor() {
        String command = ":quit";
        when(colonCommandProcessor.process(command)).thenReturn("EXIT");

        String result = commandProcessor.process(command);

        verify(colonCommandProcessor).process(command);
        assertEquals("EXIT", result);
    }

    @Test
    @DisplayName("Should route :status to ColonCommandProcessor")
    void shouldRouteStatusToColonProcessor() {
        String command = ":status";
        when(colonCommandProcessor.process(command)).thenReturn("Status info");

        commandProcessor.process(command);

        verify(colonCommandProcessor).process(command);
        verify(sqlQueryProcessor, never()).process(anyString());
    }

    @Test
    @DisplayName("Should route :debug-ast to ColonCommandProcessor")
    void shouldRouteDebugAstToColonProcessor() {
        String command = ":debug-ast on";
        when(colonCommandProcessor.process(command)).thenReturn("Debug enabled");

        commandProcessor.process(command);

        verify(colonCommandProcessor).process(command);
        verify(sqlQueryProcessor, never()).process(anyString());
    }

    @Test
    @DisplayName("Should route SQL queries to SqlQueryProcessor")
    void shouldRouteSqlQueriesToSqlProcessor() {
        String sql = "SELECT * FROM users";
        when(sqlQueryProcessor.process(sql)).thenReturn("Query result");

        String result = commandProcessor.process(sql);

        verify(sqlQueryProcessor).process(sql);
        verify(colonCommandProcessor, never()).process(anyString());
        assertEquals("Query result", result);
    }

    @Test
    @DisplayName("Should route INSERT queries to SqlQueryProcessor")
    void shouldRouteInsertToSqlProcessor() {
        String sql = "INSERT INTO users (name) VALUES ('John')";
        when(sqlQueryProcessor.process(sql)).thenReturn("Insert result");

        commandProcessor.process(sql);

        verify(sqlQueryProcessor).process(sql);
        verify(colonCommandProcessor, never()).process(anyString());
    }

    @Test
    @DisplayName("Should route UPDATE queries to SqlQueryProcessor")
    void shouldRouteUpdateToSqlProcessor() {
        String sql = "UPDATE users SET age = 26";
        when(sqlQueryProcessor.process(sql)).thenReturn("Update result");

        commandProcessor.process(sql);

        verify(sqlQueryProcessor).process(sql);
        verify(colonCommandProcessor, never()).process(anyString());
    }

    @Test
    @DisplayName("Should route DELETE queries to SqlQueryProcessor")
    void shouldRouteDeleteToSqlProcessor() {
        String sql = "DELETE FROM users WHERE id = 1";
        when(sqlQueryProcessor.process(sql)).thenReturn("Delete result");

        commandProcessor.process(sql);

        verify(sqlQueryProcessor).process(sql);
        verify(colonCommandProcessor, never()).process(anyString());
    }

    @Test
    @DisplayName("Should route CREATE queries to SqlQueryProcessor")
    void shouldRouteCreateToSqlProcessor() {
        String sql = "CREATE TABLE users (id INT)";
        when(sqlQueryProcessor.process(sql)).thenReturn("Create result");

        commandProcessor.process(sql);

        verify(sqlQueryProcessor).process(sql);
        verify(colonCommandProcessor, never()).process(anyString());
    }

    @Test
    @DisplayName("Should route text starting with colon to ColonCommandProcessor")
    void shouldRouteAnyColonStartingTextToColonProcessor() {
        String command = ":unknown-command";
        when(colonCommandProcessor.process(command)).thenReturn("Unknown command");

        commandProcessor.process(command);

        verify(colonCommandProcessor).process(command);
        verify(sqlQueryProcessor, never()).process(anyString());
    }

    @Test
    @DisplayName("Should handle lowercase select")
    void shouldHandleLowercaseSelect() {
        String sql = "select * from users";
        when(sqlQueryProcessor.process(sql)).thenReturn("Result");

        commandProcessor.process(sql);

        verify(sqlQueryProcessor).process(sql);
    }

    @Test
    @DisplayName("Should handle uppercase SELECT")
    void shouldHandleUppercaseSelect() {
        String sql = "SELECT * FROM USERS";
        when(sqlQueryProcessor.process(sql)).thenReturn("Result");

        commandProcessor.process(sql);

        verify(sqlQueryProcessor).process(sql);
    }

    @Test
    @DisplayName("Should handle mixed case SQL")
    void shouldHandleMixedCaseSql() {
        String sql = "SeLeCt * FrOm UsErS";
        when(sqlQueryProcessor.process(sql)).thenReturn("Result");

        commandProcessor.process(sql);

        verify(sqlQueryProcessor).process(sql);
    }

    @Test
    @DisplayName("Should route plain text to SqlQueryProcessor")
    void shouldRoutePlainTextToSqlProcessor() {
        String text = "some random text";
        when(sqlQueryProcessor.process(text)).thenReturn("Error");

        commandProcessor.process(text);

        verify(sqlQueryProcessor).process(text);
        verify(colonCommandProcessor, never()).process(anyString());
    }

    @Test
    @DisplayName("Should not modify the input before routing")
    void shouldNotModifyInputBeforeRouting() {
        String input = "  SELECT * FROM users  ";
        when(sqlQueryProcessor.process(input)).thenReturn("Result");

        commandProcessor.process(input);

        verify(sqlQueryProcessor).process(input);
    }
}

