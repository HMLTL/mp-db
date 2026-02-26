package com.mpdb.repl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("ColonCommandProcessor Tests")
class ColonCommandProcessorTest {

    private ColonCommandProcessor processor;

    @Mock
    private DbState dbState;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        processor = new ColonCommandProcessor(dbState);
    }

    @Test
    @DisplayName("Should return EXIT for :quit command")
    void shouldReturnExitForQuitCommand() {
        String result = processor.process(":quit");
        assertEquals("EXIT", result);
    }

    @Test
    @DisplayName("Should return EXIT for :exit command")
    void shouldReturnExitForExitCommand() {
        String result = processor.process(":exit");
        assertEquals("EXIT", result);
    }

    @Test
    @DisplayName("Should return EXIT for :q command")
    void shouldReturnExitForQCommand() {
        String result = processor.process(":q");
        assertEquals("EXIT", result);
    }

    @Test
    @DisplayName("Should return help text for :help command")
    void shouldReturnHelpForHelpCommand() {
        String result = processor.process(":help");
        assertNotNull(result);
        assertTrue(result.contains("Commands:"));
        assertTrue(result.contains(":quit"));
        assertTrue(result.contains(":debug-ast"));
    }

    @Test
    @DisplayName("Should return help text for :h command")
    void shouldReturnHelpForHCommand() {
        String result = processor.process(":h");
        assertNotNull(result);
        assertTrue(result.contains("Commands:"));
    }

    @Test
    @DisplayName("Should return help text for :? command")
    void shouldReturnHelpForQuestionMarkCommand() {
        String result = processor.process(":?");
        assertNotNull(result);
        assertTrue(result.contains("Commands:"));
    }

    @Test
    @DisplayName("Should enable debug AST mode with :debug-ast on")
    void shouldEnableDebugAstMode() {
        String result = processor.process(":debug-ast on");

        verify(dbState).setDebugAstMode(true);
        assertTrue(result.contains("enabled"));
        assertTrue(result.contains("✅"));
    }

    @Test
    @DisplayName("Should disable debug AST mode with :debug-ast off")
    void shouldDisableDebugAstMode() {
        String result = processor.process(":debug-ast off");

        verify(dbState).setDebugAstMode(false);
        assertTrue(result.contains("disabled"));
        assertTrue(result.contains("✅"));
    }

    @Test
    @DisplayName("Should show current debug AST mode status when no argument provided")
    void shouldShowCurrentDebugAstModeStatus() {
        when(dbState.isDebugAstMode()).thenReturn(true);

        String result = processor.process(":debug-ast");

        assertTrue(result.contains("currently"));
        assertTrue(result.contains("ON"));
        assertTrue(result.contains("Usage"));
    }

    @Test
    @DisplayName("Should show OFF status when debug AST mode is disabled")
    void shouldShowOffStatusWhenDisabled() {
        when(dbState.isDebugAstMode()).thenReturn(false);

        String result = processor.process(":debug-ast");

        assertTrue(result.contains("OFF"));
    }

    @Test
    @DisplayName("Should return error for invalid :debug-ast argument")
    void shouldReturnErrorForInvalidDebugAstArgument() {
        String result = processor.process(":debug-ast invalid");

        assertTrue(result.contains("Invalid argument"));
        assertTrue(result.contains("Usage"));
    }

    @Test
    @DisplayName("Should return system status for :status command")
    void shouldReturnSystemStatusForStatusCommand() {
        when(dbState.isDebugAstMode()).thenReturn(true);

        String result = processor.process(":status");

        assertNotNull(result);
        assertTrue(result.contains("System Status"));
        assertTrue(result.contains("Debug AST mode"));
        assertTrue(result.contains("ON"));
    }

    @Test
    @DisplayName("Should handle case-insensitive commands")
    void shouldHandleCaseInsensitiveCommands() {
        String result1 = processor.process(":QUIT");
        String result2 = processor.process(":Quit");
        String result3 = processor.process(":QuIt");

        assertEquals("EXIT", result1);
        assertEquals("EXIT", result2);
        assertEquals("EXIT", result3);
    }

    @Test
    @DisplayName("Should return error for unknown colon command")
    void shouldReturnErrorForUnknownCommand() {
        String result = processor.process(":unknown");

        assertTrue(result.contains("Unknown command"));
        assertTrue(result.contains(":unknown"));
        assertTrue(result.contains(":help"));
    }

    @Test
    @DisplayName("Should handle commands with multiple spaces")
    void shouldHandleCommandsWithMultipleSpaces() {
        String result = processor.process(":debug-ast    on");

        verify(dbState).setDebugAstMode(true);
        assertTrue(result.contains("enabled"));
    }

    @Test
    @DisplayName("Should handle commands with mixed case arguments")
    void shouldHandleCommandsWithMixedCaseArguments() {
        String result = processor.process(":debug-ast ON");

        verify(dbState).setDebugAstMode(true);
        assertTrue(result.contains("enabled"));
    }
}

