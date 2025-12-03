package com.example.mydb.repl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DbState Tests")
class DbStateTest {

    private DbState dbState;

    @BeforeEach
    void setUp() {
        dbState = new DbState();
    }

    @Test
    @DisplayName("Should have debugAstMode enabled by default")
    void shouldHaveDebugAstModeEnabledByDefault() {
        assertTrue(dbState.isDebugAstMode(), "debugAstMode should be true by default");
    }

    @Test
    @DisplayName("Should allow setting debugAstMode to false")
    void shouldAllowSettingDebugAstModeToFalse() {
        dbState.setDebugAstMode(false);
        assertFalse(dbState.isDebugAstMode(), "debugAstMode should be false after setting");
    }

    @Test
    @DisplayName("Should allow setting debugAstMode to true")
    void shouldAllowSettingDebugAstModeToTrue() {
        dbState.setDebugAstMode(false);
        dbState.setDebugAstMode(true);
        assertTrue(dbState.isDebugAstMode(), "debugAstMode should be true after setting");
    }

    @Test
    @DisplayName("Should allow toggling debugAstMode multiple times")
    void shouldAllowTogglingDebugAstModeMultipleTimes() {
        dbState.setDebugAstMode(false);
        assertFalse(dbState.isDebugAstMode());

        dbState.setDebugAstMode(true);
        assertTrue(dbState.isDebugAstMode());

        dbState.setDebugAstMode(false);
        assertFalse(dbState.isDebugAstMode());
    }
}

