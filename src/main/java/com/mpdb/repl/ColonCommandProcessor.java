package com.mpdb.repl;

import org.springframework.stereotype.Component;

/**
 * Processor for handling colon-prefixed commands (meta commands).
 */
@Component
public class ColonCommandProcessor implements Processor {

    private final DbState dbState;

    public ColonCommandProcessor(DbState dbState) {
        this.dbState = dbState;
    }

    @Override
    public String process(String input) {
        return handleColonCommand(input);
    }

    /**
     * Handle colon-prefixed commands (meta commands).
     */
    private String handleColonCommand(String input) {
        String[] parts = input.substring(1).split("\\s+", 2);
        String commandToken = parts[0].toLowerCase();
        String arg = parts.length > 1 ? parts[1].toLowerCase() : null;

        return switch (ColonCommand.from(commandToken)) {
            case DEBUG_AST -> handleDebugAst(arg);
            case QUIT, EXIT, Q -> handleQuit();
            case HELP, H, QUESTION -> getHelp();
            case STATUS -> handleStatus();
            case WRONG -> "Unknown command: :" + commandToken + "\nType ':help' or 'help' for available commands.";
        };
    }

    private String handleDebugAst(String arg) {
        if (arg == null) {
            return "Debug AST mode is currently: " + (dbState.isDebugAstMode() ? "ON" : "OFF") +
                   "\nUsage: :debug-ast [on|off]";
        }

        if ("on".equals(arg)) {
            dbState.setDebugAstMode(true);
            return "✅ Debug AST mode enabled. AST will be shown for all queries.";
        } else if ("off".equals(arg)) {
            dbState.setDebugAstMode(false);
            return "✅ Debug AST mode disabled.";
        } else {
            return "Invalid argument. Usage: :debug-ast [on|off]";
        }
    }

    private String handleQuit() {
        return "EXIT";
    }

    private String handleStatus() {
        return "=== System Status ===\n" +
               "Debug AST mode: " + (dbState.isDebugAstMode() ? "ON" : "OFF") + "\n";
    }

    private String getHelp() {
        return """
                Colon-prefixed commands:
                  :quit, :exit, :q  - Exit the application
                  :help, :h, :?     - Show this help message
                  :status           - Show system status (debug modes)
                  :debug-ast [on|off] - Enable/disable AST debug output
                
                SQL Support (parsing & validation only):
                  SELECT ...        - Parse SELECT queries
                  INSERT ...        - Parse INSERT queries
                  UPDATE ...        - Parse UPDATE queries
                  DELETE FROM ...   - Parse DELETE queries
                  CREATE ...        - Parse CREATE queries
                
                Note: SQL queries are parsed using Apache Calcite.
                      Execution is not yet implemented.
                """;
    }

}
