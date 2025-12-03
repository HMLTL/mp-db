package com.example.mydb.repl;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class CommandProcessor {

    private final Map<String, String> dataStore = new HashMap<>();
    private final CalciteQueryParser queryParser;
    private boolean debugAstMode = true;

    public CommandProcessor(CalciteQueryParser queryParser) {
        this.queryParser = queryParser;
    }

    public String process(String input) {
        // Handle colon-prefixed commands
        if (input.startsWith(":")) {
            return handleColonCommand(input);
        }

        return handleSqlQuery(input);
    }

    /**
     * Handle colon-prefixed commands (meta commands).
     */
    private String handleColonCommand(String input) {
        String[] parts = input.substring(1).split("\\s+", 2);
        String command = parts[0].toLowerCase();
        String arg = parts.length > 1 ? parts[1].toLowerCase() : null;

        return switch (command) {
            case "debug-ast" -> handleDebugAst(arg);
            case "quit", "exit", "q" -> handleQuit();
            case "help", "h", "?" -> getHelp();
            case "status" -> handleStatus();
            default -> "Unknown command: :" + command + "\nType ':help' or 'help' for available commands.";
        };
    }

    private String handleDebugAst(String arg) {
        if (arg == null) {
            return "Debug AST mode is currently: " + (debugAstMode ? "ON" : "OFF") +
                   "\nUsage: :debug-ast [on|off]";
        }

        if ("on".equals(arg)) {
            debugAstMode = true;
            return "✅ Debug AST mode enabled. AST will be shown for all queries.";
        } else if ("off".equals(arg)) {
            debugAstMode = false;
            return "✅ Debug AST mode disabled.";
        } else {
            return "Invalid argument. Usage: :debug-ast [on|off]";
        }
    }

    private String handleQuit() {
        return "EXIT";
    }

    private String handleStatus() {
        StringBuilder status = new StringBuilder();
        status.append("=== System Status ===\n");
        status.append("Debug AST mode: ").append(debugAstMode ? "ON" : "OFF").append("\n");
        status.append("Data store size: ").append(dataStore.size()).append(" items\n");
        return status.toString();
    }

    private String getHelp() {
        return """
                Colon-prefixed commands:
                  :quit, :exit, :q  - Exit the application
                  :help, :h, :?     - Show this help message
                  :status           - Show system status (debug modes, data store size)
                  :debug-ast [on|off] - Enable/disable AST debug output
                  :debug-parse [on|off] - Enable/disable detailed parse debug output
                
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

    /**
     * Handle SQL query using Calcite parser.
     * Parses the query, builds AST, and validates syntax.
     */
    private String handleSqlQuery(String sql) {
        CalciteQueryParser.ParseResult result = queryParser.parseAndValidate(sql);

        if (!result.isValid()) {
            return "❌ SQL Parse Error:\n" + result.errorMessage();
        }

        // Build response based on debug modes
        StringBuilder response = new StringBuilder();

        if (debugAstMode) {
            response.append("Query Type: ").append(result.getSqlKind()).append("\n");
            response.append("\nAST:\n").append(formatAst(result.getAstString())).append("\n");
            response.append("\n⚠️  Note: Query execution is not yet implemented.");
        }

        return response.toString();
    }

    /**
     * Format AST string for better readability.
     */
    private String formatAst(String ast) {
        // Add indentation for better readability
        return ast.replace(", ", ",\n  ")
                  .replace("(", "(\n  ")
                  .replace(")", "\n)");
    }
}

