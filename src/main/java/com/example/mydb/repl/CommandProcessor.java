package com.example.mydb.repl;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class CommandProcessor {

    private final Map<String, String> dataStore = new HashMap<>();

    public String process(String input) {
        String[] parts = input.split("\\s+", 2);
        String command = parts[0].toLowerCase();

        return switch (command) {
            case "help" -> getHelp();
            case "set" -> handleSet(parts);
            case "get" -> handleGet(parts);
            case "delete", "del" -> handleDelete(parts);
            case "list" -> handleList();
            case "clear" -> handleClear();
            case "count" -> handleCount();
            case "echo" -> handleEcho(parts);
            default -> "Unknown command: " + command + ". Type 'help' for available commands.";
        };
    }

    private String getHelp() {
        return """
                Available commands:
                  help              - Show this help message
                  set <key> <value> - Set a key-value pair
                  get <key>         - Get value by key
                  delete <key>      - Delete a key-value pair
                  list              - List all key-value pairs
                  clear             - Clear all data
                  count             - Count stored items
                  echo <text>       - Echo back the text
                  exit/quit         - Exit the application
                """;
    }

    private String handleSet(String[] parts) {
        if (parts.length < 2) {
            return "Usage: set <key> <value>";
        }

        String[] keyValue = parts[1].split("\\s+", 2);
        if (keyValue.length < 2) {
            return "Usage: set <key> <value>";
        }

        String key = keyValue[0];
        String value = keyValue[1];
        String oldValue = dataStore.put(key, value);

        if (oldValue != null) {
            return "Updated: " + key + " = " + value + " (was: " + oldValue + ")";
        } else {
            return "Set: " + key + " = " + value;
        }
    }

    private String handleGet(String[] parts) {
        if (parts.length < 2) {
            return "Usage: get <key>";
        }

        String key = parts[1].trim();
        String value = dataStore.get(key);

        if (value != null) {
            return key + " = " + value;
        } else {
            return "Key not found: " + key;
        }
    }

    private String handleDelete(String[] parts) {
        if (parts.length < 2) {
            return "Usage: delete <key>";
        }

        String key = parts[1].trim();
        String value = dataStore.remove(key);

        if (value != null) {
            return "Deleted: " + key + " (was: " + value + ")";
        } else {
            return "Key not found: " + key;
        }
    }

    private String handleList() {
        if (dataStore.isEmpty()) {
            return "No data stored.";
        }

        StringBuilder sb = new StringBuilder("Stored data:\n");
        dataStore.forEach((key, value) ->
            sb.append("  ").append(key).append(" = ").append(value).append("\n")
        );
        return sb.toString().trim();
    }

    private String handleClear() {
        int count = dataStore.size();
        dataStore.clear();
        return "Cleared " + count + " item(s).";
    }

    private String handleCount() {
        int count = dataStore.size();
        return "Total items: " + count;
    }

    private String handleEcho(String[] parts) {
        if (parts.length < 2) {
            return "";
        }
        return parts[1];
    }
}

