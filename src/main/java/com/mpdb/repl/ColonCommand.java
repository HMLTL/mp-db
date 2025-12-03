package com.mpdb.repl;

import java.util.HashMap;
import java.util.Map;

/**
 * Enum representing colon-prefixed commands with alias support.
 */
public enum ColonCommand {
    DEBUG_AST("debug-ast"),
    QUIT("quit"),
    EXIT("exit"),
    Q("q"),
    HELP("help"),
    H("h"),
    QUESTION("?"),
    STATUS("status"),
    WRONG("wrong-command");

    private static final Map<String, ColonCommand> BY_NAME = new HashMap<>();

    static {
        for (ColonCommand c : values()) {
            BY_NAME.put(c.primaryName, c);
        }
        // Map aliases to their canonical command
        alias("quit", QUIT);
        alias("exit", EXIT);
        alias("q", Q);
        alias("help", HELP);
        alias("h", H);
        alias("?", QUESTION);
        alias("debug-ast", DEBUG_AST);
        alias("status", STATUS);
    }

    private final String primaryName;

    ColonCommand(String primaryName) {
        this.primaryName = primaryName;
    }

    public static ColonCommand from(String name) {
        if (name == null) return WRONG;
        return BY_NAME.get(name.toLowerCase());
    }

    private static void alias(String name, ColonCommand command) {
        BY_NAME.put(name.toLowerCase(), command);
    }
}

