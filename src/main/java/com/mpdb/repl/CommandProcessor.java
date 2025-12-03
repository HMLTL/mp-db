package com.mpdb.repl;

import org.springframework.stereotype.Component;

/**
 * Main command processor that delegates to specific processors.
 * Implements chain of responsibility pattern.
 */
@Component
public class CommandProcessor implements Processor {

    private final ColonCommandProcessor colonCommandProcessor;
    private final SqlQueryProcessor sqlQueryProcessor;

    public CommandProcessor(ColonCommandProcessor colonCommandProcessor,
                           SqlQueryProcessor sqlQueryProcessor) {
        this.colonCommandProcessor = colonCommandProcessor;
        this.sqlQueryProcessor = sqlQueryProcessor;
    }

    @Override
    public String process(String input) {
        // Check colon commands first
        if (input.startsWith(":")) {
            return colonCommandProcessor.process(input);
        }

        // Otherwise, treat as SQL query
        return sqlQueryProcessor.process(input);
    }
}

