package com.mpdb.repl;

/**
 * Common interface for all command processors.
 */
public interface Processor {

    /**
     * Process the input and return a response.
     *
     * @param input the input to process
     * @return the response string, or null if this processor cannot handle the input
     */
    String process(String input);
}

