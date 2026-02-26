package com.mpdb.repl;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.History;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;

@Component
public class ReplRunner implements CommandLineRunner {

    private final CommandProcessor commandProcessor;

    @Value("${app.prompt:mp-db> }")
    private String prompt;

    @Value("${app.data-dir:./data}")
    private String dataDir;

    public ReplRunner(CommandProcessor commandProcessor) {
        this.commandProcessor = commandProcessor;
    }

    @Override
    public void run(String... args) throws IOException {
        Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .build();

        Path historyFile = Path.of(dataDir, ".mpdb_history");

        LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .history(new DefaultHistory())
                .variable(LineReader.HISTORY_FILE, historyFile)
                .build();

        printBanner();

        while (true) {
            String input;
            try {
                input = reader.readLine(prompt).trim();
            } catch (UserInterruptException e) {
                // Ctrl+C
                continue;
            } catch (EndOfFileException e) {
                // Ctrl+D
                break;
            }

            if (input.isEmpty()) {
                continue;
            }

            try {
                String result = commandProcessor.process(input);

                // Handle EXIT command from colon commands
                if ("EXIT".equals(result)) {
                    System.out.println("Goodbye!");
                    break;
                }

                if (result != null && !result.isEmpty()) {
                    System.out.println(result);
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }

        terminal.close();
    }

    private void printBanner() {
        String banner = """
                ═══════════════════════════════════════════
                Type ':help' or ':h' for available commands
                Type ':quit', ':exit' or ':q' to quit
                """;
        System.out.println(banner);
    }
}
