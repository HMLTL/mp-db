package com.mpdb.repl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Scanner;

@Component
public class ReplRunner implements CommandLineRunner {

    private final CommandProcessor commandProcessor;

    @Value("${app.prompt:mp-db> }")
    private String prompt;

    public ReplRunner(CommandProcessor commandProcessor) {
        this.commandProcessor = commandProcessor;
    }

    @Override
    public void run(String... args) {
        Scanner scanner = new Scanner(System.in);

        printBanner();

        while (true) {
            System.out.print(prompt);

            if (!scanner.hasNextLine()) {
                break;
            }

            String input = scanner.nextLine().trim();

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

        scanner.close();
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
