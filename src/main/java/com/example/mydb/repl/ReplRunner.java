package com.example.mydb.repl;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Scanner;

@Component
public class ReplRunner implements CommandLineRunner {

    private final CommandProcessor commandProcessor;

    public ReplRunner(CommandProcessor commandProcessor) {
        this.commandProcessor = commandProcessor;
    }

    @Override
    public void run(String... args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        System.out.println("═══════════════════════════════════════");
        System.out.println("  Welcome to My DB CLI REPL");
        System.out.println("═══════════════════════════════════════");
        System.out.println("Type 'help' for available commands");
        System.out.println("Type 'exit' or 'quit' to quit\n");

        while (true) {
            System.out.print("my-db> ");

            if (!scanner.hasNextLine()) {
                break;
            }

            String input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                continue;
            }

            if ("exit".equalsIgnoreCase(input) || "quit".equalsIgnoreCase(input)) {
                break;
            }

            try {
                String result = commandProcessor.process(input);
                if (result != null && !result.isEmpty()) {
                    System.out.println(result);
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }

        scanner.close();
    }
}

