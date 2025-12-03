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
    public void run(String... args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("═══════════════════════════════════════");
        System.out.println("  Welcome to MP(Mykola Pikuza) DB CLI REPL");
        System.out.println("═══════════════════════════════════════");
        System.out.println("Type ':help' or ':h' for available commands");
        System.out.println("Type ':quit', ':exit' or ':q' to quit\n");

        while (true) {
            System.out.print("mp-db> ");

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
}

