# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
./gradlew clean build

# Run the REPL
./gradlew bootRun

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests com.mpdb.repl.CommandProcessorTest

# Run a single test method
./gradlew test --tests "com.mpdb.repl.CommandProcessorTest.methodName"
```

Test reports are generated at `build/reports/tests/test/index.html`.

## Architecture

This is a CLI REPL application (prompt: `mp-db>`) that parses and validates SQL using Apache Calcite. SQL execution is not yet implemented — queries are parsed and the AST is optionally displayed.

### Request Flow

```
User Input → ReplRunner (CommandLineRunner)
               ↓
         CommandProcessor
               ↓
    starts with ":"?
       ↓            ↓
     YES            NO
       ↓            ↓
ColonCommandProcessor   SqlQueryProcessor
       ↓                      ↓
    DbState             CalciteQueryParser
  (toggle flags)        (parse → SqlNode AST)
                              ↓
                         Check DbState
                         (show AST if debugAstMode=true)
```

### Key Classes (`src/main/java/com/mpdb/repl/`)

| Class | Role |
|---|---|
| `ReplRunner` | REPL loop, implements `CommandLineRunner`, reads stdin, calls `CommandProcessor` |
| `CommandProcessor` | Routes `:*` input to `ColonCommandProcessor`, everything else to `SqlQueryProcessor` |
| `ColonCommandProcessor` | Handles meta commands; returns the string `"EXIT"` to signal quit |
| `SqlQueryProcessor` | Parses SQL via `CalciteQueryParser`, formats output based on `DbState` |
| `CalciteQueryParser` | Wraps Apache Calcite; returns `ParseResult` record (isValid, ast, errorMessage) |
| `ColonCommand` | Enum with alias support for all `:command` tokens |
| `DbState` | Spring `@Component` holding mutable state (currently just `debugAstMode`, default `true`) |
| `Processor` | Interface with single method `String process(String input)` |

### Conventions

- **Quit signaling**: `ColonCommandProcessor.handleQuit()` returns the literal string `"EXIT"`, which `ReplRunner` checks to break the loop.
- **Calcite DDL support**: `CalciteQueryParser` uses `SqlDdlParserImpl.FACTORY` so DDL statements (CREATE, DROP, ALTER) are supported alongside DML.
- **State extension**: To add a new debug mode, add a field to `DbState`, add a `ColonCommand` enum entry, and handle it in `ColonCommandProcessor`.
- **New processor types**: Implement `Processor`, then add an `if` branch in `CommandProcessor.process()`.

### Tech Stack

- Java 17, Spring Boot 3.4, Gradle
- Apache Calcite 1.37.0 (SQL parsing + DDL via `calcite-server`)
- Lombok (`@Getter`/`@Setter` on `DbState`)
- JUnit 5 + Mockito for tests