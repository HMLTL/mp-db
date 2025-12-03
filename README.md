# (Mykola Pikuza) MP DB - CLI REPL Application

A SQL-powered CLI REPL (Read-Eval-Print Loop) application built with Spring Boot 3 and Apache Calcite.

## Features

- Interactive command-line interface with `mp-db>` prompt
- SQL query parsing and validation using Apache Calcite
- AST (Abstract Syntax Tree) generation for SQL queries
- Debug modes for SQL analysis
- Colon-prefixed meta commands for system control
- Built with Spring Boot 3 for easy dependency injection and extensibility
- Clean, refactored architecture with separated concerns

## Available Commands

### Colon-Prefixed Commands (Meta Commands)

- `:help`, `:h`, `:?` - Show help message
- `:quit`, `:exit`, `:q` - Exit the application
- `:status` - Show system status (debug modes)
- `:debug-ast [on|off]` - Enable/disable AST debug output

### SQL Commands

- `SELECT ...` - Parse and validate SELECT queries
- `INSERT ...` - Parse and validate INSERT queries
- `UPDATE ...` - Parse and validate UPDATE queries
- `DELETE FROM ...` - Parse and validate DELETE queries
- `CREATE ...` - Parse and validate CREATE queries

**Note:** SQL queries are currently parsed and validated only. Execution is not yet implemented.

## Requirements

- Java 17 or higher
- Maven 3.6+ (wrapper included)

## Building

```bash
./mvnw clean package
```

## Running

```bash
./mvnw spring-boot:run
```

Or run the JAR directly:

```bash
java -jar target/my-db-1.0.0-SNAPSHOT.jar
```

## Testing

The project includes a comprehensive test suite with 59 unit tests covering all major components.

### Run all tests:
```bash
./mvnw test
```

### Run specific test class:
```bash
./mvnw test -Dtest=CommandProcessorTest
```

### Test Coverage:
- **DbStateTest** (4 tests) - State management
- **CalciteQueryParserTest** (14 tests) - SQL parsing
- **ColonCommandProcessorTest** (16 tests) - Colon commands
- **SqlQueryProcessorTest** (10 tests) - SQL processing
- **CommandProcessorTest** (15 tests) - Command routing

For detailed test documentation, see [TEST_SUMMARY.md](TEST_SUMMARY.md).

## Example Usage

### Basic SQL Parsing

```
mp-db> SELECT * FROM users;


mp-db> SELECT * FROM invalid syntax;
❌ SQL Parse Error:
Syntax error at line 1, column 20: Encountered "syntax" at line 1, column 20.
```

### With Debug Mode

```
mp-db> :debug-ast on
✅ Debug AST mode enabled. AST will be shown for all queries.

mp-db> SELECT name, age FROM users WHERE id = 1;
Query Type: SELECT

AST:
SELECT `name`,
  `age`
FROM `users`
WHERE (
  `id` = 1
)

⚠️  Note: Query execution is not yet implemented.

mp-db> :debug-ast off
✅ Debug AST mode disabled.

mp-db> SELECT * FROM users;


mp-db> :status
=== System Status ===
Debug AST mode: OFF

mp-db> :quit
Goodbye!
```

## Project Structure

```
mp-db/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/example/mydb/
│       │       ├── MyDbApplication.java
│       │       └── repl/
│       │           ├── Processor.java              # Common interface
│       │           ├── CommandProcessor.java        # Main coordinator
│       │           ├── ColonCommandProcessor.java   # Handles :commands
│       │           ├── SqlQueryProcessor.java       # Handles SQL parsing
│       │           ├── CalciteQueryParser.java      # Apache Calcite integration
│       │           ├── DebugState.java             # State management
│       │           └── ReplRunner.java             # REPL loop
│       └── resources/
│           └── application.properties
├── pom.xml
├── README.md
├── ARCHITECTURE.md         # Architecture documentation
└── COLON_COMMANDS.md      # Colon commands guide
```

## Technology Stack

- **Java 17** - Modern Java LTS version
- **Spring Boot 3.4.0** - Application framework and dependency injection
- **Apache Calcite 1.37.0** - SQL parsing, validation, and AST generation
- **SLF4J with Logback** - Logging framework
- **Lombok** - Code generation for getters/setters
- **Maven** - Build and dependency management

## Architecture

The application follows a clean, modular architecture with separated concerns:

### Core Components

1. **Processor Interface** - Common contract for all command processors
2. **CommandProcessor** - Main coordinator that routes commands using if statements
3. **ColonCommandProcessor** - Handles meta commands (`:quit`, `:help`, `:debug-ast`, etc.)
4. **SqlQueryProcessor** - Handles SQL query parsing using Apache Calcite
5. **CalciteQueryParser** - Wraps Apache Calcite for SQL parsing and AST generation
6. **DebugState** - Centralized state management for debug modes
7. **ReplRunner** - Main REPL loop for user interaction

### Design Patterns

- **Chain of Responsibility** - Command routing through processors
- **Dependency Injection** - Spring Boot managed beans
- **Single Responsibility** - Each processor has one clear purpose
- **State Management** - Centralized debug state

For detailed architecture documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Extending the Application

You can easily extend the application by:

1. **Adding new colon commands** - Extend `ColonCommandProcessor`
2. **Adding new SQL features** - Extend `SqlQueryProcessor` or `CalciteQueryParser`
3. **Adding new processors** - Implement the `Processor` interface and add to `CommandProcessor`
4. **Adding new debug modes** - Add fields to `DebugState`
5. **Implementing SQL execution** - Add execution logic to `SqlQueryProcessor`
6. **Adding persistent storage** - Inject database access components

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed architecture documentation
- [COLON_COMMANDS.md](COLON_COMMANDS.md) - Guide to colon-prefixed commands
- [TEST_SUMMARY.md](TEST_SUMMARY.md) - Comprehensive test documentation

## License

MIT

