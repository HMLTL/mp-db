# Architecture Documentation

## Overview
(Mykola Pikuza) MPDB is a CLI REPL application with a clean, modular architecture that separates SQL parsing, command processing, and state management into distinct components. The system uses Apache Calcite for SQL parsing and validation, with a refactored processor-based architecture.

## Core Architecture

### Processor Interface
```
Processor (interface)
  └── String process(String input)
```

Simple interface with a single method that all command processors implement.

### Component Hierarchy

```
CommandProcessor (main coordinator)
  ├── uses if statements to route commands
  ├── delegates to ColonCommandProcessor if input starts with ":"
  └── delegates to SqlQueryProcessor for all other inputs

ColonCommandProcessor
  ├── depends on: shared debug state component
  ├── handles :quit, :exit, :q
  ├── handles :help, :h, :?
  ├── handles :status
  └── handles :debug-ast [on|off]

SqlQueryProcessor
  ├── depends on: CalciteQueryParser, shared debug state component
  ├── parses SQL queries using Calcite
  ├── accesses debug state for debug mode settings
  ├── formats AST output
  ├── logs query details at DEBUG level
  └── handles SQL validation errors

CalciteQueryParser
  ├── wraps Apache Calcite SQL parser
  ├── parses SQL into AST (SqlNode)
  ├── validates SQL syntax
  └── returns ParseResult with validation status

Debug state component
  ├── centralized state management
  └── manages debugAstMode flag (default: true)
```

## Dependency Graph

```
ReplRunner
  └── CommandProcessor
        ├── ColonCommandProcessor
        │     └── Debug state component
        └── SqlQueryProcessor
              ├── CalciteQueryParser
              └── Debug state component
```

**Key Points:**
- `CommandProcessor` coordinates routing between processors
- Both `ColonCommandProcessor` and `SqlQueryProcessor` depend on shared debug state
- No circular dependencies - clean separation of concerns
- `SqlQueryProcessor` no longer depends on `ColonCommandProcessor`

## Request Flow

```
User Input → ReplRunner
              ↓
        CommandProcessor.process(input)
              ↓
         ┌────┴────┐
         ↓         ↓
   starts with ":" ?
         ↓         ↓
       YES        NO
         ↓         ↓
  ColonCommandProcessor   SqlQueryProcessor
         ↓                      ↓
    Debug state            CalciteQueryParser
         ↓                      ↓
    Update/Query          Parse SQL → AST
         ↓                      ↓
    Return result         Check debug state
                               ↓
                          Format output
                               ↓
                          Return result
```

## Key Design Decisions

### 1. Simple Processor Interface
The `Processor` interface has only one method: `process(String input)`. We removed the `canProcess()` method to keep the interface simple. Routing logic is handled by `CommandProcessor` using straightforward if statements.

### 2. Centralized State Management
A shared component manages application state:
- Eliminates coupling between processors
- Single source of truth for debug modes
- Easy to extend with additional state flags
- Simplifies testing by allowing state injection

### 3. Chain of Responsibility Pattern
`CommandProcessor` acts as a coordinator:
- Checks if input starts with ":" → routes to `ColonCommandProcessor`
- Otherwise → routes to `SqlQueryProcessor`
- Clean, simple routing logic with if statements

### 4. Separation of Concerns
Each component has a single, well-defined responsibility:
- **CommandProcessor**: Routes commands to appropriate processor
- **ColonCommandProcessor**: Handles meta commands and updates state
- **SqlQueryProcessor**: Parses SQL queries and formats output
- **CalciteQueryParser**: Wraps Apache Calcite for SQL parsing
- **Shared Debug State**: Manages application state
- **ReplRunner**: Manages REPL loop and user interaction

### 5. Logging Strategy
`SqlQueryProcessor` logs query details at DEBUG level when debug mode is enabled:
- Uses SLF4J
- Logs query type and AST for diagnostic purposes
- Separate from console output for flexible debugging

## Files Structure

```
src/main/java/com/mpdb/repl/
├── Processor.java                  (interface - common contract)
├── CommandProcessor.java           (main coordinator)
├── ColonCommandProcessor.java      (handles :commands)
├── SqlQueryProcessor.java          (handles SQL queries)
├── CalciteQueryParser.java         (Calcite integration)
└── ReplRunner.java                 (REPL loop)
```

Resources:
```
src/main/resources/application.yml
```

## Benefits

✅ **Single Responsibility**: Each processor has one clear responsibility  
✅ **Open/Closed Principle**: Easy to add new processor types without modifying existing code  
✅ **Testability**: Each processor can be tested independently with mocked dependencies  
✅ **Maintainability**: Logic is separated and easy to locate  
✅ **Simplicity**: Simple if statements for routing, no complex loops  
✅ **Loose Coupling**: Shared state through a dedicated component, not direct processor dependencies  
✅ **Extensibility**: Easy to add new debug modes or state flags

## Extension Points

### Adding New Commands
1. **New Colon Command**: Add handler method to `ColonCommandProcessor`
2. **New SQL Feature**: Extend `SqlQueryProcessor` or `CalciteQueryParser`
3. **New Processor Type**: Implement `Processor` interface and add routing in `CommandProcessor`

### Adding New State
1. Add field to the shared state component (e.g., `debugParseMode`, `verboseMode`)
2. Add getter/setter
3. Add colon command in `ColonCommandProcessor` to toggle it
4. Use state in relevant processor

### Example: Adding a New Debug Mode

**Step 1**: Add to the state component
```java
public class DebugState {
    private boolean debugAstMode = true;
    private boolean debugParseMode = false;  // New mode
}
```

**Step 2**: Add command in `ColonCommandProcessor.java`
```java
case "debug-parse" -> handleDebugParse(arg);
```

**Step 3**: Use in `SqlQueryProcessor.java`
```java
if (debugState.isDebugParseMode()) {
    log.debug("Parse details: ...");
}
```

## Testing Strategy

### Unit Testing
- **CommandProcessor**: Test routing logic with mocked processors
- **ColonCommandProcessor**: Test each command with mocked state
- **SqlQueryProcessor**: Test SQL parsing with mocked `CalciteQueryParser` and state
- **CalciteQueryParser**: Test SQL parsing with various valid/invalid queries

### Integration Testing
- Test complete flow from user input to output
- Test state changes across multiple commands
- Test SQL parsing with different query types

## Build and Run (Gradle)

- Build: `./gradlew clean build`
- Run: `./gradlew bootRun`
- Tests: `./gradlew test`
- Test report: `build/reports/tests/test/index.html`

## Future Enhancements

1. **SQL Execution**: Implement actual query execution using Calcite's execution engine
2. **Persistent Storage**: Add database backend for data storage
3. **Query History**: Store and recall previous queries (e.g., `:history` command)
4. **Auto-completion**: Add tab completion for SQL keywords and table names
5. **Multi-line Queries**: Support queries spanning multiple lines
6. **Query Optimization**: Show query plans and optimization hints
7. **More Debug Modes**: Add debug modes for execution, planning, etc.
8. **Transaction Support**: Add `:begin`, `:commit`, `:rollback` commands
9. **Script Execution**: Load and execute SQL scripts from files (e.g., `:run script.sql`)
10. **Export Results**: Export query results to CSV, JSON, etc.

## Build Status

✅ **BUILD SUCCESS** - All files compiled successfully
