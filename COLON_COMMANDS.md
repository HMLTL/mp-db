# Colon-Prefixed Commands - Usage Examples

## Overview
The CLI REPL now supports colon-prefixed commands (meta commands) for controlling debug modes and system behavior.

## Available Colon Commands

### Help Commands
- `:help` or `:h` or `:?` - Show help message
- `:status` - Show current system status (debug modes, data store size)

### Exit Commands
- `:quit` or `:exit` or `:q` - Exit the application

### Debug Commands
- `:debug-ast [on|off]` - Enable/disable AST (Abstract Syntax Tree) output
- `:debug-parse [on|off]` - Enable/disable detailed parse information

## Example Usage

```
mydb> SELECT * FROM t;
✅ OK

mydb> :debug-ast on
✅ Debug AST mode enabled. AST will be shown for all queries.

mydb> SELECT col FROM t;
✅ OK

AST:
SELECT `col`
FROM `t`

⚠️  Note: Query execution is not yet implemented.

mydb> :debug-parse on
✅ Debug Parse mode enabled. Detailed parse information will be shown.

mydb> SELECT col FROM t;
✅ SQL parsed successfully!

Query Type: SELECT

AST:
SELECT `col`
FROM `t`

⚠️  Note: Query execution is not yet implemented.

mydb> :status
=== System Status ===
Debug AST mode: ON
Debug Parse mode: ON
Data store size: 0 items

mydb> :debug-ast off
✅ Debug AST mode disabled.

mydb> :debug-parse off
✅ Debug Parse mode disabled.

mydb> SELECT * FROM users WHERE id = 1;
✅ OK

mydb> :quit
Goodbye!
```

## Features

1. **Debug AST Mode**: When enabled, shows the Abstract Syntax Tree for each SQL query
2. **Debug Parse Mode**: When enabled, shows detailed parse information including query type
3. **Status Command**: Shows current system configuration and state
4. **Multiple Exit Options**: Use `:quit`, `:exit`, or `:q` to exit the application
5. **SQL Validation**: All SQL queries are parsed and validated using Apache Calcite

## Error Handling

Invalid SQL queries will show parse errors:
```
mydb> SELECT * FORM t;
❌ SQL Parse Error:
Syntax error at line 1, column 11: Encountered "FORM" at line 1, column 11.
```

## Notes

- SQL queries are parsed and validated using Apache Calcite
- Query execution is not yet implemented
- Debug modes are off by default for cleaner output
- All colon commands are case-insensitive

