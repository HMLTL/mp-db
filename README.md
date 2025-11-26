# My DB - CLI REPL Application

A simple CLI REPL (Read-Eval-Print Loop) application built with Spring Boot 3.

## Features

- Interactive command-line interface
- In-memory key-value store
- Built with Spring Boot 3 for easy dependency injection and extensibility

## Available Commands

- `help` - Show available commands
- `set <key> <value>` - Set a key-value pair
- `get <key>` - Get value by key
- `delete <key>` - Delete a key-value pair
- `list` - List all key-value pairs
- `clear` - Clear all data
- `count` - Count stored items
- `echo <text>` - Echo back the text
- `exit` / `quit` - Exit the application

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

## Example Usage

```
my-db> set name John
Set: name = John

my-db> set age 25
Set: age = 25

my-db> get name
name = John

my-db> list
Stored data:
  name = John
  age = 25

my-db> count
Total items: 2

my-db> delete age
Deleted: age (was: 25)

my-db> exit
Goodbye!
```

## Project Structure

```
my-db/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/example/mydb/
│       │       ├── MyDbApplication.java
│       │       └── repl/
│       │           ├── ReplRunner.java
│       │           └── CommandProcessor.java
│       └── resources/
│           └── application.properties
├── pom.xml
└── README.md
```

## Extending the Application

You can easily extend the application by:

1. Adding new commands in `CommandProcessor`
2. Injecting Spring beans for database access, external APIs, etc.
3. Adding more sophisticated parsing and validation
4. Implementing persistent storage instead of in-memory

## License

MIT

