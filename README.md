# MP-DB (Mykola Pikuza DB)

A lightweight relational database with an interactive CLI REPL, SQL parsing via Apache Calcite, and a custom page-based storage engine with disk persistence.

## Features

- Interactive REPL with command history (Up/Down arrow keys)
- Full SQL support: `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `DROP TABLE`
- WHERE clauses with `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`
- Data types: `INT`, `FLOAT`, `VARCHAR(n)`, `TEXT`, `BOOLEAN`
- Disk persistence — tables and data survive restarts
- Page-based storage engine with slotted pages (4096B) and free-space map
- Debug mode to inspect parsed AST

## Quick Start

```bash
./gradlew clean build
./gradlew bootRun
```

Or with Docker:

```bash
docker build -t mp-db .
docker run -it -v mp-db-data:/app/data mp-db
```

## Example

```
mp-db> CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)
Table 'USERS' created.

mp-db> INSERT INTO users VALUES (1, 'Alice', true)
Inserted 1 row.

mp-db> SELECT * FROM users
 ID | NAME  | ACTIVE
----+-------+-------
 1  | Alice | true
(1 row)

mp-db> :quit
Goodbye!
```

## Documentation

See [USAGE.md](USAGE.md) for full documentation including all SQL statements, REPL commands, keyboard shortcuts, persistence details, and storage architecture.

## Tech Stack

- Java 17, Spring Boot 3.4, Gradle
- Apache Calcite 1.37.0 (SQL parsing + DDL)
- JLine 3 (REPL with command history)
- JUnit 5 + Mockito (tests)

## License

MIT
