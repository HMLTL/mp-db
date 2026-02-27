# MP-DB Usage Guide

MP-DB is a lightweight relational database with a CLI REPL interface, SQL parsing powered by Apache Calcite, and a custom storage engine with disk persistence.

## Getting Started

### Prerequisites

- Java 17+
- Docker (optional)

### Build & Run

```bash
# Build the project
./gradlew clean build

# Start the REPL
./gradlew bootRun
```

### Run with Docker

```bash
# Build the image
docker build -t mp-db .

# Run interactively with persistent data volume
docker run -it -v mp-db-data:/app/data mp-db
```

The `-it` flags are required for interactive terminal input with command history support.

You will see the interactive prompt:

```
═══════════════════════════════════════════
  Welcome to MP(Mykola Pikuza) DB CLI REPL
═══════════════════════════════════════════
Powered by Spring Boot and Apache Calcite

mp-db>
```

## Data Types

| Type       | Description                  | Storage        |
|------------|------------------------------|----------------|
| INT        | 32-bit integer               | 4 bytes fixed  |
| FLOAT      | 32-bit floating point        | 4 bytes fixed  |
| BOOLEAN    | true / false                 | 1 byte fixed   |
| VARCHAR(n) | Variable-length string       | 4B prefix + data |
| TEXT       | Variable-length string (unbounded) | 4B prefix + data |

## SQL Statements

### CREATE TABLE

```sql
CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN);
CREATE TABLE products (id INT, name VARCHAR(100), price FLOAT, description TEXT);
```

### INSERT

```sql
INSERT INTO users VALUES (1, 'Alice', true);
INSERT INTO users VALUES (2, 'Bob', false);
INSERT INTO users VALUES (3, NULL, true);
INSERT INTO products VALUES (1, 'Widget', 9.99, 'A fine widget');
```

Multiple rows in a single statement:

```sql
INSERT INTO users VALUES (3, 'Charlie', true), (4, 'Diana', false);
```

### SELECT

Select all rows:

```sql
SELECT * FROM users;
```

Column projection:

```sql
SELECT id, name FROM users;
SELECT name, active FROM users;
```

With a WHERE clause:

```sql
SELECT * FROM users WHERE active = true;
SELECT * FROM products WHERE price > 10.0;
SELECT * FROM users WHERE id >= 2 AND active = true;
SELECT * FROM users WHERE name = 'Alice' OR name = 'Bob';
```

NULL checks:

```sql
SELECT * FROM users WHERE name IS NULL;
SELECT * FROM users WHERE name IS NOT NULL;
```

Supported comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`

Logical operators: `AND`, `OR`

Note: comparisons with NULL follow SQL three-valued logic — `NULL = NULL` returns false. Use `IS NULL` instead.

### JOIN

INNER JOIN — returns only matching rows from both tables:

```sql
SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;
SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id;
```

LEFT JOIN — returns all rows from the left table, with NULLs for unmatched right columns:

```sql
SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;
```

JOINs can be combined with WHERE:

```sql
SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100;
```

### Subqueries

Subquery in FROM (derived table):

```sql
SELECT * FROM (SELECT * FROM users WHERE active = true) AS active_users;
```

Subquery in WHERE with IN:

```sql
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);
```

### UPDATE

```sql
UPDATE users SET name = 'Robert' WHERE id = 2;
UPDATE users SET active = true;
UPDATE users SET name = NULL WHERE id = 3;
UPDATE products SET price = 19.99, name = 'Super Widget' WHERE id = 1;
```

### DELETE

```sql
DELETE FROM users WHERE active = false;
DELETE FROM users WHERE id = 3;
DELETE FROM users;  -- deletes all rows
```

### DROP TABLE

```sql
DROP TABLE users;
```

## REPL Commands

Commands are prefixed with `:` (colon).

| Command              | Description                        |
|----------------------|------------------------------------|
| `:help`, `:h`, `:?` | Show available commands             |
| `:quit`, `:exit`, `:q` | Exit the REPL                    |
| `:status`            | Show current system status          |
| `:debug-ast on`      | Enable AST debug output (default)   |
| `:debug-ast off`     | Disable AST debug output            |

### Command History & Keyboard Shortcuts

MP-DB supports full command history with arrow key navigation (powered by JLine):

| Key          | Action                              |
|--------------|-------------------------------------|
| Up Arrow     | Previous command from history       |
| Down Arrow   | Next command in history             |
| Left / Right | Move cursor within the current line |
| Home / End   | Jump to start / end of line         |
| Ctrl+C       | Cancel current input                |
| Ctrl+D       | Exit the REPL                       |

Command history is saved to `<data-dir>/.mpdb_history` and persists across sessions.

### Debug AST Mode

When enabled (default), each SQL statement prints the parsed query type and AST before execution:

```
mp-db> SELECT * FROM users
Query Type: SELECT
AST:
SELECT *
FROM `USERS`
 ID | NAME  | ACTIVE
----+-------+-------
 1  | Alice | true
(1 row)
```

Disable it for cleaner output:

```
mp-db> :debug-ast off
```

## Data Persistence

MP-DB persists all data to disk automatically. Tables and their data survive application restarts.

- **Catalog metadata** is stored in `<data-dir>/catalog.meta`
- **Table data** is stored in `<data-dir>/<TABLE_NAME>.dat` (one file per table)

The default data directory is `./data`. It can be changed in `application.yml`:

```yaml
app:
  data-dir: "./data"
```

## Storage Architecture

MP-DB uses a page-based storage engine:

- **Page size**: 4096 bytes (fixed)
- **Page layout**: Slotted pages with a header, slot directory, and backward-growing tuple area
- **Heap files**: One heap file per table, composed of multiple pages
- **Free-space map**: Tracks available space per page for efficient inserts
- **Serialization**: Fixed-length types stored directly; variable-length types use a 4-byte length prefix followed by UTF-8 data

## Example Session

```
mp-db> :debug-ast off
Debug AST mode disabled.

mp-db> CREATE TABLE employees (id INT, name VARCHAR(100), salary FLOAT, active BOOLEAN)
Table 'EMPLOYEES' created.

mp-db> INSERT INTO employees VALUES (1, 'Alice', 75000.0, true)
Inserted 1 row.

mp-db> INSERT INTO employees VALUES (2, 'Bob', 65000.0, true)
Inserted 1 row.

mp-db> INSERT INTO employees VALUES (3, 'Charlie', 80000.0, false)
Inserted 1 row.

mp-db> SELECT * FROM employees
 ID | NAME    | SALARY  | ACTIVE
----+---------+---------+-------
 1  | Alice   | 75000.0 | true
 2  | Bob     | 65000.0 | true
 3  | Charlie | 80000.0 | false
(3 rows)

mp-db> UPDATE employees SET salary = 70000.0 WHERE name = 'Bob'
Updated 1 row.

mp-db> SELECT * FROM employees WHERE active = true AND salary >= 70000.0
 ID | NAME  | SALARY  | ACTIVE
----+-------+---------+-------
 1  | Alice | 75000.0 | true
 2  | Bob   | 70000.0 | true
(2 rows)

mp-db> DELETE FROM employees WHERE active = false
Deleted 1 row.

mp-db> SELECT * FROM employees
 ID | NAME  | SALARY  | ACTIVE
----+-------+---------+-------
 1  | Alice | 75000.0 | true
 2  | Bob   | 70000.0 | true
(2 rows)

mp-db> DROP TABLE employees
Table 'EMPLOYEES' dropped.

mp-db> :quit
Goodbye!
```

### JOINs, Subqueries & Projection

```
mp-db> :debug-ast off
Debug AST mode disabled.

mp-db> CREATE TABLE departments (id INT, name VARCHAR(50))
Table 'DEPARTMENTS' created.

mp-db> CREATE TABLE employees (id INT, name VARCHAR(100), dept_id INT, salary FLOAT)
Table 'EMPLOYEES' created.

mp-db> INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')
Inserted 3 rows.

mp-db> INSERT INTO employees VALUES (1, 'Alice', 1, 90000.0), (2, 'Bob', 1, 75000.0)
Inserted 2 rows.

mp-db> INSERT INTO employees VALUES (3, 'Charlie', 2, 60000.0)
Inserted 1 row.

mp-db> SELECT employees.name, departments.name FROM employees INNER JOIN departments ON employees.dept_id = departments.id
 EMPLOYEES.NAME | DEPARTMENTS.NAME
----------------+-----------------
 Alice          | Engineering
 Bob            | Engineering
 Charlie        | Sales
(3 rows)

mp-db> SELECT * FROM departments LEFT JOIN employees ON departments.id = employees.dept_id
 DEPARTMENTS.ID | DEPARTMENTS.NAME | EMPLOYEES.ID | EMPLOYEES.NAME | EMPLOYEES.DEPT_ID | EMPLOYEES.SALARY
----------------+------------------+--------------+----------------+--------------------+-----------------
 1              | Engineering      | 1            | Alice          | 1                  | 90000.0
 1              | Engineering      | 2            | Bob            | 1                  | 75000.0
 2              | Sales            | 3            | Charlie        | 2                  | 60000.0
 3              | HR               | NULL         | NULL           | NULL               | NULL
(4 rows)

mp-db> SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering')
 ID | NAME  | DEPT_ID | SALARY
----+-------+---------+--------
 1  | Alice | 1       | 90000.0
 2  | Bob   | 1       | 75000.0
(2 rows)

mp-db> SELECT * FROM (SELECT * FROM employees WHERE salary > 70000.0) AS top_earners
 ID | NAME  | DEPT_ID | SALARY
----+-------+---------+--------
 1  | Alice | 1       | 90000.0
 2  | Bob   | 1       | 75000.0
(2 rows)

mp-db> SELECT id, name FROM employees
 ID | NAME
----+--------
 1  | Alice
 2  | Bob
 3  | Charlie
(3 rows)

mp-db> DROP TABLE employees
Table 'EMPLOYEES' dropped.

mp-db> DROP TABLE departments
Table 'DEPARTMENTS' dropped.
```

## Current Limitations

- No `RIGHT JOIN` or `FULL OUTER JOIN` (only `INNER JOIN` and `LEFT JOIN`)
- No `ORDER BY`, `GROUP BY`, or aggregate functions
- No transactions or concurrency control
- Deleted space within pages is not reclaimed (no compaction)
