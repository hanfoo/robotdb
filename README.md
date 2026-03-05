# RobotDB

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**RobotDB** is a high-performance embedded relational database written entirely in Rust. It provides a complete SQL database engine — including a hand-written parser, B+ Tree storage, buffer pool, write-ahead logging, and ACID transactions — all in a single library with zero C dependencies.

Built on Rust's ownership model and type system, RobotDB offers **memory safety without garbage collection**, eliminating entire classes of bugs such as use-after-free, buffer overflows, and data races at compile time. The result is a lightweight, embeddable database you can trust in safety-critical and resource-constrained environments.

---

## Features

- **Pure Rust** — No unsafe FFI, no C dependencies. Compiles to a single binary or links as a library.
- **Memory Safe** — Rust's borrow checker guarantees freedom from memory corruption, null pointer dereferences, and data races.
- **ACID Transactions** — Full transaction support with `BEGIN` / `COMMIT` / `ROLLBACK` and crash recovery via Write-Ahead Logging (WAL).
- **B+ Tree Storage Engine** — Page-based B+ Tree with leaf chaining for efficient point lookups and range scans.
- **LRU Buffer Pool** — Configurable in-memory page cache (default 256 pages) to minimize disk I/O.
- **SQL Support** — Hand-written recursive descent parser covering DDL, DML, aggregates, subqueries, expressions, and more.
- **Indexes** — Secondary and unique indexes with multi-column support, backed by the same B+ Tree engine.
- **Embeddable** — Use as a Rust library (`use robotdb::Database`) or through the interactive CLI.
- **Interactive CLI** — REPL with readline history, multi-line input, formatted table output, and meta-commands.

---

## Quick Start

### As a Library

Add RobotDB to your `Cargo.toml`:

```toml
[dependencies]
robotdb = { path = "path/to/robotdb" }
```

```rust
use robotdb::Database;

fn main() -> robotdb::Result<()> {
    let mut db = Database::open("my_app.db")?;

    db.execute("CREATE TABLE IF NOT EXISTS users (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        age  INTEGER DEFAULT 0
    )")?;

    db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")?;
    db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")?;

    let result = db.query("SELECT * FROM users WHERE age > 20 ORDER BY name")?;
    for row in &result.rows {
        println!("{:?}", row);
    }

    // Explicit transaction
    db.execute("BEGIN")?;
    db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")?;
    db.execute("COMMIT")?;

    db.close()?;
    Ok(())
}
```

### Pre-built Binaries

Go to the [GitHub Releases](https://github.com/hanfoo/robotdb/releases) page and download the archive matching your platform:

| Platform | Asset |
|---|---|
| macOS (Apple Silicon) | `robotdb-<version>-aarch64-apple-darwin.tar.gz` |
| macOS (Intel) | `robotdb-<version>-x86_64-apple-darwin.tar.gz` |
| Linux (x86_64) | `robotdb-<version>-x86_64-unknown-linux-gnu.tar.gz` |

Then extract and run:

```bash
tar xzf robotdb-*.tar.gz
./robotdb-cli my_database.db
```

### Using the CLI

```bash
# Build
cargo build --release

# Open a database file
./target/release/robotdb-cli my_database.db

# Or start with a temporary in-memory database
./target/release/robotdb-cli
```

Example session:

```
RobotDB v0.1.0 - Embedded Relational Database
Type .help for commands, .quit to exit

robotdb> CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
OK (0 rows affected)

robotdb> INSERT INTO products VALUES (1, 'Widget', 9.99);
OK (1 rows affected)

robotdb> INSERT INTO products VALUES (2, 'Gadget', 24.95);
OK (1 rows affected)

robotdb> SELECT * FROM products WHERE price < 20;
+----+--------+-------+
| id | name   | price |
+----+--------+-------+
| 1  | Widget | 9.99  |
+----+--------+-------+
1 row(s) returned

robotdb> .tables
products

robotdb> .quit
Bye!
```

**CLI Meta-Commands:**

| Command | Description |
|---|---|
| `.help` | Show help |
| `.tables` | List all tables |
| `.schema [table]` | Show table schema |
| `.checkpoint` | Run WAL checkpoint |
| `.quit` | Exit the CLI |

---

## SQL Reference

### Data Types

| Type | Description |
|---|---|
| `INTEGER` | 64-bit signed integer |
| `REAL` | 64-bit floating point |
| `TEXT` | UTF-8 string |
| `BLOB` | Binary data |
| `BOOLEAN` | `TRUE` / `FALSE` |
| `NULL` | Null value |

### CREATE TABLE

```sql
CREATE TABLE [IF NOT EXISTS] table_name (
    column_name type [PRIMARY KEY [AUTOINCREMENT]] [NOT NULL] [UNIQUE]
                     [DEFAULT expr] [REFERENCES other_table(col)],
    ...
    [PRIMARY KEY (col1, col2, ...)]
    [UNIQUE (col1, col2, ...)]
);
```

### DROP TABLE

```sql
DROP TABLE [IF EXISTS] table_name;
```

### ALTER TABLE

```sql
ALTER TABLE t ADD [COLUMN] column_name type [constraints];
ALTER TABLE t DROP [COLUMN] column_name;
ALTER TABLE t RENAME [COLUMN] old_name TO new_name;
ALTER TABLE t RENAME TO new_name;
```

### CREATE / DROP INDEX

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx ON table_name (col1, col2, ...);
DROP INDEX [IF EXISTS] idx;
```

### INSERT

```sql
INSERT INTO table_name [(col1, col2, ...)] VALUES (v1, v2, ...), (v3, v4, ...);
INSERT INTO table_name SELECT ...;
```

### SELECT

```sql
SELECT [DISTINCT] expr [AS alias], ...
  FROM table_name [AS alias]
  [WHERE condition]
  [GROUP BY col1, col2, ...]
  [HAVING condition]
  [ORDER BY col1 [ASC|DESC], ...]
  [LIMIT n]
  [OFFSET m];
```

### UPDATE

```sql
UPDATE table_name SET col1 = expr, col2 = expr, ... [WHERE condition];
```

### DELETE

```sql
DELETE FROM table_name [WHERE condition];
```

### Transactions

```sql
BEGIN;
-- ... statements ...
COMMIT;
-- or
ROLLBACK;
```

### PRAGMA

```sql
PRAGMA table_info(table_name);   -- Show column details
PRAGMA tables;                   -- List all tables
PRAGMA page_count;               -- Number of disk pages
```

### Operators

| Category | Operators |
|---|---|
| Arithmetic | `+`  `-`  `*`  `/`  `%` |
| Comparison | `=`  `!=`  `<`  `<=`  `>`  `>=` |
| Logical | `AND`  `OR`  `NOT` |
| String | `\|\|` (concatenation) |
| Other | `IS NULL`  `IS NOT NULL`  `BETWEEN`  `IN`  `NOT IN`  `LIKE`  `CAST(expr AS type)` |

### Aggregate Functions

| Function | Description |
|---|---|
| `COUNT(*)` / `COUNT(expr)` | Row count (supports `DISTINCT`) |
| `SUM(expr)` | Sum of values (supports `DISTINCT`) |
| `AVG(expr)` | Average (supports `DISTINCT`) |
| `MIN(expr)` | Minimum value |
| `MAX(expr)` | Maximum value |

### Scalar Functions

| Function | Description |
|---|---|
| `ABS(x)` | Absolute value |
| `LENGTH(s)` | String or blob length |
| `UPPER(s)` | Convert to uppercase |
| `LOWER(s)` | Convert to lowercase |
| `SUBSTR(s, start [, len])` | Extract substring |
| `COALESCE(v1, v2, ...)` | First non-NULL value |
| `IFNULL(v, default)` | Replace NULL with default |
| `TYPEOF(v)` | Type name as text |
| `ROUND(n [, decimals])` | Round to decimal places |

---

## SQL Examples

### Schema Design

```sql
CREATE TABLE employees (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    name   TEXT NOT NULL,
    dept   TEXT NOT NULL,
    salary REAL DEFAULT 0,
    active BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX idx_emp_name ON employees (name);
CREATE INDEX idx_emp_dept ON employees (dept);
```

### Inserting Data

```sql
INSERT INTO employees (name, dept, salary) VALUES ('Alice', 'Engineering', 95000);
INSERT INTO employees (name, dept, salary) VALUES ('Bob', 'Engineering', 88000);
INSERT INTO employees (name, dept, salary) VALUES ('Carol', 'Marketing', 72000);
INSERT INTO employees (name, dept, salary) VALUES ('Dave', 'Marketing', 68000);
INSERT INTO employees (name, dept, salary) VALUES ('Eve', 'Engineering', 102000);
```

### Filtering and Sorting

```sql
-- Basic filter
SELECT name, salary FROM employees WHERE dept = 'Engineering' ORDER BY salary DESC;

-- Pattern matching
SELECT * FROM employees WHERE name LIKE 'A%';

-- Range filter
SELECT * FROM employees WHERE salary BETWEEN 70000 AND 100000;

-- NULL checks
SELECT * FROM employees WHERE active IS NOT NULL;

-- IN list
SELECT * FROM employees WHERE dept IN ('Engineering', 'Marketing');
```

### Aggregation

```sql
-- Department summary
SELECT dept, COUNT(*) AS headcount, AVG(salary) AS avg_salary, MAX(salary) AS top_salary
FROM employees
GROUP BY dept
HAVING COUNT(*) > 1
ORDER BY avg_salary DESC;

-- Distinct count
SELECT COUNT(DISTINCT dept) AS num_departments FROM employees;
```

### Subqueries

```sql
-- Employees earning above average
SELECT name, salary FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

-- Employees in departments with high earners
SELECT name FROM employees
WHERE dept IN (SELECT dept FROM employees WHERE salary > 90000);
```

### Transactions

```sql
BEGIN;
UPDATE employees SET salary = salary * 1.1 WHERE dept = 'Engineering';
DELETE FROM employees WHERE active = FALSE;
COMMIT;
```

### Using Scalar Functions

```sql
SELECT UPPER(name), LENGTH(name), ROUND(salary / 12, 2) AS monthly
FROM employees;

SELECT COALESCE(dept, 'Unassigned') AS department FROM employees;
```

### ALTER TABLE

```sql
ALTER TABLE employees ADD COLUMN email TEXT;
ALTER TABLE employees RENAME COLUMN dept TO department;
ALTER TABLE employees DROP COLUMN active;
```

---

## Building & Testing

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # Run all tests
cargo test --test sql_test   # Run SQL integration tests
cargo bench              # Run benchmarks
```

---

## License

MIT License — see [LICENSE](LICENSE).
