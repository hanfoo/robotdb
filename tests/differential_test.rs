/// 差分测试套件
///
/// 以 SQLite 3.x 为黄金标准，对 RobotDB 的 SQL 执行结果进行逐一比对。
/// 每个测试用例都会同时在两个数据库上执行相同的 SQL 序列，
/// 然后对结果进行规范化比较，任何语义差异都会导致测试失败。

mod differential;
use differential::{DiffTestRunner, DiffOptions, DiffResult};

// ─────────────────────────────────────────────────────────────────────────────
// 辅助宏：执行 SQL 并断言结果一致
// ─────────────────────────────────────────────────────────────────────────────

macro_rules! diff {
    ($runner:expr, $sql:expr) => {{
        let result = $runner.run($sql);
        if !result.is_ok() {
            match result {
                DiffResult::Mismatch { sql, sqlite_result, robotdb_result, detail } => {
                    panic!(
                        "\n[差分测试失败]\nSQL:    {}\n原因:   {}\nSQLite: {}\nRobotDB: {}",
                        sql, detail, sqlite_result, robotdb_result
                    );
                }
                DiffResult::OneError { sql, sqlite_result, robotdb_result } => {
                    panic!(
                        "\n[差分测试失败 - 单方报错]\nSQL:    {}\nSQLite: {}\nRobotDB: {}",
                        sql, sqlite_result, robotdb_result
                    );
                }
                _ => {}
            }
        }
    }};
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. DDL 测试
// ─────────────────────────────────────────────────────────────────────────────

/// 基础建表与删表
#[test]
fn diff_test_ddl_create_drop_table() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
    diff!(r, "SELECT COUNT(*) FROM users");

    // 重复建表应该报错（两者都报错即可）
    diff!(r, "CREATE TABLE users (id INTEGER PRIMARY KEY)");

    // 建表后可以查询
    diff!(r, "SELECT * FROM users");
}

/// 多列类型测试
#[test]
fn diff_test_ddl_column_types() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE types_test (
        id INTEGER PRIMARY KEY,
        int_col INTEGER,
        real_col REAL,
        text_col TEXT,
        blob_col BLOB
    )");

    diff!(r, "INSERT INTO types_test VALUES (1, 42, 3.14, 'hello', NULL)");
    diff!(r, "INSERT INTO types_test VALUES (2, -100, -2.718, '', NULL)");
    diff!(r, "INSERT INTO types_test VALUES (3, 0, 0.0, 'world', NULL)");

    diff!(r, "SELECT * FROM types_test ORDER BY id");
    diff!(r, "SELECT id, int_col, real_col, text_col FROM types_test ORDER BY id");
}

/// NOT NULL 约束
#[test]
fn diff_test_ddl_not_null_constraint() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE nn_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)");
    diff!(r, "INSERT INTO nn_test VALUES (1, 'ok')");
    // 违反 NOT NULL 约束
    diff!(r, "INSERT INTO nn_test VALUES (2, NULL)");
    // 只有第一行
    diff!(r, "SELECT COUNT(*) FROM nn_test");
}

/// UNIQUE 约束
#[test]
fn diff_test_ddl_unique_constraint() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE uniq_test (id INTEGER PRIMARY KEY, email TEXT UNIQUE)");
    diff!(r, "INSERT INTO uniq_test VALUES (1, 'a@b.com')");
    diff!(r, "INSERT INTO uniq_test VALUES (2, 'c@d.com')");
    // 违反 UNIQUE 约束
    diff!(r, "INSERT INTO uniq_test VALUES (3, 'a@b.com')");
    // 只有两行
    diff!(r, "SELECT COUNT(*) FROM uniq_test");
    diff!(r, "SELECT * FROM uniq_test ORDER BY id");
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. INSERT 测试
// ─────────────────────────────────────────────────────────────────────────────

/// 基础 INSERT 和 SELECT
#[test]
fn diff_test_insert_basic() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    diff!(r, "INSERT INTO t VALUES (1, 'alpha')");
    diff!(r, "INSERT INTO t VALUES (2, 'beta')");
    diff!(r, "INSERT INTO t VALUES (3, 'gamma')");
    diff!(r, "SELECT * FROM t ORDER BY id");
    diff!(r, "SELECT COUNT(*) FROM t");
}

/// INSERT 多种数值类型
#[test]
fn diff_test_insert_numeric_types() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE nums (id INTEGER PRIMARY KEY, n INTEGER, f REAL)");
    diff!(r, "INSERT INTO nums VALUES (1, 0, 0.0)");
    diff!(r, "INSERT INTO nums VALUES (2, 2147483647, 3.14159265358979)");
    diff!(r, "INSERT INTO nums VALUES (3, -2147483648, -1.0e10)");
    diff!(r, "INSERT INTO nums VALUES (4, 9223372036854775807, 1.7976931348623157e308)");
    diff!(r, "SELECT * FROM nums ORDER BY id");
}

/// INSERT NULL 值
#[test]
fn diff_test_insert_null_values() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE nulls (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)");
    diff!(r, "INSERT INTO nulls VALUES (1, NULL, NULL, NULL)");
    diff!(r, "INSERT INTO nulls VALUES (2, 'x', NULL, 1.0)");
    diff!(r, "INSERT INTO nulls VALUES (3, NULL, 42, NULL)");
    diff!(r, "SELECT * FROM nulls ORDER BY id");
    diff!(r, "SELECT COUNT(*) FROM nulls WHERE a IS NULL");
    diff!(r, "SELECT COUNT(*) FROM nulls WHERE b IS NOT NULL");
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. SELECT 与 WHERE 测试
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE 条件过滤
#[test]
fn diff_test_select_where() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, qty INTEGER)");
    for i in 1..=10 {
        let sql = format!("INSERT INTO products VALUES ({}, 'product{}', {}.99, {})",
            i, i, i * 10, i * 5);
        diff!(r, &sql);
    }

    diff!(r, "SELECT * FROM products WHERE price > 50.0 ORDER BY id");
    diff!(r, "SELECT * FROM products WHERE qty <= 20 ORDER BY id");
    diff!(r, "SELECT * FROM products WHERE name = 'product3'");
    diff!(r, "SELECT * FROM products WHERE price BETWEEN 20.0 AND 60.0 ORDER BY id");
    diff!(r, "SELECT * FROM products WHERE id IN (1, 3, 5, 7) ORDER BY id");
    diff!(r, "SELECT * FROM products WHERE name LIKE 'product1%' ORDER BY id");
    diff!(r, "SELECT * FROM products WHERE id NOT IN (2, 4, 6, 8, 10) ORDER BY id");
}

/// 复合 WHERE 条件（AND/OR/NOT）
#[test]
fn diff_test_select_compound_where() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER, active INTEGER)");
    diff!(r, "INSERT INTO emp VALUES (1, 'eng', 80000, 1)");
    diff!(r, "INSERT INTO emp VALUES (2, 'eng', 90000, 0)");
    diff!(r, "INSERT INTO emp VALUES (3, 'hr', 60000, 1)");
    diff!(r, "INSERT INTO emp VALUES (4, 'hr', 70000, 1)");
    diff!(r, "INSERT INTO emp VALUES (5, 'sales', 55000, 0)");

    diff!(r, "SELECT * FROM emp WHERE dept = 'eng' AND salary > 85000");
    diff!(r, "SELECT * FROM emp WHERE dept = 'hr' OR salary > 85000 ORDER BY id");
    diff!(r, "SELECT * FROM emp WHERE NOT (active = 0) ORDER BY id");
    diff!(r, "SELECT * FROM emp WHERE (dept = 'eng' OR dept = 'hr') AND active = 1 ORDER BY id");
}

/// ORDER BY 和 LIMIT/OFFSET
#[test]
fn diff_test_select_order_limit() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE scores (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");
    diff!(r, "INSERT INTO scores VALUES (1, 'Alice', 95)");
    diff!(r, "INSERT INTO scores VALUES (2, 'Bob', 87)");
    diff!(r, "INSERT INTO scores VALUES (3, 'Charlie', 92)");
    diff!(r, "INSERT INTO scores VALUES (4, 'Diana', 87)");
    diff!(r, "INSERT INTO scores VALUES (5, 'Eve', 100)");

    // 使用有序查询（指定 ORDER BY 以确保结果确定性）
    let mut ordered_opts = DiffOptions::default();
    ordered_opts.ignore_order = false;
    let mut r2 = DiffTestRunner::with_opts(ordered_opts);
    diff!(r2, "CREATE TABLE scores (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");
    diff!(r2, "INSERT INTO scores VALUES (1, 'Alice', 95)");
    diff!(r2, "INSERT INTO scores VALUES (2, 'Bob', 87)");
    diff!(r2, "INSERT INTO scores VALUES (3, 'Charlie', 92)");
    diff!(r2, "INSERT INTO scores VALUES (4, 'Diana', 87)");
    diff!(r2, "INSERT INTO scores VALUES (5, 'Eve', 100)");

    diff!(r2, "SELECT * FROM scores ORDER BY score DESC LIMIT 3");
    diff!(r2, "SELECT * FROM scores ORDER BY score ASC, id ASC LIMIT 3 OFFSET 1");
    diff!(r2, "SELECT name FROM scores ORDER BY name ASC");
    diff!(r2, "SELECT * FROM scores ORDER BY score DESC, name ASC");
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. UPDATE 与 DELETE 测试
// ─────────────────────────────────────────────────────────────────────────────

/// UPDATE 基础操作
#[test]
fn diff_test_update() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)");
    diff!(r, "INSERT INTO items VALUES (1, 'a', 10)");
    diff!(r, "INSERT INTO items VALUES (2, 'b', 20)");
    diff!(r, "INSERT INTO items VALUES (3, 'c', 30)");

    diff!(r, "UPDATE items SET val = 99 WHERE id = 2");
    diff!(r, "SELECT * FROM items ORDER BY id");

    diff!(r, "UPDATE items SET val = val + 1 WHERE val < 50");
    diff!(r, "SELECT * FROM items ORDER BY id");

    diff!(r, "UPDATE items SET name = 'updated', val = 0 WHERE id = 1");
    diff!(r, "SELECT * FROM items ORDER BY id");
}

/// DELETE 基础操作
#[test]
fn diff_test_delete() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE data (id INTEGER PRIMARY KEY, v INTEGER)");
    for i in 1..=10 {
        diff!(r, &format!("INSERT INTO data VALUES ({}, {})", i, i * 10));
    }

    diff!(r, "DELETE FROM data WHERE id = 5");
    diff!(r, "SELECT COUNT(*) FROM data");

    diff!(r, "DELETE FROM data WHERE v > 70");
    diff!(r, "SELECT * FROM data ORDER BY id");

    diff!(r, "DELETE FROM data");
    diff!(r, "SELECT COUNT(*) FROM data");
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. 聚合函数测试
// ─────────────────────────────────────────────────────────────────────────────

/// 基础聚合函数：COUNT, SUM, AVG, MIN, MAX
#[test]
fn diff_test_aggregation_basic() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount REAL, qty INTEGER)");
    diff!(r, "INSERT INTO sales VALUES (1, 'north', 100.0, 5)");
    diff!(r, "INSERT INTO sales VALUES (2, 'south', 200.0, 10)");
    diff!(r, "INSERT INTO sales VALUES (3, 'north', 150.0, 7)");
    diff!(r, "INSERT INTO sales VALUES (4, 'east', 300.0, 15)");
    diff!(r, "INSERT INTO sales VALUES (5, 'south', 250.0, 12)");
    diff!(r, "INSERT INTO sales VALUES (6, 'north', NULL, 3)");

    diff!(r, "SELECT COUNT(*) FROM sales");
    diff!(r, "SELECT COUNT(amount) FROM sales");  // 不计 NULL
    diff!(r, "SELECT SUM(amount) FROM sales");
    diff!(r, "SELECT SUM(qty) FROM sales");
    diff!(r, "SELECT MIN(amount) FROM sales");
    diff!(r, "SELECT MAX(amount) FROM sales");
    diff!(r, "SELECT MIN(qty) FROM sales");
    diff!(r, "SELECT MAX(qty) FROM sales");
}

/// GROUP BY 聚合
#[test]
fn diff_test_aggregation_group_by() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE orders (id INTEGER PRIMARY KEY, dept TEXT, amount INTEGER)");
    diff!(r, "INSERT INTO orders VALUES (1, 'eng', 500)");
    diff!(r, "INSERT INTO orders VALUES (2, 'eng', 300)");
    diff!(r, "INSERT INTO orders VALUES (3, 'hr', 200)");
    diff!(r, "INSERT INTO orders VALUES (4, 'hr', 400)");
    diff!(r, "INSERT INTO orders VALUES (5, 'sales', 600)");
    diff!(r, "INSERT INTO orders VALUES (6, 'sales', 100)");

    diff!(r, "SELECT dept, COUNT(*) FROM orders GROUP BY dept ORDER BY dept");
    diff!(r, "SELECT dept, SUM(amount) FROM orders GROUP BY dept ORDER BY dept");
    diff!(r, "SELECT dept, AVG(amount) FROM orders GROUP BY dept ORDER BY dept");
    diff!(r, "SELECT dept, MIN(amount), MAX(amount) FROM orders GROUP BY dept ORDER BY dept");
}

/// HAVING 子句
#[test]
fn diff_test_aggregation_having() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE tx (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)");
    for i in 1..=12 {
        let cat = match i % 3 {
            0 => "A",
            1 => "B",
            _ => "C",
        };
        diff!(r, &format!("INSERT INTO tx VALUES ({}, '{}', {})", i, cat, i * 10));
    }

    diff!(r, "SELECT cat, SUM(val) FROM tx GROUP BY cat HAVING SUM(val) > 100 ORDER BY cat");
    diff!(r, "SELECT cat, COUNT(*) FROM tx GROUP BY cat HAVING COUNT(*) >= 4 ORDER BY cat");
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. 事务测试
// ─────────────────────────────────────────────────────────────────────────────

/// 事务提交
#[test]
fn diff_test_transaction_commit() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE acct (id INTEGER PRIMARY KEY, balance INTEGER)");
    diff!(r, "INSERT INTO acct VALUES (1, 1000)");
    diff!(r, "INSERT INTO acct VALUES (2, 2000)");

    diff!(r, "BEGIN");
    diff!(r, "UPDATE acct SET balance = balance - 100 WHERE id = 1");
    diff!(r, "UPDATE acct SET balance = balance + 100 WHERE id = 2");
    diff!(r, "COMMIT");

    diff!(r, "SELECT * FROM acct ORDER BY id");
}

/// 事务回滚
#[test]
fn diff_test_transaction_rollback() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE acct (id INTEGER PRIMARY KEY, balance INTEGER)");
    diff!(r, "INSERT INTO acct VALUES (1, 1000)");
    diff!(r, "INSERT INTO acct VALUES (2, 2000)");

    diff!(r, "BEGIN");
    diff!(r, "UPDATE acct SET balance = balance - 100 WHERE id = 1");
    diff!(r, "UPDATE acct SET balance = balance + 100 WHERE id = 2");
    diff!(r, "ROLLBACK");

    // 回滚后余额不变
    diff!(r, "SELECT * FROM acct ORDER BY id");
}

/// 嵌套事务（SAVEPOINT）— 仅测试 SQLite 支持的语法
/// 注：RobotDB 可能不支持 SAVEPOINT，两者都报错即可
#[test]
fn diff_test_transaction_error_handling() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)");
    diff!(r, "INSERT INTO t VALUES (1, 'unique_val')");

    diff!(r, "BEGIN");
    diff!(r, "INSERT INTO t VALUES (2, 'another_val')");
    // 违反 UNIQUE 约束
    diff!(r, "INSERT INTO t VALUES (3, 'unique_val')");
    diff!(r, "ROLLBACK");

    // 回滚后只有第一行
    diff!(r, "SELECT COUNT(*) FROM t");
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. 表达式与运算符测试
// ─────────────────────────────────────────────────────────────────────────────

/// 算术运算
#[test]
fn diff_test_arithmetic_expressions() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE calc (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)");
    diff!(r, "INSERT INTO calc VALUES (1, 10, 3)");
    diff!(r, "INSERT INTO calc VALUES (2, 100, 7)");
    diff!(r, "INSERT INTO calc VALUES (3, -5, 2)");

    diff!(r, "SELECT id, a + b, a - b, a * b FROM calc ORDER BY id");
    diff!(r, "SELECT id, a / b FROM calc ORDER BY id");
    diff!(r, "SELECT id, a % b FROM calc ORDER BY id");
}

/// 比较运算符
#[test]
fn diff_test_comparison_operators() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE cmp (id INTEGER PRIMARY KEY, v INTEGER)");
    for i in [1, 5, 10, 15, 20] {
        diff!(r, &format!("INSERT INTO cmp VALUES ({}, {})", i, i));
    }

    diff!(r, "SELECT COUNT(*) FROM cmp WHERE v = 10");
    diff!(r, "SELECT COUNT(*) FROM cmp WHERE v != 10");
    diff!(r, "SELECT COUNT(*) FROM cmp WHERE v < 10");
    diff!(r, "SELECT COUNT(*) FROM cmp WHERE v <= 10");
    diff!(r, "SELECT COUNT(*) FROM cmp WHERE v > 10");
    diff!(r, "SELECT COUNT(*) FROM cmp WHERE v >= 10");
}

/// NULL 语义
#[test]
fn diff_test_null_semantics() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE n (id INTEGER PRIMARY KEY, v INTEGER)");
    diff!(r, "INSERT INTO n VALUES (1, 1)");
    diff!(r, "INSERT INTO n VALUES (2, NULL)");
    diff!(r, "INSERT INTO n VALUES (3, 3)");

    // NULL 比较
    diff!(r, "SELECT COUNT(*) FROM n WHERE v IS NULL");
    diff!(r, "SELECT COUNT(*) FROM n WHERE v IS NOT NULL");
    // NULL 在算术中传播
    diff!(r, "SELECT id, v + 1 FROM n ORDER BY id");
    // NULL 在聚合中被忽略
    diff!(r, "SELECT SUM(v), COUNT(v), COUNT(*) FROM n");
    // NULL 在排序中的位置
    diff!(r, "SELECT * FROM n ORDER BY v ASC");
}

/// LIKE 模式匹配
#[test]
fn diff_test_like_pattern() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE words (id INTEGER PRIMARY KEY, word TEXT)");
    diff!(r, "INSERT INTO words VALUES (1, 'apple')");
    diff!(r, "INSERT INTO words VALUES (2, 'application')");
    diff!(r, "INSERT INTO words VALUES (3, 'banana')");
    diff!(r, "INSERT INTO words VALUES (4, 'grape')");
    diff!(r, "INSERT INTO words VALUES (5, 'pineapple')");

    diff!(r, "SELECT word FROM words WHERE word LIKE 'app%' ORDER BY id");
    diff!(r, "SELECT word FROM words WHERE word LIKE '%apple' ORDER BY id");
    diff!(r, "SELECT word FROM words WHERE word LIKE '%an%' ORDER BY id");
    diff!(r, "SELECT word FROM words WHERE word LIKE '_ape' ORDER BY id");
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. 边界条件测试
// ─────────────────────────────────────────────────────────────────────────────

/// 空表操作
#[test]
fn diff_test_empty_table() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE empty (id INTEGER PRIMARY KEY, v TEXT)");
    diff!(r, "SELECT * FROM empty");
    diff!(r, "SELECT COUNT(*) FROM empty");
    diff!(r, "SELECT SUM(id) FROM empty");
    diff!(r, "SELECT MIN(id) FROM empty");
    diff!(r, "SELECT MAX(id) FROM empty");
    diff!(r, "UPDATE empty SET v = 'x'");
    diff!(r, "DELETE FROM empty");
    diff!(r, "SELECT COUNT(*) FROM empty");
}

/// 大量数据的正确性
#[test]
fn diff_test_large_dataset() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE big (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT)");

    // 插入 500 行
    for i in 1..=500 {
        let tag = if i % 2 == 0 { "even" } else { "odd" };
        diff!(r, &format!("INSERT INTO big VALUES ({}, {}, '{}')", i, i * i, tag));
    }

    diff!(r, "SELECT COUNT(*) FROM big");
    diff!(r, "SELECT COUNT(*) FROM big WHERE tag = 'even'");
    diff!(r, "SELECT SUM(val) FROM big WHERE id <= 100");
    diff!(r, "SELECT MIN(val), MAX(val) FROM big");
    diff!(r, "SELECT tag, COUNT(*) FROM big GROUP BY tag ORDER BY tag");
    diff!(r, "SELECT * FROM big WHERE id = 250");
    diff!(r, "SELECT * FROM big WHERE id IN (1, 100, 200, 300, 400, 500) ORDER BY id");
}

/// 特殊字符串值
#[test]
fn diff_test_special_string_values() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE strs (id INTEGER PRIMARY KEY, v TEXT)");
    diff!(r, "INSERT INTO strs VALUES (1, '')");           // 空字符串
    diff!(r, "INSERT INTO strs VALUES (2, ' ')");          // 空格
    diff!(r, "INSERT INTO strs VALUES (3, 'hello world')"); // 含空格
    diff!(r, "INSERT INTO strs VALUES (4, '0')");           // 数字字符串
    diff!(r, "INSERT INTO strs VALUES (5, 'NULL')");        // 字面量 NULL

    diff!(r, "SELECT * FROM strs ORDER BY id");
    diff!(r, "SELECT COUNT(*) FROM strs WHERE v = ''");
    diff!(r, "SELECT COUNT(*) FROM strs WHERE v != ''");
    diff!(r, "SELECT COUNT(*) FROM strs WHERE v IS NOT NULL");
}

/// 整数边界值
#[test]
fn diff_test_integer_boundaries() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE ints (id INTEGER PRIMARY KEY, v INTEGER)");
    diff!(r, "INSERT INTO ints VALUES (1, 0)");
    diff!(r, "INSERT INTO ints VALUES (2, 1)");
    diff!(r, "INSERT INTO ints VALUES (3, -1)");
    diff!(r, "INSERT INTO ints VALUES (4, 2147483647)");    // i32::MAX
    diff!(r, "INSERT INTO ints VALUES (5, -2147483648)");   // i32::MIN
    diff!(r, "INSERT INTO ints VALUES (6, 9223372036854775807)");  // i64::MAX

    diff!(r, "SELECT * FROM ints ORDER BY id");
    diff!(r, "SELECT SUM(v) FROM ints WHERE id <= 3");
    diff!(r, "SELECT MIN(v), MAX(v) FROM ints");
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. 多表操作测试
// ─────────────────────────────────────────────────────────────────────────────

/// 多表独立操作（不涉及 JOIN）
#[test]
fn diff_test_multiple_tables() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE cats (id INTEGER PRIMARY KEY, name TEXT)");
    diff!(r, "CREATE TABLE items (id INTEGER PRIMARY KEY, cat_id INTEGER, name TEXT, price REAL)");

    diff!(r, "INSERT INTO cats VALUES (1, 'Electronics')");
    diff!(r, "INSERT INTO cats VALUES (2, 'Books')");
    diff!(r, "INSERT INTO cats VALUES (3, 'Clothing')");

    diff!(r, "INSERT INTO items VALUES (1, 1, 'Phone', 599.99)");
    diff!(r, "INSERT INTO items VALUES (2, 1, 'Laptop', 999.99)");
    diff!(r, "INSERT INTO items VALUES (3, 2, 'Novel', 12.99)");
    diff!(r, "INSERT INTO items VALUES (4, 2, 'Textbook', 49.99)");
    diff!(r, "INSERT INTO items VALUES (5, 3, 'T-Shirt', 19.99)");

    diff!(r, "SELECT COUNT(*) FROM cats");
    diff!(r, "SELECT COUNT(*) FROM items");
    diff!(r, "SELECT * FROM items WHERE cat_id = 1 ORDER BY id");
    diff!(r, "SELECT cat_id, COUNT(*), SUM(price) FROM items GROUP BY cat_id ORDER BY cat_id");
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. 错误处理一致性测试
// ─────────────────────────────────────────────────────────────────────────────

/// 访问不存在的表
#[test]
fn diff_test_error_table_not_found() {
    let mut r = DiffTestRunner::new();
    diff!(r, "SELECT * FROM nonexistent_table");
    diff!(r, "INSERT INTO nonexistent_table VALUES (1, 'x')");
    diff!(r, "UPDATE nonexistent_table SET v = 1");
    diff!(r, "DELETE FROM nonexistent_table");
    r.assert_all_match();
}

/// 访问不存在的列
#[test]
fn diff_test_error_column_not_found() {
    let mut r = DiffTestRunner::new();
    diff!(r, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    diff!(r, "SELECT nonexistent_col FROM t");
    diff!(r, "INSERT INTO t (id, nonexistent_col) VALUES (1, 'x')");
    r.assert_all_match();
}

/// 主键冲突
#[test]
fn diff_test_error_primary_key_conflict() {
    let mut r = DiffTestRunner::new();
    diff!(r, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    diff!(r, "INSERT INTO t VALUES (1, 'first')");
    diff!(r, "INSERT INTO t VALUES (1, 'duplicate')");
    diff!(r, "SELECT COUNT(*) FROM t");
    r.assert_all_match();
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. 综合场景测试
// ─────────────────────────────────────────────────────────────────────────────

/// 模拟电商订单系统
#[test]
fn diff_test_scenario_ecommerce() {
    let mut r = DiffTestRunner::new();

    // 建表
    diff!(r, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE)");
    diff!(r, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL, status TEXT)");

    // 插入数据
    diff!(r, "INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com')");
    diff!(r, "INSERT INTO customers VALUES (2, 'Bob', 'bob@example.com')");
    diff!(r, "INSERT INTO customers VALUES (3, 'Charlie', 'charlie@example.com')");

    diff!(r, "INSERT INTO orders VALUES (1, 1, 99.99, 'pending')");
    diff!(r, "INSERT INTO orders VALUES (2, 1, 149.50, 'completed')");
    diff!(r, "INSERT INTO orders VALUES (3, 2, 29.99, 'pending')");
    diff!(r, "INSERT INTO orders VALUES (4, 3, 199.00, 'completed')");
    diff!(r, "INSERT INTO orders VALUES (5, 2, 59.99, 'cancelled')");

    // 查询
    diff!(r, "SELECT COUNT(*) FROM orders WHERE status = 'pending'");
    diff!(r, "SELECT SUM(total) FROM orders WHERE status = 'completed'");
    diff!(r, "SELECT customer_id, COUNT(*), SUM(total) FROM orders GROUP BY customer_id ORDER BY customer_id");
    diff!(r, "SELECT MIN(total), MAX(total), AVG(total) FROM orders");

    // 更新
    diff!(r, "UPDATE orders SET status = 'completed' WHERE status = 'pending' AND total > 50.0");
    diff!(r, "SELECT COUNT(*) FROM orders WHERE status = 'completed'");

    // 事务
    diff!(r, "BEGIN");
    diff!(r, "INSERT INTO orders VALUES (6, 1, 299.99, 'pending')");
    diff!(r, "UPDATE customers SET name = 'Alice Smith' WHERE id = 1");
    diff!(r, "COMMIT");

    diff!(r, "SELECT * FROM customers WHERE id = 1");
    diff!(r, "SELECT COUNT(*) FROM orders WHERE customer_id = 1");
}

/// 模拟学生成绩系统
#[test]
fn diff_test_scenario_grades() {
    let mut r = DiffTestRunner::new();

    diff!(r, "CREATE TABLE students (id INTEGER PRIMARY KEY, name TEXT, grade TEXT)");
    diff!(r, "CREATE TABLE courses (id INTEGER PRIMARY KEY, name TEXT, credits INTEGER)");
    diff!(r, "CREATE TABLE enrollments (student_id INTEGER, course_id INTEGER, score REAL)");

    // 学生
    for (id, name, grade) in [(1,"Alice","A"),(2,"Bob","B"),(3,"Charlie","A"),(4,"Diana","C")] {
        diff!(r, &format!("INSERT INTO students VALUES ({}, '{}', '{}')", id, name, grade));
    }

    // 课程
    for (id, name, credits) in [(1,"Math",4),(2,"Physics",3),(3,"English",2),(4,"History",2)] {
        diff!(r, &format!("INSERT INTO courses VALUES ({}, '{}', {})", id, name, credits));
    }

    // 成绩
    let scores = [
        (1,1,92.0),(1,2,88.0),(1,3,95.0),
        (2,1,75.0),(2,2,80.0),(2,4,70.0),
        (3,1,98.0),(3,3,91.0),(3,4,85.0),
        (4,2,65.0),(4,3,72.0),(4,4,68.0),
    ];
    for (sid, cid, score) in scores {
        diff!(r, &format!("INSERT INTO enrollments VALUES ({}, {}, {})", sid, cid, score));
    }

    // 统计查询
    diff!(r, "SELECT student_id, COUNT(*), AVG(score) FROM enrollments GROUP BY student_id ORDER BY student_id");
    diff!(r, "SELECT course_id, COUNT(*), MIN(score), MAX(score) FROM enrollments GROUP BY course_id ORDER BY course_id");
    diff!(r, "SELECT COUNT(*) FROM enrollments WHERE score >= 90.0");
    diff!(r, "SELECT COUNT(*) FROM students WHERE grade = 'A'");

    r.assert_all_match();
    let summary = r.summary();
    println!("\n差分测试摘要（成绩系统）: {}", summary);
    assert_eq!(summary.fail_count(), 0, "差分测试发现 {} 个差异", summary.fail_count());
}
