/// P2 SQL 模糊测试套件
///
/// 策略：
/// 1. **崩溃检测模糊测试**：向 RobotDB 发送随机生成的 SQL，任何 panic 都是 Bug
/// 2. **差分模糊测试**：对随机生成的合法 SQL，比对 RobotDB 与 SQLite 的结果
/// 3. **变异模糊测试**：对已知合法 SQL 进行字符级变异，检测解析器健壮性
/// 4. **边界值模糊测试**：测试极端值（空字符串、超长字符串、特殊字符）

use robotdb::{Database, Value};
use rusqlite::Connection as SqliteConn;
use std::collections::HashSet;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// 随机 SQL 生成器
// ─────────────────────────────────────────────────────────────────────────────

/// 简单的线性同余伪随机数生成器（无需外部依赖）
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed ^ 0xdeadbeef_cafebabe }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn next_usize(&mut self, max: usize) -> usize {
        (self.next_u64() as usize) % max
    }

    fn next_i64(&mut self) -> i64 {
        self.next_u64() as i64
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() as f64) / (u64::MAX as f64) * 2000.0 - 1000.0
    }

    fn next_bool(&mut self) -> bool {
        self.next_u64() % 2 == 0
    }

    fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.next_usize(items.len())]
    }
}

/// 随机 SQL 生成器
struct SqlGenerator {
    rng: Lcg,
    table_name: String,
    columns: Vec<(String, &'static str)>, // (name, type)
    row_count: usize,
}

impl SqlGenerator {
    fn new(seed: u64) -> Self {
        Self {
            rng: Lcg::new(seed),
            table_name: "fuzz_table".to_string(),
            columns: vec![
                ("id".to_string(), "INTEGER"),
                ("name".to_string(), "TEXT"),
                ("score".to_string(), "REAL"),
                ("age".to_string(), "INTEGER"),
            ],
            row_count: 0,
        }
    }

    fn create_table_sql(&self) -> String {
        "CREATE TABLE fuzz_table (id INTEGER PRIMARY KEY, name TEXT, score REAL, age INTEGER)".to_string()
    }

    fn random_int_value(&mut self) -> i64 {
        let choices = [0i64, 1, -1, 100, -100, 1000, i32::MAX as i64, i32::MIN as i64, 42, 7];
        if self.rng.next_bool() {
            *self.rng.choose(&choices)
        } else {
            self.rng.next_i64() % 10000
        }
    }

    fn random_text_value(&mut self) -> String {
        let words: &[&str] = &["alice", "bob", "charlie", "dave", "eve", "frank", "grace",
                     "hello", "world", "test", "foo", "bar", "baz", "qux",
                     "it''s", "O''Brien", "tab\there", "null", "NULL", ""];
        let s = self.rng.choose(words);
        s.to_string()
    }

    fn random_real_value(&mut self) -> f64 {
        let choices = [0.0f64, 1.0, -1.0, 3.14, -3.14, 1e10, -1e10, 0.001, 999.999];
        if self.rng.next_bool() {
            *self.rng.choose(&choices)
        } else {
            self.rng.next_f64()
        }
    }

    fn random_insert(&mut self) -> String {
        self.row_count += 1;
        let id = self.row_count as i64;
        let name = self.random_text_value();
        let score = self.random_real_value();
        let age = self.random_int_value().abs() % 120;
        format!(
            "INSERT INTO fuzz_table VALUES ({}, '{}', {:.4}, {})",
            id, name.replace('\'', "''"), score, age
        )
    }

    fn random_select(&mut self) -> String {
        let cols: &[&str] = &["id", "name", "score", "age", "*",
                    "COUNT(*)", "SUM(age)", "AVG(score)", "MIN(id)", "MAX(age)"];
        let col = self.rng.choose(cols);

        let has_where = self.rng.next_bool();
        let has_order = self.rng.next_bool();
        let has_limit = self.rng.next_bool();

        let mut sql = format!("SELECT {} FROM fuzz_table", col);

        if has_where {
            let conditions: &[&str] = &[
                "age > 18",
                "age < 100",
                "score > 0.0",
                "name LIKE 'a%'",
                "id > 0",
                "age IS NOT NULL",
                "score IS NULL",
                "id >= 1 AND id <= 10",
                "age BETWEEN 18 AND 65",
            ];
            let cond = self.rng.choose(conditions);
            sql.push_str(&format!(" WHERE {}", cond));
        }

        if col.starts_with("COUNT") || col.starts_with("SUM") || col.starts_with("AVG")
            || col.starts_with("MIN") || col.starts_with("MAX") {
            // aggregates don't need ORDER BY on the aggregate itself
        } else if has_order {
            let order_cols: &[&str] = &["id", "name", "score", "age", "id DESC", "score ASC"];
            let oc = self.rng.choose(order_cols);
            sql.push_str(&format!(" ORDER BY {}", oc));
        }

        if has_limit {
            let limit = self.rng.next_usize(20) + 1;
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        sql
    }

    fn random_update(&mut self) -> String {
        let id = (self.rng.next_usize(self.row_count.max(1)) + 1) as i64;
        let new_age = self.random_int_value().abs() % 120;
        format!("UPDATE fuzz_table SET age = {} WHERE id = {}", new_age, id)
    }

    fn random_delete(&mut self) -> String {
        let id = (self.rng.next_usize(self.row_count.max(1)) + 1) as i64;
        if self.rng.next_bool() {
            format!("DELETE FROM fuzz_table WHERE id = {}", id)
        } else {
            format!("DELETE FROM fuzz_table WHERE age > {}", self.rng.next_usize(100))
        }
    }

    fn random_dml(&mut self) -> String {
        if self.row_count == 0 {
            return self.random_insert();
        }
        let choice = self.rng.next_usize(10);
        match choice {
            0..=4 => self.random_insert(),
            5..=6 => self.random_update(),
            7 => self.random_delete(),
            _ => self.random_select(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 辅助函数
// ─────────────────────────────────────────────────────────────────────────────

fn open_robotdb(dir: &TempDir) -> Database {
    Database::open(dir.path().join("fuzz.db").to_str().unwrap()).unwrap()
}

fn open_sqlite(dir: &TempDir) -> SqliteConn {
    SqliteConn::open(dir.path().join("fuzz_sqlite.db")).unwrap()
}

fn sqlite_exec(conn: &SqliteConn, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let col_count = stmt.column_count();
    let rows: Vec<Vec<String>> = stmt.query_map([], |row| {
        let mut r = Vec::new();
        for i in 0..col_count {
            let v: rusqlite::types::Value = row.get(i)?;
            r.push(match v {
                rusqlite::types::Value::Null => "NULL".to_string(),
                rusqlite::types::Value::Integer(n) => n.to_string(),
                rusqlite::types::Value::Real(f) => format!("{:.4}", f),
                rusqlite::types::Value::Text(s) => s,
                rusqlite::types::Value::Blob(b) => format!("{:?}", b),
            });
        }
        Ok(r)
    }).map_err(|e| e.to_string())?
    .filter_map(|r| r.ok())
    .collect();
    Ok(rows)
}

fn robotdb_exec(db: &mut Database, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rs = db.query(sql).map_err(|e| e.to_string())?;
    Ok(rs.rows.iter().map(|row| {
        row.iter().map(|v| match v {
            Value::Null => "NULL".to_string(),
            Value::Integer(n) => n.to_string(),
            Value::Real(f) => format!("{:.4}", f),
            Value::Text(s) => s.clone(),
            Value::Boolean(b) => if *b { "1".to_string() } else { "0".to_string() },
            Value::Blob(b) => format!("{:?}", b),
        }).collect()
    }).collect())
}

fn normalize_rows(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 1：崩溃检测模糊测试
// 向 RobotDB 发送随机 SQL，任何 panic 都是 Bug
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn fuzz_crash_detection_random_sql() {
    let dir = TempDir::new().unwrap();
    let mut db = open_robotdb(&dir);
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    // 插入一些初始数据
    for i in 1..=10i64 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 'v{}')", i, i)).unwrap();
    }

    let mut gen = SqlGenerator::new(42);
    let mut panics = 0;

    // 发送 500 条随机 SQL，不应该 panic
    for seed in 0..500u64 {
        gen.rng = Lcg::new(seed * 31337 + 42);
        let sql = gen.random_dml();

        // 使用 catch_unwind 检测 panic
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let dir2 = TempDir::new().unwrap();
            let mut db2 = open_robotdb(&dir2);
            let _ = db2.execute("CREATE TABLE fuzz_table (id INTEGER PRIMARY KEY, name TEXT, score REAL, age INTEGER)");
            for i in 1..=5i64 {
                let _ = db2.execute(&format!("INSERT INTO fuzz_table VALUES ({}, 'name{}', {}.0, {})", i, i, i, i * 10));
            }
            let _ = db2.query(&sql);
        }));

        if result.is_err() {
            panics += 1;
            eprintln!("PANIC on SQL: {}", sql);
        }
    }

    assert_eq!(panics, 0, "{} panics detected in crash detection fuzzing", panics);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 2：变异模糊测试（Mutation Fuzzing）
// 对已知合法 SQL 进行字符级变异，检测解析器健壮性
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn fuzz_mutation_parser_robustness() {
    let valid_sqls = vec![
        "SELECT * FROM t WHERE id = 1",
        "SELECT id, name FROM t ORDER BY id DESC LIMIT 10",
        "INSERT INTO t VALUES (1, 'hello')",
        "UPDATE t SET val = 'world' WHERE id = 1",
        "DELETE FROM t WHERE id > 5",
        "SELECT COUNT(*), SUM(id) FROM t GROUP BY val",
        "BEGIN; INSERT INTO t VALUES (99, 'tx'); COMMIT",
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, x TEXT NOT NULL)",
        "SELECT * FROM t WHERE val LIKE 'h%' AND id BETWEEN 1 AND 10",
        "SELECT id FROM t WHERE val IS NULL OR val IS NOT NULL",
    ];

    let mutations: Vec<Box<dyn Fn(&str, &mut Lcg) -> String>> = vec![
        // 截断
        Box::new(|s: &str, rng: &mut Lcg| {
            let len = rng.next_usize(s.len().max(1));
            s[..len].to_string()
        }),
        // 随机字符替换
        Box::new(|s: &str, rng: &mut Lcg| {
            let mut bytes = s.as_bytes().to_vec();
            if !bytes.is_empty() {
                let pos = rng.next_usize(bytes.len());
                bytes[pos] = (rng.next_u64() % 128) as u8;
            }
            String::from_utf8_lossy(&bytes).to_string()
        }),
        // 插入随机字符
        Box::new(|s: &str, rng: &mut Lcg| {
            let mut chars: Vec<char> = s.chars().collect();
            let pos = rng.next_usize(chars.len().max(1));
            let c = char::from_u32((rng.next_u64() % 95 + 32) as u32).unwrap_or('?');
            chars.insert(pos, c);
            chars.into_iter().collect()
        }),
        // 重复子串
        Box::new(|s: &str, rng: &mut Lcg| {
            if s.len() < 2 { return s.to_string(); }
            let start = rng.next_usize(s.len() / 2);
            let end = start + rng.next_usize(s.len() - start).max(1);
            let end = end.min(s.len());
            format!("{}{}{}", &s[..end], &s[start..end], &s[end..])
        }),
        // SQL 关键字替换
        Box::new(|s: &str, _rng: &mut Lcg| {
            s.replace("SELECT", "SELCT")
             .replace("WHERE", "WHER")
             .replace("FROM", "FRM")
        }),
    ];

    let mut rng = Lcg::new(12345);
    let mut panics = 0;
    let mut total = 0;

    for sql in &valid_sqls {
        for _ in 0..20 {
            let mutation_idx = rng.next_usize(mutations.len());
            let mutated = mutations[mutation_idx](sql, &mut rng);
            total += 1;

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let dir = TempDir::new().unwrap();
                let mut db = open_robotdb(&dir);
                let _ = db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
                for i in 1..=5i64 {
                    let _ = db.execute(&format!("INSERT INTO t VALUES ({}, 'v{}')", i, i));
                }
                let _ = db.query(&mutated);
            }));

            if result.is_err() {
                panics += 1;
                eprintln!("PANIC on mutated SQL: {:?}", mutated);
            }
        }
    }

    assert_eq!(panics, 0,
        "{}/{} mutations caused panics", panics, total);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 3：差分模糊测试（Differential Fuzzing）
// 对随机生成的合法 SQL，比对 RobotDB 与 SQLite 的结果
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn fuzz_differential_random_selects() {
    let dir = TempDir::new().unwrap();
    let mut robotdb = open_robotdb(&dir);
    let sqlite = open_sqlite(&dir);

    // 建表并插入相同数据
    let create = "CREATE TABLE fuzz_table (id INTEGER PRIMARY KEY, name TEXT, score REAL, age INTEGER)";
    robotdb.execute(create).unwrap();
    sqlite.execute(create, []).unwrap();

    let mut gen = SqlGenerator::new(99999);
    // 插入 50 行固定数据
    for i in 1..=50usize {
        let mut g = SqlGenerator::new(i as u64 * 7919);
        let name = g.random_text_value();
        let score = (i as f64) * 1.5;
        let age = (i % 80 + 18) as i64;
        let sql = format!(
            "INSERT INTO fuzz_table VALUES ({}, '{}', {:.4}, {})",
            i, name.replace('\'', "''"), score, age
        );
        robotdb.execute(&sql).unwrap();
        sqlite.execute(&sql, []).unwrap();
    }

    let select_templates = vec![
        "SELECT * FROM fuzz_table ORDER BY id LIMIT 5",
        "SELECT id, age FROM fuzz_table WHERE age > 50 ORDER BY id",
        "SELECT COUNT(*) FROM fuzz_table",
        "SELECT MIN(age), MAX(age) FROM fuzz_table",
        "SELECT SUM(age) FROM fuzz_table WHERE age < 60",
        "SELECT * FROM fuzz_table WHERE id > 10 AND id < 20 ORDER BY id",
        "SELECT * FROM fuzz_table ORDER BY age DESC LIMIT 10",
        "SELECT * FROM fuzz_table WHERE name LIKE 'a%' ORDER BY id",
        "SELECT id, score FROM fuzz_table ORDER BY score DESC LIMIT 5",
        "SELECT * FROM fuzz_table WHERE age BETWEEN 25 AND 45 ORDER BY id",
        "SELECT COUNT(*), AVG(age) FROM fuzz_table",
        "SELECT * FROM fuzz_table WHERE score > 30.0 ORDER BY id LIMIT 10",
        "SELECT * FROM fuzz_table WHERE id IN (1, 5, 10, 15, 20) ORDER BY id",
        "SELECT * FROM fuzz_table ORDER BY id LIMIT 10 OFFSET 5",
        "SELECT * FROM fuzz_table WHERE age IS NOT NULL ORDER BY id LIMIT 5",
    ];

    let mut mismatches = 0;
    let mut mismatch_details = Vec::new();

    for sql in &select_templates {
        let robotdb_result = robotdb_exec(&mut robotdb, sql);
        let sqlite_result = sqlite_exec(&sqlite, sql);

        match (robotdb_result, sqlite_result) {
            (Ok(r), Ok(s)) => {
                let r_norm = normalize_rows(r);
                let s_norm = normalize_rows(s);
                if r_norm != s_norm {
                    mismatches += 1;
                    mismatch_details.push(format!(
                        "SQL: {}\n  RobotDB: {:?}\n  SQLite: {:?}",
                        sql, r_norm, s_norm
                    ));
                }
            }
            (Err(_), Err(_)) => {
                // Both errored - acceptable
            }
            (Ok(r), Err(e)) => {
                mismatches += 1;
                mismatch_details.push(format!(
                    "SQL: {}\n  RobotDB: Ok({:?})\n  SQLite: Err({})",
                    sql, r, e
                ));
            }
            (Err(e), Ok(s)) => {
                mismatches += 1;
                mismatch_details.push(format!(
                    "SQL: {}\n  RobotDB: Err({})\n  SQLite: Ok({:?})",
                    sql, e, s
                ));
            }
        }
    }

    assert_eq!(mismatches, 0,
        "{} differential mismatches:\n{}", mismatches, mismatch_details.join("\n\n"));
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 4：边界值模糊测试
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn fuzz_boundary_values_no_panic() {
    let long_insert = format!("INSERT INTO t VALUES (1, '{}')", "x".repeat(10000));
    let boundary_sqls: Vec<&str> = vec![
        // 空字符串
        "SELECT '' FROM t",
        "INSERT INTO t VALUES (1, '')",
        // 超长字符串
        &long_insert,
        // 特殊字符
        "INSERT INTO t VALUES (2, 'hello\nworld')",
        "INSERT INTO t VALUES (3, 'tab\there')",
        "INSERT INTO t VALUES (4, 'quote''here')",
        // 极端数值
        "INSERT INTO t VALUES (9223372036854775807, 'max_i64')",
        "INSERT INTO t VALUES (-9223372036854775808, 'min_i64')",
        // NULL 值
        "INSERT INTO t VALUES (100, NULL)",
        "SELECT * FROM t WHERE val = NULL",
        "SELECT * FROM t WHERE val IS NULL",
        "SELECT * FROM t WHERE val IS NOT NULL",
        // 嵌套表达式
        "SELECT 1 + 2 * 3 - 4 / 2 FROM t",
        "SELECT (1 + 2) * (3 - 4) FROM t",
        // 空结果集
        "SELECT * FROM t WHERE 1 = 0",
        "SELECT * FROM t WHERE id > 999999999",
        // 重复列名
        "SELECT id, id FROM t",
        // 聚合在空表上
        "SELECT COUNT(*), SUM(id), AVG(id), MIN(id), MAX(id) FROM t WHERE 1 = 0",
        // LIMIT 0
        "SELECT * FROM t LIMIT 0",
        "SELECT * FROM t LIMIT 0 OFFSET 0",
        // 大 OFFSET
        "SELECT * FROM t LIMIT 10 OFFSET 999999",
        // 布尔表达式
        "SELECT * FROM t WHERE 1 = 1",
        "SELECT * FROM t WHERE 0 = 1",
        // 字符串比较
        "SELECT * FROM t WHERE val > 'a' AND val < 'z'",
        // LIKE 边界
        "SELECT * FROM t WHERE val LIKE '%'",
        "SELECT * FROM t WHERE val LIKE ''",
        "SELECT * FROM t WHERE val LIKE '_'",
    ];

    let mut panics = 0;

    for sql in &boundary_sqls {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let dir = TempDir::new().unwrap();
            let mut db = open_robotdb(&dir);
            let _ = db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
            let _ = db.query(sql);
        }));

        if result.is_err() {
            panics += 1;
            eprintln!("PANIC on boundary SQL: {:?}", &sql[..sql.len().min(100)]);
        }
    }

    assert_eq!(panics, 0, "{} boundary value SQLs caused panics", panics);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 5：DDL 模糊测试
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn fuzz_ddl_no_panic() {
    let ddl_sqls = vec![
        // 合法 DDL
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY)",
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE t3 (id INTEGER PRIMARY KEY, val REAL, tag TEXT UNIQUE)",
        // 重复创建
        "CREATE TABLE t4 (id INTEGER PRIMARY KEY)",
        "CREATE TABLE t4 (id INTEGER PRIMARY KEY)",  // duplicate - should error, not panic
        // IF NOT EXISTS
        "CREATE TABLE IF NOT EXISTS t5 (id INTEGER PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS t5 (id INTEGER PRIMARY KEY)",  // no error
        // DROP
        "DROP TABLE t1",
        "DROP TABLE IF EXISTS t1",  // already dropped - no error
        "DROP TABLE IF EXISTS nonexistent",
        // 非法 DDL（应该返回错误，不应该 panic）
        "CREATE TABLE (id INTEGER)",  // missing table name
        "CREATE TABLE t6 ()",         // no columns
        "DROP TABLE",                  // missing table name
        // 多列主键
        "CREATE TABLE t7 (a INTEGER, b TEXT, PRIMARY KEY (a, b))",
        // 各种数据类型
        "CREATE TABLE t8 (a INTEGER, b TEXT, c REAL, d BLOB, e BOOLEAN)",
        // 约束组合
        "CREATE TABLE t9 (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, age INTEGER)",
    ];

    let mut panics = 0;

    for sql in &ddl_sqls {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let dir = TempDir::new().unwrap();
            let mut db = open_robotdb(&dir);
            let _ = db.execute(sql);
        }));

        if result.is_err() {
            panics += 1;
            eprintln!("PANIC on DDL SQL: {}", sql);
        }
    }

    assert_eq!(panics, 0, "{} DDL SQLs caused panics", panics);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 6：事务模糊测试
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn fuzz_transaction_no_panic() {
    let tx_sqls = vec![
        // 正常事务
        vec!["BEGIN", "INSERT INTO t VALUES (1, 'a')", "COMMIT"],
        vec!["BEGIN", "INSERT INTO t VALUES (2, 'b')", "ROLLBACK"],
        // 嵌套 BEGIN（应该报错，不应该 panic）
        vec!["BEGIN", "BEGIN", "COMMIT"],
        // 无事务的 COMMIT（应该报错）
        vec!["COMMIT"],
        // 无事务的 ROLLBACK（应该报错）
        vec!["ROLLBACK"],
        // 事务中的错误
        vec!["BEGIN", "INSERT INTO nonexistent VALUES (1)", "ROLLBACK"],
        // 大事务
        vec!["BEGIN"],
    ];

    let mut panics = 0;

    for sqls in &tx_sqls {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let dir = TempDir::new().unwrap();
            let mut db = open_robotdb(&dir);
            let _ = db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
            for sql in sqls {
                let _ = db.execute(sql);
            }
        }));

        if result.is_err() {
            panics += 1;
            eprintln!("PANIC on transaction sequence: {:?}", sqls);
        }
    }

    assert_eq!(panics, 0, "{} transaction sequences caused panics", panics);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 7：大规模随机操作后数据一致性
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn fuzz_large_scale_consistency() {
    let dir = TempDir::new().unwrap();
    let mut db = open_robotdb(&dir);
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

    // 插入 200 行
    for i in 1..=200i64 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 7)).unwrap();
    }

    // 随机更新 100 行
    let mut rng = Lcg::new(777);
    for _ in 0..100 {
        let id = rng.next_usize(200) as i64 + 1;
        let new_val = rng.next_i64().abs() % 10000;
        db.execute(&format!("UPDATE t SET val = {} WHERE id = {}", new_val, id)).unwrap();
    }

    // 随机删除 50 行
    let mut deleted_ids: HashSet<i64> = HashSet::new();
    for _ in 0..50 {
        let id = rng.next_usize(200) as i64 + 1;
        if deleted_ids.insert(id) {
            let _ = db.execute(&format!("DELETE FROM t WHERE id = {}", id));
        }
    }

    // 验证：COUNT(*) 应该等于 200 - 实际删除的行数
    let rs = db.query("SELECT COUNT(*) FROM t").unwrap();
    let count = match rs.rows.first().and_then(|r| r.first()) {
        Some(Value::Integer(c)) => *c as usize,
        _ => panic!("COUNT(*) returned unexpected value"),
    };

    let expected = 200 - deleted_ids.len();
    assert_eq!(count, expected,
        "Expected {} rows after inserts/updates/deletes, got {}", expected, count);

    // 验证：所有剩余行的 id 都在有效范围内
    let rs = db.query("SELECT id FROM t ORDER BY id").unwrap();
    for row in &rs.rows {
        if let Some(Value::Integer(id)) = row.first() {
            assert!(*id >= 1 && *id <= 200, "Invalid id: {}", id);
            assert!(!deleted_ids.contains(id), "Deleted id {} still present", id);
        }
    }

    // 验证 B-Tree 完整性
    let integrity = db.integrity_check().unwrap();
    assert!(integrity.is_valid(), "B-Tree integrity check failed: {:?}", integrity.violations);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 8：SQL 注入安全性（不应该 panic 或产生意外行为）
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn fuzz_sql_injection_no_panic() {
    let injection_sqls = vec![
        "SELECT * FROM t WHERE val = ''; DROP TABLE t; --'",
        "SELECT * FROM t WHERE val = '1' OR '1'='1'",
        "SELECT * FROM t WHERE val = '1'; SELECT * FROM t; --",
        "INSERT INTO t VALUES (1, 'x'); DELETE FROM t; --')",
        "SELECT * FROM t WHERE id = 1 UNION SELECT * FROM t",
        "SELECT * FROM t WHERE val = '\\x00\\x01\\x02'",
        "SELECT * FROM t WHERE val = '\0'",
        "SELECT 1/0 FROM t",
        "SELECT * FROM t WHERE id = 1/0",
    ];

    let mut panics = 0;

    for sql in &injection_sqls {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let dir = TempDir::new().unwrap();
            let mut db = open_robotdb(&dir);
            let _ = db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
            for i in 1..=5i64 {
                let _ = db.execute(&format!("INSERT INTO t VALUES ({}, 'v{}')", i, i));
            }
            let _ = db.query(sql);
        }));

        if result.is_err() {
            panics += 1;
            eprintln!("PANIC on injection SQL: {}", sql);
        }
    }

    assert_eq!(panics, 0, "{} injection SQLs caused panics", panics);
}
