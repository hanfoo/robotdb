/// 差分测试核心引擎
///
/// 设计原则：
/// - 以 SQLite 3.x 为"黄金标准"（oracle）
/// - 对每条 SQL 语句，同时在 SQLite 和 RobotDB 上执行
/// - 将两者的结果规范化后逐行比对
/// - 任何语义差异都会导致测试失败，并输出详细的 diff 报告

use robotdb::Database;
use rusqlite::{Connection as SqliteConn, params};
use tempfile::TempDir;
use std::collections::HashSet;

// ─────────────────────────────────────────────────────────────────────────────
// 核心数据结构
// ─────────────────────────────────────────────────────────────────────────────

/// 规范化后的单个单元格值
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum NormalizedValue {
    Null,
    Integer(i64),
    /// 浮点数规范化为字符串（保留 6 位有效数字，避免精度差异）
    Real(String),
    Text(String),
    Blob(Vec<u8>),
}

impl std::fmt::Display for NormalizedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NormalizedValue::Null => write!(f, "NULL"),
            NormalizedValue::Integer(n) => write!(f, "{}", n),
            NormalizedValue::Real(s) => write!(f, "{}", s),
            NormalizedValue::Text(s) => write!(f, "\"{}\"", s),
            NormalizedValue::Blob(b) => write!(f, "<BLOB:{}>", hex::encode_upper(b)),
        }
    }
}

/// 规范化后的一行数据
pub type NormalizedRow = Vec<NormalizedValue>;

/// 规范化后的结果集
#[derive(Debug, Clone)]
pub struct NormalizedResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<NormalizedRow>,
}

impl NormalizedResultSet {
    /// 对行进行排序（用于无序比较）
    pub fn sorted(&self) -> Vec<NormalizedRow> {
        let mut rows = self.rows.clone();
        rows.sort();
        rows
    }
}

/// 单条 SQL 的执行结果
#[derive(Debug, Clone)]
pub enum ExecResult {
    /// 查询成功，返回结果集
    Query(NormalizedResultSet),
    /// 非查询语句成功（INSERT/UPDATE/DELETE/DDL）
    Ok { affected_rows: usize },
    /// 执行失败
    Err(String),
}

impl ExecResult {
    pub fn is_err(&self) -> bool {
        matches!(self, ExecResult::Err(_))
    }
    pub fn is_ok(&self) -> bool {
        !self.is_err()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 浮点规范化辅助函数
// ─────────────────────────────────────────────────────────────────────────────

/// 将浮点数规范化为字符串，保留 6 位有效数字
/// 这样可以避免 SQLite 和 RobotDB 在浮点精度上的微小差异
fn normalize_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Inf".to_string() } else { "-Inf".to_string() };
    }
    // 使用 6 位有效数字
    format!("{:.6e}", f)
}

// ─────────────────────────────────────────────────────────────────────────────
// SQLite 适配器
// ─────────────────────────────────────────────────────────────────────────────

/// SQLite 数据库适配器
pub struct SqliteAdapter {
    conn: SqliteConn,
    _dir: TempDir,
}

impl SqliteAdapter {
    pub fn new() -> Self {
        let dir = TempDir::new().expect("Failed to create temp dir for SQLite");
        let db_path = dir.path().join("oracle.db");
        let conn = SqliteConn::open(&db_path).expect("Failed to open SQLite");
        // 开启 WAL 模式，与 RobotDB 行为更一致
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .expect("Failed to set SQLite pragmas");
        SqliteAdapter { conn, _dir: dir }
    }

    /// 执行一条 SQL 语句，返回规范化结果
    pub fn execute(&mut self, sql: &str) -> ExecResult {
        // 判断是否为查询语句
        let sql_upper = sql.trim().to_uppercase();
        let is_query = sql_upper.starts_with("SELECT")
            || sql_upper.starts_with("WITH")
            || sql_upper.starts_with("VALUES")
            || sql_upper.starts_with("EXPLAIN");

        if is_query {
            self.execute_query(sql)
        } else {
            self.execute_dml(sql)
        }
    }

    fn execute_query(&mut self, sql: &str) -> ExecResult {
        let mut stmt = match self.conn.prepare(sql) {
            Ok(s) => s,
            Err(e) => return ExecResult::Err(normalize_error_msg(&e.to_string())),
        };

        let col_names: Vec<String> = stmt.column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let col_count = col_names.len();
        let mut rows = Vec::new();

        let mut result_rows = match stmt.query(params![]) {
            Ok(r) => r,
            Err(e) => return ExecResult::Err(normalize_error_msg(&e.to_string())),
        };

        loop {
            match result_rows.next() {
                Ok(Some(row)) => {
                    let mut norm_row = Vec::with_capacity(col_count);
                    for i in 0..col_count {
                        let val = normalize_sqlite_value(row, i);
                        norm_row.push(val);
                    }
                    rows.push(norm_row);
                }
                Ok(None) => break,
                Err(e) => return ExecResult::Err(normalize_error_msg(&e.to_string())),
            }
        }

        ExecResult::Query(NormalizedResultSet {
            columns: col_names,
            rows,
        })
    }

    fn execute_dml(&mut self, sql: &str) -> ExecResult {
        match self.conn.execute_batch(sql) {
            Ok(_) => ExecResult::Ok { affected_rows: self.conn.changes() as usize },
            Err(e) => ExecResult::Err(normalize_error_msg(&e.to_string())),
        }
    }
}

/// 从 SQLite 行中提取并规范化值
fn normalize_sqlite_value(row: &rusqlite::Row, idx: usize) -> NormalizedValue {
    use rusqlite::types::ValueRef;
    match row.get_ref(idx).unwrap_or(rusqlite::types::ValueRef::Null) {
        ValueRef::Null => NormalizedValue::Null,
        ValueRef::Integer(n) => NormalizedValue::Integer(n),
        ValueRef::Real(f) => NormalizedValue::Real(normalize_float(f)),
        ValueRef::Text(s) => NormalizedValue::Text(
            String::from_utf8_lossy(s).into_owned()
        ),
        ValueRef::Blob(b) => NormalizedValue::Blob(b.to_vec()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RobotDB 适配器
// ─────────────────────────────────────────────────────────────────────────────

/// RobotDB 数据库适配器
pub struct RobotDbAdapter {
    db: Database,
    _dir: TempDir,
}

impl RobotDbAdapter {
    pub fn new() -> Self {
        let dir = TempDir::new().expect("Failed to create temp dir for RobotDB");
        let db_path = dir.path().join("robotdb.db");
        let db = Database::open(&db_path).expect("Failed to open RobotDB");
        RobotDbAdapter { db, _dir: dir }
    }

    /// 执行一条 SQL 语句，返回规范化结果
    pub fn execute(&mut self, sql: &str) -> ExecResult {
        let sql_upper = sql.trim().to_uppercase();
        let is_query = sql_upper.starts_with("SELECT")
            || sql_upper.starts_with("WITH")
            || sql_upper.starts_with("VALUES")
            || sql_upper.starts_with("EXPLAIN");

        if is_query {
            match self.db.query(sql) {
                Ok(rs) => {
                    let columns = rs.columns.clone();
                    let rows = rs.rows.iter().map(|row| {
                        row.iter().map(|v| normalize_robotdb_value(v)).collect()
                    }).collect();
                    ExecResult::Query(NormalizedResultSet { columns, rows })
                }
                Err(e) => ExecResult::Err(normalize_error_msg(&e.to_string())),
            }
        } else {
            match self.db.execute(sql) {
                Ok(_) => ExecResult::Ok { affected_rows: 0 },
                Err(e) => ExecResult::Err(normalize_error_msg(&e.to_string())),
            }
        }
    }
}

/// 将 RobotDB 的 Value 规范化
fn normalize_robotdb_value(v: &robotdb::Value) -> NormalizedValue {
    match v {
        robotdb::Value::Null => NormalizedValue::Null,
        robotdb::Value::Integer(n) => NormalizedValue::Integer(*n),
        robotdb::Value::Real(f) => NormalizedValue::Real(normalize_float(*f)),
        robotdb::Value::Text(s) => NormalizedValue::Text(s.clone()),
        robotdb::Value::Blob(b) => NormalizedValue::Blob(b.clone()),
        robotdb::Value::Boolean(b) => NormalizedValue::Integer(if *b { 1 } else { 0 }),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 错误消息规范化
// ─────────────────────────────────────────────────────────────────────────────

/// 将不同数据库的错误消息规范化为统一的错误类别
/// 避免因错误消息格式不同而导致误报
pub fn normalize_error_msg(msg: &str) -> String {
    let msg_lower = msg.to_lowercase();
    if msg_lower.contains("unique") || msg_lower.contains("duplicate") {
        "ERROR:UNIQUE_CONSTRAINT".to_string()
    } else if msg_lower.contains("already exists") || msg_lower.contains("already exist") {
        // Table/index already exists
        "ERROR:ALREADY_EXISTS".to_string()
    } else if msg_lower.contains("no such column") || msg_lower.contains("column not found")
           || msg_lower.contains("columnnotfound")
           || msg_lower.contains("has no column named")
           || msg_lower.contains("no column named")
           || (msg_lower.contains("not found") && msg_lower.contains("column")) {
        // Check COLUMN_NOT_FOUND before TABLE_NOT_FOUND to avoid false matches
        // (e.g. "Column 'x' not found in table 't'" contains both "column" and "table")
        "ERROR:COLUMN_NOT_FOUND".to_string()
    } else if msg_lower.contains("no such table") || msg_lower.contains("table not found")
           || msg_lower.contains("tablenotfound")
           || (msg_lower.contains("not found") && msg_lower.contains("table")) {
        "ERROR:TABLE_NOT_FOUND".to_string()
    } else if msg_lower.contains("syntax") || msg_lower.contains("parse error") || msg_lower.contains("unexpected token") {
        "ERROR:SYNTAX".to_string()
    } else if msg_lower.contains("type mismatch") || msg_lower.contains("typemismatch") {
        "ERROR:TYPE_MISMATCH".to_string()
    } else if msg_lower.contains("not null") || msg_lower.contains("null constraint")
           || msg_lower.contains("cannot be null") || msg_lower.contains("notnull")
           || msg_lower.contains("null value violation") || msg_lower.contains("nullviolation") {
        "ERROR:NOT_NULL_CONSTRAINT".to_string()
    } else if msg_lower.contains("foreign key") {
        "ERROR:FOREIGN_KEY".to_string()
    } else if msg_lower.contains("division by zero") || msg_lower.contains("divide by zero") {
        "ERROR:DIVISION_BY_ZERO".to_string()
    } else {
        format!("ERROR:OTHER({})", &msg[..msg.len().min(80)])
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 差分比较引擎
// ─────────────────────────────────────────────────────────────────────────────

/// 差分比较选项
#[derive(Debug, Clone)]
pub struct DiffOptions {
    /// 是否忽略行顺序（对于无 ORDER BY 的查询）
    pub ignore_order: bool,
    /// 是否忽略列名大小写
    pub ignore_column_case: bool,
    /// 是否只检查错误类别（不检查具体错误消息）
    pub normalize_errors: bool,
}

impl Default for DiffOptions {
    fn default() -> Self {
        DiffOptions {
            ignore_order: true,
            ignore_column_case: true,
            normalize_errors: true,
        }
    }
}

/// 差分比较结果
#[derive(Debug)]
pub enum DiffResult {
    /// 结果一致
    Match,
    /// 结果不一致
    Mismatch {
        sql: String,
        sqlite_result: String,
        robotdb_result: String,
        detail: String,
    },
    /// 两者都报错，且错误类别相同（可接受）
    BothError { sql: String, error: String },
    /// 一方报错，另一方成功（严重差异）
    OneError {
        sql: String,
        sqlite_result: String,
        robotdb_result: String,
    },
}

impl DiffResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, DiffResult::Match | DiffResult::BothError { .. })
    }
}

/// 比较两个执行结果
pub fn compare_results(
    sql: &str,
    sqlite_res: &ExecResult,
    robotdb_res: &ExecResult,
    opts: &DiffOptions,
) -> DiffResult {
    match (sqlite_res, robotdb_res) {
        // 两者都成功查询
        (ExecResult::Query(sq), ExecResult::Query(rq)) => {
            compare_result_sets(sql, sq, rq, opts)
        }

        // 两者都是 DML 成功
        (ExecResult::Ok { .. }, ExecResult::Ok { .. }) => DiffResult::Match,

        // SQLite 查询成功，RobotDB 也成功但返回 Ok（不应该发生）
        (ExecResult::Query(sq), ExecResult::Ok { .. }) => {
            if sq.rows.is_empty() {
                // 空结果集可以接受
                DiffResult::Match
            } else {
                DiffResult::Mismatch {
                    sql: sql.to_string(),
                    sqlite_result: format_result_set(sq),
                    robotdb_result: "Ok (no rows returned)".to_string(),
                    detail: "SQLite returned rows but RobotDB returned Ok".to_string(),
                }
            }
        }

        // 两者都报错
        (ExecResult::Err(se), ExecResult::Err(re)) => {
            let se_norm = if opts.normalize_errors { normalize_error_msg(se) } else { se.clone() };
            let re_norm = if opts.normalize_errors { normalize_error_msg(re) } else { re.clone() };
            if se_norm == re_norm {
                DiffResult::BothError { sql: sql.to_string(), error: se_norm }
            } else {
                DiffResult::Mismatch {
                    sql: sql.to_string(),
                    sqlite_result: se_norm,
                    robotdb_result: re_norm,
                    detail: "Both errored but with different error categories".to_string(),
                }
            }
        }

        // 一方报错，另一方成功
        _ => {
            DiffResult::OneError {
                sql: sql.to_string(),
                sqlite_result: format_exec_result(sqlite_res),
                robotdb_result: format_exec_result(robotdb_res),
            }
        }
    }
}

/// 比较两个结果集
fn compare_result_sets(
    sql: &str,
    sqlite: &NormalizedResultSet,
    robotdb: &NormalizedResultSet,
    opts: &DiffOptions,
) -> DiffResult {
    // 比较行数
    if sqlite.rows.len() != robotdb.rows.len() {
        return DiffResult::Mismatch {
            sql: sql.to_string(),
            sqlite_result: format_result_set(sqlite),
            robotdb_result: format_result_set(robotdb),
            detail: format!(
                "Row count mismatch: SQLite={}, RobotDB={}",
                sqlite.rows.len(),
                robotdb.rows.len()
            ),
        };
    }

    // 比较列数
    if sqlite.columns.len() != robotdb.columns.len() {
        return DiffResult::Mismatch {
            sql: sql.to_string(),
            sqlite_result: format_result_set(sqlite),
            robotdb_result: format_result_set(robotdb),
            detail: format!(
                "Column count mismatch: SQLite={}, RobotDB={}",
                sqlite.columns.len(),
                robotdb.columns.len()
            ),
        };
    }

    // 比较行内容
    let (sqlite_rows, robotdb_rows) = if opts.ignore_order {
        (sqlite.sorted(), robotdb.sorted())
    } else {
        (sqlite.rows.clone(), robotdb.rows.clone())
    };

    for (i, (sq_row, rq_row)) in sqlite_rows.iter().zip(robotdb_rows.iter()).enumerate() {
        if sq_row != rq_row {
            return DiffResult::Mismatch {
                sql: sql.to_string(),
                sqlite_result: format_result_set(sqlite),
                robotdb_result: format_result_set(robotdb),
                detail: format!(
                    "Row {} mismatch:\n  SQLite: {:?}\n  RobotDB: {:?}",
                    i, sq_row, rq_row
                ),
            };
        }
    }

    DiffResult::Match
}

// ─────────────────────────────────────────────────────────────────────────────
// 格式化辅助函数
// ─────────────────────────────────────────────────────────────────────────────

pub fn format_result_set(rs: &NormalizedResultSet) -> String {
    let mut out = format!("columns: {:?}\n", rs.columns);
    for (i, row) in rs.rows.iter().enumerate() {
        let row_str: Vec<String> = row.iter().map(|v| v.to_string()).collect();
        out.push_str(&format!("  row[{}]: [{}]\n", i, row_str.join(", ")));
    }
    out
}

pub fn format_exec_result(res: &ExecResult) -> String {
    match res {
        ExecResult::Query(rs) => format!("Query({} rows)", rs.rows.len()),
        ExecResult::Ok { affected_rows } => format!("Ok(affected={})", affected_rows),
        ExecResult::Err(e) => format!("Err({})", e),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 差分测试运行器
// ─────────────────────────────────────────────────────────────────────────────

/// 差分测试运行器：同时驱动 SQLite 和 RobotDB
pub struct DiffTestRunner {
    pub sqlite: SqliteAdapter,
    pub robotdb: RobotDbAdapter,
    pub opts: DiffOptions,
    pub results: Vec<DiffResult>,
}

impl DiffTestRunner {
    pub fn new() -> Self {
        DiffTestRunner {
            sqlite: SqliteAdapter::new(),
            robotdb: RobotDbAdapter::new(),
            opts: DiffOptions::default(),
            results: Vec::new(),
        }
    }

    pub fn with_opts(opts: DiffOptions) -> Self {
        DiffTestRunner {
            sqlite: SqliteAdapter::new(),
            robotdb: RobotDbAdapter::new(),
            opts,
            results: Vec::new(),
        }
    }

    /// 执行一条 SQL，比较结果，记录差异
    pub fn run(&mut self, sql: &str) -> &DiffResult {
        let sqlite_res = self.sqlite.execute(sql);
        let robotdb_res = self.robotdb.execute(sql);
        let diff = compare_results(sql, &sqlite_res, &robotdb_res, &self.opts);
        self.results.push(diff);
        self.results.last().unwrap()
    }

    /// 执行一批 SQL，返回所有差异
    pub fn run_batch(&mut self, sqls: &[&str]) {
        for sql in sqls {
            self.run(sql);
        }
    }

    /// 断言所有执行结果一致，否则 panic 并输出详细报告
    pub fn assert_all_match(&self) {
        let failures: Vec<&DiffResult> = self.results.iter()
            .filter(|r| !r.is_ok())
            .collect();

        if failures.is_empty() {
            return;
        }

        let mut report = format!(
            "\n╔══════════════════════════════════════════════════════════════╗\n\
             ║           差分测试失败报告 ({} 个差异)                        \n\
             ╚══════════════════════════════════════════════════════════════╝\n",
            failures.len()
        );

        for (i, failure) in failures.iter().enumerate() {
            report.push_str(&format!("\n─── 差异 #{} ───\n", i + 1));
            match failure {
                DiffResult::Mismatch { sql, sqlite_result, robotdb_result, detail } => {
                    report.push_str(&format!("SQL:    {}\n", sql));
                    report.push_str(&format!("原因:   {}\n", detail));
                    report.push_str(&format!("SQLite: {}\n", sqlite_result));
                    report.push_str(&format!("RobotDB: {}\n", robotdb_result));
                }
                DiffResult::OneError { sql, sqlite_result, robotdb_result } => {
                    report.push_str(&format!("SQL:    {}\n", sql));
                    report.push_str("原因:   一方报错，另一方成功（严重差异）\n");
                    report.push_str(&format!("SQLite: {}\n", sqlite_result));
                    report.push_str(&format!("RobotDB: {}\n", robotdb_result));
                }
                _ => {}
            }
        }

        panic!("{}", report);
    }

    /// 获取测试摘要
    pub fn summary(&self) -> TestSummary {
        let total = self.results.len();
        let matched = self.results.iter().filter(|r| matches!(r, DiffResult::Match)).count();
        let both_err = self.results.iter().filter(|r| matches!(r, DiffResult::BothError { .. })).count();
        let mismatched = self.results.iter().filter(|r| matches!(r, DiffResult::Mismatch { .. })).count();
        let one_err = self.results.iter().filter(|r| matches!(r, DiffResult::OneError { .. })).count();
        TestSummary { total, matched, both_err, mismatched, one_err }
    }
}

/// 测试摘要统计
#[derive(Debug)]
pub struct TestSummary {
    pub total: usize,
    pub matched: usize,
    pub both_err: usize,
    pub mismatched: usize,
    pub one_err: usize,
}

impl TestSummary {
    pub fn pass_count(&self) -> usize {
        self.matched + self.both_err
    }
    pub fn fail_count(&self) -> usize {
        self.mismatched + self.one_err
    }
    pub fn pass_rate(&self) -> f64 {
        if self.total == 0 { 1.0 } else { self.pass_count() as f64 / self.total as f64 }
    }
}

impl std::fmt::Display for TestSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,
            "总计: {} | 一致: {} | 双方报错: {} | 不一致: {} | 单方报错: {} | 通过率: {:.1}%",
            self.total, self.matched, self.both_err,
            self.mismatched, self.one_err,
            self.pass_rate() * 100.0
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 便捷宏
// ─────────────────────────────────────────────────────────────────────────────

/// 差分测试便捷宏：执行 SQL 并断言结果一致
#[macro_export]
macro_rules! diff_assert {
    ($runner:expr, $sql:expr) => {{
        let result = $runner.run($sql);
        if !result.is_ok() {
            match result {
                DiffResult::Mismatch { sql, sqlite_result, robotdb_result, detail } => {
                    panic!(
                        "差分测试失败!\nSQL: {}\n原因: {}\nSQLite: {}\nRobotDB: {}",
                        sql, detail, sqlite_result, robotdb_result
                    );
                }
                DiffResult::OneError { sql, sqlite_result, robotdb_result } => {
                    panic!(
                        "差分测试失败（单方报错）!\nSQL: {}\nSQLite: {}\nRobotDB: {}",
                        sql, sqlite_result, robotdb_result
                    );
                }
                _ => {}
            }
        }
    }};
}

// hex 编码辅助（用于 BLOB 显示）
mod hex {
    pub fn encode_upper(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02X}", b)).collect()
    }
}
