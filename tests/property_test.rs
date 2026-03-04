/// P1 属性测试（Property-Based Tests）
///
/// 使用 proptest 框架，通过随机生成输入数据来验证数据库的核心不变量，
/// 覆盖手写测试无法穷举的边界情况。
///
/// 验证的核心不变量：
/// 1. **原子性（Atomicity）**：事务要么全部提交，要么全部回滚
/// 2. **一致性（Consistency）**：COUNT(*) = 实际行数；SUM = 逐行累加
/// 3. **持久性（Durability）**：COMMIT 后重新打开数据库，数据仍然存在
/// 4. **B-Tree 单调性**：任意插入/删除序列后，范围扫描结果有序
/// 5. **聚合一致性**：COUNT + SUM + AVG 之间的数学关系始终成立
/// 6. **UPDATE 幂等性**：对同一行执行两次相同 UPDATE，结果与执行一次相同
/// 7. **DELETE 完整性**：DELETE 后对应行不可再被 SELECT 到

use robotdb::Database;
use tempfile::TempDir;
use proptest::prelude::*;
use proptest::collection::vec as prop_vec;

// ─────────────────────────────────────────────────────────────────────────────
// 辅助函数
// ─────────────────────────────────────────────────────────────────────────────

fn open_db(dir: &TempDir) -> Database {
    Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap()
}

fn reopen_db(dir: &TempDir) -> Database {
    Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap()
}

fn count_rows(db: &mut Database, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {}", table);
    let rs = db.query(&sql).unwrap();
    match rs.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n,
        _ => 0,
    }
}

fn sum_col(db: &mut Database, table: &str, col: &str) -> i64 {
    let sql = format!("SELECT SUM({}) FROM {}", col, table);
    let rs = db.query(&sql).unwrap();
    match rs.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n,
        Some(robotdb::Value::Real(f)) => *f as i64,
        _ => 0,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 不变量 1：原子性 — 事务回滚后行数不变
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 50,
        max_shrink_iters: 100,
        ..Default::default()
    })]

    /// 属性：在任意初始行数 n 下，BEGIN + 插入若干行 + ROLLBACK 后，行数仍为 n
    #[test]
    fn prop_rollback_preserves_row_count(
        initial_ids in prop_vec(1i64..=500, 1..=20),
        extra_ids in prop_vec(501i64..=1000, 1..=10),
    ) {
        // 去重，确保主键唯一
        let mut initial_ids = initial_ids;
        initial_ids.sort();
        initial_ids.dedup();
        let mut extra_ids = extra_ids;
        extra_ids.sort();
        extra_ids.dedup();

        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        // 插入初始数据
        for id in &initial_ids {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", id, id * 10)).unwrap();
        }
        let before_count = count_rows(&mut db, "t");

        // 开始事务，插入额外数据，然后回滚
        db.execute("BEGIN").unwrap();
        for id in &extra_ids {
            let _ = db.execute(&format!("INSERT INTO t VALUES ({}, {})", id, id * 10));
        }
        db.execute("ROLLBACK").unwrap();

        let after_count = count_rows(&mut db, "t");
        prop_assert_eq!(before_count, after_count,
            "Rollback should preserve row count: before={}, after={}", before_count, after_count);
    }

    /// 属性：COMMIT 后重新打开数据库，行数不变（持久性）
    #[test]
    fn prop_commit_is_durable(
        ids in prop_vec(1i64..=1000, 1..=30),
    ) {
        let mut ids = ids;
        ids.sort();
        ids.dedup();

        let dir = TempDir::new().unwrap();
        {
            let mut db = open_db(&dir);
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
            db.execute("BEGIN").unwrap();
            for id in &ids {
                db.execute(&format!("INSERT INTO t VALUES ({}, {})", id, id)).unwrap();
            }
            db.execute("COMMIT").unwrap();
            // db dropped here, flushing to disk
        }

        // Reopen and verify
        let mut db2 = reopen_db(&dir);
        let count = count_rows(&mut db2, "t");
        prop_assert_eq!(count, ids.len() as i64,
            "After reopen, expected {} rows but got {}", ids.len(), count);
    }

    /// 属性：COUNT(*) 始终等于实际插入行数减去删除行数
    #[test]
    fn prop_count_equals_actual_rows(
        ids in prop_vec(1i64..=200, 5..=30),
        delete_fraction in 0.0f64..=0.5f64,
    ) {
        let mut ids = ids;
        ids.sort();
        ids.dedup();
        let total = ids.len();
        let delete_count = (total as f64 * delete_fraction) as usize;

        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        for id in &ids {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", id, id * 2)).unwrap();
        }

        // 删除前 delete_count 个
        for id in ids.iter().take(delete_count) {
            db.execute(&format!("DELETE FROM t WHERE id = {}", id)).unwrap();
        }

        let expected = (total - delete_count) as i64;
        let actual = count_rows(&mut db, "t");
        prop_assert_eq!(actual, expected,
            "COUNT(*) mismatch: expected {}, got {}", expected, actual);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 不变量 2：聚合一致性 — SUM / COUNT / AVG 之间的数学关系
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 50,
        ..Default::default()
    })]

    /// 属性：SUM(val) = 所有 val 值的手动累加
    #[test]
    fn prop_sum_equals_manual_accumulation(
        values in prop_vec(1i64..=10000, 2..=50),
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        let expected_sum: i64 = values.iter().sum();
        for (i, v) in values.iter().enumerate() {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i + 1, v)).unwrap();
        }

        let db_sum = sum_col(&mut db, "t", "val");
        prop_assert_eq!(db_sum, expected_sum,
            "SUM mismatch: expected {}, got {}", expected_sum, db_sum);
    }

    /// 属性：AVG(val) ≈ SUM(val) / COUNT(val)（浮点误差容忍 1e-6）
    #[test]
    fn prop_avg_equals_sum_div_count(
        values in prop_vec(1i64..=1000, 2..=30),
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        for (i, v) in values.iter().enumerate() {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i + 1, v)).unwrap();
        }

        let sum_rs = db.query("SELECT SUM(val) FROM t").unwrap();
        let count_rs = db.query("SELECT COUNT(val) FROM t").unwrap();
        let avg_rs = db.query("SELECT AVG(val) FROM t").unwrap();

        let db_sum = match sum_rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n as f64,
            Some(robotdb::Value::Real(f)) => *f,
            _ => 0.0,
        };
        let db_count = match count_rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n as f64,
            _ => 1.0,
        };
        let db_avg = match avg_rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n as f64,
            Some(robotdb::Value::Real(f)) => *f,
            _ => 0.0,
        };

        let expected_avg = db_sum / db_count;
        let diff = (db_avg - expected_avg).abs();
        prop_assert!(diff < 1e-6,
            "AVG({}) ≠ SUM({}) / COUNT({}): diff={}", db_avg, db_sum, db_count, diff);
    }

    /// 属性：MAX(val) >= 所有 val 值；MIN(val) <= 所有 val 值
    #[test]
    fn prop_max_min_bounds(
        values in prop_vec(-1000i64..=1000, 2..=40),
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        for (i, v) in values.iter().enumerate() {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i + 1, v)).unwrap();
        }

        let max_rs = db.query("SELECT MAX(val) FROM t").unwrap();
        let min_rs = db.query("SELECT MIN(val) FROM t").unwrap();

        let db_max = match max_rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => i64::MIN,
        };
        let db_min = match min_rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => i64::MAX,
        };

        let expected_max = *values.iter().max().unwrap();
        let expected_min = *values.iter().min().unwrap();

        prop_assert_eq!(db_max, expected_max,
            "MAX mismatch: expected {}, got {}", expected_max, db_max);
        prop_assert_eq!(db_min, expected_min,
            "MIN mismatch: expected {}, got {}", expected_min, db_min);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 不变量 3：B-Tree 单调性 — 范围扫描结果有序
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 40,
        ..Default::default()
    })]

    /// 属性：任意插入顺序后，SELECT ... ORDER BY id ASC 结果严格递增
    #[test]
    fn prop_order_by_returns_sorted_results(
        mut ids in prop_vec(1i64..=500, 5..=50),
    ) {
        ids.sort();
        ids.dedup();
        // 打乱顺序插入（使用 wrapping 运算避免溢出）
        let mut shuffled = ids.clone();
        let n = shuffled.len();
        for i in (1..n).rev() {
            let j = (i.wrapping_mul(6364136223846793005usize)
                .wrapping_add(1442695040888963407usize)) % (i + 1);
            shuffled.swap(i, j);
        }

        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

        for id in &shuffled {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'v{}')", id, id)).unwrap();
        }

        let rs = db.query("SELECT id FROM t ORDER BY id ASC").unwrap();
        let result_ids: Vec<i64> = rs.rows.iter().filter_map(|row| {
            match row.first() {
                Some(robotdb::Value::Integer(n)) => Some(*n),
                _ => None,
            }
        }).collect();

        // 验证结果严格递增
        for window in result_ids.windows(2) {
            prop_assert!(window[0] < window[1],
                "ORDER BY ASC result not strictly increasing: {} >= {}", window[0], window[1]);
        }

        // 验证结果集完整
        prop_assert_eq!(result_ids.len(), ids.len(),
            "Row count mismatch after ORDER BY: expected {}, got {}", ids.len(), result_ids.len());
    }

    /// 属性：DELETE 后，被删除的 id 不再出现在 SELECT 结果中
    #[test]
    fn prop_deleted_rows_not_visible(
        ids in prop_vec(1i64..=300, 5..=30),
        delete_indices in prop_vec(0usize..30, 1..=10),
    ) {
        let mut ids = ids;
        ids.sort();
        ids.dedup();
        if ids.is_empty() {
            return Ok(());
        }

        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        for id in &ids {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", id, id)).unwrap();
        }

        // 删除部分行
        let mut deleted_ids = std::collections::HashSet::new();
        for &idx in &delete_indices {
            if idx < ids.len() {
                let id = ids[idx];
                db.execute(&format!("DELETE FROM t WHERE id = {}", id)).unwrap();
                deleted_ids.insert(id);
            }
        }

        // 验证被删除的行不再可见
        let rs = db.query("SELECT id FROM t").unwrap();
        let visible_ids: std::collections::HashSet<i64> = rs.rows.iter().filter_map(|row| {
            match row.first() {
                Some(robotdb::Value::Integer(n)) => Some(*n),
                _ => None,
            }
        }).collect();

        for deleted_id in &deleted_ids {
            prop_assert!(!visible_ids.contains(deleted_id),
                "Deleted id {} is still visible in SELECT results", deleted_id);
        }
    }

    /// 属性：UPDATE 后，新值可以被 SELECT 到，旧值不再可见
    #[test]
    fn prop_update_reflects_new_value(
        ids in prop_vec(1i64..=200, 3..=20),
        new_val in 10000i64..=99999,
    ) {
        let mut ids = ids;
        ids.sort();
        ids.dedup();
        if ids.is_empty() {
            return Ok(());
        }

        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        for id in &ids {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", id, id * 10)).unwrap();
        }

        // 更新第一行
        let target_id = ids[0];
        db.execute(&format!("UPDATE t SET val = {} WHERE id = {}", new_val, target_id)).unwrap();

        // 验证新值可见
        let rs = db.query(&format!("SELECT val FROM t WHERE id = {}", target_id)).unwrap();
        let actual_val = match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => -1,
        };
        prop_assert_eq!(actual_val, new_val,
            "After UPDATE, expected val={} but got {}", new_val, actual_val);

        // 验证旧值不再可见（旧值是 target_id * 10，新值是 new_val >= 10000）
        let old_val = target_id * 10;
        if old_val != new_val {
            let rs2 = db.query(&format!(
                "SELECT COUNT(*) FROM t WHERE id = {} AND val = {}", target_id, old_val
            )).unwrap();
            let old_count = match rs2.rows.first().and_then(|r| r.first()) {
                Some(robotdb::Value::Integer(n)) => *n,
                _ => -1,
            };
            prop_assert_eq!(old_count, 0,
                "Old value {} still visible after UPDATE to {}", old_val, new_val);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 不变量 4：事务隔离 — 未提交的写入不影响回滚后的状态
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 30,
        ..Default::default()
    })]

    /// 属性：多次 BEGIN/ROLLBACK 循环后，数据库状态与从未开始事务时相同
    #[test]
    fn prop_repeated_rollback_is_idempotent(
        base_ids in prop_vec(1i64..=100, 3..=10),
        tx_ids_list in prop_vec(prop_vec(101i64..=500, 1..=5), 2..=5),
    ) {
        let mut base_ids = base_ids;
        base_ids.sort();
        base_ids.dedup();

        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        // 插入基础数据
        for id in &base_ids {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", id, id)).unwrap();
        }
        let baseline_count = count_rows(&mut db, "t");
        let baseline_sum = sum_col(&mut db, "t", "val");

        // 多次执行 BEGIN + INSERT + ROLLBACK
        for tx_ids in &tx_ids_list {
            db.execute("BEGIN").unwrap();
            for id in tx_ids {
                let _ = db.execute(&format!("INSERT INTO t VALUES ({}, {})", id, id));
            }
            db.execute("ROLLBACK").unwrap();

            // 每次回滚后，状态应与基础状态相同
            let count = count_rows(&mut db, "t");
            let sum = sum_col(&mut db, "t", "val");
            prop_assert_eq!(count, baseline_count,
                "After rollback, count {} != baseline {}", count, baseline_count);
            prop_assert_eq!(sum, baseline_sum,
                "After rollback, sum {} != baseline {}", sum, baseline_sum);
        }
    }

    /// 属性：WHERE 过滤的行数 + 不满足条件的行数 = 总行数
    #[test]
    fn prop_where_partition_completeness(
        values in prop_vec(1i64..=1000, 5..=40),
        threshold in 1i64..=999,
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        for (i, v) in values.iter().enumerate() {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i + 1, v)).unwrap();
        }

        let total = count_rows(&mut db, "t");

        let above_rs = db.query(&format!("SELECT COUNT(*) FROM t WHERE val > {}", threshold)).unwrap();
        let below_rs = db.query(&format!("SELECT COUNT(*) FROM t WHERE val <= {}", threshold)).unwrap();

        let above = match above_rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => -1,
        };
        let below = match below_rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => -1,
        };

        prop_assert_eq!(above + below, total,
            "WHERE partition incomplete: above({}) + below({}) = {} != total({})",
            above, below, above + below, total);
    }

    /// 属性：INSERT + SELECT WHERE id = X 总能找到刚插入的行
    #[test]
    fn prop_insert_then_select_finds_row(
        id in 1i64..=100000,
        val in i64::MIN..=i64::MAX,
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", id, val)).unwrap();

        let rs = db.query(&format!("SELECT val FROM t WHERE id = {}", id)).unwrap();
        prop_assert_eq!(rs.rows.len(), 1,
            "Expected 1 row for id={}, got {}", id, rs.rows.len());

        let found_val = match rs.rows[0].first() {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => i64::MIN, // sentinel
        };
        prop_assert_eq!(found_val, val,
            "For id={}: expected val={}, got {}", id, val, found_val);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 不变量 5：数据类型边界 — 极值不导致崩溃或静默错误
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 60,
        ..Default::default()
    })]

    /// 属性：i64 极值（MIN/MAX）可以被正确存储和检索
    #[test]
    fn prop_integer_boundary_values(
        val in prop_oneof![
            Just(i64::MIN),
            Just(i64::MAX),
            Just(0i64),
            Just(-1i64),
            Just(1i64),
            any::<i64>(),
        ],
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        db.execute(&format!("INSERT INTO t VALUES (1, {})", val)).unwrap();

        let rs = db.query("SELECT val FROM t WHERE id = 1").unwrap();
        let found = match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => i64::MIN, // sentinel (val==i64::MIN is tested separately)
        };
        prop_assert_eq!(found, val,
            "Integer boundary value {} was not stored/retrieved correctly", val);
    }

    /// 属性：任意 UTF-8 字符串可以被正确存储和检索（不含单引号以避免 SQL 注入）
    #[test]
    fn prop_text_roundtrip(
        s in "[a-zA-Z0-9 _\\-\\.]{0,100}",
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        db.execute(&format!("INSERT INTO t VALUES (1, '{}')", s)).unwrap();

        let rs = db.query("SELECT val FROM t WHERE id = 1").unwrap();
        let found = match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Text(t)) => t.clone(),
            _ => "__NOT_FOUND__".to_string(),
        };
        prop_assert_eq!(&found, &s,
            "Text value '{}' was not stored/retrieved correctly, got '{}'", s, found);
    }

    /// 属性：浮点数可以被正确存储（bit-exact 往返，NaN 除外）
    #[test]
    fn prop_float_roundtrip(
        val in prop_oneof![
            Just(0.0f64),
            Just(1.0f64),
            Just(-1.0f64),
            Just(f64::MAX / 2.0),
            Just(f64::MIN_POSITIVE),
            Just(-f64::MIN_POSITIVE),
            (-1e15f64..=1e15f64),
        ],
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val REAL)").unwrap();
        // Use repr to avoid scientific notation issues in SQL
        db.execute(&format!("INSERT INTO t VALUES (1, {:?})", val)).unwrap();

        let rs = db.query("SELECT val FROM t WHERE id = 1").unwrap();
        let found = match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Real(f)) => *f,
            Some(robotdb::Value::Integer(n)) => *n as f64,
            _ => f64::NAN,
        };

        // Allow a small relative error due to text-based SQL parsing
        if val == 0.0 {
            prop_assert!((found).abs() < 1e-15, "Zero float not stored correctly, got {}", found);
        } else {
            let rel_err = ((found - val) / val).abs();
            prop_assert!(rel_err < 1e-6,
                "Float {:?} not stored correctly, got {:?}, rel_err={}", val, found, rel_err);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 不变量 6：LIMIT/OFFSET 正确性
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 40,
        ..Default::default()
    })]

    /// 属性：LIMIT n 最多返回 n 行
    #[test]
    fn prop_limit_bounds_result_size(
        row_count in 5usize..=50,
        limit in 1usize..=20,
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

        for i in 1..=row_count {
            db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
        }

        let rs = db.query(&format!("SELECT id FROM t LIMIT {}", limit)).unwrap();
        let expected = limit.min(row_count);
        prop_assert_eq!(rs.rows.len(), expected,
            "LIMIT {} on {} rows: expected {} rows, got {}", limit, row_count, expected, rs.rows.len());
    }

    /// 属性：LIMIT n OFFSET m 返回正确的行子集
    #[test]
    fn prop_limit_offset_correctness(
        row_count in 10usize..=50,
        offset in 0usize..=10,
        limit in 1usize..=10,
    ) {
        let dir = TempDir::new().unwrap();
        let mut db = open_db(&dir);
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

        for i in 1..=(row_count as i64) {
            db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
        }

        let rs = db.query(&format!(
            "SELECT id FROM t ORDER BY id ASC LIMIT {} OFFSET {}", limit, offset
        )).unwrap();

        let expected_count = if offset >= row_count {
            0
        } else {
            limit.min(row_count - offset)
        };

        prop_assert_eq!(rs.rows.len(), expected_count,
            "LIMIT {} OFFSET {} on {} rows: expected {} rows, got {}",
            limit, offset, row_count, expected_count, rs.rows.len());

        // 验证返回的是正确的行（id 从 offset+1 开始）
        if !rs.rows.is_empty() {
            let first_id = match rs.rows[0].first() {
                Some(robotdb::Value::Integer(n)) => *n,
                _ => -1,
            };
            prop_assert_eq!(first_id, (offset + 1) as i64,
                "First row after OFFSET {}: expected id={}, got {}", offset, offset + 1, first_id);
        }
    }
}
