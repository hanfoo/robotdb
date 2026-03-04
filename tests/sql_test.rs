use robotdb::{Database, Result, Value};
use tempfile::tempdir;

/// 辅助函数：初始化内存数据库并执行一系列 SQL
fn setup_db(sqls: &[&str]) -> Result<Database> {
    let mut db = Database::open_in_memory()?;
    for sql in sqls {
        db.execute(sql)?;
    }
    Ok(db)
}

#[test]
fn test_create_table() -> Result<()> {
    let mut db = Database::open_in_memory()?;
    db.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )",
    )?;

    let tables = db.table_names();
    assert_eq!(tables, vec!["users"]);

    let res = db.query("PRAGMA table_info(\'users\')")?;
    assert_eq!(res.rows.len(), 3);
    assert_eq!(res.rows[0][1], Value::Text("id".into()));
    assert_eq!(res.rows[1][1], Value::Text("name".into()));
    assert_eq!(res.rows[2][1], Value::Text("email".into()));

    Ok(())
}

#[test]
fn test_insert_and_select() -> Result<()> {
    let mut db = setup_db(&[
        "CREATE TABLE t1 (a INTEGER, b TEXT)",
        "INSERT INTO t1 VALUES (1, \'one\')",
        "INSERT INTO t1 VALUES (2, \'two\')",
        "INSERT INTO t1 (b, a) VALUES (\'three\', 3)",
    ])?;

    let res = db.query("SELECT * FROM t1 ORDER BY a")?;
    assert_eq!(res.columns, vec!["a", "b"]);
    assert_eq!(res.rows.len(), 3);
    assert_eq!(res.rows[0], vec![Value::Integer(1), Value::Text("one".into())]);
    assert_eq!(res.rows[1], vec![Value::Integer(2), Value::Text("two".into())]);
    assert_eq!(res.rows[2], vec![Value::Integer(3), Value::Text("three".into())]);

    Ok(())
}

#[test]
fn test_select_with_where_clause() -> Result<()> {
    let mut db = setup_db(&[
        "CREATE TABLE t1 (a INTEGER, b REAL, c TEXT)",
        "INSERT INTO t1 VALUES (1, 1.1, \'one\')",
        "INSERT INTO t1 VALUES (2, 2.2, \'two\')",
        "INSERT INTO t1 VALUES (3, 3.3, \'three\')",
        "INSERT INTO t1 VALUES (4, 4.4, \'four\')",
    ])?;

    let res = db.query("SELECT a, c FROM t1 WHERE a > 2 AND c LIKE \'%hree\'")?;
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0], vec![Value::Integer(3), Value::Text("three".into())]);

    let res2 = db.query("SELECT b FROM t1 WHERE a = 4")?;
    assert_eq!(res2.rows.len(), 1);
    assert_eq!(res2.rows[0][0], Value::Real(4.4));

    Ok(())
}

#[test]
fn test_update() -> Result<()> {
    let mut db = setup_db(&[
        "CREATE TABLE t1 (a INTEGER, b TEXT)",
        "INSERT INTO t1 VALUES (1, \'one\')",
        "INSERT INTO t1 VALUES (2, \'two\')",
    ])?;

    let affected = db.execute("UPDATE t1 SET b = \'new_two\' WHERE a = 2")?;
    assert_eq!(affected, 1);

    let res = db.query("SELECT b FROM t1 WHERE a = 2")?;
    assert_eq!(res.rows[0][0], Value::Text("new_two".into()));

    let res2 = db.query("SELECT b FROM t1 WHERE a = 1")?;
    assert_eq!(res2.rows[0][0], Value::Text("one".into()));

    Ok(())
}

#[test]
fn test_delete() -> Result<()> {
    let mut db = setup_db(&[
        "CREATE TABLE t1 (a INTEGER, b TEXT)",
        "INSERT INTO t1 VALUES (1, \'one\')",
        "INSERT INTO t1 VALUES (2, \'two\')",
    ])?;

    let affected = db.execute("DELETE FROM t1 WHERE a = 1")?;
    assert_eq!(affected, 1);

    let res = db.query("SELECT * FROM t1")?;
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], Value::Integer(2));

    Ok(())
}

#[test]
fn test_transaction_commit() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("test.db");
    {
        let mut db = Database::open(&path)?;
        db.execute("CREATE TABLE t1 (a INTEGER)")?;
        db.execute("BEGIN")?;
        db.execute("INSERT INTO t1 VALUES (1)")?;
        db.execute("INSERT INTO t1 VALUES (2)")?;
        db.execute("COMMIT")?;
    }

    {
        let mut db = Database::open(&path)?;
        let res = db.query("SELECT COUNT(*) FROM t1")?;
        assert_eq!(res.rows[0][0], Value::Integer(2));
    }
    Ok(())
}

#[test]
fn test_transaction_rollback() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("test.db");
    {
        let mut db = Database::open(&path)?;
        db.execute("CREATE TABLE t1 (a INTEGER)")?;
        db.execute("INSERT INTO t1 VALUES (100)")?;
        db.execute("BEGIN")?;
        db.execute("INSERT INTO t1 VALUES (1)")?;
        db.execute("INSERT INTO t1 VALUES (2)")?;
        db.execute("ROLLBACK")?;
    }

    {
        let mut db = Database::open(&path)?;
        let res = db.query("SELECT * FROM t1")?;
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0], Value::Integer(100));
    }
    Ok(())
}

#[test]
fn test_auto_rollback_on_error() -> Result<()> {
    let mut db = Database::open_in_memory()?;
    db.execute("CREATE TABLE t1 (a INTEGER UNIQUE)")?;
    db.execute("INSERT INTO t1 VALUES (1)")?;

    // This will fail due to UNIQUE constraint, and should auto-rollback
    let result = db.execute("INSERT INTO t1 VALUES (1)");
    assert!(result.is_err());

    // This should succeed as the failed transaction was rolled back
    db.execute("INSERT INTO t1 VALUES (2)")?;

    let res = db.query("SELECT COUNT(*) FROM t1")?;
    assert_eq!(res.rows[0][0], Value::Integer(2));

    Ok(())
}

#[test]
fn test_aggregation() -> Result<()> {
    let mut db = setup_db(&[
        "CREATE TABLE t1 (a INTEGER, b INTEGER)",
        "INSERT INTO t1 VALUES (1, 10)",
        "INSERT INTO t1 VALUES (1, 20)",
        "INSERT INTO t1 VALUES (2, 30)",
        "INSERT INTO t1 VALUES (2, 40)",
        "INSERT INTO t1 VALUES (3, 50)",
    ])?;

    let res = db.query("SELECT a, COUNT(*), SUM(b), AVG(b), MIN(b), MAX(b) FROM t1 GROUP BY a ORDER BY a")?;
    assert_eq!(res.rows.len(), 3);

    // Group 1
    assert_eq!(res.rows[0][0], Value::Integer(1));
    assert_eq!(res.rows[0][1], Value::Integer(2));
    assert_eq!(res.rows[0][2], Value::Real(30.0));
    assert_eq!(res.rows[0][3], Value::Real(15.0));
    assert_eq!(res.rows[0][4], Value::Integer(10));
    assert_eq!(res.rows[0][5], Value::Integer(20));

    // Group 2
    assert_eq!(res.rows[1][0], Value::Integer(2));
    assert_eq!(res.rows[1][1], Value::Integer(2));
    assert_eq!(res.rows[1][2], Value::Real(70.0));
    assert_eq!(res.rows[1][3], Value::Real(35.0));
    assert_eq!(res.rows[1][4], Value::Integer(30));
    assert_eq!(res.rows[1][5], Value::Integer(40));

    Ok(())
}

#[test]
fn test_negative_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("neg.db");
    let mut db = robotdb::Database::open(path.to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, n INTEGER, f REAL)").unwrap();
    
    // simple negative
    match db.execute("INSERT INTO nums VALUES (1, -100, -1.5)") {
        Ok(_) => println!("simple neg: OK"),
        Err(e) => println!("simple neg FAIL: {:?}", e),
    }
    // i32::MIN
    match db.execute("INSERT INTO nums VALUES (2, -2147483648, 0.0)") {
        Ok(_) => println!("i32::MIN: OK"),
        Err(e) => println!("i32::MIN FAIL: {:?}", e),
    }
    // scientific notation
    match db.execute("INSERT INTO nums VALUES (3, 0, -1.0e10)") {
        Ok(_) => println!("sci notation: OK"),
        Err(e) => println!("sci notation FAIL: {:?}", e),
    }
    let r = db.query("SELECT * FROM nums ORDER BY id").unwrap();
    println!("rows: {:?}", r.rows);
}

#[test]
fn test_i64_min_boundary() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = robotdb::Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    let val = i64::MIN;
    let sql = format!("INSERT INTO t VALUES (1, {})", val);
    println!("SQL: {}", sql);
    db.execute(&sql).unwrap();
    let rs = db.query("SELECT val FROM t WHERE id = 1").unwrap();
    println!("Rows: {:?}", rs.rows);
    assert_eq!(rs.rows.len(), 1);
}

#[test]
fn test_i64_min_detailed() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = robotdb::Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    
    // Test with 0 first
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    let rs = db.query("SELECT val FROM t WHERE id = 1").unwrap();
    println!("val=0: {:?}", rs.rows);
    
    // Test with i64::MIN
    db.execute("INSERT INTO t VALUES (2, -9223372036854775808)").unwrap();
    let rs2 = db.query("SELECT val FROM t WHERE id = 2").unwrap();
    println!("val=i64::MIN: {:?}", rs2.rows);
    
    // Test with -1
    db.execute("INSERT INTO t VALUES (3, -1)").unwrap();
    let rs3 = db.query("SELECT val FROM t WHERE id = 3").unwrap();
    println!("val=-1: {:?}", rs3.rows);
    
    // Scan all
    let rs_all = db.query("SELECT id, val FROM t ORDER BY id").unwrap();
    println!("All rows: {:?}", rs_all.rows);
}

#[test]
fn test_update_no_new_rows() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = robotdb::Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO data VALUES ({}, {})", i, i * 10)).unwrap();
    }
    let rs_before = db.query("SELECT COUNT(*) FROM data").unwrap();
    let count_before = match rs_before.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n,
        _ => -1,
    };
    println!("Count before update: {}", count_before);
    
    // Update all rows
    for i in 1..=10 {
        db.execute(&format!("UPDATE data SET val = {} WHERE id = {}", i * 100, i)).unwrap();
    }
    
    let rs_after = db.query("SELECT COUNT(*) FROM data").unwrap();
    let count_after = match rs_after.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n,
        _ => -1,
    };
    println!("Count after update: {}", count_after);
    assert_eq!(count_before, count_after, "UPDATE should not change row count");
}

#[test]
fn test_count_after_updates() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = robotdb::Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    for i in 1..=100i64 {
        db.execute(&format!("INSERT INTO data VALUES ({}, {})", i, i * 10)).unwrap();
    }
    
    let count_before = {
        let rs = db.query("SELECT COUNT(*) FROM data").unwrap();
        match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => -1,
        }
    };
    println!("Count before: {}", count_before);
    
    // Simulate what the concurrent test does: 5 threads x 20 updates
    for t in 0..5usize {
        for i in 0..20usize {
            let id = (t * 20 + i) % 100 + 1;
            let new_val = (t * 100 + i) as i64;
            db.execute(&format!("UPDATE data SET val = {} WHERE id = {}", new_val, id)).unwrap();
        }
    }
    
    let count_after = {
        let rs = db.query("SELECT COUNT(*) FROM data").unwrap();
        match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => -1,
        }
    };
    println!("Count after: {}", count_after);
    assert_eq!(count_before, count_after, "UPDATE should not change row count");
}

#[test]
fn test_update_single_row_debug() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = robotdb::Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO data VALUES (1, 10)").unwrap();
    
    let count_before = {
        let rs = db.query("SELECT COUNT(*) FROM data").unwrap();
        match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => -1,
        }
    };
    println!("Count before update: {}", count_before);
    
    db.execute("UPDATE data SET val = 100 WHERE id = 1").unwrap();
    
    let count_after = {
        let rs = db.query("SELECT COUNT(*) FROM data").unwrap();
        match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(n)) => *n,
            _ => -1,
        }
    };
    println!("Count after update: {}", count_after);
    
    let rs = db.query("SELECT id, val FROM data").unwrap();
    println!("All rows: {:?}", rs.rows);
    
    assert_eq!(count_before, count_after, "UPDATE should not change row count");
}

#[test]
fn test_update_threshold() {
    for n in [5, 10, 20, 50, 100usize] {
        let dir = tempfile::TempDir::new().unwrap();
        let mut db = robotdb::Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap();
        db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        for i in 1..=n {
            db.execute(&format!("INSERT INTO data VALUES ({}, {})", i, i * 10)).unwrap();
        }
        for i in 1..=n {
            db.execute(&format!("UPDATE data SET val = {} WHERE id = {}", i * 100, i)).unwrap();
        }
        let rs = db.query("SELECT COUNT(*) FROM data").unwrap();
        let count = match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(c)) => *c as usize,
            _ => 0,
        };
        println!("n={}: count={}", n, count);
        assert_eq!(count, n, "n={}: expected {} rows, got {}", n, n, count);
    }
}

#[test]
fn test_update_threshold2() {
    for n in [5, 10, 15, 20, 25, 30, 40, 50usize] {
        let dir = tempfile::TempDir::new().unwrap();
        let mut db = robotdb::Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap();
        db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        for i in 1..=n {
            db.execute(&format!("INSERT INTO data VALUES ({}, {})", i, i * 10)).unwrap();
        }
        for i in 1..=n {
            db.execute(&format!("UPDATE data SET val = {} WHERE id = {}", i * 100, i)).unwrap();
        }
        let rs = db.query("SELECT COUNT(*) FROM data").unwrap();
        let count = match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(c)) => *c as usize,
            _ => 0,
        };
        println!("n={}: count={} ({})", n, count, if count == n { "OK" } else { "FAIL" });
    }
}

#[test]
fn test_update_same_rows_multiple_times() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = robotdb::Database::open(dir.path().join("test.db").to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    for i in 1..=100i64 {
        db.execute(&format!("INSERT INTO data VALUES ({}, {})", i, i * 10)).unwrap();
    }
    
    // Update each row 5 times (same pattern as concurrent test)
    for round in 0..5usize {
        for i in 1..=100usize {
            db.execute(&format!("UPDATE data SET val = {} WHERE id = {}", round * 1000 + i, i)).unwrap();
        }
        let rs = db.query("SELECT COUNT(*) FROM data").unwrap();
        let count = match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Integer(c)) => *c,
            _ => -1,
        };
        println!("After round {}: count={}", round, count);
        if count != 100 {
            break;
        }
    }
}

#[test]
fn test_deep_insert_10000() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_deep.db");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, score REAL)").unwrap();
    db.execute("BEGIN").unwrap();
    for i in 0..10000usize {
        db.execute(&format!(
            "INSERT INTO bench VALUES ({}, 'name{}', {}, {})",
            i, i, i * 10, i as f64 * 1.5
        )).unwrap();
    }
    db.execute("COMMIT").unwrap();
    let result = db.query("SELECT COUNT(*) FROM bench").unwrap();
    println!("Count: {:?}", result);
}

// ─────────────────────────────────────────────────────────────────────────────
// PRAGMA synchronous
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_pragma_synchronous_default() -> Result<()> {
    let mut db = Database::open_in_memory()?;
    let res = db.query("PRAGMA synchronous")?;
    assert_eq!(res.columns, vec!["synchronous"]);
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], Value::Integer(2)); // FULL
    Ok(())
}

#[test]
fn test_pragma_synchronous_set_and_get() -> Result<()> {
    let mut db = Database::open_in_memory()?;

    // Set by integer
    db.execute("PRAGMA synchronous = 0")?;
    let res = db.query("PRAGMA synchronous")?;
    assert_eq!(res.rows[0][0], Value::Integer(0));

    db.execute("PRAGMA synchronous = 1")?;
    let res = db.query("PRAGMA synchronous")?;
    assert_eq!(res.rows[0][0], Value::Integer(1));

    db.execute("PRAGMA synchronous = 2")?;
    let res = db.query("PRAGMA synchronous")?;
    assert_eq!(res.rows[0][0], Value::Integer(2));

    // Set by name (case-insensitive)
    db.execute("PRAGMA synchronous = OFF")?;
    let res = db.query("PRAGMA synchronous")?;
    assert_eq!(res.rows[0][0], Value::Integer(0));

    db.execute("PRAGMA synchronous = normal")?;
    let res = db.query("PRAGMA synchronous")?;
    assert_eq!(res.rows[0][0], Value::Integer(1));

    db.execute("PRAGMA synchronous = Full")?;
    let res = db.query("PRAGMA synchronous")?;
    assert_eq!(res.rows[0][0], Value::Integer(2));

    Ok(())
}

#[test]
fn test_pragma_synchronous_off() -> Result<()> {
    let mut db = Database::open_in_memory()?;
    db.execute("PRAGMA synchronous = OFF")?;
    db.execute("CREATE TABLE t1 (a INTEGER, b TEXT)")?;
    db.execute("INSERT INTO t1 VALUES (1, 'hello')")?;
    db.execute("INSERT INTO t1 VALUES (2, 'world')")?;

    let res = db.query("SELECT * FROM t1 ORDER BY a")?;
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0], Value::Integer(1));
    assert_eq!(res.rows[1][1], Value::Text("world".into()));
    Ok(())
}

#[test]
fn test_pragma_synchronous_normal() -> Result<()> {
    let mut db = Database::open_in_memory()?;
    db.execute("PRAGMA synchronous = NORMAL")?;
    db.execute("CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT)")?;
    db.execute("INSERT INTO t1 VALUES (1, 'alpha')")?;
    db.execute("INSERT INTO t1 VALUES (2, 'beta')")?;

    // Test with explicit transaction
    db.execute("BEGIN")?;
    db.execute("INSERT INTO t1 VALUES (3, 'gamma')")?;
    db.execute("COMMIT")?;

    let res = db.query("SELECT COUNT(*) FROM t1")?;
    assert_eq!(res.rows[0][0], Value::Integer(3));
    Ok(())
}

#[test]
fn test_pragma_synchronous_full_persists() -> Result<()> {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("sync_test.db");

    // Write data with FULL sync mode (default)
    {
        let mut db = Database::open(&db_path)?;
        db.execute("CREATE TABLE t1 (a INTEGER, b TEXT)")?;
        db.execute("INSERT INTO t1 VALUES (1, 'persist')")?;
        db.execute("INSERT INTO t1 VALUES (2, 'me')")?;
        db.close()?;
    }

    // Reopen and verify data survived
    {
        let mut db = Database::open(&db_path)?;
        let res = db.query("SELECT * FROM t1 ORDER BY a")?;
        assert_eq!(res.rows.len(), 2);
        assert_eq!(res.rows[0][1], Value::Text("persist".into()));
        assert_eq!(res.rows[1][1], Value::Text("me".into()));
    }

    Ok(())
}

#[test]
fn test_pragma_synchronous_invalid_value() -> Result<()> {
    let mut db = Database::open_in_memory()?;

    assert!(db.execute("PRAGMA synchronous = 3").is_err());
    assert!(db.execute("PRAGMA synchronous = INVALID").is_err());

    // Verify the mode didn't change from default
    let res = db.query("PRAGMA synchronous")?;
    assert_eq!(res.rows[0][0], Value::Integer(2));

    Ok(())
}
