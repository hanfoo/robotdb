/// P2 并发事务测试
///
/// 测试策略：
/// 1. 通过 Arc<Mutex<Database>> 模拟多线程共享单一数据库连接（串行化并发）
/// 2. 验证在高并发场景下，ACID 属性依然成立
/// 3. 测试并发读写的数据一致性
/// 4. 测试并发事务的隔离性（通过互斥锁保证串行化隔离级别）
///
/// 注意：RobotDB 当前使用互斥锁实现串行化隔离（Serializable Isolation），
/// 这是最强的隔离级别，完全避免了脏读、不可重复读和幻读。

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use robotdb::Database;

/// 创建一个线程安全的数据库连接
fn open_shared_db(dir: &TempDir) -> Arc<Mutex<Database>> {
    let path = dir.path().join("concurrent.db");
    let mut db = Database::open(path.to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE accounts (
        id INTEGER PRIMARY KEY,
        name TEXT,
        balance INTEGER
    )").unwrap();
    // 初始化 5 个账户，每个余额 1000
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO accounts VALUES ({}, 'user{}', 1000)", i, i)).unwrap();
    }
    Arc::new(Mutex::new(db))
}

fn total_balance(db: &mut Database) -> i64 {
    let rs = db.query("SELECT SUM(balance) FROM accounts").unwrap();
    match rs.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n,
        _ => 0,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 1：并发转账 — 总余额不变（守恒不变量）
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_concurrent_transfer_balance_conservation() {
    let dir = TempDir::new().unwrap();
    let db = open_shared_db(&dir);

    let initial_total = {
        let mut guard = db.lock().unwrap();
        total_balance(&mut guard)
    };
    assert_eq!(initial_total, 5000, "Initial total balance should be 5000");

    // 启动 10 个线程，每个线程执行 20 次转账
    let num_threads = 10;
    let transfers_per_thread = 20;
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..transfers_per_thread {
                let from_id = ((t * transfers_per_thread + i) % 5) + 1;
                let to_id = (from_id % 5) + 1;
                let amount = 10i64;

                let mut guard = db_clone.lock().unwrap();
                // 执行原子转账事务
                let _ = guard.execute("BEGIN");
                
                // 检查余额是否足够
                let rs = guard.query(&format!(
                    "SELECT balance FROM accounts WHERE id = {}", from_id
                )).unwrap();
                
                let balance = match rs.rows.first().and_then(|r| r.first()) {
                    Some(robotdb::Value::Integer(n)) => *n,
                    _ => 0,
                };

                if balance >= amount {
                    guard.execute(&format!(
                        "UPDATE accounts SET balance = balance - {} WHERE id = {}",
                        amount, from_id
                    )).unwrap();
                    guard.execute(&format!(
                        "UPDATE accounts SET balance = balance + {} WHERE id = {}",
                        amount, to_id
                    )).unwrap();
                    let _ = guard.execute("COMMIT");
                } else {
                    let _ = guard.execute("ROLLBACK");
                }
            }
        });
        handles.push(handle);
    }

    // 等待所有线程完成
    for h in handles {
        h.join().expect("Thread panicked");
    }

    // 验证总余额不变
    let final_total = {
        let mut guard = db.lock().unwrap();
        total_balance(&mut guard)
    };
    assert_eq!(final_total, 5000,
        "Total balance should be conserved after {} concurrent transfers",
        num_threads * transfers_per_thread);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 2：并发插入 — 无重复主键（唯一性不变量）
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_concurrent_insert_no_duplicate_pk() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let db = {
        let mut d = Database::open(path.to_str().unwrap()).unwrap();
        d.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        Arc::new(Mutex::new(d))
    };

    let num_threads = 8;
    let inserts_per_thread = 50;
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let mut success_count = 0usize;
            for i in 0..inserts_per_thread {
                let id = t * inserts_per_thread + i + 1; // unique across threads
                let mut guard = db_clone.lock().unwrap();
                match guard.execute(&format!("INSERT INTO counters VALUES ({}, {})", id, i)) {
                    Ok(_) => success_count += 1,
                    Err(_) => {} // PK conflict expected for some
                }
            }
            success_count
        });
        handles.push(handle);
    }

    let total_inserted: usize = handles.into_iter()
        .map(|h| h.join().expect("Thread panicked"))
        .sum();

    // Verify no duplicates exist
    let mut guard = db.lock().unwrap();
    let rs = guard.query("SELECT COUNT(*) FROM counters").unwrap();
    let count = match rs.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n as usize,
        _ => 0,
    };

    assert_eq!(count, total_inserted,
        "Row count ({}) should equal successful inserts ({})", count, total_inserted);
    assert_eq!(count, num_threads * inserts_per_thread,
        "All {} inserts should succeed (unique IDs)", num_threads * inserts_per_thread);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 3：并发读写 — 读操作不受写操作影响（读一致性）
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_concurrent_read_write_consistency() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let db = {
        let mut d = Database::open(path.to_str().unwrap()).unwrap();
        d.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        for i in 1..=100 {
            d.execute(&format!("INSERT INTO data VALUES ({}, {})", i, i * 10)).unwrap();
        }
        Arc::new(Mutex::new(d))
    };

    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut handles = Vec::new();

    // 5 个写线程：随机更新行
    for t in 0..5usize {
        let db_clone = Arc::clone(&db);
        let errors_clone = Arc::clone(&errors);
        let handle = thread::spawn(move || {
            for i in 0..20 {
                let id = (t * 20 + i) % 100 + 1;
                let new_val = (t * 100 + i) as i64;
                let mut guard = db_clone.lock().unwrap();
                if let Err(e) = guard.execute(&format!(
                    "UPDATE data SET val = {} WHERE id = {}", new_val, id
                )) {
                    errors_clone.lock().unwrap().push(format!("Write error: {}", e));
                }
            }
        });
        handles.push(handle);
    }

    // 5 个读线程：验证 SUM 和 COUNT 的一致性
    for _ in 0..5usize {
        let db_clone = Arc::clone(&db);
        let errors_clone = Arc::clone(&errors);
        let handle = thread::spawn(move || {
            for _ in 0..20 {
                let mut guard = db_clone.lock().unwrap();
                let rs_count = guard.query("SELECT COUNT(*) FROM data").unwrap();
                let count = match rs_count.rows.first().and_then(|r| r.first()) {
                    Some(robotdb::Value::Integer(n)) => *n,
                    _ => -1,
                };
                if count != 100 {
                    errors_clone.lock().unwrap().push(
                        format!("COUNT(*) should be 100, got {}", count)
                    );
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("Thread panicked");
    }

    let errs = errors.lock().unwrap();
    assert!(errs.is_empty(), "Concurrent read/write errors: {:?}", *errs);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 4：并发事务回滚 — 回滚不影响其他已提交数据
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_concurrent_rollback_isolation() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let db = {
        let mut d = Database::open(path.to_str().unwrap()).unwrap();
        d.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT)").unwrap();
        for i in 1..=20 {
            d.execute(&format!("INSERT INTO items VALUES ({}, 'initial')", i)).unwrap();
        }
        Arc::new(Mutex::new(d))
    };

    let mut handles = Vec::new();

    // 10 个线程：一半提交，一半回滚
    for t in 0..10usize {
        let db_clone = Arc::clone(&db);
        let should_commit = t % 2 == 0;
        let handle = thread::spawn(move || {
            let id = t + 1;
            let mut guard = db_clone.lock().unwrap();
            guard.execute("BEGIN").unwrap();
            guard.execute(&format!(
                "UPDATE items SET status = 'modified_by_t{}' WHERE id = {}", t, id
            )).unwrap();
            
            if should_commit {
                guard.execute("COMMIT").unwrap();
            } else {
                guard.execute("ROLLBACK").unwrap();
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // 验证：偶数线程（提交）的行应该被修改，奇数线程（回滚）的行应该保持 'initial'
    let mut guard = db.lock().unwrap();
    for t in 0..10usize {
        let id = t + 1;
        let rs = guard.query(&format!("SELECT status FROM items WHERE id = {}", id)).unwrap();
        let status = match rs.rows.first().and_then(|r| r.first()) {
            Some(robotdb::Value::Text(s)) => s.clone(),
            _ => String::from("MISSING"),
        };
        
        if t % 2 == 0 {
            // 提交的线程
            assert_eq!(status, format!("modified_by_t{}", t),
                "Thread {} committed but status is '{}'", t, status);
        } else {
            // 回滚的线程
            assert_eq!(status, "initial",
                "Thread {} rolled back but status is '{}' (should be 'initial')", t, status);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 5：高并发计数器 — 原子递增（无丢失更新）
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_concurrent_atomic_counter() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let db = {
        let mut d = Database::open(path.to_str().unwrap()).unwrap();
        d.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        d.execute("INSERT INTO counter VALUES (1, 0)").unwrap();
        Arc::new(Mutex::new(d))
    };

    let num_threads = 20;
    let increments_per_thread = 50;
    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for _ in 0..increments_per_thread {
                let mut guard = db_clone.lock().unwrap();
                guard.execute("BEGIN").unwrap();
                guard.execute("UPDATE counter SET val = val + 1 WHERE id = 1").unwrap();
                guard.execute("COMMIT").unwrap();
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("Thread panicked");
    }

    let expected = (num_threads * increments_per_thread) as i64;
    let mut guard = db.lock().unwrap();
    let rs = guard.query("SELECT val FROM counter WHERE id = 1").unwrap();
    let actual = match rs.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n,
        _ => -1,
    };

    assert_eq!(actual, expected,
        "Counter should be {} after {} concurrent increments, got {}",
        expected, num_threads * increments_per_thread, actual);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 6：并发 DDL + DML — 表创建后立即写入
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_concurrent_ddl_dml_ordering() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let db = {
        let d = Database::open(path.to_str().unwrap()).unwrap();
        Arc::new(Mutex::new(d))
    };

    // Thread 1: Create table
    {
        let mut guard = db.lock().unwrap();
        guard.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, msg TEXT)").unwrap();
    }

    // Multiple threads insert concurrently after table creation
    let num_threads = 10;
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let mut guard = db_clone.lock().unwrap();
            guard.execute(&format!(
                "INSERT INTO events VALUES ({}, 'event_from_thread_{}')", t + 1, t
            )).unwrap();
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("Thread panicked");
    }

    let mut guard = db.lock().unwrap();
    let rs = guard.query("SELECT COUNT(*) FROM events").unwrap();
    let count = match rs.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n,
        _ => 0,
    };
    assert_eq!(count, num_threads as i64,
        "All {} concurrent inserts should succeed", num_threads);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 7：并发删除 — 无幻行（DELETE 正确性）
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_concurrent_delete_correctness() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let db = {
        let mut d = Database::open(path.to_str().unwrap()).unwrap();
        d.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, done INTEGER)").unwrap();
        for i in 1..=100 {
            d.execute(&format!("INSERT INTO tasks VALUES ({}, 0)", i)).unwrap();
        }
        Arc::new(Mutex::new(d))
    };

    // 10 个线程，每个线程删除 10 个不重叠的行
    let mut handles = Vec::new();
    for t in 0..10usize {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let start_id = t * 10 + 1;
            let end_id = start_id + 9;
            let mut guard = db_clone.lock().unwrap();
            guard.execute(&format!(
                "DELETE FROM tasks WHERE id >= {} AND id <= {}", start_id, end_id
            )).unwrap();
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("Thread panicked");
    }

    let mut guard = db.lock().unwrap();
    let rs = guard.query("SELECT COUNT(*) FROM tasks").unwrap();
    let remaining = match rs.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n,
        _ => -1,
    };
    assert_eq!(remaining, 0,
        "All 100 rows should be deleted by concurrent threads, {} remain", remaining);
}

// ─────────────────────────────────────────────────────────────────────────────
// 测试 8：并发混合工作负载 — 读写删除同时进行
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_concurrent_mixed_workload() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let db = {
        let mut d = Database::open(path.to_str().unwrap()).unwrap();
        d.execute("CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price INTEGER,
            stock INTEGER
        )").unwrap();
        for i in 1..=50 {
            d.execute(&format!(
                "INSERT INTO products VALUES ({}, 'product_{}', {}, 100)",
                i, i, i * 10
            )).unwrap();
        }
        Arc::new(Mutex::new(d))
    };

    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut handles = Vec::new();

    // 写线程：更新库存
    for t in 0..5usize {
        let db_clone = Arc::clone(&db);
        let errors_clone = Arc::clone(&errors);
        let handle = thread::spawn(move || {
            for i in 0..10 {
                let id = (t * 10 + i) % 50 + 1;
                let mut guard = db_clone.lock().unwrap();
                if let Err(e) = guard.execute(&format!(
                    "UPDATE products SET stock = stock - 1 WHERE id = {} AND stock > 0", id
                )) {
                    errors_clone.lock().unwrap().push(format!("Update error: {}", e));
                }
            }
        });
        handles.push(handle);
    }

    // 读线程：查询总库存
    for _ in 0..3usize {
        let db_clone = Arc::clone(&db);
        let errors_clone = Arc::clone(&errors);
        let handle = thread::spawn(move || {
            for _ in 0..10 {
                let mut guard = db_clone.lock().unwrap();
                let rs = guard.query("SELECT SUM(stock) FROM products").unwrap();
                let total_stock = match rs.rows.first().and_then(|r| r.first()) {
                    Some(robotdb::Value::Integer(n)) => *n,
                    _ => -1,
                };
                if total_stock < 0 {
                    errors_clone.lock().unwrap().push(
                        format!("Negative total stock: {}", total_stock)
                    );
                }
            }
        });
        handles.push(handle);
    }

    // 删除线程：删除价格过低的产品
    for t in 0..2usize {
        let db_clone = Arc::clone(&db);
        let errors_clone = Arc::clone(&errors);
        let handle = thread::spawn(move || {
            let price_threshold = (t + 1) as i64 * 50;
            let mut guard = db_clone.lock().unwrap();
            if let Err(e) = guard.execute(&format!(
                "DELETE FROM products WHERE price < {}", price_threshold
            )) {
                errors_clone.lock().unwrap().push(format!("Delete error: {}", e));
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("Thread panicked");
    }

    let errs = errors.lock().unwrap();
    assert!(errs.is_empty(), "Mixed workload errors: {:?}", *errs);

    // 最终一致性检查：所有剩余行的库存应该 >= 0
    let mut guard = db.lock().unwrap();
    let rs = guard.query("SELECT COUNT(*) FROM products WHERE stock < 0").unwrap();
    let negative_stock_count = match rs.rows.first().and_then(|r| r.first()) {
        Some(robotdb::Value::Integer(n)) => *n,
        _ => -1,
    };
    assert_eq!(negative_stock_count, 0, "No product should have negative stock");
}
