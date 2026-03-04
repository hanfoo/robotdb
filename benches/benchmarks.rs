use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use robotdb::{Database, Value};
use rusqlite::Connection;
use tempfile::tempdir;

fn robotdb_empty() -> (tempfile::TempDir, Database) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bench.db");
    let mut db = Database::open(&path).unwrap();
    db.execute("PRAGMA synchronous = NORMAL").unwrap();
    db.execute(
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, score REAL)",
    )
    .unwrap();
    (dir, db)
}

fn robotdb_prefilled(n: usize) -> (tempfile::TempDir, Database) {
    let (dir, mut db) = robotdb_empty();
    db.execute("BEGIN").unwrap();
    for i in 0..n {
        db.execute(&format!(
            "INSERT INTO bench VALUES ({}, 'name{}', {}, {})",
            i, i, i * 10, i as f64 * 1.5
        ))
        .unwrap();
    }
    db.execute("COMMIT").unwrap();
    (dir, db)
}

fn sqlite_empty() -> (tempfile::TempDir, Connection) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bench.sqlite");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, score REAL);",
    )
    .unwrap();
    (dir, conn)
}

fn sqlite_prefilled(n: usize) -> (tempfile::TempDir, Connection) {
    let (dir, conn) = sqlite_empty();
    conn.execute_batch("BEGIN").unwrap();
    for i in 0..n {
        conn.execute(
            "INSERT INTO bench VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![i as i64, format!("name{}", i), (i * 10) as i64, i as f64 * 1.5],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
    (dir, conn)
}

fn bench_single_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_insert");
    group.throughput(Throughput::Elements(1));
    group.bench_function("RobotDB", |b| {
        let (_dir, mut db) = robotdb_empty();
        let mut id = 0i64;
        b.iter(|| {
            db.execute(&format!(
                "INSERT INTO bench VALUES ({}, 'name{}', {}, {})",
                id, id, id * 10, id as f64 * 1.5
            ))
            .unwrap();
            id += 1;
        });
    });
    group.bench_function("SQLite", |b| {
        let (_dir, conn) = sqlite_empty();
        let mut id = 0i64;
        b.iter(|| {
            conn.execute(
                "INSERT INTO bench VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![id, format!("name{}", id), id * 10, id as f64 * 1.5],
            )
            .unwrap();
            id += 1;
        });
    });
    group.finish();
}

fn bench_bulk_insert(c: &mut Criterion) {
    const ROWS: usize = 1000;
    let mut group = c.benchmark_group("bulk_insert_1000");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("RobotDB", |b| {
        b.iter_batched(
            robotdb_empty,
            |(_dir, mut db)| {
                let stmt = db.prepare("INSERT INTO bench VALUES (?1, ?2, ?3, ?4)").unwrap();
                db.execute("BEGIN").unwrap();
                for i in 0..ROWS {
                    db.execute_prepared(&stmt, &[
                        Value::Integer(i as i64),
                        Value::Text(format!("name{}", i)),
                        Value::Integer((i * 10) as i64),
                        Value::Real(i as f64 * 1.5),
                    ]).unwrap();
                }
                db.execute("COMMIT").unwrap();
                // Return db and _dir so their Drop runs AFTER criterion's timer stops.
                // Database::drop calls flush_all() which does fsync — that must not
                // be included in the measurement.
                (_dir, db)
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.bench_function("SQLite", |b| {
        b.iter_batched(
            sqlite_empty,
            |(_dir, conn)| {
                conn.execute_batch("BEGIN").unwrap();
                for i in 0..ROWS {
                    conn.execute(
                        "INSERT INTO bench VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![
                            i as i64,
                            format!("name{}", i),
                            (i * 10) as i64,
                            i as f64 * 1.5
                        ],
                    )
                    .unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
                // Return so Drop runs after timing
                (_dir, conn)
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_point_select(c: &mut Criterion) {
    const ROWS: usize = 10_000;
    let mut group = c.benchmark_group("point_select");
    group.throughput(Throughput::Elements(1));
    group.bench_function("RobotDB", |b| {
        let (_dir, mut db) = robotdb_prefilled(ROWS);
        let mut id = 0usize;
        b.iter(|| {
            let result = db
                .query(&format!("SELECT * FROM bench WHERE id = {}", id % ROWS))
                .unwrap();
            black_box(result);
            id += 1;
        });
    });
    group.bench_function("SQLite", |b| {
        let (_dir, conn) = sqlite_prefilled(ROWS);
        let mut id = 0usize;
        b.iter(|| {
            let mut stmt = conn
                .prepare_cached("SELECT * FROM bench WHERE id = ?1")
                .unwrap();
            let rows: Vec<(i64, String, i64, f64)> = stmt
                .query_map(rusqlite::params![id as i64 % ROWS as i64], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            black_box(rows);
            id += 1;
        });
    });
    group.finish();
}

fn bench_range_scan(c: &mut Criterion) {
    const ROWS: usize = 10_000;
    const RANGE: usize = 500;
    let mut group = c.benchmark_group("range_scan_500");
    group.throughput(Throughput::Elements(RANGE as u64));
    group.bench_function("RobotDB", |b| {
        let (_dir, mut db) = robotdb_prefilled(ROWS);
        let mut offset = 0usize;
        b.iter(|| {
            let lo = (offset * 13) % (ROWS - RANGE);
            let hi = lo + RANGE;
            let result = db
                .query(&format!(
                    "SELECT * FROM bench WHERE id >= {} AND id < {}",
                    lo, hi
                ))
                .unwrap();
            black_box(result);
            offset += 1;
        });
    });
    group.bench_function("SQLite", |b| {
        let (_dir, conn) = sqlite_prefilled(ROWS);
        let mut offset = 0usize;
        b.iter(|| {
            let lo = (offset * 13) % (ROWS - RANGE);
            let hi = lo + RANGE;
            let mut stmt = conn
                .prepare_cached("SELECT * FROM bench WHERE id >= ?1 AND id < ?2")
                .unwrap();
            let rows: Vec<(i64, String, i64, f64)> = stmt
                .query_map(rusqlite::params![lo as i64, hi as i64], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            black_box(rows);
            offset += 1;
        });
    });
    group.finish();
}

fn bench_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_table_scan");
    for &rows in &[1_000usize, 5_000, 10_000] {
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(BenchmarkId::new("RobotDB", rows), &rows, |b, &n| {
            let (_dir, mut db) = robotdb_prefilled(n);
            b.iter(|| {
                let result = db.query("SELECT * FROM bench").unwrap();
                black_box(result);
            });
        });
        group.bench_with_input(BenchmarkId::new("SQLite", rows), &rows, |b, &n| {
            let (_dir, conn) = sqlite_prefilled(n);
            b.iter(|| {
                let mut stmt = conn.prepare_cached("SELECT * FROM bench").unwrap();
                let rows_vec: Vec<(i64, String, i64, f64)> = stmt
                    .query_map([], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect();
                black_box(rows_vec);
            });
        });
    }
    group.finish();
}

fn bench_update(c: &mut Criterion) {
    const ROWS: usize = 5_000;
    let mut group = c.benchmark_group("update_by_pk");
    group.throughput(Throughput::Elements(1));
    group.bench_function("RobotDB", |b| {
        let (_dir, mut db) = robotdb_prefilled(ROWS);
        let mut id = 0usize;
        b.iter(|| {
            db.execute(&format!(
                "UPDATE bench SET value = {} WHERE id = {}",
                id * 99,
                id % ROWS
            ))
            .unwrap();
            id += 1;
        });
    });
    group.bench_function("SQLite", |b| {
        let (_dir, conn) = sqlite_prefilled(ROWS);
        let mut id = 0usize;
        b.iter(|| {
            conn.execute(
                "UPDATE bench SET value = ?1 WHERE id = ?2",
                rusqlite::params![(id * 99) as i64, id as i64 % ROWS as i64],
            )
            .unwrap();
            id += 1;
        });
    });
    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    const ROWS: usize = 1_000;
    let mut group = c.benchmark_group("delete_by_pk");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("RobotDB", |b| {
        b.iter_batched(
            || robotdb_prefilled(ROWS),
            |(_dir, mut db)| {
                for i in 0..ROWS {
                    db.execute(&format!("DELETE FROM bench WHERE id = {}", i))
                        .unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.bench_function("SQLite", |b| {
        b.iter_batched(
            || sqlite_prefilled(ROWS),
            |(_dir, conn)| {
                for i in 0..ROWS {
                    conn.execute(
                        "DELETE FROM bench WHERE id = ?1",
                        rusqlite::params![i as i64],
                    )
                    .unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_aggregates(c: &mut Criterion) {
    const ROWS: usize = 10_000;
    let mut group = c.benchmark_group("aggregates");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("RobotDB/COUNT", |b| {
        let (_dir, mut db) = robotdb_prefilled(ROWS);
        b.iter(|| black_box(db.query("SELECT COUNT(*) FROM bench").unwrap()));
    });
    group.bench_function("SQLite/COUNT", |b| {
        let (_dir, conn) = sqlite_prefilled(ROWS);
        b.iter(|| {
            let n: i64 = conn
                .query_row("SELECT COUNT(*) FROM bench", [], |r| r.get(0))
                .unwrap();
            black_box(n);
        });
    });

    group.bench_function("RobotDB/SUM", |b| {
        let (_dir, mut db) = robotdb_prefilled(ROWS);
        b.iter(|| black_box(db.query("SELECT SUM(value) FROM bench").unwrap()));
    });
    group.bench_function("SQLite/SUM", |b| {
        let (_dir, conn) = sqlite_prefilled(ROWS);
        b.iter(|| {
            let s: i64 = conn
                .query_row("SELECT SUM(value) FROM bench", [], |r| r.get(0))
                .unwrap();
            black_box(s);
        });
    });

    group.bench_function("RobotDB/AVG", |b| {
        let (_dir, mut db) = robotdb_prefilled(ROWS);
        b.iter(|| black_box(db.query("SELECT AVG(score) FROM bench").unwrap()));
    });
    group.bench_function("SQLite/AVG", |b| {
        let (_dir, conn) = sqlite_prefilled(ROWS);
        b.iter(|| {
            let avg: f64 = conn
                .query_row("SELECT AVG(score) FROM bench", [], |r| r.get(0))
                .unwrap();
            black_box(avg);
        });
    });

    // Filtered aggregates: WHERE value > 50000 selects ~half the rows
    group.bench_function("RobotDB/SUM+WHERE", |b| {
        let (_dir, mut db) = robotdb_prefilled(ROWS);
        b.iter(|| black_box(db.query("SELECT SUM(value) FROM bench WHERE value > 50000").unwrap()));
    });
    group.bench_function("SQLite/SUM+WHERE", |b| {
        let (_dir, conn) = sqlite_prefilled(ROWS);
        b.iter(|| {
            let s: i64 = conn
                .query_row("SELECT SUM(value) FROM bench WHERE value > 50000", [], |r| r.get(0))
                .unwrap();
            black_box(s);
        });
    });

    group.bench_function("RobotDB/COUNT+WHERE", |b| {
        let (_dir, mut db) = robotdb_prefilled(ROWS);
        b.iter(|| black_box(db.query("SELECT COUNT(*) FROM bench WHERE value > 50000").unwrap()));
    });
    group.bench_function("SQLite/COUNT+WHERE", |b| {
        let (_dir, conn) = sqlite_prefilled(ROWS);
        b.iter(|| {
            let n: i64 = conn
                .query_row("SELECT COUNT(*) FROM bench WHERE value > 50000", [], |r| r.get(0))
                .unwrap();
            black_box(n);
        });
    });

    group.finish();
}

fn bench_transaction_throughput(c: &mut Criterion) {
    const TXN_COUNT: usize = 100;
    const ROWS_PER_TXN: usize = 10;
    let mut group = c.benchmark_group("transaction_throughput");
    group.throughput(Throughput::Elements((TXN_COUNT * ROWS_PER_TXN) as u64));

    group.bench_function("RobotDB", |b| {
        b.iter_batched(
            robotdb_empty,
            |(_dir, mut db)| {
                for t in 0..TXN_COUNT {
                    db.execute("BEGIN").unwrap();
                    for r in 0..ROWS_PER_TXN {
                        let id = t * ROWS_PER_TXN + r;
                        db.execute(&format!(
                            "INSERT INTO bench VALUES ({}, 'n{}', {}, {})",
                            id, id, id, id as f64
                        ))
                        .unwrap();
                    }
                    db.execute("COMMIT").unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("SQLite", |b| {
        b.iter_batched(
            sqlite_empty,
            |(_dir, conn)| {
                for t in 0..TXN_COUNT {
                    conn.execute_batch("BEGIN").unwrap();
                    for r in 0..ROWS_PER_TXN {
                        let id = t * ROWS_PER_TXN + r;
                        conn.execute(
                            "INSERT INTO bench VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![id as i64, format!("n{}", id), id as i64, id as f64],
                        )
                        .unwrap();
                    }
                    conn.execute_batch("COMMIT").unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_order_by_limit(c: &mut Criterion) {
    const ROWS: usize = 5_000;
    let mut group = c.benchmark_group("order_by_limit_10");
    group.throughput(Throughput::Elements(10));

    group.bench_function("RobotDB", |b| {
        let (_dir, mut db) = robotdb_prefilled(ROWS);
        b.iter(|| {
            let r = db
                .query("SELECT * FROM bench ORDER BY value DESC LIMIT 10")
                .unwrap();
            black_box(r);
        });
    });

    group.bench_function("SQLite", |b| {
        let (_dir, conn) = sqlite_prefilled(ROWS);
        b.iter(|| {
            let mut stmt = conn
                .prepare_cached("SELECT * FROM bench ORDER BY value DESC LIMIT 10")
                .unwrap();
            let rows: Vec<(i64, String, i64, f64)> = stmt
                .query_map([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            black_box(rows);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_insert,
    bench_bulk_insert,
    bench_point_select,
    bench_range_scan,
    bench_full_scan,
    bench_update,
    bench_delete,
    bench_aggregates,
    bench_transaction_throughput,
    bench_order_by_limit,
);
criterion_main!(benches);
