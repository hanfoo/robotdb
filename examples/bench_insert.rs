use std::time::Instant;
use robotdb::{Database, Value};
use tempfile::tempdir;

fn main() {
    const ROWS: usize = 1000;
    const RUNS: usize = 10;

    // Match criterion benchmark setup exactly
    println!("=== Criterion-equivalent benchmark ({} rows × {} runs) ===\n", ROWS, RUNS);
    let mut times = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bench.db");
        let mut db = Database::open(&path).unwrap();
        db.execute("PRAGMA synchronous = NORMAL").unwrap();
        db.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, score REAL)").unwrap();

        let stmt = db.prepare("INSERT INTO bench VALUES (?1, ?2, ?3, ?4)").unwrap();

        let t0 = Instant::now();
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
        let elapsed = t0.elapsed();
        times.push(elapsed);
        // db dropped AFTER timing (matching the criterion fix)
        drop(db);
        drop(dir);
    }
    times.sort();
    println!("Median: {:?} ({:.0} ns/row)", times[RUNS/2], times[RUNS/2].as_nanos() as f64 / ROWS as f64);
    println!("Min:    {:?}", times[0]);
    println!("Max:    {:?}", times[RUNS-1]);
    println!("All: {:?}\n", times);

    // Test with sync=OFF to isolate flush_dirty cost
    println!("=== sync=OFF ===");
    let mut times_off = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bench.db");
        let mut db = Database::open(&path).unwrap();
        db.execute("PRAGMA synchronous = OFF").unwrap();
        db.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, score REAL)").unwrap();
        let stmt = db.prepare("INSERT INTO bench VALUES (?1, ?2, ?3, ?4)").unwrap();

        let t0 = Instant::now();
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
        let elapsed = t0.elapsed();
        times_off.push(elapsed);
        drop(db);
        drop(dir);
    }
    times_off.sort();
    println!("Median: {:?} ({:.0} ns/row)", times_off[RUNS/2], times_off[RUNS/2].as_nanos() as f64 / ROWS as f64);
    println!("Min:    {:?}", times_off[0]);
    println!("flush_dirty cost: {:?}\n", times[RUNS/2].saturating_sub(times_off[RUNS/2]));

    // Test B-tree insert only (pre-built keys/values, no SQL overhead)
    println!("=== Raw B-tree insert (1000 rows) ===");
    {
        use robotdb::Value as V;
        let dir = tempdir().unwrap();
        let path = dir.path().join("btree.db");
        let mut db = Database::open(&path).unwrap();
        db.execute("PRAGMA synchronous = OFF").unwrap();
        db.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, score REAL)").unwrap();

        // Pre-build all keys and values
        let mut keys_values: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(ROWS);
        for i in 0..ROWS {
            let mut key = Vec::with_capacity(16);
            // encode integer key (order-preserving)
            key.push(0x02);
            let n = i as i64;
            let mut bytes = n.to_be_bytes();
            bytes[0] ^= 0x80; // flip sign bit
            key.extend_from_slice(&bytes);

            let row = vec![
                V::Integer(i as i64),
                V::Text(format!("name{}", i)),
                V::Integer((i * 10) as i64),
                V::Real(i as f64 * 1.5),
            ];
            let value = robotdb::catalog::value::serialize_row(&row);
            keys_values.push((key, value));
        }

        // Time just the B-tree inserts
        db.execute("BEGIN").unwrap();
        let t0 = Instant::now();
        // We can't access the B-tree directly from here, so let's skip this test
        // and use the prepared statement path instead
        for (i, (_k, _v)) in keys_values.iter().enumerate() {
            db.execute_prepared(&db.prepare("INSERT INTO bench VALUES (?1, ?2, ?3, ?4)").unwrap(), &[
                Value::Integer(i as i64),
                Value::Text(format!("name{}", i)),
                Value::Integer((i * 10) as i64),
                Value::Real(i as f64 * 1.5),
            ]).unwrap();
        }
        println!("Insert loop: {:?}", t0.elapsed());
        db.execute("COMMIT").unwrap();
    }
}
