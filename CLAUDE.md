# RobotDB — Project Guide

## Project Overview

RobotDB is a high-performance embedded relational database written in Rust, inspired by SQLite. It implements a complete SQL database engine with ACID transactions, B+ tree indexing, and crash recovery via a Write-Ahead Log (WAL).

**Goals:** Educational/reference implementation demonstrating how a real database engine works end-to-end — from SQL parsing to disk persistence.

**License:** MIT

---

## Architecture

The system is organized into four main layers:

```
SQL Layer      → lexer.rs → parser.rs → ast.rs → engine.rs / eval.rs
Schema Layer   → schema.rs (in-memory catalog, serialized to disk)
Storage Layer  → buffer.rs (LRU pool) → disk.rs → page.rs
Index Layer    → tree.rs + node.rs (B+ Tree) + integrity.rs
Durability     → wal.rs + manager.rs (WAL + ARIES redo recovery)
Public API     → lib.rs (Database struct)
CLI            → bin/cli.rs (REPL)
```

### Key Structs

- **`Database`** (`lib.rs`) — Primary public API. Owns buffer pool, catalog, and transaction manager.
- **`BufferPool`** (`buffer.rs`) — LRU page cache (default 256 pages). Pin/unpin protocol.
- **`DiskManager`** (`disk.rs`) — File-based page store. 4KB fixed page size.
- **`BPlusTree`** (`tree.rs`) — B+ Tree with leaf chaining for range scans.
- **`TransactionManager`** (`manager.rs`) — ARIES redo-only crash recovery.
- **`Wal`** (`wal.rs`) — Append-only write-ahead log with xxhash3 checksums.
- **`Catalog`** (`schema.rs`) — In-memory schema registry, persisted via bincode.
- **`Executor`** (`engine.rs`) — Volcano/iterator model query execution.

### Page Layout

```
[0..1]    page_type (u8)
[1..5]    page_id   (u32 LE)
[5..9]    checksum  (u32 CRC via xxhash3)
[9..4096] payload
```

Page types: `Header`, `BTreeInternal`, `BTreeLeaf`, `Overflow`, `FreeList`.

### Order-Preserving Key Encoding

B-Tree keys use a byte-comparable encoding so `memcmp` matches semantic ordering:

```
Null:    [0x00]
Boolean: [0x01, 0|1]
Integer: [0x02, big-endian with sign bit flipped]
Real:    [0x03, IEEE 754 order-preserving]
Text:    [0x04, utf8, 0xFF terminator]
Blob:    [0x05, raw bytes, 0xFF terminator]
```

---

## Tech Stack

| Area | Crate |
|---|---|
| Error handling | `thiserror`, `anyhow` |
| Serialization | `serde`, `bincode` |
| Checksums | `xxhash-rust` (xxh3) |
| Concurrency | `parking_lot`, `crossbeam` |
| CLI / REPL | `rustyline`, `dirs` |
| Logging | `log`, `env_logger` |
| Benchmarking | `criterion` |
| Property testing | `proptest` |
| Differential testing | `rusqlite` |
| Randomized tests | `rand`, `tempfile`, `rayon` |

**Rust edition:** 2021. **MSRV:** implied by dependencies.

---

## Common Commands

```bash
# Build
cargo build
cargo build --release

# Test
cargo test
cargo test <test_name>       # run a specific test
cargo test --test sql_test   # run a specific test file

# Benchmarks
cargo bench

# CLI
cargo run --bin robotdb-cli
cargo run --bin robotdb-cli -- path/to/db.robotdb

# Integrity check (in CLI)
PRAGMA integrity_check;
```

---

## Coding Guidelines

### Error Handling
- Define errors in `error.rs` using `thiserror`. Use specific variants — avoid catch-all variants.
- Propagate errors with `?`. Reserve `unwrap()`/`expect()` for truly infallible cases; add a comment explaining why.
- Storage errors should bubble up through the full stack; callers (e.g., `Database::execute`) handle rollback.

### Memory and Ownership
- Pages must be pinned (`buffer.pin()`) before access and unpinned immediately after. Never hold a pin across an `await` or lock boundary.
- `BufferPool` uses `parking_lot::Mutex` — keep critical sections short.
- Prefer stack allocation for small buffers; heap-allocate page-sized buffers.

### B+ Tree
- The tree does **not** rebalance on delete — deleted space is not reclaimed.
- Always use `upsert` for updates to existing keys; `insert` returns an error on duplicates.
- Path-tracking insertion is critical for correctness during cascading splits — do not bypass it.

### Transactions
- Every mutation goes through `tx_manager.before_write_page()` before modifying a page, so the WAL has a before-image for rollback.
- Call `tx_manager.log_dirty_pages()` at commit time to write after-images.
- Snapshots (`BufferSnapshot`) capture the full buffer state at `BEGIN`; restore them on `ROLLBACK`.
- Isolation level is **Read Committed**. Do not assume stronger isolation.

### SQL Engine
- The parser is a hand-written recursive descent parser in `parser.rs`. Add new SQL constructs there first, then extend the AST in `ast.rs`, then handle them in `engine.rs`.
- `eval.rs` handles expression evaluation. Add new functions/operators there. Always handle `Value::Null` explicitly — never treat NULL as zero or empty string.
- Value encoding for B-Tree keys lives in `engine.rs` (`encode_value`). Any new `Value` variant must also get an encoding here.

### Testing
- **Unit tests**: colocate with the module they test using `#[cfg(test)]` blocks.
- **Integration tests**: add to the appropriate `*_test.rs` file. Use `tempfile::TempDir` for on-disk tests.
- **Differential tests**: use `differential_test.rs` to compare behavior against SQLite (`rusqlite`).
- **Property tests**: use `proptest` macros in `property_test.rs` for invariant checking.
- Always run `PRAGMA integrity_check;` (or call `db.integrity_check()`) after any test that mutates the B-Tree.

### Style
- Follow standard `rustfmt` formatting (`cargo fmt`).
- Run `cargo clippy` before committing; fix all warnings.
- Module-level doc comments (`//!`) describe purpose. Item-level doc comments (`///`) for public API only.
- Avoid `pub` on internals — prefer `pub(crate)` for cross-module access within the library.
- Constants for magic values (page size, WAL magic, etc.) should be `const` at the module level, not inline literals.

---

## Known Limitations

- B-Tree deletes do not rebalance — fragmentation accumulates over time.
- No multi-table JOINs (the executor handles single-table queries).
- Isolation is Read Committed; no MVCC or serializable isolation.
- Catalog is stored in a single page — schema size is bounded by 4KB after bincode encoding.
- No WAL compaction beyond manual `PRAGMA wal_checkpoint`.
