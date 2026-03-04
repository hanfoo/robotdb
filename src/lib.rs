//! # RobotDB - High-Performance Embedded Relational Database in Rust

pub mod btree;
pub mod catalog;
pub mod error;
pub mod executor;
pub mod sql;
pub mod storage;
pub mod transaction;

pub use error::{Error, Result};
pub use executor::ResultSet;
pub use catalog::Value;

use std::path::Path;
use catalog::Catalog;
use storage::{BufferPool, BufferSnapshot, DiskManager, INVALID_PAGE_ID};
use transaction::TransactionManager;
use executor::Executor;
use sql::parse;

const DEFAULT_BUFFER_POOL_SIZE: usize = 256;

/// Controls the durability/performance trade-off for disk synchronization.
///
/// Matches SQLite's `PRAGMA synchronous` semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// No fsync at all. Fastest but data may corrupt on OS crash.
    Off = 0,
    /// WAL fsync on explicit commit; data pages written but not fsynced.
    /// Safe on process crash; may lose recent auto-commits on OS crash.
    Normal = 1,
    /// WAL fsync + data file fsync on every commit. Full ACID durability.
    Full = 2,
}

/// A pre-parsed SQL statement that can be executed multiple times with different parameters.
pub struct PreparedStatement {
    stmt: sql::ast::Statement,
    /// Cached insert metadata for fast-path repeated inserts
    insert_cache: Option<InsertCache>,
}

/// Pre-computed metadata for fast INSERT execution
struct InsertCache {
    /// Table name (already lowercased for direct HashMap lookup)
    table_key: String,
    table_name: String,
    root_page: u32,
    /// Column indices mapping INSERT columns to table columns
    col_indices: Vec<usize>,
    /// PK column indices (into the table's column list)
    pk_col_indices: Vec<usize>,
    /// Column data types for type checking (parallel to col_indices)
    col_types: Vec<sql::ast::DataType>,
    /// Whether each column is nullable (parallel to col_indices)
    col_nullable: Vec<bool>,
    /// Number of columns in the table
    num_cols: usize,
    /// Whether any non-PK column has UNIQUE constraint
    has_unique: bool,
    /// Index of autoincrement column, if any
    autoincrement_col: Option<usize>,
}

/// Returns true if the statement is read-only (does not modify data or schema).
/// Read-only statements can bypass the WAL and disk-flush overhead.
fn is_read_only_stmt(stmt: &sql::ast::Statement) -> bool {
    matches!(
        stmt,
        sql::ast::Statement::Select(_)
            | sql::ast::Statement::Explain(_)
            | sql::ast::Statement::Pragma(_)
    )
}

/// Returns true if the statement modifies the catalog (schema).
/// DML statements (INSERT/UPDATE/DELETE) do not modify the catalog,
/// so we can skip save_catalog() for them.
fn is_schema_changing_stmt(stmt: &sql::ast::Statement) -> bool {
    matches!(
        stmt,
        sql::ast::Statement::CreateTable(_)
            | sql::ast::Statement::DropTable(_)
            | sql::ast::Statement::CreateIndex(_)
            | sql::ast::Statement::AlterTable(_)
    )
}

/// Returns true if the statement is a DML statement (INSERT/UPDATE/DELETE).
fn is_dml_stmt(stmt: &sql::ast::Statement) -> bool {
    matches!(
        stmt,
        sql::ast::Statement::Insert(_)
            | sql::ast::Statement::Update(_)
            | sql::ast::Statement::Delete(_)
    )
}

pub struct Database {
    pool: BufferPool,
    catalog: Catalog,
    tx_manager: TransactionManager,
    /// Whether we are inside an explicit BEGIN...COMMIT/ROLLBACK block
    in_explicit_tx: bool,
    /// WAL transaction ID, lazily allocated only for schema-changing stmts in explicit tx
    tx_wal_id: Option<transaction::TxId>,
    catalog_page: u32,
    /// Snapshot taken at the start of each transaction for rollback support
    tx_snapshot: Option<(BufferSnapshot, Catalog)>,
    sync_mode: SyncMode,
    /// Tracks whether the current explicit transaction has executed schema-changing statements.
    tx_schema_dirty: bool,
    /// Reusable scratch buffers for execute_insert_fast to avoid per-call allocations
    scratch_pk_key: Vec<u8>,
    scratch_row_buf: Vec<u8>,
    scratch_row: catalog::Row,
    /// Cached leaf page hint for sequential inserts (avoids re-searching from root)
    insert_leaf_hint: Option<storage::PageId>,
    /// Cached root page for the last table we inserted into (avoids catalog HashMap lookup)
    insert_cached_root: Option<u32>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let wal_path = path.with_extension("wal");

        let mut disk = DiskManager::open(path)?;
        let is_new = disk.num_pages() == 0;

        let header = if is_new {
            disk.initialize()?
        } else {
            disk.read_header()?
        };

        let mut pool = BufferPool::new(disk, DEFAULT_BUFFER_POOL_SIZE);
        let tx_manager = TransactionManager::open(&wal_path, &mut pool)?;

        let (catalog, catalog_page) = if is_new {
            let cat = Catalog::new();
            let cat_page = Self::persist_catalog_new(&mut pool, &cat)?;
            (cat, cat_page)
        } else {
            let schema_root = header.schema_root;
            if schema_root == INVALID_PAGE_ID {
                let cat = Catalog::new();
                let cat_page = Self::persist_catalog_new(&mut pool, &cat)?;
                (cat, cat_page)
            } else {
                let cat = Self::load_catalog(&mut pool, schema_root)?;
                (cat, schema_root)
            }
        };

        Ok(Self {
            pool,
            catalog,
            tx_manager,
            in_explicit_tx: false,
            tx_wal_id: None,
            catalog_page,
            tx_snapshot: None,
            sync_mode: SyncMode::Full,
            tx_schema_dirty: false,
            scratch_pk_key: Vec::with_capacity(32),
            scratch_row_buf: Vec::with_capacity(128),
            scratch_row: Vec::new(),
            insert_leaf_hint: None,
            insert_cached_root: None,
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        use std::env::temp_dir;
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = temp_dir().join(format!("robotdb_mem_{}.db", ts));
        Self::open(&path)
    }

    /// Parse a SQL string into a prepared statement for repeated execution.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let stmt = parse(sql)?;
        let insert_cache = self.build_insert_cache(&stmt);
        Ok(PreparedStatement { stmt, insert_cache })
    }

    fn build_insert_cache(&self, stmt: &sql::ast::Statement) -> Option<InsertCache> {
        let ins = match stmt {
            sql::ast::Statement::Insert(ins) => ins,
            _ => return None,
        };
        // Only cache for VALUES source (not INSERT ... SELECT)
        if !matches!(&ins.source, sql::ast::InsertSource::Values(_)) {
            return None;
        }
        let table = self.catalog.get_table(&ins.table).ok()?;

        let col_indices: Vec<usize> = if let Some(cols) = &ins.columns {
            cols.iter().filter_map(|c| table.column_index(c)).collect()
        } else {
            (0..table.columns.len()).collect()
        };

        let pk_col_indices: Vec<usize> = table.primary_key.iter()
            .filter_map(|pk_col| table.column_index(pk_col))
            .collect();

        let col_types: Vec<sql::ast::DataType> = col_indices.iter()
            .map(|&i| table.columns[i].data_type.clone())
            .collect();

        let col_nullable: Vec<bool> = col_indices.iter()
            .map(|&i| table.columns[i].nullable)
            .collect();

        let has_unique = table.columns.iter().any(|c| c.unique && !c.primary_key);

        let autoincrement_col = table.columns.iter().enumerate()
            .find(|(_, c)| c.autoincrement)
            .map(|(i, _)| i);

        Some(InsertCache {
            table_key: ins.table.to_lowercase(),
            table_name: ins.table.clone(),
            root_page: table.root_page,
            col_indices,
            pk_col_indices,
            col_types,
            col_nullable,
            num_cols: table.columns.len(),
            has_unique,
            autoincrement_col,
        })
    }

    /// Execute a prepared statement with bound parameters. Returns rows affected.
    pub fn execute_prepared(&mut self, prepared: &PreparedStatement, params: &[Value]) -> Result<usize> {
        // Ultra-fast path: INSERT with cached metadata inside explicit tx
        if self.in_explicit_tx {
            if let Some(ref cache) = prepared.insert_cache {
                return self.execute_insert_fast(cache, &prepared.stmt, params);
            }
        }
        let result = self.query_prepared(prepared, params)?;
        Ok(result.rows_affected)
    }

    /// Execute a prepared statement with bound parameters. Returns full result set.
    pub fn query_prepared(&mut self, prepared: &PreparedStatement, params: &[Value]) -> Result<ResultSet> {
        let stmt = &prepared.stmt;

        // DML fast path inside explicit transaction: skip all tx management overhead
        if self.in_explicit_tx && is_dml_stmt(stmt) {
            let mut executor = Executor::with_params(&mut self.pool, &mut self.catalog, params);
            return executor.execute(stmt);
        }

        // For non-DML or outside explicit tx, delegate to the full path
        self.execute_stmt_with_params(stmt, params)
    }

    /// Ultra-fast INSERT path: uses pre-computed metadata and scratch buffers
    /// to avoid per-call overhead. Bypasses Executor entirely.
    fn execute_insert_fast(
        &mut self,
        cache: &InsertCache,
        stmt: &sql::ast::Statement,
        params: &[Value],
    ) -> Result<usize> {
        use crate::btree::BTree;
        use crate::catalog::serialize_row_into;
        use crate::executor::engine::encode_value_for_key;

        let ins = match stmt {
            sql::ast::Statement::Insert(ins) => ins,
            _ => unreachable!(),
        };

        let row_exprs = match &ins.source {
            sql::ast::InsertSource::Values(rows) => &rows[0],
            _ => unreachable!(),
        };

        // Reuse scratch row buffer — resize to num_cols and fill with Null
        let row = &mut self.scratch_row;
        row.clear();
        row.resize(cache.num_cols, Value::Null);

        // Evaluate each expression — for placeholders, directly index into params
        for (expr_idx, &col_idx) in cache.col_indices.iter().enumerate() {
            let val = match &row_exprs[expr_idx] {
                sql::ast::Expr::Placeholder(idx) => {
                    if *idx == 0 || *idx > params.len() {
                        return Err(Error::ExecutionError(format!(
                            "Parameter index {} out of range (have {} params)", idx, params.len()
                        )));
                    }
                    params[*idx - 1].clone()
                }
                expr => {
                    let dummy_schema = catalog::TableSchema {
                        name: String::new(),
                        columns: Vec::new(),
                        root_page: 0,
                        primary_key: Vec::new(),
                        auto_increment: 0,
                        row_count: 0,
                    };
                    let dummy_row: catalog::Row = Vec::new();
                    let ctx = executor::EvalContext::with_params(&dummy_row, &dummy_schema, params);
                    executor::eval_expr(expr, &ctx)?
                }
            };

            // Type check — skip cast when type already matches
            if val.is_null() {
                if !cache.col_nullable[expr_idx] {
                    return Err(Error::ExecutionError(
                        "NOT NULL constraint violated".to_string(),
                    ));
                }
            } else if !val.matches_type(&cache.col_types[expr_idx]) {
                row[col_idx] = val.cast(&cache.col_types[expr_idx]).unwrap_or(val);
                continue;
            }
            row[col_idx] = val;
        }

        // Handle autoincrement
        if let Some(ai_col) = cache.autoincrement_col {
            if row[ai_col].is_null() {
                let table = self.catalog.get_table(&cache.table_name)?;
                row[ai_col] = Value::Integer(table.auto_increment + 1);
            }
        }

        // Build PK key using scratch buffer (reuse across calls)
        self.scratch_pk_key.clear();
        if cache.pk_col_indices.is_empty() {
            serialize_row_into(row, &mut self.scratch_pk_key);
        } else {
            for &idx in &cache.pk_col_indices {
                encode_value_for_key(&row[idx], &mut self.scratch_pk_key);
            }
        }

        // UNIQUE constraint check (rare path — most tables don't have non-PK UNIQUE)
        if cache.has_unique {
            let mut executor = Executor::with_params(&mut self.pool, &mut self.catalog, params);
            return executor.execute(stmt).map(|r| r.rows_affected);
        }

        // Use cached root page (avoids catalog HashMap lookup per insert)
        let root_page = self.insert_cached_root.unwrap_or(cache.root_page);

        // Serialize row into scratch buffer
        serialize_row_into(row, &mut self.scratch_row_buf);

        // Insert into B-tree using slices with leaf hint (avoids re-searching from root)
        let mut btree = BTree::open(root_page);
        btree.insert_with_hint(
            &mut self.pool,
            &self.scratch_pk_key,
            &self.scratch_row_buf,
            &mut self.insert_leaf_hint,
        ).map_err(|e| match e {
            Error::DuplicateKey => Error::ExecutionError(
                "UNIQUE constraint failed: PRIMARY KEY".to_string(),
            ),
            other => other,
        })?;

        // Update auto_increment, root_page, and row_count in catalog (use pre-lowercased key)
        let new_root = btree.root_id;
        self.insert_cached_root = Some(new_root);
        if let Some(t) = self.catalog.tables.get_mut(&cache.table_key) {
            t.auto_increment += 1;
            t.root_page = new_root;
            t.row_count += 1;
        }

        Ok(1)
    }

    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let result = self.query(sql)?;
        Ok(result.rows_affected)
    }

    pub fn query(&mut self, sql: &str) -> Result<ResultSet> {
        let stmt = parse(sql)?;
        self.execute_stmt_with_params(&stmt, &[])
    }

    fn execute_stmt_with_params(&mut self, stmt: &sql::ast::Statement, params: &[Value]) -> Result<ResultSet> {
        match stmt {
            sql::ast::Statement::Begin => {
                if self.in_explicit_tx {
                    return Err(Error::TransactionAlreadyActive);
                }
                self.in_explicit_tx = true;
                self.tx_schema_dirty = false;
                self.insert_leaf_hint = None;
                self.insert_cached_root = None;
                // Take a snapshot for rollback support
                let snapshot = self.pool.take_snapshot();
                let catalog_snapshot = self.catalog.clone();
                self.tx_snapshot = Some((snapshot, catalog_snapshot));
                return Ok(ResultSet::empty());
            }
            sql::ast::Statement::Commit => {
                if self.in_explicit_tx {
                    // Only do WAL work if we lazily opened a WAL transaction
                    if let Some(tx_id) = self.tx_wal_id.take() {
                        self.tx_manager.log_dirty_pages(tx_id, &self.pool)?;
                        match self.sync_mode {
                            SyncMode::Off => {
                                self.tx_manager.commit_no_flush(tx_id)?;
                            }
                            SyncMode::Normal | SyncMode::Full => {
                                self.tx_manager.commit_wal_only(tx_id)?;
                            }
                        }
                    }
                    // Only save catalog if schema was modified during this transaction
                    if self.tx_schema_dirty {
                        self.save_catalog()?;
                        self.tx_schema_dirty = false;
                    }
                    self.in_explicit_tx = false;
                    self.tx_snapshot = None;
                    // Flush dirty pages to disk
                    if self.sync_mode != SyncMode::Off {
                        self.pool.flush_dirty()?;
                    }
                }
                return Ok(ResultSet::empty());
            }
            sql::ast::Statement::Rollback => {
                if self.in_explicit_tx {
                    if let Some(tx_id) = self.tx_wal_id.take() {
                        let _ = self.tx_manager.rollback(tx_id, &mut self.pool);
                    }
                    self.in_explicit_tx = false;
                    self.tx_schema_dirty = false;
                    // Restore the snapshot
                    if let Some((snapshot, catalog_snapshot)) = self.tx_snapshot.take() {
                        self.pool.rollback_to_snapshot(&snapshot)?;
                        self.catalog = catalog_snapshot;
                    }
                }
                return Ok(ResultSet::empty());
            }
            _ => {}
        }

        // Handle PRAGMA synchronous before the read-only fast path,
        // since the setter mutates Database-level state.
        if let sql::ast::Statement::Pragma(ref p) = stmt {
            if p.name.eq_ignore_ascii_case("synchronous") {
                return self.handle_pragma_synchronous(&p.value);
            }
        }

        // Fast path for read-only statements outside an explicit transaction:
        // Skip the WAL, snapshot, and disk-flush overhead entirely.
        // SELECT / EXPLAIN / read-only PRAGMAs don't modify any data or catalog.
        if !self.in_explicit_tx && is_read_only_stmt(stmt) {
            let mut executor = Executor::with_params(&mut self.pool, &mut self.catalog, params);
            return executor.execute(stmt);
        }

        // Inside an explicit transaction: DML fast path (no WAL, no snapshot overhead)
        if self.in_explicit_tx && is_dml_stmt(stmt) {
            let mut executor = Executor::with_params(&mut self.pool, &mut self.catalog, params);
            return executor.execute(stmt);
        }

        // Inside an explicit transaction: read-only queries just execute
        if self.in_explicit_tx && is_read_only_stmt(stmt) {
            let mut executor = Executor::with_params(&mut self.pool, &mut self.catalog, params);
            return executor.execute(stmt);
        }

        let auto_tx = !self.in_explicit_tx;
        let schema_change = is_schema_changing_stmt(stmt);

        // Fast path for auto-commit DML (INSERT/UPDATE/DELETE):
        // Skip the WAL entirely — the executor doesn't call before_write_page(),
        // so the WAL has no page images anyway. Without a WAL record there is no
        // WAL-before-data invariant to enforce, so fsync adds no crash-safety
        // benefit here. Both NORMAL and FULL just write dirty pages to the OS
        // buffer (matching SQLite's behavior for non-WAL auto-commits).
        if auto_tx && !schema_change {
            let result = {
                let mut executor = Executor::with_params(&mut self.pool, &mut self.catalog, params);
                executor.execute(stmt)
            };
            if result.is_ok() && self.sync_mode != SyncMode::Off {
                self.pool.flush_dirty()?;
            }
            return result;
        }

        // Schema-changing statement inside explicit tx: lazily create WAL tx
        if self.in_explicit_tx && schema_change {
            self.tx_schema_dirty = true;
            if self.tx_wal_id.is_none() {
                let tx_id = self.tx_manager.begin()?;
                self.tx_wal_id = Some(tx_id);
            }
            let mut executor = Executor::with_params(&mut self.pool, &mut self.catalog, params);
            return executor.execute(stmt);
        }

        // Auto-commit schema-changing statement
        let auto_snapshot = if auto_tx {
            let tx_id = self.tx_manager.begin()?;
            self.tx_wal_id = Some(tx_id);
            let snapshot = self.pool.take_snapshot();
            let catalog_snapshot = self.catalog.clone();
            Some((snapshot, catalog_snapshot))
        } else {
            None
        };

        let result = {
            let mut executor = Executor::with_params(&mut self.pool, &mut self.catalog, params);
            executor.execute(stmt)
        };

        if auto_tx {
            if let Some(tx_id) = self.tx_wal_id.take() {
                match &result {
                    Ok(_) => {
                        match self.sync_mode {
                            SyncMode::Off => {
                                // Skip WAL commit entirely; just save catalog and write pages
                                let _ = self.tx_manager.rollback(tx_id, &mut self.pool);
                                self.save_catalog()?;
                                self.pool.flush_dirty()?;
                            }
                            SyncMode::Normal => {
                                self.tx_manager.log_dirty_pages(tx_id, &self.pool)?;
                                self.tx_manager.commit(tx_id, &mut self.pool)?;
                                self.save_catalog()?;
                                self.pool.flush_dirty()?;
                            }
                            SyncMode::Full => {
                                self.tx_manager.log_dirty_pages(tx_id, &self.pool)?;
                                self.tx_manager.commit(tx_id, &mut self.pool)?;
                                self.save_catalog()?;
                                self.pool.flush_all()?;
                            }
                        }
                    }
                    Err(_) => {
                        let _ = self.tx_manager.rollback(tx_id, &mut self.pool);
                        if let Some((snapshot, catalog_snapshot)) = auto_snapshot {
                            let _ = self.pool.rollback_to_snapshot(&snapshot);
                            self.catalog = catalog_snapshot;
                        }
                    }
                }
            }
        }

        result
    }

    pub fn execute_batch(&mut self, sql: &str) -> Result<()> {
        let stmts = sql::parse_statements(sql)?;
        for stmt in stmts {
            let mut executor = Executor::new(&mut self.pool, &mut self.catalog);
            executor.execute(&stmt)?;
        }
        self.save_catalog()?;
        Ok(())
    }

    pub fn table_names(&self) -> Vec<String> {
        self.catalog.table_names()
    }

    /// Run a comprehensive integrity check on all B-Trees in the database.
    /// Returns Ok(report) if all checks pass, or the report containing errors.
    pub fn integrity_check(&mut self) -> Result<btree::IntegrityReport> {
        use btree::{BTree, IntegrityReport, TreeStats};
        let mut all_violations = Vec::new();
        let mut checks_passed = 0usize;
        let mut total_keys = 0usize;
        let mut total_nodes = 0usize;
        let table_names = self.catalog.table_names();
        for name in table_names {
            if let Ok(table) = self.catalog.get_table(&name) {
                let btree = BTree::open(table.root_page);
                match btree.integrity_check(&mut self.pool) {
                    Ok(report) => {
                        all_violations.extend(report.violations.iter().map(|e| format!("[{}] {}", name, e)));
                        checks_passed += report.checks_passed;
                        total_keys += report.stats.total_keys;
                        total_nodes += report.stats.total_nodes;
                    }
                    Err(e) => {
                        all_violations.push(format!("[{}] integrity_check failed: {}", name, e));
                    }
                }
            }
        }
        Ok(IntegrityReport {
            checks_passed,
            violations: all_violations,
            stats: TreeStats {
                total_nodes,
                internal_nodes: 0,
                leaf_nodes: 0,
                total_keys,
                height: 0,
                min_leaf_keys: 0,
                max_leaf_keys: 0,
            },
        })
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        self.tx_manager.checkpoint(&mut self.pool)?;
        self.save_catalog()?;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        if self.in_explicit_tx {
            if let Some(tx_id) = self.tx_wal_id.take() {
                self.tx_manager.rollback(tx_id, &mut self.pool)?;
            }
            self.in_explicit_tx = false;
        }
        self.save_catalog()?;
        self.pool.flush_all()?;
        Ok(())
    }

    fn handle_pragma_synchronous(&mut self, value: &Option<sql::ast::Expr>) -> Result<ResultSet> {
        match value {
            None => {
                // Getter: return current mode as integer
                Ok(ResultSet {
                    columns: vec!["synchronous".to_string()],
                    rows: vec![vec![Value::Integer(self.sync_mode as i64)]],
                    rows_affected: 0,
                })
            }
            Some(expr) => {
                // Setter: parse value and set mode
                let mode = match expr {
                    sql::ast::Expr::Literal(sql::ast::Literal::Integer(n)) => match n {
                        0 => SyncMode::Off,
                        1 => SyncMode::Normal,
                        2 => SyncMode::Full,
                        _ => return Err(Error::ExecutionError(
                            format!("Invalid synchronous value: {}. Expected 0 (OFF), 1 (NORMAL), or 2 (FULL)", n),
                        )),
                    },
                    sql::ast::Expr::Column { table: None, name } => {
                        match name.to_ascii_uppercase().as_str() {
                            "OFF" => SyncMode::Off,
                            "NORMAL" => SyncMode::Normal,
                            "FULL" => SyncMode::Full,
                            _ => return Err(Error::ExecutionError(
                                format!("Invalid synchronous value: '{}'. Expected OFF, NORMAL, or FULL", name),
                            )),
                        }
                    }
                    sql::ast::Expr::Literal(sql::ast::Literal::String(s)) => {
                        match s.to_ascii_uppercase().as_str() {
                            "OFF" => SyncMode::Off,
                            "NORMAL" => SyncMode::Normal,
                            "FULL" => SyncMode::Full,
                            _ => return Err(Error::ExecutionError(
                                format!("Invalid synchronous value: '{}'. Expected OFF, NORMAL, or FULL", s),
                            )),
                        }
                    }
                    _ => return Err(Error::ExecutionError(
                        "Invalid synchronous value. Expected 0/1/2 or OFF/NORMAL/FULL".to_string(),
                    )),
                };
                self.sync_mode = mode;
                Ok(ResultSet::empty())
            }
        }
    }

    fn persist_catalog_new(pool: &mut BufferPool, catalog: &Catalog) -> Result<u32> {
        let data = catalog.serialize();
        let page = pool.new_page()?;
        let page_id = page.page_id;
        let payload = page.payload_mut();
        let len = data.len().min(payload.len());
        payload[..len].copy_from_slice(&data[..len]);
        pool.unpin_page(page_id, true)?;
        Ok(page_id)
    }

    fn save_catalog(&mut self) -> Result<()> {
        let data = self.catalog.serialize();
        if self.catalog_page == INVALID_PAGE_ID {
            self.catalog_page = Self::persist_catalog_new(&mut self.pool, &self.catalog)?;
        } else {
            let page = self.pool.fetch_page(self.catalog_page)?;
            let payload = page.payload_mut();
            let len = data.len().min(payload.len());
            payload[..len].copy_from_slice(&data[..len]);
            self.pool.unpin_page(self.catalog_page, true)?;
        }
        // Update the schema_root in the disk header so it persists across restarts
        self.pool.update_schema_root(self.catalog_page)?;
        Ok(())
    }

    fn load_catalog(pool: &mut BufferPool, page_id: u32) -> Result<Catalog> {
        let page = pool.fetch_page(page_id)?;
        let data = page.payload().to_vec();
        pool.unpin_page(page_id, false)?;
        Catalog::deserialize(&data)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.in_explicit_tx {
            if let Some(tx_id) = self.tx_wal_id.take() {
                let _ = self.tx_manager.rollback(tx_id, &mut self.pool);
            }
            self.in_explicit_tx = false;
        }
        let _ = self.save_catalog();
        let _ = self.pool.flush_all();
    }
}

pub fn open(path: impl AsRef<Path>) -> Result<Database> {
    Database::open(path)
}

pub fn open_in_memory() -> Result<Database> {
    Database::open_in_memory()
}
