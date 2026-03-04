/// 查询执行引擎
///
/// 将 SQL AST 转换为物理执行计划并执行，返回结果集。
/// 当前实现为火山模型（Volcano/Iterator model）的简化版本。

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::sql::ast::*;
use crate::catalog::{Catalog, Value, Row, TableSchema, serialize_row, deserialize_row, deserialize_row_projected};
use crate::storage::BufferPool;
use crate::btree::BTree;
use super::eval::{eval_expr, EvalContext};

/// 查询结果集
#[derive(Debug, Clone)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub rows_affected: usize,
}

impl ResultSet {
    pub fn empty() -> Self {
        Self { columns: Vec::new(), rows: Vec::new(), rows_affected: 0 }
    }

    pub fn affected(n: usize) -> Self {
        Self { columns: Vec::new(), rows: Vec::new(), rows_affected: n }
    }
}

/// 保序键编码（order-preserving encoding）
/// 确保 B-Tree 中的字节序比较与值的自然顺序一致。
///
/// 编码规则：
/// - Null:    [0x00]
/// - Integer: [0x02, big-endian bytes with sign bit flipped]
///            i64::MIN → [0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
///            -1       → [0x02, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
///            0        → [0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
///            i64::MAX → [0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
/// - Real:    [0x03, order-preserving f64 bytes]
/// - Text:    [0x04, utf8 bytes, 0xFF terminator]
/// - Boolean: [0x01, 0 or 1]
/// Extract a literal value from `WHERE pk_col = literal` (or `literal = pk_col`).
/// Returns `None` if the WHERE clause is not this exact pattern (single-column PK only).
fn extract_pk_value(table: &TableSchema, where_clause: &Option<Expr>) -> Option<Value> {
    if table.primary_key.len() != 1 {
        return None;
    }
    let pk_col = &table.primary_key[0];

    let lit = match where_clause {
        Some(Expr::BinaryOp { left, op: BinaryOp::Eq, right }) => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column { name, table: None }, Expr::Literal(lit))
                    if name.eq_ignore_ascii_case(pk_col) => lit,
                (Expr::Column { name, table: Some(tbl) }, Expr::Literal(lit))
                    if name.eq_ignore_ascii_case(pk_col)
                    && tbl.eq_ignore_ascii_case(&table.name) => lit,
                (Expr::Literal(lit), Expr::Column { name, table: None })
                    if name.eq_ignore_ascii_case(pk_col) => lit,
                _ => return None,
            }
        }
        _ => return None,
    };

    Some(match lit {
        Literal::Integer(n) => Value::Integer(*n),
        Literal::Float(f) => Value::Real(*f),
        Literal::String(s) => Value::Text(s.clone()),
        Literal::Boolean(b) => Value::Boolean(*b),
        Literal::Null => Value::Null,
    })
}

pub fn encode_value_for_key(val: &Value, buf: &mut Vec<u8>) {
    match val {
        Value::Null => buf.push(0x00),
        Value::Boolean(b) => {
            buf.push(0x01);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Integer(n) => {
            buf.push(0x02);
            // Flip the sign bit so that the byte ordering matches the integer ordering
            let bits = (*n as u64) ^ 0x8000_0000_0000_0000u64;
            buf.extend_from_slice(&bits.to_be_bytes());
        }
        Value::Real(f) => {
            buf.push(0x03);
            // IEEE 754 order-preserving encoding:
            // For positive floats, flip the sign bit.
            // For negative floats, flip all bits.
            let bits = f.to_bits();
            let ordered = if *f >= 0.0 {
                bits ^ 0x8000_0000_0000_0000u64
            } else {
                bits ^ 0xFFFF_FFFF_FFFF_FFFFu64
            };
            buf.extend_from_slice(&ordered.to_be_bytes());
        }
        Value::Text(s) => {
            buf.push(0x04);
            buf.extend_from_slice(s.as_bytes());
            buf.push(0xFF); // terminator
        }
        Value::Blob(b) => {
            buf.push(0x05);
            buf.extend_from_slice(b);
            buf.push(0xFF);
        }
    }
}

/// Stack-allocated key encoding for fixed-size types (Null, Boolean, Integer, Real).
/// Returns the number of bytes written. Falls back to heap for variable-length types.
/// `buf` must be at least 10 bytes.
fn encode_value_for_key_buf(val: &Value, buf: &mut [u8]) -> usize {
    match val {
        Value::Null => {
            buf[0] = 0x00;
            1
        }
        Value::Boolean(b) => {
            buf[0] = 0x01;
            buf[1] = if *b { 1 } else { 0 };
            2
        }
        Value::Integer(n) => {
            buf[0] = 0x02;
            let bits = (*n as u64) ^ 0x8000_0000_0000_0000u64;
            buf[1..9].copy_from_slice(&bits.to_be_bytes());
            9
        }
        Value::Real(f) => {
            buf[0] = 0x03;
            let bits = f.to_bits();
            let ordered = if *f >= 0.0 {
                bits ^ 0x8000_0000_0000_0000u64
            } else {
                bits ^ 0xFFFF_FFFF_FFFF_FFFFu64
            };
            buf[1..9].copy_from_slice(&ordered.to_be_bytes());
            9
        }
        Value::Text(s) => {
            buf[0] = 0x04;
            let bytes = s.as_bytes();
            buf[1..1 + bytes.len()].copy_from_slice(bytes);
            buf[1 + bytes.len()] = 0xFF;
            2 + bytes.len()
        }
        Value::Blob(b) => {
            buf[0] = 0x05;
            buf[1..1 + b.len()].copy_from_slice(b);
            buf[1 + b.len()] = 0xFF;
            2 + b.len()
        }
    }
}

/// 查询执行引擎
pub struct Executor<'a> {
    pool: &'a mut BufferPool,
    catalog: &'a mut Catalog,
    params: &'a [Value],
}

/// A simple filter extracted from WHERE clause: column_idx op literal_value
struct SimpleFilter {
    col_idx: usize,
    op: BinaryOp,
    literal: Value,
}

impl<'a> Executor<'a> {
    pub fn new(pool: &'a mut BufferPool, catalog: &'a mut Catalog) -> Self {
        Self { pool, catalog, params: &[] }
    }

    pub fn with_params(pool: &'a mut BufferPool, catalog: &'a mut Catalog, params: &'a [Value]) -> Self {
        Self { pool, catalog, params }
    }

    /// 执行一条 SQL 语句
    pub fn execute(&mut self, stmt: &Statement) -> Result<ResultSet> {
        match stmt {
            Statement::Select(s) => self.execute_select(s),
            Statement::Insert(s) => self.execute_insert(s),
            Statement::Update(s) => self.execute_update(s),
            Statement::Delete(s) => self.execute_delete(s),
            Statement::CreateTable(s) => self.execute_create_table(s),
            Statement::DropTable(s) => self.execute_drop_table(s),
            Statement::CreateIndex(s) => self.execute_create_index(s),
            Statement::DropIndex(s) => self.execute_drop_index(s),
            Statement::AlterTable(s) => self.execute_alter_table(s),
            Statement::Begin | Statement::Commit | Statement::Rollback => {
                Ok(ResultSet::empty())
            }
            Statement::Explain(inner) => self.execute_explain(inner),
            Statement::Pragma(p) => self.execute_pragma(p),
            Statement::Vacuum => {
                // 简化实现：flush all pages
                self.pool.flush_all()?;
                Ok(ResultSet::affected(0))
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE TABLE
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_create_table(&mut self, stmt: &CreateTableStatement) -> Result<ResultSet> {
        // 分配新的 B-Tree 根页
        let btree = BTree::create(self.pool)?;
        self.catalog.create_table(stmt, btree.root_id)?;
        Ok(ResultSet::affected(0))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DROP TABLE
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_drop_table(&mut self, stmt: &DropTableStatement) -> Result<ResultSet> {
        self.catalog.drop_table(&stmt.name, stmt.if_exists)?;
        Ok(ResultSet::affected(0))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE INDEX
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_create_index(&mut self, stmt: &CreateIndexStatement) -> Result<ResultSet> {
        let btree = BTree::create(self.pool)?;
        self.catalog.create_index(stmt, btree.root_id)?;

        // 为现有数据建立索引
        let table = self.catalog.get_table(&stmt.table)?.clone();
        let col_indices: Vec<usize> = stmt.columns.iter()
            .map(|c| table.column_index(c)
                .ok_or_else(|| Error::ColumnNotFound(c.clone(), stmt.table.clone())))
            .collect::<Result<Vec<_>>>()?;

        let rows = self.scan_table(&table)?;
        let idx_schema = self.catalog.indexes.get(&stmt.name.to_lowercase())
            .ok_or_else(|| Error::IndexNotFound(stmt.name.clone()))?.clone();
        let mut idx_tree = BTree::open(idx_schema.root_page);

        for (pk_key, row) in rows {
            let idx_key = self.make_index_key(&row, &col_indices);
            // 索引键 = index_columns + pk（保证唯一性）
            let mut full_key = idx_key;
            full_key.extend_from_slice(&pk_key);
            idx_tree.upsert(self.pool, full_key, pk_key)?;
        }

        Ok(ResultSet::affected(0))
    }

    fn execute_drop_index(&mut self, stmt: &DropIndexStatement) -> Result<ResultSet> {
        let name_lower = stmt.name.to_lowercase();
        if self.catalog.indexes.remove(&name_lower).is_none() {
            if stmt.if_exists {
                return Ok(ResultSet::affected(0));
            }
            return Err(Error::IndexNotFound(stmt.name.clone()));
        }
        Ok(ResultSet::affected(0))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ALTER TABLE
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_alter_table(&mut self, stmt: &AlterTableStatement) -> Result<ResultSet> {
        match &stmt.action {
            AlterAction::AddColumn(col_def) => {
                let table = self.catalog.get_table_mut(&stmt.table)?;
                let col = crate::catalog::schema::ColumnSchema::from_ast(col_def);
                table.columns.push(col);
            }
            AlterAction::DropColumn(col_name) => {
                let table = self.catalog.get_table_mut(&stmt.table)?;
                let idx = table.column_index(col_name)
                    .ok_or_else(|| Error::ColumnNotFound(col_name.clone(), stmt.table.clone()))?;
                table.columns.remove(idx);
            }
            AlterAction::RenameColumn { old, new } => {
                let table = self.catalog.get_table_mut(&stmt.table)?;
                let col = table.columns.iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(old))
                    .ok_or_else(|| Error::ColumnNotFound(old.clone(), stmt.table.clone()))?;
                col.name = new.clone();
            }
            AlterAction::RenameTable(new_name) => {
                let old_lower = stmt.table.to_lowercase();
                let new_lower = new_name.to_lowercase();
                if let Some(mut table) = self.catalog.tables.remove(&old_lower) {
                    table.name = new_name.clone();
                    self.catalog.tables.insert(new_lower.clone(), table);
                    if let Some(idxs) = self.catalog.table_indexes.remove(&old_lower) {
                        self.catalog.table_indexes.insert(new_lower, idxs);
                    }
                } else {
                    return Err(Error::TableNotFound(stmt.table.clone()));
                }
            }
        }
        Ok(ResultSet::affected(0))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INSERT
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_insert(&mut self, stmt: &InsertStatement) -> Result<ResultSet> {
        let table = self.catalog.get_table(&stmt.table)?.clone();

        // Pre-compute PK column indices to avoid per-row string lookups
        let pk_col_indices: Vec<usize> = table.primary_key.iter()
            .filter_map(|pk_col| table.column_index(pk_col))
            .collect();

        let rows_to_insert = match &stmt.source {
            InsertSource::Values(rows) => {
                rows.iter().map(|row_exprs| {
                    self.eval_insert_row(&table, stmt.columns.as_deref(), row_exprs)
                }).collect::<Result<Vec<Row>>>()?
            }
            InsertSource::Select(sel) => {
                let result = self.execute_select(sel)?;
                result.rows
            }
        };

        let n = rows_to_insert.len();
        let mut btree = BTree::open(table.root_page);

        // Collect unique column indices for UNIQUE constraint checking
        let unique_col_indices: Vec<(usize, String)> = table.columns.iter().enumerate()
            .filter(|(_, col)| col.unique && !col.primary_key)
            .map(|(i, col)| (i, col.name.clone()))
            .collect();

        // Pre-allocate key buffer for PK encoding
        let mut pk_key_buf = Vec::with_capacity(32);

        for row in rows_to_insert {
            // Build PK key using pre-computed column indices (avoids per-row string lookups)
            pk_key_buf.clear();
            if pk_col_indices.is_empty() {
                pk_key_buf = serialize_row(&row);
            } else {
                for &idx in &pk_col_indices {
                    encode_value_for_key(&row[idx], &mut pk_key_buf);
                }
            }

            // Check UNIQUE constraint for non-PK columns
            if !unique_col_indices.is_empty() {
                let table_lower = stmt.table.to_lowercase();
                // Separate columns into those with an index and those without
                let mut cols_without_index: Vec<(usize, &String)> = Vec::new();
                for (col_idx, col_name) in &unique_col_indices {
                    let new_val = &row[*col_idx];
                    if new_val.is_null() {
                        // NULL values don't violate UNIQUE (SQL standard)
                        continue;
                    }
                    let col_lower = col_name.to_lowercase();
                    // Find a single-column index covering this UNIQUE column
                    let idx_root = self.catalog.table_indexes.get(&table_lower)
                        .and_then(|idx_names| {
                            idx_names.iter().find_map(|idx_name| {
                                self.catalog.indexes.get(idx_name.as_str())
                                    .filter(|idx| {
                                        idx.columns.len() == 1
                                            && idx.columns[0].to_lowercase() == col_lower
                                    })
                                    .map(|idx| idx.root_page)
                            })
                        });
                    if let Some(idx_root_page) = idx_root {
                        // O(log n) check via index: index key = val_bytes + pk_bytes
                        let val_bytes = new_val.serialize();
                        let idx_tree = BTree::open(idx_root_page);
                        let results = idx_tree.range_scan(self.pool, Some(&val_bytes), None)?;
                        if results.first().map(|(k, _)| k.starts_with(&val_bytes)).unwrap_or(false) {
                            return Err(Error::UniqueConstraintViolation(
                                table.name.clone(),
                                col_name.clone(),
                            ));
                        }
                    } else {
                        cols_without_index.push((*col_idx, col_name));
                    }
                }
                // Fall back to a single full scan for columns without an index
                if !cols_without_index.is_empty() {
                    let existing_rows = self.scan_table_rows(&table)?;
                    for (col_idx, col_name) in &cols_without_index {
                        let new_val = &row[*col_idx];
                        for existing_row in &existing_rows {
                            if let Some(existing_val) = existing_row.get(*col_idx) {
                                if !existing_val.is_null() && existing_val == new_val {
                                    return Err(Error::UniqueConstraintViolation(
                                        table.name.clone(),
                                        (*col_name).clone(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }

            let value = serialize_row(&row);
            // Let insert() handle PK uniqueness check during its tree traversal,
            // avoiding a redundant get() pre-check that doubles B-Tree lookups.
            let pk_key = std::mem::take(&mut pk_key_buf);
            btree.insert(self.pool, pk_key, value).map_err(|e| match e {
                Error::DuplicateKey => Error::UniqueConstraintViolation(
                    table.name.clone(),
                    "PRIMARY KEY".to_string(),
                ),
                other => other,
            })?;
        }

        // 更新自增计数器，同时同步 B-Tree 根节点（可能因分裂而变化）
        if let Some(t) = self.catalog.tables.get_mut(&stmt.table.to_lowercase()) {
            t.auto_increment += n as i64;
            t.root_page = btree.root_id;  // sync root after possible splits
            t.row_count += n as u64;
        }

        Ok(ResultSet::affected(n))
    }

    fn eval_insert_row(
        &mut self,
        table: &TableSchema,
        columns: Option<&[String]>,
        exprs: &[Expr],
    ) -> Result<Row> {
        // 构建一个空行（所有列为 NULL）
        let mut row: Row = vec![Value::Null; table.columns.len()];

        // 确定列映射
        let col_indices: Vec<usize> = if let Some(cols) = columns {
            cols.iter().map(|c| {
                table.column_index(c)
                    .ok_or_else(|| Error::ColumnNotFound(c.clone(), table.name.clone()))
            }).collect::<Result<Vec<_>>>()?
        } else {
            (0..table.columns.len()).collect()
        };

        if exprs.len() != col_indices.len() {
            return Err(Error::ExecutionError(format!(
                "Column count ({}) doesn't match value count ({})",
                col_indices.len(), exprs.len()
            )));
        }

        self.eval_insert_values(&mut row, table, &col_indices, exprs)?;

        // 处理自增主键
        for (i, col) in table.columns.iter().enumerate() {
            if col.autoincrement && row[i].is_null() {
                row[i] = Value::Integer(table.auto_increment + 1);
            }
        }

        Ok(row)
    }

    /// Evaluate insert value expressions into a pre-allocated row.
    /// Uses a zero-allocation dummy context for expression evaluation.
    fn eval_insert_values(
        &self,
        row: &mut Row,
        table: &TableSchema,
        col_indices: &[usize],
        exprs: &[Expr],
    ) -> Result<()> {
        // Static-like dummy schema — avoids String allocation per call
        let dummy_schema = TableSchema {
            name: String::new(),
            columns: Vec::new(),
            root_page: 0,
            primary_key: Vec::new(),
            auto_increment: 0,
            row_count: 0,
        };
        let dummy_row: Row = Vec::new();
        let ctx = EvalContext::with_params(&dummy_row, &dummy_schema, self.params);

        for (expr_idx, &col_idx) in col_indices.iter().enumerate() {
            let val = eval_expr(&exprs[expr_idx], &ctx)?;
            // Skip cast when type already matches (avoids unnecessary clone)
            let col = &table.columns[col_idx];
            let val = if val.is_null() {
                if !col.nullable {
                    return Err(Error::NullViolation(col.name.clone()));
                }
                val
            } else if val.matches_type(&col.data_type) {
                val
            } else {
                val.cast(&col.data_type).unwrap_or(val)
            };
            row[col_idx] = val;
        }
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SELECT
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_select(&mut self, stmt: &SelectStatement) -> Result<ResultSet> {
        // 1. 从 FROM 子句获取基础行集
        let mut pk_lookup_used = false;
        let (schema, mut rows) = if let Some(table_ref) = &stmt.from {
            // Split borrows to allow simultaneous access to pool and catalog
            let pool = &mut *self.pool;
            let catalog = &*self.catalog;
            let table = catalog.get_table(&table_ref.name)?;

            // Fast path: COUNT(*) / COUNT(1) with no WHERE, GROUP BY, or HAVING
            if stmt.where_clause.is_none()
                && stmt.group_by.is_empty()
                && stmt.having.is_none()
                && stmt.columns.len() == 1
            {
                if let SelectColumn::Expr { expr, ref alias } = &stmt.columns[0] {
                    if let Expr::Function { name, args, .. } = expr {
                        if name.eq_ignore_ascii_case("COUNT")
                            && (args.is_empty()
                                || matches!(args[0], Expr::Wildcard)
                                || matches!(args[0], Expr::Literal(Literal::Integer(1))))
                        {
                            let count = table.row_count;
                            let col_name = alias
                                .clone()
                                .unwrap_or_else(|| Self::expr_display_name_static(expr));
                            return Ok(ResultSet {
                                columns: vec![col_name],
                                rows: vec![vec![Value::Integer(count as i64)]],
                                rows_affected: 0,
                            });
                        }
                    }
                }
            }

            // Fast path: single aggregate (SUM/AVG/MIN/MAX) on a column with no WHERE/GROUP BY
            if stmt.where_clause.is_none()
                && stmt.group_by.is_empty()
                && stmt.having.is_none()
                && stmt.order_by.is_empty()
                && stmt.columns.len() == 1
            {
                if let SelectColumn::Expr { expr, ref alias } = &stmt.columns[0] {
                    if let Expr::Function { name: func_name, args, .. } = expr {
                        let func_upper = func_name.to_uppercase();
                        if matches!(func_upper.as_str(), "SUM" | "AVG" | "MIN" | "MAX")
                            && args.len() == 1
                        {
                            if let Expr::Column { name: col_name, .. } = &args[0] {
                                if let Some(col_idx) = table.column_index(col_name) {
                                    let col_indices = [col_idx];
                                    let btree = BTree::open(table.root_page);
                                    let col_name_display = alias
                                        .clone()
                                        .unwrap_or_else(|| Self::expr_display_name_static(expr));

                                    // Precompute byte offset to skip fixed-width columns
                                    let skip_bytes = Self::compute_column_skip_bytes(table, col_idx);

                                    let result = Self::eval_aggregate_streaming(
                                        &func_upper, &btree, pool, &col_indices, skip_bytes,
                                    )?;
                                    return Ok(ResultSet {
                                        columns: vec![col_name_display],
                                        rows: vec![vec![result]],
                                        rows_affected: 0,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            // Fast path: single aggregate with simple WHERE (no GROUP BY)
            // Fuses scan + filter + aggregate to avoid materializing all rows
            if stmt.where_clause.is_some()
                && stmt.group_by.is_empty()
                && stmt.having.is_none()
                && stmt.order_by.is_empty()
                && stmt.columns.len() == 1
            {
                if let Some(agg_result) = Self::try_fused_aggregate_where(
                    &stmt.columns[0],
                    stmt.where_clause.as_ref().unwrap(),
                    table,
                    pool,
                )? {
                    return Ok(agg_result);
                }
            }

            // Try PK point lookup for `WHERE pk_col = literal` before falling back to full scan
            let rows = if let Some(pk_val) = extract_pk_value(table, &stmt.where_clause) {
                let mut key_buf = [0u8; 32];
                let key_len = encode_value_for_key_buf(&pk_val, &mut key_buf);
                let btree = BTree::open(table.root_page);
                pk_lookup_used = true;

                // Ultra-fast path: SELECT * WHERE pk = literal, no ORDER BY/GROUP BY/LIMIT/OFFSET
                let is_select_star = stmt.columns.len() == 1
                    && matches!(stmt.columns[0], SelectColumn::Wildcard);
                if is_select_star
                    && stmt.order_by.is_empty()
                    && stmt.group_by.is_empty()
                    && stmt.having.is_none()
                    && stmt.limit.is_none()
                    && stmt.offset.is_none()
                {
                    let columns = table.columns.iter().map(|c| c.name.clone()).collect();
                    let rows = match btree.get(pool, &key_buf[..key_len])? {
                        Some(v) => vec![deserialize_row(&v)?],
                        None => Vec::new(),
                    };
                    return Ok(ResultSet { columns, rows, rows_affected: 0 });
                }

                match btree.get(pool, &key_buf[..key_len])? {
                    Some(v) => vec![deserialize_row(&v)?],
                    None => Vec::new(),
                }
            } else {
                self.scan_table_rows(&table.clone())?
            };
            (Some(self.catalog.get_table(&table_ref.name)?.clone()), rows)
        } else {
            // SELECT without FROM (e.g., SELECT 1+1)
            (None, vec![Vec::new()])
        };

        // 2. WHERE 过滤 — skip when PK point lookup already returned the exact match
        if !pk_lookup_used {
            if let Some(where_expr) = &stmt.where_clause {
                if let Some(ref s) = schema {
                    let params = self.params;
                    rows = rows.into_iter().filter(|row| {
                        let ctx = EvalContext::with_params(row, s, params);
                        eval_expr(where_expr, &ctx).map_or(false, |v| v.is_truthy())
                    }).collect();
                }
            }
        }

        // 3. GROUP BY + 聚合
        let (columns, mut rows) = if !stmt.group_by.is_empty() || self.has_aggregate(&stmt.columns) {
            self.execute_aggregate(stmt, schema.as_ref(), rows)?
        } else {
            // 4. 投影（SELECT columns）
            self.project(stmt, schema.as_ref(), rows)?
        };

        // 5. ORDER BY — 使用投影后的列名构建临时 schema
        if !stmt.order_by.is_empty() {
            // 构建一个与投影结果匹配的临时 schema，用于 ORDER BY 求值
            let proj_schema = self.build_projected_schema(&columns, schema.as_ref());
            let sort_schema = proj_schema.as_ref().unwrap_or_else(|| {
                schema.as_ref().unwrap()
            });
            self.sort_rows_with_fallback(&mut rows, &stmt.order_by, sort_schema, schema.as_ref(), &columns)?;
        }

        // 6. LIMIT / OFFSET
        let offset = if let Some(off_expr) = &stmt.offset {
            self.eval_limit_offset(off_expr)? as usize
        } else {
            0
        };
        let limit = if let Some(lim_expr) = &stmt.limit {
            Some(self.eval_limit_offset(lim_expr)? as usize)
        } else {
            None
        };

        let rows: Vec<Row> = rows.into_iter().skip(offset).take(limit.unwrap_or(usize::MAX)).collect();

        Ok(ResultSet {
            columns,
            rows,
            rows_affected: 0,
        })
    }

    fn has_aggregate(&self, cols: &[SelectColumn]) -> bool {
        cols.iter().any(|c| {
            if let SelectColumn::Expr { expr, .. } = c {
                self.expr_has_aggregate(expr)
            } else {
                false
            }
        })
    }

    fn expr_has_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_aggregate(left) || self.expr_has_aggregate(right)
            }
            _ => false,
        }
    }

    /// Collect the sorted set of column indices referenced by a SELECT statement
    /// (columns, WHERE, GROUP BY, HAVING, ORDER BY). Returns None if a wildcard
    /// is used (meaning all columns are needed).
    #[allow(dead_code)]
    fn collect_referenced_columns(stmt: &SelectStatement, schema: &TableSchema) -> Option<Vec<usize>> {
        let mut indices = std::collections::BTreeSet::new();
        for col in &stmt.columns {
            match col {
                SelectColumn::Wildcard | SelectColumn::TableWildcard(_) => return None,
                SelectColumn::Expr { expr, .. } => {
                    Self::collect_expr_columns(expr, schema, &mut indices);
                }
            }
        }
        if let Some(ref w) = stmt.where_clause {
            Self::collect_expr_columns(w, schema, &mut indices);
        }
        for g in &stmt.group_by {
            Self::collect_expr_columns(g, schema, &mut indices);
        }
        if let Some(ref h) = stmt.having {
            Self::collect_expr_columns(h, schema, &mut indices);
        }
        for o in &stmt.order_by {
            Self::collect_expr_columns(&o.expr, schema, &mut indices);
        }
        Some(indices.into_iter().collect())
    }

    #[allow(dead_code)]
    fn collect_expr_columns(expr: &Expr, schema: &TableSchema, out: &mut std::collections::BTreeSet<usize>) {
        match expr {
            Expr::Column { name, .. } => {
                if let Some(idx) = schema.column_index(name) {
                    out.insert(idx);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_expr_columns(left, schema, out);
                Self::collect_expr_columns(right, schema, out);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_expr_columns(expr, schema, out);
            }
            Expr::Function { args, .. } => {
                for a in args {
                    Self::collect_expr_columns(a, schema, out);
                }
            }
            Expr::Between { expr, low, high, .. } => {
                Self::collect_expr_columns(expr, schema, out);
                Self::collect_expr_columns(low, schema, out);
                Self::collect_expr_columns(high, schema, out);
            }
            Expr::InList { expr, list, .. } => {
                Self::collect_expr_columns(expr, schema, out);
                for l in list {
                    Self::collect_expr_columns(l, schema, out);
                }
            }
            Expr::IsNull { expr, .. } | Expr::Cast { expr, .. } => {
                Self::collect_expr_columns(expr, schema, out);
            }
            Expr::Case { operand, branches, else_branch } => {
                if let Some(op) = operand {
                    Self::collect_expr_columns(op, schema, out);
                }
                for (w, t) in branches {
                    Self::collect_expr_columns(w, schema, out);
                    Self::collect_expr_columns(t, schema, out);
                }
                if let Some(e) = else_branch {
                    Self::collect_expr_columns(e, schema, out);
                }
            }
            Expr::Subquery(_) => {
                // Subqueries reference their own schemas
            }
            _ => {}
        }
    }

    fn execute_aggregate(
        &mut self,
        stmt: &SelectStatement,
        schema: Option<&TableSchema>,
        rows: Vec<Row>,
    ) -> Result<(Vec<String>, Vec<Row>)> {
        let dummy_schema = TableSchema {
            name: "__agg__".into(),
            columns: Vec::new(),
            root_page: 0,
            primary_key: Vec::new(),
            auto_increment: 0,
            row_count: 0,
        };
        let s = schema.unwrap_or(&dummy_schema);

        // 按 GROUP BY 键分组
        // 保持插入顺序，使用 Vec<(key, rows)> 而不是 HashMap
        let mut group_order: Vec<Vec<String>> = Vec::new();
        let mut groups: HashMap<Vec<String>, Vec<Row>> = HashMap::new();

        if stmt.group_by.is_empty() {
            // 全局聚合：即使没有行也要返回一行结果（COUNT(*) = 0）
            group_order.push(Vec::new());
            groups.insert(Vec::new(), rows);
        } else {
            for row in rows {
                let ctx = EvalContext::new(&row, s);
                let key: Vec<String> = stmt.group_by.iter()
                    .map(|e| eval_expr(e, &ctx).map(|v| v.to_string()))
                    .collect::<Result<Vec<_>>>()?;
                if !groups.contains_key(&key) {
                    group_order.push(key.clone());
                }
                groups.entry(key).or_default().push(row);
            }
        }

        // 确定列名（使用可读的名称）
        let columns: Vec<String> = stmt.columns.iter().flat_map(|col| {
            match col {
                SelectColumn::Wildcard => s.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
                SelectColumn::Expr { expr, alias } => {
                    vec![alias.clone().unwrap_or_else(|| self.expr_display_name(expr))]
                }
                SelectColumn::TableWildcard(tbl) => {
                    if tbl.eq_ignore_ascii_case(&s.name) {
                        s.columns.iter().map(|c| c.name.clone()).collect()
                    } else {
                        Vec::new()
                    }
                }
            }
        }).collect();

        // 对每个分组计算聚合
        let mut result_rows: Vec<Row> = Vec::new();

        for group_key in &group_order {
            let group_rows = groups.get(group_key).map(|v| v.as_slice()).unwrap_or(&[]);

            let mut result_row: Row = Vec::new();

            for col in &stmt.columns {
                match col {
                    SelectColumn::Wildcard => {
                        if !group_rows.is_empty() {
                            result_row.extend(group_rows[0].clone());
                        } else {
                            result_row.extend(vec![Value::Null; s.columns.len()]);
                        }
                    }
                    SelectColumn::Expr { expr, .. } => {
                        let val = self.eval_aggregate(expr, group_rows, s)?;
                        result_row.push(val);
                    }
                    SelectColumn::TableWildcard(tbl) => {
                        if tbl.eq_ignore_ascii_case(&s.name) {
                            if !group_rows.is_empty() {
                                result_row.extend(group_rows[0].clone());
                            } else {
                                result_row.extend(vec![Value::Null; s.columns.len()]);
                            }
                        }
                    }
                }
            }

            // HAVING 过滤
            if let Some(having_expr) = &stmt.having {
                // 为 HAVING 构建一个临时 schema，将聚合结果映射到列名
                let having_schema = TableSchema {
                    name: "__having__".into(),
                    columns: columns.iter().map(|name| crate::catalog::schema::ColumnSchema {
                        name: name.clone(),
                        data_type: crate::sql::ast::DataType::Text,
                        nullable: true,
                        primary_key: false,
                        unique: false,
                        autoincrement: false,
                        default_value: None,
                    }).collect(),
                    root_page: 0,
                    primary_key: Vec::new(),
                    auto_increment: 0,
                    row_count: 0,
                };
                let ctx = EvalContext::new(&result_row, &having_schema);
                // HAVING 中的聚合函数需要对分组数据重新计算
                let having_val = self.eval_having_expr(having_expr, group_rows, s, &ctx)?;
                if !having_val.is_truthy() {
                    continue;
                }
            }

            result_rows.push(result_row);
        }

        Ok((columns, result_rows))
    }

    /// 对 HAVING 子句中的表达式求值，支持其中包含聚合函数
    fn eval_having_expr(
        &self,
        expr: &Expr,
        group_rows: &[Row],
        schema: &TableSchema,
        ctx: &EvalContext,
    ) -> Result<Value> {
        match expr {
            Expr::Function { name, .. } if matches!(name.to_uppercase().as_str(), "COUNT"|"SUM"|"AVG"|"MIN"|"MAX") => {
                self.eval_aggregate(expr, group_rows, schema)
            }
            Expr::BinaryOp { left, op, right } => {
                let lv = self.eval_having_expr(left, group_rows, schema, ctx)?;
                let rv = self.eval_having_expr(right, group_rows, schema, ctx)?;
                super::eval::eval_binary_op_pub(&lv, op, &rv)
            }
            _ => eval_expr(expr, ctx),
        }
    }

    /// Compute (fixed_skip_bytes, remaining_cols_to_skip) for column access optimization.
    /// Scans columns before col_idx: accumulates byte offsets for fixed-width columns,
    /// stops at the first variable-width column.
    /// Returns (byte_offset_after_fixed_prefix, number_of_variable_cols_still_to_skip).
    fn compute_column_skip_bytes(table: &TableSchema, col_idx: usize) -> usize {
        let mut offset = 4usize; // num_cols header
        for i in 0..col_idx {
            if i >= table.columns.len() { return 0; }
            let fixed_size = match table.columns[i].data_type {
                DataType::Integer => 9,
                DataType::Real => 9,
                DataType::Boolean => 2,
                DataType::Null => 1,
                DataType::Text | DataType::Blob => {
                    // Can't pre-compute past a variable-width column.
                    // Return what we have so far (0 means "use dynamic skip for all").
                    // We return the offset to indicate "skip these fixed bytes, then
                    // dynamic-skip the remaining (col_idx - i) columns".
                    return 0;
                }
            };
            offset += 4 + fixed_size;
        }
        offset
    }

    /// Skip to a specific column in the raw row buffer and return a slice to its value data.
    /// Row format: [num_cols:u32][col0_len:u32][col0_data]...[colN_len:u32][colN_data]
    /// Value data format: [type_tag:u8][payload...]
    #[inline(always)]
    fn skip_to_column(buf: &[u8], col_idx: usize) -> Option<&[u8]> {
        if buf.len() < 4 { return None; }
        let n = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if col_idx >= n { return None; }
        let mut offset = 4usize;
        // Skip columns before the target
        for _ in 0..col_idx {
            if offset + 4 > buf.len() { return None; }
            let vlen = u32::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as usize;
            offset += 4 + vlen;
        }
        // Read the target column
        if offset + 4 > buf.len() { return None; }
        let vlen = u32::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as usize;
        offset += 4;
        if offset + vlen > buf.len() { return None; }
        Some(&buf[offset..offset + vlen])
    }

    /// Read a numeric value directly from raw value bytes without allocating.
    /// Returns (is_float, int_val, float_val). Returns None for NULL or non-numeric.
    #[inline(always)]
    #[allow(dead_code)]
    fn read_numeric_raw(val_data: &[u8]) -> Option<(bool, i64, f64)> {
        if val_data.len() < 9 {
            // Only NULL (tag 0, len 1) or Boolean (tag 1, len 2) are < 9 bytes
            return None;
        }
        match val_data[0] {
            2 => { // Integer
                let n = i64::from_le_bytes([
                    val_data[1], val_data[2], val_data[3], val_data[4],
                    val_data[5], val_data[6], val_data[7], val_data[8],
                ]);
                Some((false, n, n as f64))
            }
            3 => { // Real
                let f = f64::from_le_bytes([
                    val_data[1], val_data[2], val_data[3], val_data[4],
                    val_data[5], val_data[6], val_data[7], val_data[8],
                ]);
                Some((true, 0, f))
            }
            _ => None,
        }
    }

    /// Streaming aggregate: walks the B-Tree leaf chain, reads column values directly
    /// from raw page bytes without allocating Vec<Value> or Row. Zero heap allocation
    /// in the hot loop for numeric aggregates.
    fn eval_aggregate_streaming(
        func: &str,
        btree: &BTree,
        pool: &mut BufferPool,
        col_indices: &[usize],
        skip_bytes: usize,
    ) -> Result<Value> {
        let col_idx = col_indices[0];

        // Ultra-fast path for SUM/AVG: use dedicated btree method
        if matches!(func, "SUM" | "AVG") {
            let (sum_int, sum_float, has_float, count) = btree.sum_column_raw(pool, col_idx, skip_bytes)?;
            return match func {
                "SUM" => {
                    if count == 0 { Ok(Value::Null) }
                    else if has_float { Ok(Value::Real(sum_float + sum_int as f64)) }
                    else { Ok(Value::Integer(sum_int)) }
                }
                "AVG" => {
                    if count > 0 {
                        let total = if has_float { sum_float + sum_int as f64 } else { sum_int as f64 };
                        Ok(Value::Real(total / count as f64))
                    } else { Ok(Value::Null) }
                }
                _ => unreachable!(),
            };
        }

        // MIN/MAX path: use for_each_leaf_value with column extraction
        let mut min_val: Option<Value> = None;
        let mut max_val: Option<Value> = None;

        btree.for_each_leaf_value(pool, |val_bytes| {
            let val_data = match Self::skip_to_column(val_bytes, col_idx) {
                Some(d) => d,
                None => return,
            };
            if val_data.is_empty() || val_data[0] == 0 { return; } // NULL
            if let Ok((v, _)) = Value::deserialize(val_data) {
                if func == "MIN" {
                    min_val = Some(match min_val.take() {
                        None => v,
                        Some(m) => if v < m { v } else { m },
                    });
                } else {
                    max_val = Some(match max_val.take() {
                        None => v,
                        Some(m) => if v > m { v } else { m },
                    });
                }
            }
        })?;

        match func {
            "MIN" => Ok(min_val.unwrap_or(Value::Null)),
            "MAX" => Ok(max_val.unwrap_or(Value::Null)),
            _ => Err(Error::ExecutionError(format!("Unknown aggregate: {}", func))),
        }
    }

    /// Fused scan + filter + aggregate for `SELECT AGG(col) FROM t WHERE simple_cond`.
    /// Returns None if the pattern is not optimizable (falls back to general path).
    fn try_fused_aggregate_where(
        select_col: &SelectColumn,
        where_expr: &Expr,
        table: &TableSchema,
        pool: &mut BufferPool,
    ) -> Result<Option<ResultSet>> {
        // Extract the aggregate function, name, and column
        let (func_name, agg_args, alias) = match select_col {
            SelectColumn::Expr { expr, alias } => match expr {
                Expr::Function { name, args, .. } => {
                    let upper = name.to_uppercase();
                    if !matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                        return Ok(None);
                    }
                    (upper, args, alias)
                }
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        // For COUNT(*)/COUNT(1), agg_col_idx is None (count all passing rows)
        // For SUM(col)/etc, extract the column index
        let is_count_star = func_name == "COUNT"
            && (agg_args.is_empty()
                || matches!(agg_args.get(0), Some(Expr::Wildcard))
                || matches!(agg_args.get(0), Some(Expr::Literal(Literal::Integer(1)))));

        let agg_col_idx = if is_count_star {
            None
        } else if agg_args.len() == 1 {
            if let Expr::Column { name: col_name, .. } = &agg_args[0] {
                table.column_index(col_name)
            } else {
                return Ok(None); // Complex expression, can't optimize
            }
        } else {
            return Ok(None);
        };

        // Extract simple WHERE pattern: `col op literal` or `literal op col`
        let filter = match Self::extract_simple_filter(where_expr, table) {
            Some(f) => f,
            None => return Ok(None),
        };

        let btree = BTree::open(table.root_page);
        let col_name_display = alias.clone().unwrap_or_else(|| {
            if let SelectColumn::Expr { expr, .. } = select_col {
                Self::expr_display_name_static(expr)
            } else {
                String::new()
            }
        });

        // Walk leaves with fused filter + aggregate
        let result = Self::fused_scan_filter_aggregate(
            &func_name, agg_col_idx, &filter, &btree, pool, table,
        )?;

        Ok(Some(ResultSet {
            columns: vec![col_name_display],
            rows: vec![vec![result]],
            rows_affected: 0,
        }))
    }

    /// Try to extract `col op literal` or `literal op col` from a WHERE expression.
    fn extract_simple_filter(expr: &Expr, table: &TableSchema) -> Option<SimpleFilter> {
        if let Expr::BinaryOp { left, op, right } = expr {
            // Only handle comparison operators
            if !matches!(op,
                BinaryOp::Eq | BinaryOp::NotEq |
                BinaryOp::Lt | BinaryOp::Le |
                BinaryOp::Gt | BinaryOp::Ge
            ) {
                return None;
            }
            // Try col op literal
            if let (Expr::Column { name, .. }, Some(lit)) = (left.as_ref(), Self::expr_to_value(right)) {
                if let Some(idx) = table.column_index(name) {
                    return Some(SimpleFilter { col_idx: idx, op: op.clone(), literal: lit });
                }
            }
            // Try literal op col (flip the operator)
            if let (Some(lit), Expr::Column { name, .. }) = (Self::expr_to_value(left), right.as_ref()) {
                if let Some(idx) = table.column_index(name) {
                    let flipped_op = match op {
                        BinaryOp::Lt => BinaryOp::Gt,
                        BinaryOp::Le => BinaryOp::Ge,
                        BinaryOp::Gt => BinaryOp::Lt,
                        BinaryOp::Ge => BinaryOp::Le,
                        other => other.clone(),
                    };
                    return Some(SimpleFilter { col_idx: idx, op: flipped_op, literal: lit });
                }
            }
        }
        // AND of two simple filters — not handled yet, fall back
        None
    }

    /// Convert a literal expression to a Value.
    fn expr_to_value(expr: &Expr) -> Option<Value> {
        match expr {
            Expr::Literal(Literal::Integer(n)) => Some(Value::Integer(*n)),
            Expr::Literal(Literal::Float(f)) => Some(Value::Real(*f)),
            Expr::Literal(Literal::String(s)) => Some(Value::Text(s.clone())),
            Expr::Literal(Literal::Boolean(b)) => Some(Value::Boolean(*b)),
            Expr::Literal(Literal::Null) => Some(Value::Null),
            Expr::UnaryOp { op: UnaryOp::Neg, expr } => {
                match expr.as_ref() {
                    Expr::Literal(Literal::Integer(n)) => Some(Value::Integer(-n)),
                    Expr::Literal(Literal::Float(f)) => Some(Value::Real(-f)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Compare a Value against the filter literal using the given operator.
    #[inline(always)]
    fn filter_matches(val: &Value, op: &BinaryOp, literal: &Value) -> bool {
        match op {
            BinaryOp::Gt => val > literal,
            BinaryOp::Ge => val >= literal,
            BinaryOp::Lt => val < literal,
            BinaryOp::Le => val <= literal,
            BinaryOp::Eq => val == literal,
            BinaryOp::NotEq => val != literal,
            _ => false,
        }
    }

    /// Fused scan + filter + aggregate: walks leaf pages, applies filter on raw bytes,
    /// and accumulates the aggregate without materializing rows.
    fn fused_scan_filter_aggregate(
        func: &str,
        agg_col_idx: Option<usize>,
        filter: &SimpleFilter,
        btree: &BTree,
        pool: &mut BufferPool,
        _table: &TableSchema,
    ) -> Result<Value> {
        let filter_col_idx = filter.col_idx;

        // Check if we can use the raw integer fast path via btree-level fused scan:
        // filter literal is integer AND aggregate is COUNT/SUM/AVG on numeric column
        if let Value::Integer(lit_int) = &filter.literal {
            let filter_op_code = match &filter.op {
                BinaryOp::Gt => 0u8,
                BinaryOp::Ge => 1,
                BinaryOp::Lt => 2,
                BinaryOp::Le => 3,
                BinaryOp::Eq => 4,
                BinaryOp::NotEq => 5,
                _ => 255,
            };
            if filter_op_code < 255 && matches!(func, "COUNT" | "SUM" | "AVG") {
                let filter_skip = Self::compute_column_skip_bytes(_table, filter_col_idx);
                let agg_skip = agg_col_idx.map_or(0, |idx| Self::compute_column_skip_bytes(_table, idx));

                let (sum_int, sum_float, has_float, count) = btree.count_filtered_raw(
                    pool, filter_col_idx, filter_skip, filter_op_code, *lit_int,
                    agg_col_idx, agg_skip,
                )?;

                return match func {
                    "COUNT" => Ok(Value::Integer(count as i64)),
                    "SUM" => {
                        if count == 0 { Ok(Value::Null) }
                        else if has_float { Ok(Value::Real(sum_float + sum_int as f64)) }
                        else { Ok(Value::Integer(sum_int)) }
                    }
                    "AVG" => {
                        if count > 0 {
                            let total = if has_float { sum_float + sum_int as f64 } else { sum_int as f64 };
                            Ok(Value::Real(total / count as f64))
                        } else { Ok(Value::Null) }
                    }
                    _ => unreachable!(),
                };
            }
        }

        // General fallback: deserialize values for filter + aggregate
        let mut count: i64 = 0;
        let mut sum_int: i64 = 0;
        let mut sum_float: f64 = 0.0;
        let mut has_float = false;
        let mut min_val: Option<Value> = None;
        let mut max_val: Option<Value> = None;

        btree.for_each_leaf_value(pool, |row_bytes| {

            // Read filter column value from raw bytes
            let filter_data = match Self::skip_to_column(row_bytes, filter_col_idx) {
                Some(d) => d,
                None => return,
            };
            if filter_data.is_empty() { return; }
            let filter_val = match Value::deserialize(filter_data) {
                Ok((v, _)) => v,
                Err(_) => return,
            };

            // Apply WHERE filter
            if !Self::filter_matches(&filter_val, &filter.op, &filter.literal) {
                return;
            }

            // Row passed the filter — accumulate aggregate
            match func {
                "COUNT" => {
                    count += 1;
                }
                "SUM" | "AVG" => {
                    // If aggregating the same column as filter, reuse filter_val
                    let val = if agg_col_idx == Some(filter_col_idx) {
                        filter_val
                    } else if let Some(idx) = agg_col_idx {
                        match Self::skip_to_column(row_bytes, idx) {
                            Some(d) => match Value::deserialize(d) {
                                Ok((v, _)) => v,
                                Err(_) => return,
                            },
                            None => return,
                        }
                    } else {
                        return;
                    };
                    match val {
                        Value::Integer(n) => {
                            sum_int = sum_int.wrapping_add(n);
                            count += 1;
                        }
                        Value::Real(f) => {
                            sum_float += f;
                            has_float = true;
                            count += 1;
                        }
                        _ => {}
                    }
                }
                "MIN" | "MAX" => {
                    let val = if agg_col_idx == Some(filter_col_idx) {
                        filter_val
                    } else if let Some(idx) = agg_col_idx {
                        match Self::skip_to_column(row_bytes, idx) {
                            Some(d) => match Value::deserialize(d) {
                                Ok((v, _)) => v,
                                Err(_) => return,
                            },
                            None => return,
                        }
                    } else {
                        return;
                    };
                    if !val.is_null() {
                        if func == "MIN" {
                            min_val = Some(match min_val.take() {
                                None => val,
                                Some(m) => if val < m { val } else { m },
                            });
                        } else {
                            max_val = Some(match max_val.take() {
                                None => val,
                                Some(m) => if val > m { val } else { m },
                            });
                        }
                    }
                }
                _ => {}
            }
        })?;

        match func {
            "COUNT" => Ok(Value::Integer(count)),
            "SUM" => {
                if count == 0 { Ok(Value::Null) }
                else if has_float { Ok(Value::Real(sum_float + sum_int as f64)) }
                else { Ok(Value::Integer(sum_int)) }
            }
            "AVG" => {
                if count > 0 {
                    let total = if has_float { sum_float + sum_int as f64 } else { sum_int as f64 };
                    Ok(Value::Real(total / count as f64))
                } else { Ok(Value::Null) }
            }
            "MIN" => Ok(min_val.unwrap_or(Value::Null)),
            "MAX" => Ok(max_val.unwrap_or(Value::Null)),
            _ => Err(Error::ExecutionError(format!("Unknown aggregate: {}", func))),
        }
    }

    /// Try to resolve a simple column reference to a direct index.
    #[inline]
    fn resolve_column_index(arg: &Expr, schema: &TableSchema) -> Option<usize> {
        if let Expr::Column { name, .. } = arg {
            schema.column_index(name)
        } else {
            None
        }
    }

    /// Get a value from a row, either by direct index or via eval_expr.
    #[inline]
    fn get_agg_value(arg: &Expr, row: &Row, schema: &TableSchema, col_idx: Option<usize>) -> Result<Value> {
        if let Some(idx) = col_idx {
            Ok(row.get(idx).cloned().unwrap_or(Value::Null))
        } else {
            let ctx = EvalContext::new(row, schema);
            eval_expr(arg, &ctx)
        }
    }

    fn eval_aggregate(&self, expr: &Expr, rows: &[Row], schema: &TableSchema) -> Result<Value> {
        match expr {
            Expr::Function { name, args, distinct: _ } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => {
                        if args.is_empty() || matches!(args[0], Expr::Wildcard) {
                            return Ok(Value::Integer(rows.len() as i64));
                        }
                        let col_idx = Self::resolve_column_index(&args[0], schema);
                        let count = rows.iter().filter(|row| {
                            Self::get_agg_value(&args[0], row, schema, col_idx)
                                .map_or(false, |v| !v.is_null())
                        }).count();
                        Ok(Value::Integer(count as i64))
                    }
                    "SUM" => {
                        let arg = args.first().ok_or(Error::ExecutionError("SUM requires 1 arg".into()))?;
                        let col_idx = Self::resolve_column_index(arg, schema);
                        let mut sum_int: i64 = 0;
                        let mut sum_float: f64 = 0.0;
                        let mut has_float = false;
                        let mut has_value = false;
                        for row in rows {
                            if let Ok(v) = Self::get_agg_value(arg, row, schema, col_idx) {
                                match v {
                                    Value::Integer(n) => {
                                        sum_int = sum_int.saturating_add(n);
                                        sum_float += n as f64;
                                        has_value = true;
                                    }
                                    Value::Real(f) => {
                                        sum_float += f;
                                        has_float = true;
                                        has_value = true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        if !has_value { return Ok(Value::Null); }
                        if has_float { Ok(Value::Real(sum_float)) } else { Ok(Value::Integer(sum_int)) }
                    }
                    "AVG" => {
                        let arg = args.first().ok_or(Error::ExecutionError("AVG requires 1 arg".into()))?;
                        let col_idx = Self::resolve_column_index(arg, schema);
                        let mut sum = 0f64;
                        let mut count = 0usize;
                        for row in rows {
                            if let Ok(v) = Self::get_agg_value(arg, row, schema, col_idx) {
                                match v {
                                    Value::Integer(n) => { sum += n as f64; count += 1; }
                                    Value::Real(f) => { sum += f; count += 1; }
                                    _ => {}
                                }
                            }
                        }
                        if count > 0 { Ok(Value::Real(sum / count as f64)) } else { Ok(Value::Null) }
                    }
                    "MIN" => {
                        let arg = args.first().ok_or(Error::ExecutionError("MIN requires 1 arg".into()))?;
                        let col_idx = Self::resolve_column_index(arg, schema);
                        let mut min: Option<Value> = None;
                        for row in rows {
                            if let Ok(v) = Self::get_agg_value(arg, row, schema, col_idx) {
                                if !v.is_null() {
                                    min = Some(match min {
                                        None => v,
                                        Some(m) => if v < m { v } else { m },
                                    });
                                }
                            }
                        }
                        Ok(min.unwrap_or(Value::Null))
                    }
                    "MAX" => {
                        let arg = args.first().ok_or(Error::ExecutionError("MAX requires 1 arg".into()))?;
                        let col_idx = Self::resolve_column_index(arg, schema);
                        let mut max: Option<Value> = None;
                        for row in rows {
                            if let Ok(v) = Self::get_agg_value(arg, row, schema, col_idx) {
                                if !v.is_null() {
                                    max = Some(match max {
                                        None => v,
                                        Some(m) => if v > m { v } else { m },
                                    });
                                }
                            }
                        }
                        Ok(max.unwrap_or(Value::Null))
                    }
                    _ => {
                        // 非聚合函数：对第一行求值
                        if let Some(row) = rows.first() {
                            let ctx = EvalContext::new(row, schema);
                            eval_expr(expr, &ctx)
                        } else {
                            Ok(Value::Null)
                        }
                    }
                }
            }
            _ => {
                // 非聚合表达式：对第一行求值
                if let Some(row) = rows.first() {
                    let ctx = EvalContext::new(row, schema);
                    eval_expr(expr, &ctx)
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }

    fn project(
        &self,
        stmt: &SelectStatement,
        schema: Option<&TableSchema>,
        rows: Vec<Row>,
    ) -> Result<(Vec<String>, Vec<Row>)> {
        let dummy_schema = TableSchema {
            name: "__proj__".into(),
            columns: Vec::new(),
            root_page: 0,
            primary_key: Vec::new(),
            auto_increment: 0,
            row_count: 0,
        };
        let s = schema.unwrap_or(&dummy_schema);

        let mut columns: Vec<String> = Vec::new();
        let mut result_rows: Vec<Row> = Vec::new();

        // 确定列名
        for col in &stmt.columns {
            match col {
                SelectColumn::Wildcard => {
                    for c in &s.columns {
                        columns.push(c.name.clone());
                    }
                }
                SelectColumn::TableWildcard(tbl) => {
                    if tbl.eq_ignore_ascii_case(&s.name) {
                        for c in &s.columns {
                            columns.push(c.name.clone());
                        }
                    }
                }
                SelectColumn::Expr { expr, alias } => {
                    // Validate column existence early to return proper error
                    if alias.is_none() {
                        if let Expr::Column { name: col_name, table: tbl } = expr {
                            // Only validate if we have a real schema (not dummy)
                            if !s.columns.is_empty() {
                                if let Some(tbl_name) = tbl {
                                    if !tbl_name.eq_ignore_ascii_case(&s.name) {
                                        // Table qualifier doesn't match - skip validation
                                    } else if s.column_index(col_name).is_none() {
                                        return Err(Error::ColumnNotFound(col_name.clone(), s.name.clone()));
                                    }
                                } else if s.column_index(col_name).is_none() {
                                    return Err(Error::ColumnNotFound(col_name.clone(), s.name.clone()));
                                }
                            }
                        }
                    }
                    let name = alias.clone().unwrap_or_else(|| self.expr_display_name(expr));
                    columns.push(name);
                }
            }
        }

        // Fast path: SELECT * — avoid per-row copy when all columns are returned
        let is_select_star = stmt.columns.len() == 1
            && matches!(stmt.columns[0], SelectColumn::Wildcard);
        if is_select_star {
            return Ok((columns, rows));
        }

        // 投影每一行
        for row in rows {
            let ctx = EvalContext::new(&row, s);
            let mut result_row: Row = Vec::with_capacity(columns.len());

            for col in &stmt.columns {
                match col {
                    SelectColumn::Wildcard => {
                        result_row.extend(row.iter().cloned());
                    }
                    SelectColumn::TableWildcard(tbl) => {
                        if tbl.eq_ignore_ascii_case(&s.name) {
                            result_row.extend(row.iter().cloned());
                        }
                    }
                    SelectColumn::Expr { expr, .. } => {
                        result_row.push(eval_expr(expr, &ctx)?);
                    }
                }
            }
            result_rows.push(result_row);
        }

        Ok((columns, result_rows))
    }

    /// Sort rows using the projected schema (after SELECT projection/aggregation).
    /// Falls back to positional index if ORDER BY expr is a column name matching a projected column.
    fn sort_rows_with_fallback(
        &self,
        rows: &mut Vec<Row>,
        order_by: &[OrderByItem],
        proj_schema: &TableSchema,
        _orig_schema: Option<&TableSchema>,
        columns: &[String],
    ) -> Result<()> {
        rows.sort_by(|a, b| {
            for item in order_by {
                // Try evaluating against projected schema first
                let ctx_a = EvalContext::new(a, proj_schema);
                let ctx_b = EvalContext::new(b, proj_schema);
                let va = eval_expr(&item.expr, &ctx_a).unwrap_or(Value::Null);
                let vb = eval_expr(&item.expr, &ctx_b).unwrap_or(Value::Null);

                // If both are Null and the expr is a column name, try positional lookup
                let (va, vb) = if va.is_null() && vb.is_null() {
                    if let Expr::Column { name, .. } = &item.expr {
                        if let Some(idx) = columns.iter().position(|c| c.eq_ignore_ascii_case(name)) {
                            let va2 = a.get(idx).cloned().unwrap_or(Value::Null);
                            let vb2 = b.get(idx).cloned().unwrap_or(Value::Null);
                            (va2, vb2)
                        } else {
                            (va, vb)
                        }
                    } else {
                        (va, vb)
                    }
                } else {
                    (va, vb)
                };

                let ord = va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal);
                let ord = if item.asc { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
        Ok(())
    }

    /// Build a temporary TableSchema from projected column names, for ORDER BY evaluation.
    fn build_projected_schema(
        &self,
        columns: &[String],
        orig_schema: Option<&TableSchema>,
    ) -> Option<TableSchema> {
        if columns.is_empty() {
            return None;
        }
        let col_schemas: Vec<crate::catalog::schema::ColumnSchema> = columns.iter().map(|name| {
            // Try to find the original column type
            let data_type = orig_schema
                .and_then(|s| s.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name)))
                .map(|c| c.data_type.clone())
                .unwrap_or(crate::sql::ast::DataType::Text);
            crate::catalog::schema::ColumnSchema {
                name: name.clone(),
                data_type,
                nullable: true,
                primary_key: false,
                unique: false,
                autoincrement: false,
                default_value: None,
            }
        }).collect();
        Some(TableSchema {
            name: "__proj__".into(),
            columns: col_schemas,
            root_page: 0,
            primary_key: Vec::new(),
            auto_increment: 0,
            row_count: 0,
        })
    }

    /// Generate a human-readable display name for an expression (for column headers).
    fn expr_display_name(&self, expr: &Expr) -> String {
        Self::expr_display_name_static(expr)
    }

    fn expr_display_name_static(expr: &Expr) -> String {
        match expr {
            Expr::Column { name, table } => {
                if let Some(t) = table {
                    format!("{}.{}", t, name)
                } else {
                    name.clone()
                }
            }
            Expr::Function { name, args, .. } => {
                let args_str: Vec<String> = args.iter().map(Self::expr_display_name_static).collect();
                format!("{}({})", name.to_uppercase(), args_str.join(", "))
            }
            Expr::Wildcard => "*".to_string(),
            Expr::Literal(Literal::Integer(n)) => n.to_string(),
            Expr::Literal(Literal::Float(f)) => f.to_string(),
            Expr::Literal(Literal::String(s)) => s.clone(),
            Expr::Literal(Literal::Null) => "NULL".to_string(),
            Expr::Literal(Literal::Boolean(b)) => b.to_string(),
            Expr::BinaryOp { left, op, right } => {
                format!("{} {:?} {}", Self::expr_display_name_static(left), op, Self::expr_display_name_static(right))
            }
            _ => "expr".to_string(),
        }
    }

    fn eval_limit_offset(&self, expr: &Expr) -> Result<i64> {
        let dummy_schema = TableSchema {
            name: "__limit__".into(),
            columns: Vec::new(),
            root_page: 0,
            primary_key: Vec::new(),
            auto_increment: 0,
            row_count: 0,
        };
        let dummy_row: Row = Vec::new();
        let ctx = EvalContext::with_params(&dummy_row, &dummy_schema, self.params);
        match eval_expr(expr, &ctx)? {
            Value::Integer(n) => Ok(n),
            _ => Err(Error::ExecutionError("LIMIT/OFFSET must be an integer".into())),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UPDATE
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_update(&mut self, stmt: &UpdateStatement) -> Result<ResultSet> {
        let table = self.catalog.get_table(&stmt.table)?.clone();
        // Try PK point lookup for `WHERE pk_col = literal` before full scan.
        // We must use the actual stored key (not a recomputed one) for the B-Tree operation.
        let pairs = if let Some(pk_val) = extract_pk_value(&table, &stmt.where_clause) {
            let mut key = Vec::with_capacity(32);
            encode_value_for_key(&pk_val, &mut key);
            let btree = BTree::open(table.root_page);
            match btree.get(self.pool, &key)? {
                Some(v) => vec![(key, deserialize_row(&v)?)],
                None => Vec::new(),
            }
        } else {
            self.scan_table(&table)?
        };

        let mut updated = 0;
        let mut btree = BTree::open(table.root_page);

        for (original_key, row) in pairs {
            let ctx = EvalContext::with_params(&row, &table, self.params);

            // WHERE 过滤
            if let Some(where_expr) = &stmt.where_clause {
                if !eval_expr(where_expr, &ctx)?.is_truthy() {
                    continue;
                }
            }

            // 应用更新
            let mut new_row = row.clone();
            for assignment in &stmt.assignments {
                let col_idx = table.column_index(&assignment.column)
                    .ok_or_else(|| Error::ColumnNotFound(
                        assignment.column.clone(), stmt.table.clone()
                    ))?;
                let val = eval_expr(&assignment.value, &ctx)?;
                new_row[col_idx] = val;
            }

            // Compute new key using current encoding
            let new_pk = self.make_pk_key(&table, &new_row)?;

            // If primary key changed, delete the old entry first
            if original_key != new_pk {
                btree.delete(self.pool, &original_key)?;
                let value = serialize_row(&new_row);
                btree.upsert(self.pool, new_pk, value)?;
            } else {
                // Same key: update in-place using the original key
                let value = serialize_row(&new_row);
                btree.upsert(self.pool, original_key, value)?;
            }
            updated += 1;
        }

        // Sync B-Tree root back to catalog (root may have changed due to splits)
        if let Some(t) = self.catalog.tables.get_mut(&stmt.table.to_lowercase()) {
            t.root_page = btree.root_id;
        }

        Ok(ResultSet::affected(updated))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DELETE
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_delete(&mut self, stmt: &DeleteStatement) -> Result<ResultSet> {
        let table = self.catalog.get_table(&stmt.table)?.clone();
        // Try PK point lookup for `WHERE pk_col = literal` before full scan.
        let pairs = if let Some(pk_val) = extract_pk_value(&table, &stmt.where_clause) {
            let mut key = Vec::with_capacity(32);
            encode_value_for_key(&pk_val, &mut key);
            let btree = BTree::open(table.root_page);
            match btree.get(self.pool, &key)? {
                Some(v) => vec![(key, deserialize_row(&v)?)],
                None => Vec::new(),
            }
        } else {
            // Use scan_table to get (original_key, row) pairs so we delete using
            // the actual stored key rather than a recomputed one.
            self.scan_table(&table)?
        };

        let mut deleted = 0;
        let mut btree = BTree::open(table.root_page);
        let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();

        for (original_key, row) in &pairs {
            let ctx = EvalContext::with_params(row, &table, self.params);
            if let Some(where_expr) = &stmt.where_clause {
                if !eval_expr(where_expr, &ctx)?.is_truthy() {
                    continue;
                }
            }
            keys_to_delete.push(original_key.clone());
        }

        for key in keys_to_delete {
            btree.delete(self.pool, &key)?;
            deleted += 1;
        }

        // Sync B-Tree root back to catalog (root may have changed)
        if let Some(t) = self.catalog.tables.get_mut(&stmt.table.to_lowercase()) {
            t.root_page = btree.root_id;
            t.row_count = t.row_count.saturating_sub(deleted as u64);
        }

        Ok(ResultSet::affected(deleted))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EXPLAIN
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_explain(&mut self, stmt: &Statement) -> Result<ResultSet> {
        let plan = format!("{:#?}", stmt);
        Ok(ResultSet {
            columns: vec!["plan".to_string()],
            rows: vec![vec![Value::Text(plan)]],
            rows_affected: 0,
        })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PRAGMA
    // ─────────────────────────────────────────────────────────────────────────

    fn execute_pragma(&mut self, stmt: &PragmaStatement) -> Result<ResultSet> {
        match stmt.name.to_lowercase().as_str() {
            "table_info" => {
                let table_name = if let Some(expr) = &stmt.value {
                    let dummy_schema = TableSchema { name: "__pragma__".into(), columns: vec![], root_page: 0, primary_key: vec![], auto_increment: 0, row_count: 0 };
                    let dummy_row = vec![];
                    let ctx = EvalContext::new(&dummy_row, &dummy_schema);
                    match eval_expr(expr, &ctx)? {
                        Value::Text(s) => s,
                        v => return Err(Error::ExecutionError(format!("Expected string for table name, got {}", v.type_name())))
                    }
                } else {
                    return Err(Error::ExecutionError("PRAGMA table_info requires a table name".into()));
                };

                let table = self.catalog.get_table(&table_name)?;
                let rows: Vec<Row> = table.columns.iter().enumerate().map(|(i, col)| {
                    vec![
                        Value::Integer(i as i64),
                        Value::Text(col.name.clone()),
                        Value::Text(col.data_type.to_string()),
                        Value::Boolean(!col.nullable),
                        Value::Null, // Default value not implemented yet
                        Value::Boolean(col.primary_key),
                    ]
                }).collect();
                return Ok(ResultSet {
                    columns: vec!["cid".into(), "name".into(), "type".into(),
                                  "notnull".into(), "dflt_value".into(), "pk".into()],
                    rows,
                    rows_affected: 0,
                });
            }
            "tables" => {
                let names = self.catalog.table_names();
                let rows = names.into_iter()
                    .map(|n| vec![Value::Text(n)])
                    .collect();
                Ok(ResultSet {
                    columns: vec!["name".into()],
                    rows,
                    rows_affected: 0,
                })
            }
            "page_count" => {
                let n = self.pool.num_disk_pages();
                Ok(ResultSet {
                    columns: vec!["page_count".into()],
                    rows: vec![vec![Value::Integer(n as i64)]],
                    rows_affected: 0,
                })
            }
            _ => Ok(ResultSet::empty()),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 辅助方法
    // ─────────────────────────────────────────────────────────────────────────

    /// 全表扫描，返回 (pk_key, row) 对
    fn scan_table(&mut self, table: &TableSchema) -> Result<Vec<(Vec<u8>, Row)>> {
        let btree = BTree::open(table.root_page);
        let pairs = btree.scan_all(self.pool)?;
        pairs.into_iter().map(|(k, v)| {
            let row = deserialize_row(&v)?;
            Ok((k, row))
        }).collect()
    }

    /// 全表扫描，只返回行（zero-alloc leaf walk, no key/value cloning）
    fn scan_table_rows(&mut self, table: &TableSchema) -> Result<Vec<Row>> {
        let btree = BTree::open(table.root_page);
        let mut rows = Vec::new();
        let mut err: Option<Error> = None;
        btree.for_each_leaf_value(self.pool, |val_bytes| {
            if err.is_none() {
                match deserialize_row(val_bytes) {
                    Ok(row) => rows.push(row),
                    Err(e) => err = Some(e),
                }
            }
        })?;
        if let Some(e) = err {
            return Err(e);
        }
        Ok(rows)
    }

    /// Scan table rows, deserializing only the specified columns.
    /// `col_indices` must be sorted.
    #[allow(dead_code)]
    fn scan_table_rows_projected(&mut self, table: &TableSchema, col_indices: &[usize]) -> Result<Vec<Row>> {
        let btree = BTree::open(table.root_page);
        let mut rows = Vec::new();
        let mut err: Option<Error> = None;
        btree.for_each_leaf_value(self.pool, |val_bytes| {
            if err.is_none() {
                match deserialize_row_projected(val_bytes, col_indices) {
                    Ok(row) => rows.push(row),
                    Err(e) => err = Some(e),
                }
            }
        })?;
        if let Some(e) = err {
            return Err(e);
        }
        Ok(rows)
    }

    /// 构建主键字节序列（用于 B-Tree 键）
    fn make_pk_key(&self, table: &TableSchema, row: &Row) -> Result<Vec<u8>> {
        if table.primary_key.is_empty() {
            // 无主键：使用行的完整序列化作为键（行 ID 模式）
            return Ok(serialize_row(row));
        }
        let mut key = Vec::with_capacity(32);
        for pk_col in &table.primary_key {
            let idx = table.column_index(pk_col)
                .ok_or_else(|| Error::ColumnNotFound(pk_col.clone(), table.name.clone()))?;
            // 使用保序编码（order-preserving encoding）确保 B-Tree 字节比较与值比较一致
            encode_value_for_key(&row[idx], &mut key);
        }
        Ok(key)
    }

    /// 构建索引键
    fn make_index_key(&self, row: &Row, col_indices: &[usize]) -> Vec<u8> {
        let mut key = Vec::new();
        for &idx in col_indices {
            key.extend_from_slice(&row[idx].serialize());
        }
        key
    }
}
