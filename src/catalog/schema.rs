/// Schema 与 Catalog 管理
///
/// Catalog 存储所有表的元数据（列定义、约束、索引等），
/// 持久化在数据库文件的 Schema 表中（类似 SQLite 的 sqlite_master）。

use std::collections::HashMap;
use crate::error::{Error, Result};
use crate::sql::ast::{DataType, ColumnConstraint, TableConstraint};

/// 列定义（运行时）
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub autoincrement: bool,
    pub unique: bool,
    pub default_value: Option<String>,
}

impl ColumnSchema {
    pub fn from_ast(col: &crate::sql::ast::ColumnDef) -> Self {
        let mut nullable = true;
        let mut primary_key = false;
        let mut autoincrement = false;
        let mut unique = false;
        let mut default_value = None;

        for constraint in &col.constraints {
            match constraint {
                ColumnConstraint::PrimaryKey { autoincrement: ai } => {
                    primary_key = true;
                    autoincrement = *ai;
                    nullable = false;
                }
                ColumnConstraint::NotNull => nullable = false,
                ColumnConstraint::Unique => unique = true,
                ColumnConstraint::Default(expr) => {
                    default_value = Some(format!("{:?}", expr));
                }
                _ => {}
            }
        }

        Self {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            nullable,
            primary_key,
            autoincrement,
            unique,
            default_value,
        }
    }
}

/// 索引定义
#[derive(Debug, Clone)]
pub struct IndexSchema {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    /// B-Tree 根页 ID
    pub root_page: u32,
}

/// 表定义
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    /// B-Tree 根页 ID（存储表数据）
    pub root_page: u32,
    /// 主键列名（可能是复合主键）
    pub primary_key: Vec<String>,
    /// 自增计数器
    pub auto_increment: i64,
    /// Cached row count for O(1) COUNT(*)
    pub row_count: u64,
}

impl TableSchema {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn pk_column_index(&self) -> Option<usize> {
        self.primary_key.first().and_then(|pk| self.column_index(pk))
    }
}

/// Catalog：所有表和索引的内存注册表
#[derive(Clone)]
pub struct Catalog {
    pub tables: HashMap<String, TableSchema>,
    pub indexes: HashMap<String, IndexSchema>,
    /// 表名 -> 索引名列表
    pub table_indexes: HashMap<String, Vec<String>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            table_indexes: HashMap::new(),
        }
    }

    /// 注册新表
    pub fn create_table(
        &mut self,
        stmt: &crate::sql::ast::CreateTableStatement,
        root_page: u32,
    ) -> Result<()> {
        let name_lower = stmt.name.to_lowercase();
        if self.tables.contains_key(&name_lower) {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(Error::TableAlreadyExists(stmt.name.clone()));
        }

        let mut columns: Vec<ColumnSchema> = stmt.columns
            .iter()
            .map(ColumnSchema::from_ast)
            .collect();

        // 处理表级主键约束
        let mut primary_key: Vec<String> = Vec::new();
        for tc in &stmt.constraints {
            match tc {
                TableConstraint::PrimaryKey(cols) => {
                    primary_key = cols.clone();
                    // 标记对应列
                    for col_name in cols {
                        if let Some(col) = columns.iter_mut()
                            .find(|c| c.name.eq_ignore_ascii_case(col_name))
                        {
                            col.primary_key = true;
                            col.nullable = false;
                        }
                    }
                }
                TableConstraint::Unique(cols) => {
                    for col_name in cols {
                        if let Some(col) = columns.iter_mut()
                            .find(|c| c.name.eq_ignore_ascii_case(col_name))
                        {
                            col.unique = true;
                        }
                    }
                }
                _ => {}
            }
        }

        // 从列级约束中提取主键
        if primary_key.is_empty() {
            for col in &columns {
                if col.primary_key {
                    primary_key.push(col.name.clone());
                }
            }
        }

        self.tables.insert(name_lower.clone(), TableSchema {
            name: stmt.name.clone(),
            columns,
            root_page,
            primary_key,
            auto_increment: 0,
            row_count: 0,
        });
        self.table_indexes.insert(name_lower, Vec::new());
        Ok(())
    }

    /// 删除表
    pub fn drop_table(&mut self, name: &str, if_exists: bool) -> Result<()> {
        let name_lower = name.to_lowercase();
        if self.tables.remove(&name_lower).is_none() {
            if if_exists {
                return Ok(());
            }
            return Err(Error::TableNotFound(name.to_string()));
        }
        // 删除关联索引
        if let Some(idx_names) = self.table_indexes.remove(&name_lower) {
            for idx_name in idx_names {
                self.indexes.remove(&idx_name.to_lowercase());
            }
        }
        Ok(())
    }

    /// 获取表定义
    pub fn get_table(&self, name: &str) -> Result<&TableSchema> {
        self.tables.get(&name.to_lowercase())
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut TableSchema> {
        self.tables.get_mut(&name.to_lowercase())
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    /// 注册索引
    pub fn create_index(
        &mut self,
        stmt: &crate::sql::ast::CreateIndexStatement,
        root_page: u32,
    ) -> Result<()> {
        let idx_name_lower = stmt.name.to_lowercase();
        if self.indexes.contains_key(&idx_name_lower) {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(Error::IndexAlreadyExists(stmt.name.clone()));
        }
        // 验证表存在
        let table_lower = stmt.table.to_lowercase();
        if !self.tables.contains_key(&table_lower) {
            return Err(Error::TableNotFound(stmt.table.clone()));
        }

        self.indexes.insert(idx_name_lower.clone(), IndexSchema {
            name: stmt.name.clone(),
            table: stmt.table.clone(),
            columns: stmt.columns.clone(),
            unique: stmt.unique,
            root_page,
        });
        self.table_indexes
            .entry(table_lower)
            .or_default()
            .push(idx_name_lower);
        Ok(())
    }

    /// 列出所有表名
    pub fn table_names(&self) -> Vec<String> {
        self.tables.values().map(|t| t.name.clone()).collect()
    }

    /// 序列化 Catalog 到字节（用于持久化）
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // 简化实现：使用 JSON 风格文本序列化
        let n_tables = self.tables.len() as u32;
        buf.extend_from_slice(&n_tables.to_le_bytes());
        for (_, table) in &self.tables {
            let json = self.serialize_table(table);
            buf.extend_from_slice(&(json.len() as u32).to_le_bytes());
            buf.extend_from_slice(json.as_bytes());
        }
        let n_indexes = self.indexes.len() as u32;
        buf.extend_from_slice(&n_indexes.to_le_bytes());
        for (_, idx) in &self.indexes {
            let json = self.serialize_index(idx);
            buf.extend_from_slice(&(json.len() as u32).to_le_bytes());
            buf.extend_from_slice(json.as_bytes());
        }
        buf
    }

    fn serialize_table(&self, t: &TableSchema) -> String {
        let cols: Vec<String> = t.columns.iter().map(|c| {
            format!("{}:{}:{}:{}:{}:{}",
                c.name, c.data_type, c.nullable, c.primary_key, c.autoincrement, c.unique)
        }).collect();
        format!("{}|{}|{}|{}|{}|{}",
            t.name, t.root_page, t.auto_increment,
            t.primary_key.join(","),
            cols.join(";"),
            t.row_count)
    }

    fn serialize_index(&self, idx: &IndexSchema) -> String {
        format!("{}|{}|{}|{}|{}",
            idx.name, idx.table, idx.root_page, idx.unique,
            idx.columns.join(","))
    }

    /// 从字节反序列化 Catalog
    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        let mut catalog = Self::new();
        if buf.len() < 4 {
            return Ok(catalog);
        }
        let mut offset = 0;

        let n_tables = u32::from_le_bytes(buf[offset..offset+4].try_into().unwrap()) as usize;
        offset += 4;
        for _ in 0..n_tables {
            if offset + 4 > buf.len() { break; }
            let len = u32::from_le_bytes(buf[offset..offset+4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + len > buf.len() { break; }
            let s = std::str::from_utf8(&buf[offset..offset+len])
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            offset += len;
            if let Some(table) = Self::parse_table(s) {
                let name_lower = table.name.to_lowercase();
                catalog.table_indexes.insert(name_lower.clone(), Vec::new());
                catalog.tables.insert(name_lower, table);
            }
        }

        if offset + 4 > buf.len() { return Ok(catalog); }
        let n_indexes = u32::from_le_bytes(buf[offset..offset+4].try_into().unwrap()) as usize;
        offset += 4;
        for _ in 0..n_indexes {
            if offset + 4 > buf.len() { break; }
            let len = u32::from_le_bytes(buf[offset..offset+4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + len > buf.len() { break; }
            let s = std::str::from_utf8(&buf[offset..offset+len])
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            offset += len;
            if let Some(idx) = Self::parse_index(s) {
                let table_lower = idx.table.to_lowercase();
                let idx_lower = idx.name.to_lowercase();
                catalog.table_indexes.entry(table_lower).or_default().push(idx_lower.clone());
                catalog.indexes.insert(idx_lower, idx);
            }
        }

        Ok(catalog)
    }

    fn parse_table(s: &str) -> Option<TableSchema> {
        let parts: Vec<&str> = s.splitn(6, '|').collect();
        if parts.len() < 5 { return None; }
        let name = parts[0].to_string();
        let root_page: u32 = parts[1].parse().ok()?;
        let auto_increment: i64 = parts[2].parse().ok()?;
        let primary_key: Vec<String> = if parts[3].is_empty() {
            Vec::new()
        } else {
            parts[3].split(',').map(|s| s.to_string()).collect()
        };
        let columns: Vec<ColumnSchema> = parts[4].split(';').filter_map(|cs| {
            let cp: Vec<&str> = cs.splitn(6, ':').collect();
            if cp.len() < 6 { return None; }
            Some(ColumnSchema {
                name: cp[0].to_string(),
                data_type: match cp[1] {
                    "INTEGER" => DataType::Integer,
                    "REAL" => DataType::Real,
                    "BLOB" => DataType::Blob,
                    "BOOLEAN" => DataType::Boolean,
                    _ => DataType::Text,
                },
                nullable: cp[2] == "true",
                primary_key: cp[3] == "true",
                autoincrement: cp[4] == "true",
                unique: cp[5] == "true",
                default_value: None,
            })
        }).collect();

        // 6th field is row_count (added later; default 0 for old DBs)
        let row_count: u64 = parts.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);

        Some(TableSchema { name, columns, root_page, primary_key, auto_increment, row_count })
    }

    fn parse_index(s: &str) -> Option<IndexSchema> {
        let parts: Vec<&str> = s.splitn(5, '|').collect();
        if parts.len() < 5 { return None; }
        Some(IndexSchema {
            name: parts[0].to_string(),
            table: parts[1].to_string(),
            root_page: parts[2].parse().ok()?,
            unique: parts[3] == "true",
            columns: parts[4].split(',').map(|s| s.to_string()).collect(),
        })
    }
}
