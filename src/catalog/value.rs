/// 运行时值类型系统
///
/// 支持 SQLite 的动态类型系统（Type Affinity）：
/// NULL < INTEGER < REAL < TEXT < BLOB

use std::cmp::Ordering;
use crate::error::{Error, Result};
use crate::sql::ast::DataType;

/// 运行时值
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
}

impl Value {
    /// 获取值的类型名称
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Integer(_) => "INTEGER",
            Value::Real(_) => "REAL",
            Value::Text(_) => "TEXT",
            Value::Blob(_) => "BLOB",
            Value::Boolean(_) => "BOOLEAN",
        }
    }

    /// 判断是否为 NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// 转换为布尔值（用于 WHERE 条件判断）
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Boolean(b) => *b,
            Value::Integer(n) => *n != 0,
            Value::Real(f) => *f != 0.0,
            Value::Text(s) => !s.is_empty(),
            Value::Blob(b) => !b.is_empty(),
        }
    }

    /// Returns true if this value already matches the target type (no cast needed).
    #[inline]
    pub fn matches_type(&self, target: &DataType) -> bool {
        matches!(
            (self, target),
            (Value::Integer(_), DataType::Integer)
                | (Value::Real(_), DataType::Real)
                | (Value::Text(_), DataType::Text)
                | (Value::Blob(_), DataType::Blob)
                | (Value::Boolean(_), DataType::Boolean)
                | (Value::Null, _)
        )
    }

    /// 强制转换为目标类型
    pub fn cast(&self, target: &DataType) -> Result<Value> {
        match target {
            DataType::Integer => match self {
                Value::Integer(n) => Ok(Value::Integer(*n)),
                Value::Real(f) => Ok(Value::Integer(*f as i64)),
                Value::Text(s) => s.parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|_| Error::TypeMismatch {
                        expected: "INTEGER".into(),
                        got: format!("TEXT({})", s),
                    }),
                Value::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
                Value::Null => Ok(Value::Null),
                Value::Blob(_) => Err(Error::TypeMismatch {
                    expected: "INTEGER".into(),
                    got: "BLOB".into(),
                }),
            },
            DataType::Real => match self {
                Value::Real(f) => Ok(Value::Real(*f)),
                Value::Integer(n) => Ok(Value::Real(*n as f64)),
                Value::Text(s) => s.parse::<f64>()
                    .map(Value::Real)
                    .map_err(|_| Error::TypeMismatch {
                        expected: "REAL".into(),
                        got: format!("TEXT({})", s),
                    }),
                Value::Boolean(b) => Ok(Value::Real(if *b { 1.0 } else { 0.0 })),
                Value::Null => Ok(Value::Null),
                Value::Blob(_) => Err(Error::TypeMismatch {
                    expected: "REAL".into(),
                    got: "BLOB".into(),
                }),
            },
            DataType::Text => match self {
                Value::Text(s) => Ok(Value::Text(s.clone())),
                Value::Integer(n) => Ok(Value::Text(n.to_string())),
                Value::Real(f) => Ok(Value::Text(f.to_string())),
                Value::Boolean(b) => Ok(Value::Text(b.to_string())),
                Value::Null => Ok(Value::Null),
                Value::Blob(b) => Ok(Value::Text(String::from_utf8_lossy(b).into_owned())),
            },
            DataType::Boolean => match self {
                Value::Boolean(b) => Ok(Value::Boolean(*b)),
                Value::Integer(n) => Ok(Value::Boolean(*n != 0)),
                Value::Real(f) => Ok(Value::Boolean(*f != 0.0)),
                Value::Text(s) => match s.to_lowercase().as_str() {
                    "true" | "1" | "yes" => Ok(Value::Boolean(true)),
                    "false" | "0" | "no" => Ok(Value::Boolean(false)),
                    _ => Err(Error::TypeMismatch {
                        expected: "BOOLEAN".into(),
                        got: format!("TEXT({})", s),
                    }),
                },
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "BOOLEAN".into(),
                    got: self.type_name().into(),
                }),
            },
            DataType::Blob => match self {
                Value::Blob(b) => Ok(Value::Blob(b.clone())),
                Value::Text(s) => Ok(Value::Blob(s.as_bytes().to_vec())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "BLOB".into(),
                    got: self.type_name().into(),
                }),
            },
            DataType::Null => Ok(Value::Null),
        }
    }

    /// Returns the serialized byte length without allocating.
    pub fn serialized_size(&self) -> usize {
        match self {
            Value::Null => 1,
            Value::Boolean(_) => 2,
            Value::Integer(_) => 9,
            Value::Real(_) => 9,
            Value::Text(s) => 1 + 4 + s.len(),
            Value::Blob(b) => 1 + 4 + b.len(),
        }
    }

    /// Append the serialized bytes of this value to `buf` (zero intermediate allocations).
    #[inline]
    pub fn write_to(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Null => buf.push(0),
            Value::Boolean(b) => {
                buf.push(1);
                buf.push(if *b { 1 } else { 0 });
            }
            Value::Integer(n) => {
                buf.push(2);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Real(f) => {
                buf.push(3);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::Text(s) => {
                buf.push(4);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Blob(b) => {
                buf.push(5);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
    }

    /// 序列化为字节（用于 B-Tree 存储）
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.serialized_size());
        self.write_to(&mut buf);
        buf
    }

    /// 从字节反序列化
    pub fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(Error::SerializationError("Empty buffer".into()));
        }
        match buf[0] {
            0 => Ok((Value::Null, 1)),
            1 => {
                if buf.len() < 2 {
                    return Err(Error::SerializationError("Boolean too short".into()));
                }
                Ok((Value::Boolean(buf[1] != 0), 2))
            }
            2 => {
                if buf.len() < 9 {
                    return Err(Error::SerializationError("Integer too short".into()));
                }
                let n = i64::from_le_bytes(buf[1..9].try_into().unwrap());
                Ok((Value::Integer(n), 9))
            }
            3 => {
                if buf.len() < 9 {
                    return Err(Error::SerializationError("Real too short".into()));
                }
                let f = f64::from_le_bytes(buf[1..9].try_into().unwrap());
                Ok((Value::Real(f), 9))
            }
            4 => {
                if buf.len() < 5 {
                    return Err(Error::SerializationError("Text length too short".into()));
                }
                let len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
                if buf.len() < 5 + len {
                    return Err(Error::SerializationError("Text data too short".into()));
                }
                let s = String::from_utf8(buf[5..5 + len].to_vec())
                    .map_err(|e| Error::SerializationError(e.to_string()))?;
                Ok((Value::Text(s), 5 + len))
            }
            5 => {
                if buf.len() < 5 {
                    return Err(Error::SerializationError("Blob length too short".into()));
                }
                let len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
                if buf.len() < 5 + len {
                    return Err(Error::SerializationError("Blob data too short".into()));
                }
                Ok((Value::Blob(buf[5..5 + len].to_vec()), 5 + len))
            }
            t => Err(Error::SerializationError(format!("Unknown type tag: {}", t))),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Real(a), Value::Real(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Real(b)) => (*a as f64).partial_cmp(b),
            (Value::Real(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(n) => write!(f, "{}", n),
            Value::Real(r) => write!(f, "{}", r),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "<BLOB {} bytes>", b.len()),
        }
    }
}

/// 行（一组有序的值）
pub type Row = Vec<Value>;

/// Serialize a row into an existing buffer (clears it first). Zero intermediate allocations.
pub fn serialize_row_into(row: &Row, buf: &mut Vec<u8>) {
    let total = 4 + row.iter().map(|v| 4 + v.serialized_size()).sum::<usize>();
    buf.clear();
    buf.reserve(total);
    buf.extend_from_slice(&(row.len() as u32).to_le_bytes());
    for val in row {
        let size = val.serialized_size() as u32;
        buf.extend_from_slice(&size.to_le_bytes());
        val.write_to(buf);
    }
}

/// 序列化一行数据
pub fn serialize_row(row: &Row) -> Vec<u8> {
    let mut buf = Vec::new();
    serialize_row_into(row, &mut buf);
    buf
}

/// 反序列化一行数据
pub fn deserialize_row(buf: &[u8]) -> Result<Row> {
    if buf.len() < 4 {
        return Err(Error::SerializationError("Row too short".into()));
    }
    let n = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut row = Vec::with_capacity(n);
    let mut offset = 4;
    for _ in 0..n {
        if offset + 4 > buf.len() {
            return Err(Error::SerializationError("Row value length out of bounds".into()));
        }
        let vlen = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + vlen > buf.len() {
            return Err(Error::SerializationError("Row value data out of bounds".into()));
        }
        let (val, _) = Value::deserialize(&buf[offset..offset + vlen])?;
        row.push(val);
        offset += vlen;
    }
    Ok(row)
}

/// Deserialize only the columns at the given indices from a row buffer.
/// `col_indices` must be sorted. Columns not in the set are skipped without parsing.
/// Returns a Row containing only the requested columns (in the order given by col_indices).
pub fn deserialize_row_projected(buf: &[u8], col_indices: &[usize]) -> Result<Row> {
    if buf.len() < 4 {
        return Err(Error::SerializationError("Row too short".into()));
    }
    let n = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut row = Vec::with_capacity(col_indices.len());
    let mut offset = 4;
    let mut idx_pos = 0; // pointer into col_indices
    for col in 0..n {
        if offset + 4 > buf.len() {
            return Err(Error::SerializationError("Row value length out of bounds".into()));
        }
        let vlen = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + vlen > buf.len() {
            return Err(Error::SerializationError("Row value data out of bounds".into()));
        }
        if idx_pos < col_indices.len() && col_indices[idx_pos] == col {
            let (val, _) = Value::deserialize(&buf[offset..offset + vlen])?;
            row.push(val);
            idx_pos += 1;
            if idx_pos >= col_indices.len() {
                // All requested columns found — early exit
                break;
            }
        }
        offset += vlen;
    }
    Ok(row)
}
