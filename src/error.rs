use thiserror::Error;

/// RobotDB 统一错误类型
#[derive(Debug, Error)]
pub enum Error {
    // ── I/O 层 ──────────────────────────────────────────────────────────────
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    // ── 存储引擎 ─────────────────────────────────────────────────────────────
    #[error("Invalid page id: {0}")]
    InvalidPageId(u32),

    #[error("Page {0} is not in buffer pool")]
    PageNotInBuffer(u32),

    #[error("Buffer pool is full")]
    BufferPoolFull,

    #[error("Corrupt page: {0}")]
    CorruptPage(String),

    #[error("Database file is corrupt: {0}")]
    CorruptDatabase(String),

    // ── B-Tree ────────────────────────────────────────────────────────────────
    #[error("Key not found")]
    KeyNotFound,

    #[error("Duplicate key")]
    DuplicateKey,

    #[error("B-Tree node overflow")]
    NodeOverflow,

    // ── SQL 解析 ──────────────────────────────────────────────────────────────
    #[error("Parse error at position {pos}: {msg}")]
    ParseError { pos: usize, msg: String },

    #[error("Unexpected token: '{0}'")]
    UnexpectedToken(String),

    #[error("Unexpected end of input")]
    UnexpectedEof,

    // ── 类型系统 ──────────────────────────────────────────────────────────────
    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("Null value violation on column '{0}'")]
    NullViolation(String),

    #[error("Value out of range for type {0}")]
    ValueOutOfRange(String),

    // ── Schema / Catalog ──────────────────────────────────────────────────────
    #[error("Table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Column '{0}' not found in table '{1}'")]
    ColumnNotFound(String, String),

    #[error("Index '{0}' already exists")]
    IndexAlreadyExists(String),

    #[error("Index '{0}' not found")]
    IndexNotFound(String),

    // ── 事务 ──────────────────────────────────────────────────────────────────
    #[error("No active transaction")]
    NoActiveTransaction,

    #[error("Transaction already active")]
    TransactionAlreadyActive,

    #[error("Transaction conflict: write-write conflict detected")]
    TransactionConflict,

    #[error("Deadlock detected")]
    Deadlock,

    // ── 执行器 ────────────────────────────────────────────────────────────────
    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Division by zero")]
    DivisionByZero,

    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("UNIQUE constraint failed: {0}.{1}")]
    UniqueConstraintViolation(String, String),

    // ── WAL ───────────────────────────────────────────────────────────────────
    #[error("WAL error: {0}")]
    WalError(String),

    #[error("Checksum mismatch in WAL record")]
    WalChecksumMismatch,

    // ── 序列化 ────────────────────────────────────────────────────────────────
    #[error("Serialization error: {0}")]
    SerializationError(String),

    // ── 通用 ──────────────────────────────────────────────────────────────────
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::SerializationError(e.to_string())
    }
}
