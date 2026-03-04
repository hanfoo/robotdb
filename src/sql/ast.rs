/// SQL 抽象语法树（AST）定义

// ─────────────────────────────────────────────────────────────────────────────
// 顶层语句
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    AlterTable(AlterTableStatement),
    Begin,
    Commit,
    Rollback,
    Explain(Box<Statement>),
    Pragma(PragmaStatement),
    Vacuum,
}

// ─────────────────────────────────────────────────────────────────────────────
// SELECT
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    pub from: Option<TableRef>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    /// SELECT *
    Wildcard,
    /// SELECT table.*
    TableWildcard(String),
    /// SELECT expr [AS alias]
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableRef,
    pub condition: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub asc: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// INSERT
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
}

#[derive(Debug, Clone)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Select(Box<SelectStatement>),
}

// ─────────────────────────────────────────────────────────────────────────────
// UPDATE
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

// ─────────────────────────────────────────────────────────────────────────────
// DELETE
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expr>,
}

// ─────────────────────────────────────────────────────────────────────────────
// CREATE TABLE
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub if_not_exists: bool,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Real,
    Text,
    Blob,
    Boolean,
    Null,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Real => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnConstraint {
    PrimaryKey { autoincrement: bool },
    NotNull,
    Unique,
    Default(Expr),
    Check(Expr),
    References { table: String, column: Option<String> },
}

#[derive(Debug, Clone)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    Check(Expr),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// DROP TABLE / INDEX
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DropTableStatement {
    pub if_exists: bool,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStatement {
    pub unique: bool,
    pub if_not_exists: bool,
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DropIndexStatement {
    pub if_exists: bool,
    pub name: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// ALTER TABLE
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AlterTableStatement {
    pub table: String,
    pub action: AlterAction,
}

#[derive(Debug, Clone)]
pub enum AlterAction {
    AddColumn(ColumnDef),
    DropColumn(String),
    RenameColumn { old: String, new: String },
    RenameTable(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// PRAGMA
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PragmaStatement {
    pub name: String,
    pub value: Option<Expr>,
}

// ─────────────────────────────────────────────────────────────────────────────
// 表达式
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Expr {
    /// 字面量值
    Literal(Literal),
    /// 列引用（可选表名前缀）
    Column { table: Option<String>, name: String },
    /// 二元运算
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    /// 一元运算
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    /// 函数调用
    Function { name: String, args: Vec<Expr>, distinct: bool },
    /// IS NULL / IS NOT NULL
    IsNull { expr: Box<Expr>, negated: bool },
    /// BETWEEN
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// IN (list)
    InList {
        expr: Box<Expr>,
        negated: bool,
        list: Vec<Expr>,
    },
    /// IN (subquery)
    InSubquery {
        expr: Box<Expr>,
        negated: bool,
        subquery: Box<SelectStatement>,
    },
    /// LIKE
    Like {
        expr: Box<Expr>,
        negated: bool,
        pattern: Box<Expr>,
    },
    /// 子查询（标量）
    Subquery(Box<SelectStatement>),
    /// CASE WHEN ... THEN ... END
    Case {
        operand: Option<Box<Expr>>,
        branches: Vec<(Expr, Expr)>,
        else_branch: Option<Box<Expr>>,
    },
    /// CAST(expr AS type)
    Cast { expr: Box<Expr>, data_type: DataType },
    /// 通配符 *
    Wildcard,
    /// Placeholder parameter `?` or `?N` (1-based index)
    Placeholder(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    Add, Sub, Mul, Div, Mod,
    Eq, NotEq, Lt, Le, Gt, Ge,
    And, Or,
    Concat,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BinaryOp::Add => "+", BinaryOp::Sub => "-",
            BinaryOp::Mul => "*", BinaryOp::Div => "/", BinaryOp::Mod => "%",
            BinaryOp::Eq => "=", BinaryOp::NotEq => "!=",
            BinaryOp::Lt => "<", BinaryOp::Le => "<=",
            BinaryOp::Gt => ">", BinaryOp::Ge => ">=",
            BinaryOp::And => "AND", BinaryOp::Or => "OR",
            BinaryOp::Concat => "||",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}
