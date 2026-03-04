use crate::error::{Error, Result};
use super::lexer::{Lexer, Token, TokenWithPos};
use super::ast::*;

/// 递归下降 SQL 解析器
pub struct Parser {
    tokens: Vec<TokenWithPos>,
    pos: usize,
    placeholder_count: usize,
}

impl Parser {
    pub fn new(sql: &str) -> Result<Self> {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize()?;
        Ok(Self { tokens, pos: 0, placeholder_count: 0 })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 辅助方法
    // ─────────────────────────────────────────────────────────────────────────

    fn peek(&self) -> &Token {
        &self.tokens[self.pos].token
    }

    fn peek_pos(&self) -> usize {
        self.tokens[self.pos].pos
    }

    fn advance(&mut self) -> &Token {
        let t = &self.tokens[self.pos].token;
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        if self.peek() == expected {
            self.advance();
            Ok(())
        } else {
            Err(Error::ParseError {
                pos: self.peek_pos(),
                msg: format!("Expected {:?}, got {:?}", expected, self.peek()),
            })
        }
    }

    /// Take ownership of the current token's string, replacing it with an empty placeholder.
    fn take_token(&mut self) -> Token {
        let placeholder = Token::Eof;
        std::mem::replace(&mut self.tokens[self.pos].token, placeholder)
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.take_token() {
            Token::Ident(s) => {
                self.advance();
                Ok(s)
            }
            tok => {
                // Put it back for error reporting
                self.tokens[self.pos].token = tok;
                let s = self.token_as_ident();
                if let Some(name) = s {
                    self.advance();
                    Ok(name)
                } else {
                    Err(Error::ParseError {
                        pos: self.peek_pos(),
                        msg: format!("Expected identifier, got {:?}", self.peek()),
                    })
                }
            }
        }
    }

    fn token_as_ident(&self) -> Option<String> {
        match self.peek() {
            Token::Ident(s) => Some(s.clone()),
            Token::Table => Some("table".into()),
            Token::Index => Some("index".into()),
            Token::Key => Some("key".into()),
            Token::Column => Some("column".into()),
            Token::Count => Some("count".into()),
            Token::Sum => Some("sum".into()),
            Token::Avg => Some("avg".into()),
            Token::Min => Some("min".into()),
            Token::Max => Some("max".into()),
            _ => None,
        }
    }

    fn consume_if(&mut self, tok: &Token) -> bool {
        if self.peek() == tok {
            self.advance();
            true
        } else {
            false
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 顶层解析
    // ─────────────────────────────────────────────────────────────────────────

    /// 解析一条 SQL 语句
    pub fn parse_statement(&mut self) -> Result<Statement> {
        let stmt = match self.peek().clone() {
            Token::Select => Statement::Select(self.parse_select()?),
            Token::Insert => Statement::Insert(self.parse_insert()?),
            Token::Update => Statement::Update(self.parse_update()?),
            Token::Delete => Statement::Delete(self.parse_delete()?),
            Token::Create => self.parse_create()?,
            Token::Drop => self.parse_drop()?,
            Token::Alter => Statement::AlterTable(self.parse_alter_table()?),
            Token::Begin => { self.advance(); Statement::Begin }
            Token::Commit => { self.advance(); Statement::Commit }
            Token::Rollback => { self.advance(); Statement::Rollback }
            Token::Explain => {
                self.advance();
                let inner = self.parse_statement()?;
                Statement::Explain(Box::new(inner))
            }
            Token::Pragma => Statement::Pragma(self.parse_pragma()?),
            Token::Vacuum => { self.advance(); Statement::Vacuum }
            tok => {
                return Err(Error::ParseError {
                    pos: self.peek_pos(),
                    msg: format!("Unexpected token: {:?}", tok),
                })
            }
        };
        self.consume_if(&Token::Semicolon);
        Ok(stmt)
    }

    /// 解析多条语句
    pub fn parse_statements(&mut self) -> Result<Vec<Statement>> {
        let mut stmts = Vec::new();
        while !matches!(self.peek(), Token::Eof) {
            if self.consume_if(&Token::Semicolon) {
                continue;
            }
            stmts.push(self.parse_statement()?);
        }
        Ok(stmts)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SELECT
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_select(&mut self) -> Result<SelectStatement> {
        self.expect(&Token::Select)?;
        let distinct = self.consume_if(&Token::Distinct);

        let columns = self.parse_select_columns()?;

        let from = if self.consume_if(&Token::From) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        let joins = self.parse_joins()?;

        let where_clause = if self.consume_if(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.consume_if(&Token::Group) {
            self.expect(&Token::By)?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };

        let having = if self.consume_if(&Token::Having) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.consume_if(&Token::Order) {
            self.expect(&Token::By)?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        let limit = if self.consume_if(&Token::Limit) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.consume_if(&Token::Offset) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(SelectStatement {
            distinct,
            columns,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut cols = Vec::new();
        loop {
            let col = if self.consume_if(&Token::Star) {
                SelectColumn::Wildcard
            } else {
                let expr = self.parse_expr()?;
                // 检查 table.*
                if let Expr::Column { table: None, name } = &expr {
                    if self.consume_if(&Token::Dot) && self.consume_if(&Token::Star) {
                        cols.push(SelectColumn::TableWildcard(name.clone()));
                        if !self.consume_if(&Token::Comma) {
                            break;
                        }
                        continue;
                    }
                }
                let alias = if self.consume_if(&Token::As) {
                    Some(self.expect_ident()?)
                } else if let Token::Ident(_) = self.peek() {
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                SelectColumn::Expr { expr, alias }
            };
            cols.push(col);
            if !self.consume_if(&Token::Comma) {
                break;
            }
        }
        Ok(cols)
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let name = self.expect_ident()?;
        let alias = if self.consume_if(&Token::As) {
            Some(self.expect_ident()?)
        } else if let Token::Ident(_) = self.peek() {
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(TableRef { name, alias })
    }

    fn parse_joins(&mut self) -> Result<Vec<JoinClause>> {
        let mut joins = Vec::new();
        loop {
            let join_type = match self.peek() {
                Token::Join | Token::Inner => {
                    if matches!(self.peek(), Token::Inner) {
                        self.advance();
                    }
                    self.consume_if(&Token::Join);
                    JoinType::Inner
                }
                Token::Left => {
                    self.advance();
                    self.consume_if(&Token::Outer);
                    self.consume_if(&Token::Join);
                    JoinType::Left
                }
                Token::Right => {
                    self.advance();
                    self.consume_if(&Token::Outer);
                    self.consume_if(&Token::Join);
                    JoinType::Right
                }
                Token::Full => {
                    self.advance();
                    self.consume_if(&Token::Outer);
                    self.consume_if(&Token::Join);
                    JoinType::Full
                }
                Token::Cross => {
                    self.advance();
                    self.consume_if(&Token::Join);
                    JoinType::Cross
                }
                _ => break,
            };
            let table = self.parse_table_ref()?;
            let condition = if self.consume_if(&Token::On) {
                Some(self.parse_expr()?)
            } else {
                None
            };
            joins.push(JoinClause { join_type, table, condition });
        }
        Ok(joins)
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let asc = !self.consume_if(&Token::Desc);
            if !asc { /* already consumed */ } else { self.consume_if(&Token::Asc); }
            items.push(OrderByItem { expr, asc });
            if !self.consume_if(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INSERT
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_insert(&mut self) -> Result<InsertStatement> {
        self.expect(&Token::Insert)?;
        self.expect(&Token::Into)?;
        let table = self.expect_ident()?;

        let columns = if self.consume_if(&Token::LParen) {
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if !self.consume_if(&Token::Comma) {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        let source = if self.consume_if(&Token::Values) {
            let mut rows = Vec::new();
            loop {
                self.expect(&Token::LParen)?;
                let mut row = Vec::new();
                loop {
                    row.push(self.parse_expr()?);
                    if !self.consume_if(&Token::Comma) {
                        break;
                    }
                }
                self.expect(&Token::RParen)?;
                rows.push(row);
                if !self.consume_if(&Token::Comma) {
                    break;
                }
            }
            InsertSource::Values(rows)
        } else if matches!(self.peek(), Token::Select) {
            InsertSource::Select(Box::new(self.parse_select()?))
        } else {
            return Err(Error::ParseError {
                pos: self.peek_pos(),
                msg: "Expected VALUES or SELECT after INSERT INTO".into(),
            });
        };

        Ok(InsertStatement { table, columns, source })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UPDATE
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_update(&mut self) -> Result<UpdateStatement> {
        self.expect(&Token::Update)?;
        let table = self.expect_ident()?;
        self.expect(&Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });
            if !self.consume_if(&Token::Comma) {
                break;
            }
        }

        let where_clause = if self.consume_if(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(UpdateStatement { table, assignments, where_clause })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DELETE
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_delete(&mut self) -> Result<DeleteStatement> {
        self.expect(&Token::Delete)?;
        self.expect(&Token::From)?;
        let table = self.expect_ident()?;
        let where_clause = if self.consume_if(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(DeleteStatement { table, where_clause })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CREATE
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(&Token::Create)?;
        match self.peek().clone() {
            Token::Table => Ok(Statement::CreateTable(self.parse_create_table()?)),
            Token::Unique | Token::Index => Ok(Statement::CreateIndex(self.parse_create_index()?)),
            tok => Err(Error::ParseError {
                pos: self.peek_pos(),
                msg: format!("Expected TABLE or INDEX after CREATE, got {:?}", tok),
            }),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStatement> {
        self.expect(&Token::Table)?;
        let if_not_exists = if self.consume_if(&Token::If) {
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        self.expect(&Token::LParen)?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            match self.peek().clone() {
                Token::Primary => {
                    self.advance();
                    self.expect(&Token::Key)?;
                    self.expect(&Token::LParen)?;
                    let cols = self.parse_ident_list()?;
                    self.expect(&Token::RParen)?;
                    constraints.push(TableConstraint::PrimaryKey(cols));
                }
                Token::Unique => {
                    self.advance();
                    self.expect(&Token::LParen)?;
                    let cols = self.parse_ident_list()?;
                    self.expect(&Token::RParen)?;
                    constraints.push(TableConstraint::Unique(cols));
                }
                Token::Check => {
                    self.advance();
                    self.expect(&Token::LParen)?;
                    let expr = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    constraints.push(TableConstraint::Check(expr));
                }
                Token::Foreign => {
                    self.advance();
                    self.expect(&Token::Key)?;
                    self.expect(&Token::LParen)?;
                    let cols = self.parse_ident_list()?;
                    self.expect(&Token::RParen)?;
                    self.expect(&Token::References)?;
                    let ref_table = self.expect_ident()?;
                    self.expect(&Token::LParen)?;
                    let ref_cols = self.parse_ident_list()?;
                    self.expect(&Token::RParen)?;
                    constraints.push(TableConstraint::ForeignKey {
                        columns: cols,
                        ref_table,
                        ref_columns: ref_cols,
                    });
                }
                _ => {
                    columns.push(self.parse_column_def()?);
                }
            }
            if !self.consume_if(&Token::Comma) {
                break;
            }
            if matches!(self.peek(), Token::RParen) {
                break;
            }
        }

        self.expect(&Token::RParen)?;
        Ok(CreateTableStatement { if_not_exists, name, columns, constraints })
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.expect_ident()?;
        let data_type = self.parse_data_type()?;
        let mut constraints = Vec::new();

        loop {
            match self.peek().clone() {
                Token::Primary => {
                    self.advance();
                    self.expect(&Token::Key)?;
                    let autoincrement = self.consume_if(&Token::Autoincrement);
                    constraints.push(ColumnConstraint::PrimaryKey { autoincrement });
                }
                Token::Not => {
                    self.advance();
                    self.expect(&Token::NullLiteral)?;
                    constraints.push(ColumnConstraint::NotNull);
                }
                Token::Unique => {
                    self.advance();
                    constraints.push(ColumnConstraint::Unique);
                }
                Token::Default => {
                    self.advance();
                    let expr = self.parse_primary()?;
                    constraints.push(ColumnConstraint::Default(expr));
                }
                Token::Check => {
                    self.advance();
                    self.expect(&Token::LParen)?;
                    let expr = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    constraints.push(ColumnConstraint::Check(expr));
                }
                Token::References => {
                    self.advance();
                    let table = self.expect_ident()?;
                    let column = if self.consume_if(&Token::LParen) {
                        let c = self.expect_ident()?;
                        self.expect(&Token::RParen)?;
                        Some(c)
                    } else {
                        None
                    };
                    constraints.push(ColumnConstraint::References { table, column });
                }
                _ => break,
            }
        }

        Ok(ColumnDef { name, data_type, constraints })
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        let dt = match self.peek() {
            Token::Integer => DataType::Integer,
            Token::Real => DataType::Real,
            Token::Text => DataType::Text,
            Token::Blob => DataType::Blob,
            Token::Boolean => DataType::Boolean,
            Token::NullLiteral => DataType::Null,
            _ => DataType::Text, // 默认 TEXT（宽松模式）
        };
        if !matches!(self.peek(), Token::Comma | Token::RParen | Token::Primary
            | Token::Not | Token::Unique | Token::Default | Token::Check | Token::References) {
            self.advance();
            // 消费可选的长度参数，如 VARCHAR(255)
            if self.consume_if(&Token::LParen) {
                while !matches!(self.peek(), Token::RParen | Token::Eof) {
                    self.advance();
                }
                self.consume_if(&Token::RParen);
            }
        }
        Ok(dt)
    }

    fn parse_create_index(&mut self) -> Result<CreateIndexStatement> {
        let unique = self.consume_if(&Token::Unique);
        self.expect(&Token::Index)?;
        let if_not_exists = if self.consume_if(&Token::If) {
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        self.expect(&Token::On)?;
        let table = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let columns = self.parse_ident_list()?;
        self.expect(&Token::RParen)?;
        Ok(CreateIndexStatement { unique, if_not_exists, name, table, columns })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DROP
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(&Token::Drop)?;
        match self.peek().clone() {
            Token::Table => {
                self.advance();
                let if_exists = if self.consume_if(&Token::If) {
                    self.expect(&Token::Exists)?;
                    true
                } else {
                    false
                };
                let name = self.expect_ident()?;
                Ok(Statement::DropTable(DropTableStatement { if_exists, name }))
            }
            Token::Index => {
                self.advance();
                let if_exists = if self.consume_if(&Token::If) {
                    self.expect(&Token::Exists)?;
                    true
                } else {
                    false
                };
                let name = self.expect_ident()?;
                Ok(Statement::DropIndex(DropIndexStatement { if_exists, name }))
            }
            tok => Err(Error::ParseError {
                pos: self.peek_pos(),
                msg: format!("Expected TABLE or INDEX after DROP, got {:?}", tok),
            }),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ALTER TABLE
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_alter_table(&mut self) -> Result<AlterTableStatement> {
        self.expect(&Token::Alter)?;
        self.expect(&Token::Table)?;
        let table = self.expect_ident()?;
        let action = match self.peek().clone() {
            Token::Add => {
                self.advance();
                self.consume_if(&Token::Column);
                let col = self.parse_column_def()?;
                AlterAction::AddColumn(col)
            }
            Token::Rename => {
                self.advance();
                if self.consume_if(&Token::To) {
                    let new_name = self.expect_ident()?;
                    AlterAction::RenameTable(new_name)
                } else {
                    self.consume_if(&Token::Column);
                    let old = self.expect_ident()?;
                    self.expect(&Token::To)?;
                    let new = self.expect_ident()?;
                    AlterAction::RenameColumn { old, new }
                }
            }
            Token::Drop => {
                self.advance();
                self.consume_if(&Token::Column);
                let col = self.expect_ident()?;
                AlterAction::DropColumn(col)
            }
            tok => {
                return Err(Error::ParseError {
                    pos: self.peek_pos(),
                    msg: format!("Unknown ALTER TABLE action: {:?}", tok),
                })
            }
        };
        Ok(AlterTableStatement { table, action })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PRAGMA
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_pragma(&mut self) -> Result<PragmaStatement> {
        self.expect(&Token::Pragma)?;
        let name = self.expect_ident()?;
        let value = if self.consume_if(&Token::Eq) {
            Some(self.parse_pragma_value()?)
        } else if self.consume_if(&Token::LParen) {
            let val = self.parse_pragma_value()?;
            self.expect(&Token::RParen)?;
            Some(val)
        } else {
            None
        };
        Ok(PragmaStatement { name, value })
    }

    /// Parse a PRAGMA value, handling keyword tokens (like FULL) as bare identifiers.
    fn parse_pragma_value(&mut self) -> Result<Expr> {
        if let Some(name) = self.try_consume_keyword_as_ident() {
            return Ok(Expr::Column { table: None, name });
        }
        self.parse_expr()
    }

    /// If the current token is a keyword that could be a pragma value,
    /// consume it and return its name as a string.
    fn try_consume_keyword_as_ident(&mut self) -> Option<String> {
        let name = match self.peek() {
            Token::Full => Some("FULL".to_string()),
            _ => None,
        };
        if name.is_some() {
            self.advance();
        }
        name
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 表达式解析（Pratt 解析器）
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.consume_if(&Token::Or) {
            let right = self.parse_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_not()?;
        while self.consume_if(&Token::And) {
            let right = self.parse_not()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr> {
        if self.consume_if(&Token::Not) {
            let expr = self.parse_not()?;
            return Ok(Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(expr) });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_addition()?;

        // IS NULL / IS NOT NULL
        if self.consume_if(&Token::Is) {
            let negated = self.consume_if(&Token::Not);
            self.expect(&Token::NullLiteral)?;
            return Ok(Expr::IsNull { expr: Box::new(left), negated });
        }

        // BETWEEN
        if self.consume_if(&Token::Between) {
            let low = self.parse_addition()?;
            self.expect(&Token::And)?;
            let high = self.parse_addition()?;
            return Ok(Expr::Between {
                expr: Box::new(left),
                negated: false,
                low: Box::new(low),
                high: Box::new(high),
            });
        }

        // NOT BETWEEN / NOT IN / NOT LIKE
        if matches!(self.peek(), Token::Not) {
            let saved = self.pos;
            self.advance();
            match self.peek().clone() {
                Token::Between => {
                    self.advance();
                    let low = self.parse_addition()?;
                    self.expect(&Token::And)?;
                    let high = self.parse_addition()?;
                    return Ok(Expr::Between {
                        expr: Box::new(left),
                        negated: true,
                        low: Box::new(low),
                        high: Box::new(high),
                    });
                }
                Token::In => {
                    self.advance();
                    return self.parse_in(left, true);
                }
                Token::Like => {
                    self.advance();
                    let pattern = self.parse_addition()?;
                    return Ok(Expr::Like {
                        expr: Box::new(left),
                        negated: true,
                        pattern: Box::new(pattern),
                    });
                }
                _ => {
                    self.pos = saved; // 回退
                }
            }
        }

        // IN
        if self.consume_if(&Token::In) {
            return self.parse_in(left, false);
        }

        // LIKE
        if self.consume_if(&Token::Like) {
            let pattern = self.parse_addition()?;
            return Ok(Expr::Like {
                expr: Box::new(left),
                negated: false,
                pattern: Box::new(pattern),
            });
        }

        // 比较运算符
        let op = match self.peek() {
            Token::Eq => BinaryOp::Eq,
            Token::NotEq => BinaryOp::NotEq,
            Token::Lt => BinaryOp::Lt,
            Token::Le => BinaryOp::Le,
            Token::Gt => BinaryOp::Gt,
            Token::Ge => BinaryOp::Ge,
            _ => return Ok(left),
        };
        self.advance();
        let right = self.parse_addition()?;
        Ok(Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn parse_in(&mut self, left: Expr, negated: bool) -> Result<Expr> {
        self.expect(&Token::LParen)?;
        if matches!(self.peek(), Token::Select) {
            let subq = self.parse_select()?;
            self.expect(&Token::RParen)?;
            return Ok(Expr::InSubquery {
                expr: Box::new(left),
                negated,
                subquery: Box::new(subq),
            });
        }
        let list = self.parse_expr_list()?;
        self.expect(&Token::RParen)?;
        Ok(Expr::InList { expr: Box::new(left), negated, list })
    }

    fn parse_addition(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplication()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinaryOp::Add,
                Token::Minus => BinaryOp::Sub,
                Token::Concat => BinaryOp::Concat,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplication()?;
            left = Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinaryOp::Mul,
                Token::Slash => BinaryOp::Div,
                Token::Percent => BinaryOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.consume_if(&Token::Minus) {
            // Special case: -(9223372036854775808) = i64::MIN
            // The lexer stores 9223372036854775808 as i64::MIN (wrapping u64 cast),
            // so negating it would overflow. Detect this and return i64::MIN directly.
            if let Token::IntLiteral(n) = self.peek().clone() {
                if n == i64::MIN {
                    self.advance();
                    return Ok(Expr::Literal(Literal::Integer(i64::MIN)));
                }
            }
            let expr = self.parse_primary()?;
            return Ok(Expr::UnaryOp { op: UnaryOp::Neg, expr: Box::new(expr) });
        }
        if self.consume_if(&Token::Plus) {
            return self.parse_primary();
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr> {
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.peek() {
            Token::IntLiteral(_) | Token::FloatLiteral(_) | Token::StringLiteral(_) => {
                match self.take_token() {
                    Token::IntLiteral(n) => {
                        self.advance();
                        Ok(Expr::Literal(Literal::Integer(n)))
                    }
                    Token::FloatLiteral(f) => {
                        self.advance();
                        Ok(Expr::Literal(Literal::Float(f)))
                    }
                    Token::StringLiteral(s) => {
                        self.advance();
                        Ok(Expr::Literal(Literal::String(s)))
                    }
                    _ => unreachable!(),
                }
            }
            Token::NullLiteral => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(false)))
            }
            Token::Star => {
                self.advance();
                Ok(Expr::Wildcard)
            }
            Token::LParen => {
                self.advance();
                if matches!(self.peek(), Token::Select) {
                    let subq = self.parse_select()?;
                    self.expect(&Token::RParen)?;
                    return Ok(Expr::Subquery(Box::new(subq)));
                }
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            // Placeholder: ?N or ?
            Token::Question => {
                self.advance();
                let idx = if let Token::IntLiteral(n) = self.peek().clone() {
                    self.advance();
                    n as usize
                } else {
                    self.placeholder_count += 1;
                    self.placeholder_count
                };
                Ok(Expr::Placeholder(idx))
            }
            // 函数调用 or 列引用
            Token::Ident(_) | Token::Count | Token::Sum | Token::Avg
            | Token::Min | Token::Max => {
                let name = self.expect_ident()?;
                if self.consume_if(&Token::LParen) {
                    // 函数调用
                    let distinct = self.consume_if(&Token::Distinct);
                    let args = if matches!(self.peek(), Token::RParen) {
                        Vec::new()
                    } else if matches!(self.peek(), Token::Star) {
                        self.advance();
                        vec![Expr::Wildcard]
                    } else {
                        self.parse_expr_list()?
                    };
                    self.expect(&Token::RParen)?;
                    Ok(Expr::Function { name, args, distinct })
                } else if self.consume_if(&Token::Dot) {
                    // table.column
                    let col = self.expect_ident()?;
                    Ok(Expr::Column { table: Some(name), name: col })
                } else {
                    Ok(Expr::Column { table: None, name })
                }
            }
            _ => Err(Error::ParseError {
                pos: self.peek_pos(),
                msg: format!("Unexpected token in expression: {:?}", self.peek()),
            }),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 辅助列表解析
    // ─────────────────────────────────────────────────────────────────────────

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = Vec::new();
        loop {
            exprs.push(self.parse_expr()?);
            if !self.consume_if(&Token::Comma) {
                break;
            }
        }
        Ok(exprs)
    }

    fn parse_ident_list(&mut self) -> Result<Vec<String>> {
        let mut idents = Vec::new();
        loop {
            idents.push(self.expect_ident()?);
            if !self.consume_if(&Token::Comma) {
                break;
            }
        }
        Ok(idents)
    }
}

/// 便捷函数：解析单条 SQL 语句
pub fn parse(sql: &str) -> Result<Statement> {
    Parser::new(sql)?.parse_statement()
}

/// 便捷函数：解析多条 SQL 语句
pub fn parse_statements(sql: &str) -> Result<Vec<Statement>> {
    Parser::new(sql)?.parse_statements()
}
