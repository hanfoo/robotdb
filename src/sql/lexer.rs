/// SQL 词法分析器
///
/// 将 SQL 字符串分解为 Token 序列，支持标准 SQL 语法。

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // ── 关键字 ────────────────────────────────────────────────────────────────
    Select, From, Where, Insert, Into, Values,
    Update, Set, Delete, Create, Drop, Table,
    Index, On, Primary, Key, Not, Null, Unique,
    And, Or, In, Like, Is, Between, Exists,
    Order, By, Asc, Desc, Limit, Offset,
    Group, Having, Join, Inner, Left, Right,
    Outer, Cross, Natural, Full, Union, All,
    Distinct, As, Begin, Commit, Rollback,
    Transaction, Explain, Pragma, Vacuum,
    Integer, Real, Text, Blob, Boolean,
    True, False, Default, Check, References,
    Foreign, Autoincrement, If, Exists2,
    Alter, Add, Column, Rename, To,
    Count, Sum, Avg, Min, Max,

    // ── 字面量 ────────────────────────────────────────────────────────────────
    /// 整数字面量
    IntLiteral(i64),
    /// 浮点字面量
    FloatLiteral(f64),
    /// 字符串字面量（已去除引号）
    StringLiteral(String),
    /// NULL 字面量
    NullLiteral,

    // ── 标识符 ────────────────────────────────────────────────────────────────
    Ident(String),

    // ── 运算符与标点 ──────────────────────────────────────────────────────────
    Plus, Minus, Star, Slash, Percent, Concat,
    Eq, NotEq, Lt, Le, Gt, Ge,
    LParen, RParen, Comma, Semicolon, Dot,
    Bang,
    /// Placeholder marker `?`
    Question,

    // ── 特殊 ──────────────────────────────────────────────────────────────────
    Eof,
}

/// 带位置信息的 Token
#[derive(Debug, Clone)]
pub struct TokenWithPos {
    pub token: Token,
    pub pos: usize,
}

/// 词法分析器 — 直接操作字节切片，零拷贝
pub struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    #[inline(always)]
    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    #[inline(always)]
    fn peek2(&self) -> Option<u8> {
        self.input.get(self.pos + 1).copied()
    }

    #[inline(always)]
    fn advance(&mut self) -> Option<u8> {
        let c = self.input.get(self.pos).copied();
        self.pos += 1;
        c
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn skip_line_comment(&mut self) {
        while let Some(c) = self.advance() {
            if c == b'\n' {
                break;
            }
        }
    }

    fn skip_block_comment(&mut self) {
        loop {
            match self.advance() {
                None => break,
                Some(b'*') if self.peek() == Some(b'/') => {
                    self.advance();
                    break;
                }
                _ => {}
            }
        }
    }

    fn read_string(&mut self, quote: u8) -> String {
        let mut s = String::new();
        loop {
            match self.advance() {
                None => break,
                Some(c) if c == quote => {
                    if self.peek() == Some(quote) {
                        self.advance();
                        s.push(quote as char);
                    } else {
                        break;
                    }
                }
                Some(b'\\') => {
                    match self.advance() {
                        Some(b'n') => s.push('\n'),
                        Some(b't') => s.push('\t'),
                        Some(b'r') => s.push('\r'),
                        Some(b'\\') => s.push('\\'),
                        Some(c) => s.push(c as char),
                        None => break,
                    }
                }
                Some(c) => s.push(c as char),
            }
        }
        s
    }

    fn read_number(&mut self, _first: u8) -> Token {
        let start = self.pos - 1; // first char already advanced
        let mut is_float = false;

        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.advance();
            } else if c == b'.' && !is_float && self.peek2().is_some_and(|d| d.is_ascii_digit()) {
                is_float = true;
                self.advance();
            } else if (c == b'e' || c == b'E') && !self.input[start..self.pos].contains(&b'e') && !self.input[start..self.pos].contains(&b'E') {
                is_float = true;
                self.advance();
                if let Some(sign) = self.peek() {
                    if sign == b'+' || sign == b'-' {
                        self.advance();
                    }
                }
                while let Some(d) = self.peek() {
                    if d.is_ascii_digit() {
                        self.advance();
                    } else {
                        break;
                    }
                }
            } else {
                break;
            }
        }

        // SAFETY: number tokens are always valid ASCII
        let s = unsafe { std::str::from_utf8_unchecked(&self.input[start..self.pos]) };

        if is_float {
            Token::FloatLiteral(s.parse().unwrap_or(0.0))
        } else if let Ok(n) = s.parse::<i64>() {
            Token::IntLiteral(n)
        } else if let Ok(u) = s.parse::<u64>() {
            Token::IntLiteral(u as i64)
        } else {
            Token::IntLiteral(0)
        }
    }

    fn read_ident_range(&mut self) -> (usize, usize) {
        let start = self.pos - 1; // first char already advanced
        while let Some(c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        (start, self.pos)
    }

    fn keyword_or_ident(&self, start: usize, end: usize) -> Token {
        let bytes = &self.input[start..end];
        let len = end - start;

        // Fast keyword matching using length + case-insensitive comparison.
        // Most common keywords first for branch prediction.
        match len {
            2 => {
                if bytes.eq_ignore_ascii_case(b"BY") { return Token::By; }
                if bytes.eq_ignore_ascii_case(b"AS") { return Token::As; }
                if bytes.eq_ignore_ascii_case(b"OR") { return Token::Or; }
                if bytes.eq_ignore_ascii_case(b"ON") { return Token::On; }
                if bytes.eq_ignore_ascii_case(b"IN") { return Token::In; }
                if bytes.eq_ignore_ascii_case(b"IS") { return Token::Is; }
                if bytes.eq_ignore_ascii_case(b"IF") { return Token::If; }
                if bytes.eq_ignore_ascii_case(b"TO") { return Token::To; }
            }
            3 => {
                if bytes.eq_ignore_ascii_case(b"AND") { return Token::And; }
                if bytes.eq_ignore_ascii_case(b"NOT") { return Token::Not; }
                if bytes.eq_ignore_ascii_case(b"SET") { return Token::Set; }
                if bytes.eq_ignore_ascii_case(b"ALL") { return Token::All; }
                if bytes.eq_ignore_ascii_case(b"ASC") { return Token::Asc; }
                if bytes.eq_ignore_ascii_case(b"AVG") { return Token::Avg; }
                if bytes.eq_ignore_ascii_case(b"KEY") { return Token::Key; }
                if bytes.eq_ignore_ascii_case(b"MAX") { return Token::Max; }
                if bytes.eq_ignore_ascii_case(b"MIN") { return Token::Min; }
                if bytes.eq_ignore_ascii_case(b"SUM") { return Token::Sum; }
                if bytes.eq_ignore_ascii_case(b"ADD") { return Token::Add; }
                if bytes.eq_ignore_ascii_case(b"INT") { return Token::Integer; }
            }
            4 => {
                if bytes.eq_ignore_ascii_case(b"FROM") { return Token::From; }
                if bytes.eq_ignore_ascii_case(b"INTO") { return Token::Into; }
                if bytes.eq_ignore_ascii_case(b"NULL") { return Token::NullLiteral; }
                if bytes.eq_ignore_ascii_case(b"LIKE") { return Token::Like; }
                if bytes.eq_ignore_ascii_case(b"DESC") { return Token::Desc; }
                if bytes.eq_ignore_ascii_case(b"DROP") { return Token::Drop; }
                if bytes.eq_ignore_ascii_case(b"JOIN") { return Token::Join; }
                if bytes.eq_ignore_ascii_case(b"LEFT") { return Token::Left; }
                if bytes.eq_ignore_ascii_case(b"FULL") { return Token::Full; }
                if bytes.eq_ignore_ascii_case(b"TRUE") { return Token::True; }
                if bytes.eq_ignore_ascii_case(b"REAL") { return Token::Real; }
                if bytes.eq_ignore_ascii_case(b"TEXT") { return Token::Text; }
                if bytes.eq_ignore_ascii_case(b"BLOB") { return Token::Blob; }
                if bytes.eq_ignore_ascii_case(b"BOOL") { return Token::Boolean; }
                if bytes.eq_ignore_ascii_case(b"CHAR") { return Token::Text; }
            }
            5 => {
                if bytes.eq_ignore_ascii_case(b"WHERE") { return Token::Where; }
                if bytes.eq_ignore_ascii_case(b"TABLE") { return Token::Table; }
                if bytes.eq_ignore_ascii_case(b"ORDER") { return Token::Order; }
                if bytes.eq_ignore_ascii_case(b"LIMIT") { return Token::Limit; }
                if bytes.eq_ignore_ascii_case(b"GROUP") { return Token::Group; }
                if bytes.eq_ignore_ascii_case(b"COUNT") { return Token::Count; }
                if bytes.eq_ignore_ascii_case(b"INDEX") { return Token::Index; }
                if bytes.eq_ignore_ascii_case(b"INNER") { return Token::Inner; }
                if bytes.eq_ignore_ascii_case(b"OUTER") { return Token::Outer; }
                if bytes.eq_ignore_ascii_case(b"CROSS") { return Token::Cross; }
                if bytes.eq_ignore_ascii_case(b"UNION") { return Token::Union; }
                if bytes.eq_ignore_ascii_case(b"ALTER") { return Token::Alter; }
                if bytes.eq_ignore_ascii_case(b"BEGIN") { return Token::Begin; }
                if bytes.eq_ignore_ascii_case(b"FALSE") { return Token::False; }
                if bytes.eq_ignore_ascii_case(b"CHECK") { return Token::Check; }
                if bytes.eq_ignore_ascii_case(b"FLOAT") { return Token::Real; }
                if bytes.eq_ignore_ascii_case(b"RIGHT") { return Token::Right; }
            }
            6 => {
                if bytes.eq_ignore_ascii_case(b"SELECT") { return Token::Select; }
                if bytes.eq_ignore_ascii_case(b"INSERT") { return Token::Insert; }
                if bytes.eq_ignore_ascii_case(b"UPDATE") { return Token::Update; }
                if bytes.eq_ignore_ascii_case(b"DELETE") { return Token::Delete; }
                if bytes.eq_ignore_ascii_case(b"CREATE") { return Token::Create; }
                if bytes.eq_ignore_ascii_case(b"VALUES") { return Token::Values; }
                if bytes.eq_ignore_ascii_case(b"UNIQUE") { return Token::Unique; }
                if bytes.eq_ignore_ascii_case(b"OFFSET") { return Token::Offset; }
                if bytes.eq_ignore_ascii_case(b"HAVING") { return Token::Having; }
                if bytes.eq_ignore_ascii_case(b"COLUMN") { return Token::Column; }
                if bytes.eq_ignore_ascii_case(b"COMMIT") { return Token::Commit; }
                if bytes.eq_ignore_ascii_case(b"RENAME") { return Token::Rename; }
                if bytes.eq_ignore_ascii_case(b"PRAGMA") { return Token::Pragma; }
                if bytes.eq_ignore_ascii_case(b"VACUUM") { return Token::Vacuum; }
                if bytes.eq_ignore_ascii_case(b"BIGINT") { return Token::Integer; }
                if bytes.eq_ignore_ascii_case(b"DOUBLE") { return Token::Real; }
                if bytes.eq_ignore_ascii_case(b"STRING") { return Token::Text; }
                if bytes.eq_ignore_ascii_case(b"BINARY") { return Token::Blob; }
                if bytes.eq_ignore_ascii_case(b"EXISTS") { return Token::Exists; }
            }
            7 => {
                if bytes.eq_ignore_ascii_case(b"PRIMARY") { return Token::Primary; }
                if bytes.eq_ignore_ascii_case(b"BETWEEN") { return Token::Between; }
                if bytes.eq_ignore_ascii_case(b"NATURAL") { return Token::Natural; }
                if bytes.eq_ignore_ascii_case(b"INTEGER") { return Token::Integer; }
                if bytes.eq_ignore_ascii_case(b"NUMERIC") { return Token::Real; }
                if bytes.eq_ignore_ascii_case(b"DECIMAL") { return Token::Real; }
                if bytes.eq_ignore_ascii_case(b"VARCHAR") { return Token::Text; }
                if bytes.eq_ignore_ascii_case(b"BOOLEAN") { return Token::Boolean; }
                if bytes.eq_ignore_ascii_case(b"DEFAULT") { return Token::Default; }
                if bytes.eq_ignore_ascii_case(b"FOREIGN") { return Token::Foreign; }
                if bytes.eq_ignore_ascii_case(b"EXPLAIN") { return Token::Explain; }
            }
            8 => {
                if bytes.eq_ignore_ascii_case(b"DISTINCT") { return Token::Distinct; }
                if bytes.eq_ignore_ascii_case(b"ROLLBACK") { return Token::Rollback; }
                if bytes.eq_ignore_ascii_case(b"SMALLINT") { return Token::Integer; }
                if bytes.eq_ignore_ascii_case(b"TINYINT") { return Token::Integer; }
                if bytes.eq_ignore_ascii_case(b"VARBINARY") { return Token::Blob; }
            }
            9 => {
                if bytes.eq_ignore_ascii_case(b"VARBINARY") { return Token::Blob; }
                if bytes.eq_ignore_ascii_case(b"REFERENCES") { return Token::References; }
            }
            10 => {
                if bytes.eq_ignore_ascii_case(b"REFERENCES") { return Token::References; }
            }
            11 => {
                if bytes.eq_ignore_ascii_case(b"TRANSACTION") { return Token::Transaction; }
            }
            13 => {
                if bytes.eq_ignore_ascii_case(b"AUTOINCREMENT") { return Token::Autoincrement; }
                if bytes.eq_ignore_ascii_case(b"AUTO_INCREMENT") { return Token::Autoincrement; }
            }
            14 => {
                if bytes.eq_ignore_ascii_case(b"AUTO_INCREMENT") { return Token::Autoincrement; }
            }
            _ => {}
        }

        // Not a keyword — allocate a String only for identifiers
        // SAFETY: identifiers are ASCII alphanumeric + underscore
        let s = unsafe { std::str::from_utf8_unchecked(bytes) };
        Token::Ident(s.to_string())
    }

    /// 对输入进行完整词法分析，返回 Token 列表
    pub fn tokenize(&mut self) -> crate::error::Result<Vec<TokenWithPos>> {
        let mut tokens = Vec::with_capacity(16);
        loop {
            self.skip_whitespace();
            let pos = self.pos;

            let c = match self.peek() {
                None => {
                    tokens.push(TokenWithPos { token: Token::Eof, pos });
                    break;
                }
                Some(c) => c,
            };

            let token = match c {
                // 注释
                b'-' if self.peek2() == Some(b'-') => {
                    self.advance();
                    self.advance();
                    self.skip_line_comment();
                    continue;
                }
                b'/' if self.peek2() == Some(b'*') => {
                    self.advance();
                    self.advance();
                    self.skip_block_comment();
                    continue;
                }
                // 字符串字面量
                b'\'' | b'"' => {
                    self.advance();
                    Token::StringLiteral(self.read_string(c))
                }
                // 反引号标识符（MySQL 风格）
                b'`' => {
                    self.advance();
                    let s = self.read_string(b'`');
                    Token::Ident(s)
                }
                // 数字
                c if c.is_ascii_digit() => {
                    self.advance();
                    self.read_number(c)
                }
                // 标识符 / 关键字
                c if c.is_ascii_alphabetic() || c == b'_' => {
                    self.advance();
                    let (start, end) = self.read_ident_range();
                    self.keyword_or_ident(start, end)
                }
                // 运算符
                b'+' => { self.advance(); Token::Plus }
                b'-' => { self.advance(); Token::Minus }
                b'*' => { self.advance(); Token::Star }
                b'/' => { self.advance(); Token::Slash }
                b'%' => { self.advance(); Token::Percent }
                b'(' => { self.advance(); Token::LParen }
                b')' => { self.advance(); Token::RParen }
                b',' => { self.advance(); Token::Comma }
                b';' => { self.advance(); Token::Semicolon }
                b'.' => { self.advance(); Token::Dot }
                b'!' => {
                    self.advance();
                    if self.peek() == Some(b'=') {
                        self.advance();
                        Token::NotEq
                    } else {
                        Token::Bang
                    }
                }
                b'=' => { self.advance(); Token::Eq }
                b'<' => {
                    self.advance();
                    match self.peek() {
                        Some(b'=') => { self.advance(); Token::Le }
                        Some(b'>') => { self.advance(); Token::NotEq }
                        _ => Token::Lt,
                    }
                }
                b'>' => {
                    self.advance();
                    if self.peek() == Some(b'=') {
                        self.advance();
                        Token::Ge
                    } else {
                        Token::Gt
                    }
                }
                b'?' => { self.advance(); Token::Question }
                b'|' if self.peek2() == Some(b'|') => {
                    self.advance();
                    self.advance();
                    Token::Concat
                }
                _ => {
                    self.advance();
                    continue; // 跳过未知字符
                }
            };

            tokens.push(TokenWithPos { token, pos });
        }
        Ok(tokens)
    }
}
