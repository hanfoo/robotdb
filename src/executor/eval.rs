/// 表达式求值器
///
/// 在给定行上下文中对 SQL 表达式进行求值，返回 Value。

use crate::error::{Error, Result};
use crate::sql::ast::{Expr, Literal, BinaryOp, UnaryOp};
use crate::catalog::value::Value;
use crate::catalog::schema::TableSchema;

/// 行上下文：列名 -> 值的映射
pub struct EvalContext<'a> {
    /// 当前行的列值（按列顺序）
    pub row: &'a [Value],
    /// 表 schema（用于列名解析）
    pub schema: &'a TableSchema,
    /// 可选的表别名
    pub table_alias: Option<&'a str>,
    /// Bound parameter values (1-based indexing, so params[0] = ?1)
    pub params: &'a [Value],
}

impl<'a> EvalContext<'a> {
    pub fn new(row: &'a [Value], schema: &'a TableSchema) -> Self {
        Self { row, schema, table_alias: None, params: &[] }
    }

    pub fn with_params(row: &'a [Value], schema: &'a TableSchema, params: &'a [Value]) -> Self {
        Self { row, schema, table_alias: None, params }
    }

    pub fn get_column(&self, table: Option<&str>, name: &str) -> Result<&Value> {
        // 如果指定了表名，验证是否匹配
        if let Some(tbl) = table {
            let matches = tbl.eq_ignore_ascii_case(&self.schema.name)
                || self.table_alias.map_or(false, |a| a.eq_ignore_ascii_case(tbl));
            if !matches {
                return Err(Error::ColumnNotFound(name.to_string(), tbl.to_string()));
            }
        }
        let idx = self.schema.column_index(name)
            .ok_or_else(|| Error::ColumnNotFound(name.to_string(), self.schema.name.clone()))?;
        Ok(&self.row[idx])
    }
}

/// 对表达式求值
pub fn eval_expr(expr: &Expr, ctx: &EvalContext) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(eval_literal(lit)),

        Expr::Column { table, name } => {
            ctx.get_column(table.as_deref(), name).map(|v| v.clone())
        }

        Expr::Wildcard => Err(Error::ExecutionError("Cannot evaluate wildcard".into())),

        Expr::BinaryOp { left, op, right } => {
            let lv = eval_expr(left, ctx)?;
            let rv = eval_expr(right, ctx)?;
            eval_binary_op(&lv, op, &rv)
        }

        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, ctx)?;
            eval_unary_op(op, val)
        }

        Expr::IsNull { expr, negated } => {
            let val = eval_expr(expr, ctx)?;
            let is_null = val.is_null();
            Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
        }

        Expr::Between { expr, negated, low, high } => {
            let val = eval_expr(expr, ctx)?;
            let lo = eval_expr(low, ctx)?;
            let hi = eval_expr(high, ctx)?;
            if val.is_null() || lo.is_null() || hi.is_null() {
                return Ok(Value::Null);
            }
            let in_range = val.partial_cmp(&lo).map_or(false, |o| o.is_ge())
                && val.partial_cmp(&hi).map_or(false, |o| o.is_le());
            Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
        }

        Expr::InList { expr, negated, list } => {
            let val = eval_expr(expr, ctx)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            let found = list.iter().any(|e| {
                eval_expr(e, ctx).map_or(false, |v| val == v)
            });
            Ok(Value::Boolean(if *negated { !found } else { found }))
        }

        Expr::Like { expr, negated, pattern } => {
            let val = eval_expr(expr, ctx)?;
            let pat = eval_expr(pattern, ctx)?;
            match (&val, &pat) {
                (Value::Text(s), Value::Text(p)) => {
                    let matches = like_match(s, p);
                    Ok(Value::Boolean(if *negated { !matches } else { matches }))
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "TEXT".into(),
                    got: val.type_name().into(),
                }),
            }
        }

        Expr::Function { name, args, distinct } => {
            eval_function(name, args, *distinct, ctx)
        }

        Expr::Cast { expr, data_type } => {
            let val = eval_expr(expr, ctx)?;
            val.cast(data_type)
        }

        Expr::Case { operand, branches, else_branch } => {
            let operand_val = operand.as_ref()
                .map(|e| eval_expr(e, ctx))
                .transpose()?;

            for (when, then) in branches {
                let when_val = eval_expr(when, ctx)?;
                let matches = if let Some(ref ov) = operand_val {
                    ov == &when_val
                } else {
                    when_val.is_truthy()
                };
                if matches {
                    return eval_expr(then, ctx);
                }
            }

            if let Some(else_expr) = else_branch {
                eval_expr(else_expr, ctx)
            } else {
                Ok(Value::Null)
            }
        }

        Expr::Placeholder(idx) => {
            if *idx == 0 || *idx > ctx.params.len() {
                return Err(Error::ExecutionError(format!(
                    "Parameter index {} out of range (have {} params)", idx, ctx.params.len()
                )));
            }
            Ok(ctx.params[*idx - 1].clone())
        }

        Expr::Subquery(_) | Expr::InSubquery { .. } => {
            Err(Error::NotImplemented("Subquery evaluation requires executor context".into()))
        }
    }
}

fn eval_literal(lit: &Literal) -> Value {
    match lit {
        Literal::Integer(n) => Value::Integer(*n),
        Literal::Float(f) => Value::Real(*f),
        Literal::String(s) => Value::Text(s.clone()),
        Literal::Boolean(b) => Value::Boolean(*b),
        Literal::Null => Value::Null,
    }
}

/// Public wrapper for binary op evaluation, used by HAVING clause in engine.rs
pub fn eval_binary_op_pub(left: &Value, op: &BinaryOp, right: &Value) -> Result<Value> {
    eval_binary_op(left, op, right)
}

fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Result<Value> {
    // NULL 传播
    if left.is_null() || right.is_null() {
        // AND/OR 有特殊的 NULL 语义
        match op {
            BinaryOp::And => {
                if matches!(left, Value::Boolean(false)) || matches!(right, Value::Boolean(false)) {
                    return Ok(Value::Boolean(false));
                }
                return Ok(Value::Null);
            }
            BinaryOp::Or => {
                if matches!(left, Value::Boolean(true)) || matches!(right, Value::Boolean(true)) {
                    return Ok(Value::Boolean(true));
                }
                return Ok(Value::Null);
            }
            _ => return Ok(Value::Null),
        }
    }

    match op {
        // 算术运算
        BinaryOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinaryOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinaryOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinaryOp::Div => {
            match (left, right) {
                (_, Value::Integer(0)) => Err(Error::DivisionByZero),
                (_, Value::Real(f)) if *f == 0.0 => Err(Error::DivisionByZero),
                _ => numeric_op(left, right, |a, b| a / b, |a, b| a / b),
            }
        }
        BinaryOp::Mod => {
            match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => {
                    if *b == 0 { return Err(Error::DivisionByZero); }
                    Ok(Value::Integer(a % b))
                }
                _ => Err(Error::TypeMismatch {
                    expected: "INTEGER".into(),
                    got: left.type_name().into(),
                }),
            }
        }

        // 比较运算
        BinaryOp::Eq => Ok(Value::Boolean(left == right)),
        BinaryOp::NotEq => Ok(Value::Boolean(left != right)),
        BinaryOp::Lt => Ok(Value::Boolean(
            left.partial_cmp(right).map_or(false, |o| o.is_lt())
        )),
        BinaryOp::Le => Ok(Value::Boolean(
            left.partial_cmp(right).map_or(false, |o| o.is_le())
        )),
        BinaryOp::Gt => Ok(Value::Boolean(
            left.partial_cmp(right).map_or(false, |o| o.is_gt())
        )),
        BinaryOp::Ge => Ok(Value::Boolean(
            left.partial_cmp(right).map_or(false, |o| o.is_ge())
        )),

        // 逻辑运算
        BinaryOp::And => Ok(Value::Boolean(left.is_truthy() && right.is_truthy())),
        BinaryOp::Or => Ok(Value::Boolean(left.is_truthy() || right.is_truthy())),

        // 字符串拼接
        BinaryOp::Concat => {
            let ls = value_to_string(left);
            let rs = value_to_string(right);
            Ok(Value::Text(ls + &rs))
        }
    }
}

fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(int_op(*a, *b))),
        (Value::Real(a), Value::Real(b)) => Ok(Value::Real(float_op(*a, *b))),
        (Value::Integer(a), Value::Real(b)) => Ok(Value::Real(float_op(*a as f64, *b))),
        (Value::Real(a), Value::Integer(b)) => Ok(Value::Real(float_op(*a, *b as f64))),
        _ => Err(Error::TypeMismatch {
            expected: "NUMBER".into(),
            got: format!("{} op {}", left.type_name(), right.type_name()),
        }),
    }
}

fn eval_unary_op(op: &UnaryOp, val: Value) -> Result<Value> {
    match op {
        UnaryOp::Neg => match val {
            Value::Integer(n) => Ok(Value::Integer(-n)),
            Value::Real(f) => Ok(Value::Real(-f)),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "NUMBER".into(),
                got: val.type_name().into(),
            }),
        },
        UnaryOp::Not => Ok(Value::Boolean(!val.is_truthy())),
    }
}

fn eval_function(
    name: &str,
    args: &[Expr],
    _distinct: bool,
    ctx: &EvalContext,
) -> Result<Value> {
    match name.to_uppercase().as_str() {
        "ABS" => {
            let v = eval_expr(args.first().ok_or(Error::ExecutionError("ABS requires 1 arg".into()))?, ctx)?;
            match v {
                Value::Integer(n) => Ok(Value::Integer(n.abs())),
                Value::Real(f) => Ok(Value::Real(f.abs())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch { expected: "NUMBER".into(), got: v.type_name().into() }),
            }
        }
        "LENGTH" => {
            let v = eval_expr(args.first().ok_or(Error::ExecutionError("LENGTH requires 1 arg".into()))?, ctx)?;
            match v {
                Value::Text(s) => Ok(Value::Integer(s.len() as i64)),
                Value::Blob(b) => Ok(Value::Integer(b.len() as i64)),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch { expected: "TEXT".into(), got: v.type_name().into() }),
            }
        }
        "UPPER" => {
            let v = eval_expr(args.first().ok_or(Error::ExecutionError("UPPER requires 1 arg".into()))?, ctx)?;
            match v {
                Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch { expected: "TEXT".into(), got: v.type_name().into() }),
            }
        }
        "LOWER" => {
            let v = eval_expr(args.first().ok_or(Error::ExecutionError("LOWER requires 1 arg".into()))?, ctx)?;
            match v {
                Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch { expected: "TEXT".into(), got: v.type_name().into() }),
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            if args.len() < 2 {
                return Err(Error::ExecutionError("SUBSTR requires at least 2 args".into()));
            }
            let s = eval_expr(&args[0], ctx)?;
            let start = eval_expr(&args[1], ctx)?;
            match (s, start) {
                (Value::Text(s), Value::Integer(start)) => {
                    let start = (start - 1).max(0) as usize;
                    let result = if args.len() >= 3 {
                        let len = eval_expr(&args[2], ctx)?;
                        if let Value::Integer(len) = len {
                            s.chars().skip(start).take(len as usize).collect()
                        } else {
                            s.chars().skip(start).collect()
                        }
                    } else {
                        s.chars().skip(start).collect()
                    };
                    Ok(Value::Text(result))
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::ExecutionError("SUBSTR type error".into())),
            }
        }
        "COALESCE" => {
            for arg in args {
                let v = eval_expr(arg, ctx)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" | "NVL" => {
            if args.len() < 2 {
                return Err(Error::ExecutionError("IFNULL requires 2 args".into()));
            }
            let v = eval_expr(&args[0], ctx)?;
            if v.is_null() {
                eval_expr(&args[1], ctx)
            } else {
                Ok(v)
            }
        }
        "TYPEOF" => {
            let v = eval_expr(args.first().ok_or(Error::ExecutionError("TYPEOF requires 1 arg".into()))?, ctx)?;
            Ok(Value::Text(v.type_name().to_lowercase()))
        }
        "ROUND" => {
            let v = eval_expr(args.first().ok_or(Error::ExecutionError("ROUND requires 1 arg".into()))?, ctx)?;
            let decimals = if args.len() >= 2 {
                if let Value::Integer(d) = eval_expr(&args[1], ctx)? { d } else { 0 }
            } else { 0 };
            match v {
                Value::Real(f) => {
                    let factor = 10f64.powi(decimals as i32);
                    Ok(Value::Real((f * factor).round() / factor))
                }
                Value::Integer(n) => Ok(Value::Integer(n)),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch { expected: "NUMBER".into(), got: v.type_name().into() }),
            }
        }
        // 聚合函数在 eval 层不处理（由执行器处理）
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
            Err(Error::ExecutionError(format!("Aggregate function {} must be used in SELECT with GROUP BY or as top-level aggregate", name)))
        }
        _ => Err(Error::ExecutionError(format!("Unknown function: {}", name))),
    }
}

/// SQL LIKE 模式匹配（% 匹配任意字符序列，_ 匹配单个字符）
fn like_match(s: &str, pattern: &str) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_match_inner(&s, &p)
}

fn like_match_inner(s: &[char], p: &[char]) -> bool {
    match (s, p) {
        (_, []) => s.is_empty(),
        (_, ['%', rest @ ..]) => {
            // % 匹配 0 到 n 个字符
            for i in 0..=s.len() {
                if like_match_inner(&s[i..], rest) {
                    return true;
                }
            }
            false
        }
        ([], _) => false,
        ([sc, s_rest @ ..], ['_', p_rest @ ..]) => {
            let _ = sc;
            like_match_inner(s_rest, p_rest)
        }
        ([sc, s_rest @ ..], [pc, p_rest @ ..]) => {
            sc.to_uppercase().eq(pc.to_uppercase()) && like_match_inner(s_rest, p_rest)
        }
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        Value::Integer(n) => n.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => String::new(),
        Value::Blob(b) => String::from_utf8_lossy(b).into_owned(),
    }
}
