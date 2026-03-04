/// RobotDB 交互式命令行工具
///
/// 类似 SQLite 的 `sqlite3` 命令行界面，支持：
/// - 交互式 SQL 输入（带历史记录）
/// - 多行语句输入
/// - .tables / .schema / .quit 等元命令

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;

use robotdb::Database;

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    let db_path = args.get(1).map(|s| s.as_str()).unwrap_or(":memory:");

    println!("RobotDB v{} - Embedded Relational Database in Rust", env!("CARGO_PKG_VERSION"));
    println!("Enter SQL statements or .help for commands. .quit to exit.");
    println!("Database: {}", db_path);
    println!();

    let mut db = match if db_path == ":memory:" {
        Database::open_in_memory()
    } else {
        Database::open(db_path)
    } {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening database: {}", e);
            std::process::exit(1);
        }
    };

    let mut rl = DefaultEditor::new().expect("Failed to create editor");
    let history_file = dirs_path();
    let _ = rl.load_history(&history_file);

    let mut multiline_buf = String::new();

    loop {
        let prompt = if multiline_buf.is_empty() { "robotdb> " } else { "   ...> " };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // 元命令
                if multiline_buf.is_empty() && trimmed.starts_with('.') {
                    handle_meta_command(trimmed, &mut db);
                    continue;
                }

                multiline_buf.push_str(&line);
                multiline_buf.push(' ');

                // 检查是否语句完整（以分号结尾）
                if trimmed.ends_with(';') || (!trimmed.is_empty() && is_complete_statement(&multiline_buf)) {
                    let sql = multiline_buf.trim().to_string();
                    multiline_buf.clear();

                    if sql.is_empty() {
                        continue;
                    }

                    let _ = rl.add_history_entry(&sql);

                    match db.query(&sql) {
                        Ok(result) => {
                            if !result.columns.is_empty() {
                                print_result(&result);
                            } else if result.rows_affected > 0 {
                                println!("OK ({} row(s) affected)", result.rows_affected);
                            } else {
                                println!("OK");
                            }
                        }
                        Err(e) => {
                            eprintln!("Error: {}", e);
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                multiline_buf.clear();
                println!("^C");
            }
            Err(ReadlineError::Eof) => {
                println!("Bye!");
                break;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }

    let _ = rl.save_history(&history_file);
    let _ = db.close();
}

fn handle_meta_command(cmd: &str, db: &mut Database) {
    let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
    match parts[0] {
        ".quit" | ".exit" | ".q" => {
            println!("Bye!");
            std::process::exit(0);
        }
        ".help" | ".h" => {
            println!("Meta commands:");
            println!("  .tables          List all tables");
            println!("  .schema [table]  Show CREATE TABLE statement");
            println!("  .quit            Exit RobotDB");
            println!("  .checkpoint      Execute WAL checkpoint");
            println!("  .help            Show this help");
        }
        ".tables" => {
            let tables = db.table_names();
            if tables.is_empty() {
                println!("(no tables)");
            } else {
                for t in tables {
                    println!("{}", t);
                }
            }
        }
        ".schema" => {
            let table_name = parts.get(1).map(|s| s.trim());
            match db.query(&format!(
                "PRAGMA table_info{}",
                table_name.map(|t| format!("('{}')", t)).unwrap_or_default()
            )) {
                Ok(result) => print_result(&result),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        ".checkpoint" => {
            match db.checkpoint() {
                Ok(_) => println!("Checkpoint complete"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        _ => {
            eprintln!("Unknown command: {}. Try .help", parts[0]);
        }
    }
}

fn print_result(result: &robotdb::ResultSet) {
    if result.rows.is_empty() {
        println!("(empty result set)");
        return;
    }

    // 计算每列宽度
    let mut col_widths: Vec<usize> = result.columns.iter().map(|c| c.len()).collect();
    for row in &result.rows {
        for (i, val) in row.iter().enumerate() {
            if i < col_widths.len() {
                col_widths[i] = col_widths[i].max(val.to_string().len());
            }
        }
    }

    // 打印表头
    let header: Vec<String> = result.columns.iter().enumerate()
        .map(|(i, c)| format!("{:<width$}", c, width = col_widths.get(i).copied().unwrap_or(10)))
        .collect();
    println!("{}", header.join(" | "));

    // 分隔线
    let sep: Vec<String> = col_widths.iter().map(|&w| "-".repeat(w)).collect();
    println!("{}", sep.join("-+-"));

    // 数据行
    for row in &result.rows {
        let cells: Vec<String> = row.iter().enumerate()
            .map(|(i, v)| format!("{:<width$}", v.to_string(), width = col_widths.get(i).copied().unwrap_or(10)))
            .collect();
        println!("{}", cells.join(" | "));
    }

    println!("({} row(s))", result.rows.len());
}

fn is_complete_statement(sql: &str) -> bool {
    let s = sql.trim();
    // 简单检查：以分号结尾或是单词关键字命令
    s.ends_with(';')
        || s.to_uppercase().starts_with("BEGIN")
        || s.to_uppercase().starts_with("COMMIT")
        || s.to_uppercase().starts_with("ROLLBACK")
        || s.to_uppercase().starts_with("VACUUM")
}

fn dirs_path() -> String {
    let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/.robotdb_history", home)
}
