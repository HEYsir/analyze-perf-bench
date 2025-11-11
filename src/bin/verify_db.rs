use rusqlite::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("验证性能测试数据库内容...");

    // 验证批量写入数据库
    println!("\n1. 批量写入数据库 (performance_batch.db):");
    let conn_batch = Connection::open("performance_batch.db")?;

    let request_count: i64 =
        conn_batch.query_row("SELECT COUNT(*) FROM request_records", [], |row| row.get(0))?;
    println!("  请求记录数量: {}", request_count);

    let message_count: i64 =
        conn_batch.query_row("SELECT COUNT(*) FROM message_records", [], |row| row.get(0))?;
    println!("  消息记录数量: {}", message_count);

    // 验证原始写入数据库
    println!("\n2. 原始写入数据库 (performance_original.db):");
    let conn_original = Connection::open("performance_original.db")?;

    let request_count_orig: i64 =
        conn_original.query_row("SELECT COUNT(*) FROM request_records", [], |row| row.get(0))?;
    println!("  请求记录数量: {}", request_count_orig);

    let message_count_orig: i64 =
        conn_original.query_row("SELECT COUNT(*) FROM message_records", [], |row| row.get(0))?;
    println!("  消息记录数量: {}", message_count_orig);

    // 验证数据一致性
    println!("\n3. 数据一致性验证:");
    println!("  批量数据库 - 请求记录: {}", request_count);
    println!("  原始数据库 - 请求记录: {}", request_count_orig);
    println!("  批量数据库 - 消息记录: {}", message_count);
    println!("  原始数据库 - 消息记录: {}", message_count_orig);

    if request_count == request_count_orig && message_count == message_count_orig {
        println!("  ✅ 数据一致性验证通过！");
    } else {
        println!("  ⚠️  数据一致性验证失败！");
    }

    Ok(())
}
