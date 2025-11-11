use crate::db_adapter::DbRecorder;
use std::time::{Duration, Instant};

/// 性能测试：比较批量写入和原始写入的性能差异
pub async fn run_performance_test() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 数据库写入性能对比测试 ===");

    // 测试批量写入性能
    println!("\n1. 测试批量写入性能:");
    let batch_recorder = DbRecorder::batch();
    batch_recorder.init("performance_batch.db").await?;

    let batch_duration = test_write_performance(batch_recorder.clone(), 1000).await?;
    println!(
        "批量写入 1000 条记录耗时: {:.2}ms",
        batch_duration.as_millis()
    );
    println!(
        "平均每条记录: {:.2}ms",
        batch_duration.as_micros() as f64 / 1000.0
    );

    // 关闭批量写入器
    batch_recorder.shutdown().await;

    // 测试原始写入性能
    println!("\n2. 测试原始写入性能:");
    let original_recorder = DbRecorder::original();
    original_recorder.init("performance_original.db").await?;

    let original_duration = test_write_performance(original_recorder.clone(), 1000).await?;
    println!(
        "原始写入 1000 条记录耗时: {:.2}ms",
        original_duration.as_millis()
    );
    println!(
        "平均每条记录: {:.2}ms",
        original_duration.as_micros() as f64 / 1000.0
    );

    // 关闭原始写入器
    original_recorder.shutdown().await;

    // 性能对比
    println!("\n3. 性能对比结果:");
    let speedup = original_duration.as_micros() as f64 / batch_duration.as_micros() as f64;
    println!("批量写入比原始写入快 {:.2}x", speedup);
    println!("性能提升: {:.2}%", (speedup - 1.0) * 100.0);

    Ok(())
}

/// 测试写入性能
async fn test_write_performance(
    recorder: DbRecorder,
    num_records: usize,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    // 并发写入测试数据
    let mut tasks = Vec::new();

    for i in 0..num_records {
        let recorder_clone = recorder.clone();
        let task = tokio::spawn(async move {
            // 模拟请求记录
            let result = recorder_clone
                .insert_request(
                    1700000000 + i as i64,
                    Some(format!("test-uuid-{}", i)),
                    i,
                    i % 100,
                    true,
                    None,
                )
                .await;

            // 模拟消息记录
            if result.is_ok() {
                let _ = recorder_clone
                    .insert_message(
                        i as u64,
                        Some(format!("test-uuid-{}", i)),
                        "hawkResult".to_string(),
                        1700000000 + i as i64,
                        1700000100 + i as i64,
                    )
                    .await;
            }

            result
        });
        tasks.push(task);
    }

    // 等待所有任务完成
    for task in tasks {
        let _ = task.await?;
    }

    // 如果是批量写入，强制刷新缓冲区
    // 注意：这里不能使用 ? 操作符，因为错误类型不兼容
    match recorder.flush().await {
        Ok(_) => {}
        Err(e) => eprintln!("刷新缓冲区失败: {}", e),
    }

    Ok(start_time.elapsed())
}

/// 创建性能测试二进制程序
pub async fn main_performance_test() -> Result<(), Box<dyn std::error::Error>> {
    println!("数据库写入性能测试工具");
    println!("=========================");

    run_performance_test().await?;

    println!("\n性能测试完成！");
    Ok(())
}
