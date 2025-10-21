use std::fs;

use HMSClipTest_rs::{
    concurrency_test::ConcurrencyTestService,
    config::ConfigManager,
    http_client::HttpClientService,
    http_server::{check_server_health, start_http_server_background},
    init_logging,
    json_processor::JsonProcessor,
};
use serde_json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志系统
    init_logging();

    // 创建配置管理器
    let config_manager = ConfigManager::new();

    // 验证配置
    if let Err(errors) = config_manager.validate() {
        eprintln!("配置验证失败:");
        for error in errors {
            eprintln!("  - {}", error);
        }
        return Ok(());
    }
    // 在后台启动 HTTP 服务器（不阻塞主线程）
    let port = 8081;
    match start_http_server_background(port).await {
        Ok(server_handle) => {
            println!("HTTP 服务器已在后台启动，端口: {}", port);

            // 等待服务器启动并检查状态
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            match check_server_health(port).await {
                Ok(true) => println!("HTTP 服务器健康检查通过"),
                Ok(false) => eprintln!("HTTP 服务器健康检查失败"),
                Err(e) => eprintln!("HTTP 服务器健康检查错误: {}", e),
            }

            // 保存服务器句柄（如果需要后续控制）
            let _server_handle = server_handle;
        }
        Err(e) => {
            eprintln!("HTTP 服务器启动失败: {}", e);
            return Ok(());
        }
    }

    // 创建 HTTP 客户端
    let http_config = config_manager
        .create_http_client_config_with_digest_auth("admin".to_string(), "DCtest@v587".to_string());
    let http_client = HttpClientService::new(http_config)?;

    // 读取或创建示例 JSON 文件
    let json_content = match JsonProcessor::read_json_file("post_data.json") {
        Ok(content) => content,
        Err(_) => {
            println!("未找到 post_data.json 文件，创建示例文件...");
            return Ok(());
        }
    };

    // 配置并发请求参数
    let _concurrency_config = config_manager.create_concurrency_config();
    let test_config = HMSClipTest_rs::concurrency_test::ConcurrencyConfig {
        requests_per_second: 10, // 测试时使用较小的并发数
        duration_seconds: 5,     // 测试时使用较短的持续时间
        url: "https://10.41.131.103/ISAPI/Intelligent/AIOpenPlatform/pictureTask?format=json"
            .to_string(),
    };

    println!("并发请求配置: {:?}", test_config);
    println!("开始并发请求测试...");

    // 执行并发请求测试
    let test_service = ConcurrencyTestService::new(http_client);
    let test_result = test_service.run_test(&test_config, &json_content).await?;

    // 打印测试结果
    test_service.print_test_result(&test_result);

    // 保存测试结果到文件
    let detailed_json = serde_json::to_string_pretty(&test_result)?;
    fs::write("detailed_concurrency_results.json", &detailed_json)?;
    println!("已保存详细并发测试结果到 detailed_concurrency_results.json");

    // 同时保存简版结果
    let test_result_json = serde_json::to_string_pretty(&serde_json::json!({
        "concurrency_test_results": test_result.summary,
        "config": test_result.config
    }))?;

    fs::write("concurrency_test_results.json", &test_result_json)?;
    println!("已保存并发测试结果到 concurrency_test_results.json");

    // 示例 6: 启动 HTTP 服务接收报警
    println!("\n=== 示例 6: 启动 HTTP 服务接收报警 ===");

    println!("请求完成！所有示例执行完毕。");

    // 保持程序运行，等待用户输入
    println!("\nHTTP 服务器仍在后台运行，端口: {}", port);
    println!("可以继续发送报警到: http://localhost:{}/alert", port);
    println!("查看所有报警: http://localhost:{}/alerts", port);
    println!("健康检查: http://localhost:{}/health", port);
    println!("\n按 'q' + Enter 退出程序，或按其他键 + Enter 检查服务器状态...");

    loop {
        let mut input = String::new();
        match std::io::stdin().read_line(&mut input) {
            Ok(_) => {
                match input.trim() {
                    "q" | "Q" => {
                        println!("正在退出程序...");
                        break;
                    }
                    _ => {
                        // 检查服务器状态
                        match check_server_health(port).await {
                            Ok(true) => println!("✅ HTTP 服务器运行正常"),
                            Ok(false) => println!("❌ HTTP 服务器健康检查失败"),
                            Err(e) => println!("❌ HTTP 服务器检查错误: {}", e),
                        }
                        println!("按 'q' + Enter 退出程序，或按其他键 + Enter 再次检查状态...");
                    }
                }
            }
            Err(error) => {
                eprintln!("读取输入错误: {}", error);
                println!("按 'q' + Enter 退出程序...");
            }
        }
    }

    println!("程序已退出。");
    Ok(())
}
