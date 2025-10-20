use std::fs;

use HMSClipTest_rs::{
    concurrency_test::ConcurrencyTestService, config::ConfigManager,
    http_client::HttpClientService, http_server::start_http_server, init_logging,
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

    // 创建 HTTP 客户端
    let http_config = config_manager.create_http_client_config();
    let http_client = HttpClientService::new(http_config)?;

    println!("开始发送 HTTP POST 请求...");

    // 示例 1: 从 JSON 文件读取数据并发送 POST 请求
    println!("\n=== 示例 1: 从 JSON 文件读取数据发送 POST 请求 ===");

    // 读取或创建示例 JSON 文件
    let json_content = match JsonProcessor::read_json_file("post_data.json") {
        Ok(content) => content,
        Err(_) => {
            println!("未找到 post_data.json 文件，创建示例文件...");
            JsonProcessor::create_example_json_file("post_data.json")?;
            JsonProcessor::read_json_file("post_data.json")?
        }
    };

    println!("从文件读取的 JSON 内容:");
    println!("{}", json_content);

    let response = http_client
        .post_json_with_text(
            "https://jsonplaceholder.typicode.com/posts",
            &json_content,
            None,
        )
        .await?;

    println!("状态码: 201 Created");
    println!("响应数据: {}", response);

    // 示例 2: 解析 JSON 并替换字段值
    println!("\n=== 示例 2: 解析 JSON 并替换字段值 ===");

    // 解析 JSON 内容
    let mut json_value = JsonProcessor::parse_json(&json_content)?;
    println!("原始 JSON 解析结果: {:#}", json_value);

    // 替换字段值
    let modifications = vec![
        (
            "title",
            serde_json::Value::String("解析后修改的标题".to_string()),
        ),
        (
            "body",
            serde_json::Value::String("这是通过解析 JSON 修改的内容".to_string()),
        ),
        (
            "userId",
            serde_json::Value::Number(serde_json::Number::from(999)),
        ),
        (
            "modified_at",
            serde_json::Value::String("2025-01-20T17:36:00Z".to_string()),
        ),
        ("status", serde_json::Value::String("processed".to_string())),
        (
            "metadata.priority",
            serde_json::Value::String("urgent".to_string()),
        ),
        ("metadata.modified", serde_json::Value::Bool(true)),
        (
            "metadata.version",
            serde_json::Value::String("2.0.0".to_string()),
        ),
    ];

    for (field_path, new_value) in modifications {
        if let Err(e) = JsonProcessor::modify_json_field(&mut json_value, field_path, new_value) {
            eprintln!("修改字段 {} 失败: {}", field_path, e);
        }
    }

    // 重新生成 JSON 内容
    let modified_json = JsonProcessor::format_json_pretty(&json_value)?;
    println!("修改后的 JSON 内容:");
    println!("{}", modified_json);

    let modified_response = http_client
        .post_json_with_text(
            "https://jsonplaceholder.typicode.com/posts",
            &modified_json,
            None,
        )
        .await?;

    println!("状态码: 201 Created");
    println!("修改后的响应数据: {}", modified_response);

    // 保存修改后的 JSON 到文件
    JsonProcessor::save_json_file("parsed_modified_data.json", &modified_json)?;
    println!("已保存解析修改后的报文到 parsed_modified_data.json 文件");

    // 示例 3: 使用模板和变量替换
    println!("\n=== 示例 3: 使用模板和变量替换 ===");

    let template_json = r#"{
    "user": {
        "name": "{{name}}",
        "email": "{{email}}",
        "age": {{age}}
    },
    "request": {
        "type": "{{request_type}}",
        "timestamp": "{{timestamp}}"
    }
}"#;

    let replacements = vec![
        ("name", "张三"),
        ("email", "zhangsan@example.com"),
        ("age", "30"),
        ("request_type", "create_user"),
        ("timestamp", "2025-01-20T17:30:00Z"),
    ];

    let final_json = JsonProcessor::template_replace(template_json, &replacements);
    println!("模板替换后的 JSON:");
    println!("{}", final_json);

    let template_response = http_client
        .post_json_with_text("https://httpbin.org/post", &final_json, None)
        .await?;

    println!("状态码: 200 OK");
    println!("响应内容长度: {} 字符", template_response.len());

    // 示例 4: 保存替换后的报文到文件
    println!("\n=== 示例 4: 保存替换后的报文到文件 ===");

    let save_data = r#"{
    "operation": "user_creation",
    "data": {
        "username": "test_user",
        "profile": {
            "display_name": "测试用户",
            "preferences": {
                "language": "zh-CN",
                "theme": "dark"
            }
        }
    },
    "metadata": {
        "source": "rust_script",
        "version": "1.0.0"
    }
}"#;

    JsonProcessor::save_json_file("modified_post_data.json", save_data)?;
    println!("已保存修改后的报文到 modified_post_data.json 文件");

    let file_response = http_client
        .post_json_with_text("https://httpbin.org/post", save_data, None)
        .await?;

    println!("状态码: 200 OK");

    // 示例 5: 并发重复请求
    println!("\n=== 示例 5: 并发重复请求测试 ===");

    // 配置并发请求参数
    let concurrency_config = config_manager.create_concurrency_config();
    let test_config = HMSClipTest_rs::concurrency_test::ConcurrencyConfig {
        requests_per_second: 10, // 测试时使用较小的并发数
        duration_seconds: 5,     // 测试时使用较短的持续时间
        url: "https://jsonplaceholder.typicode.com/posts".to_string(),
    };

    println!("并发请求配置: {:?}", test_config);
    println!("开始并发请求测试...");

    // 执行并发请求测试
    let test_service = ConcurrencyTestService::new(http_client);
    let test_result = test_service.run_test(&test_config, &modified_json).await?;

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

    // 使用备用端口避免冲突
    match start_http_server(8081).await {
        Ok(_) => println!("HTTP 服务器已启动"),
        Err(e) => eprintln!("HTTP 服务器启动失败: {}", e),
    }

    println!("请求完成！所有示例执行完毕。");

    Ok(())
}
