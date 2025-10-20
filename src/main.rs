use reqwest::Error;
use serde_json::Value;
use std::fs;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time::sleep;

// 并发请求配置
#[derive(Debug)]
struct ConcurrencyConfig {
    requests_per_second: usize, // 每秒并发数
    duration_seconds: u64,      // 持续时间（秒）
    url: String,                // 请求URL
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 120,
            duration_seconds: 30,
            url: "https://jsonplaceholder.typicode.com/posts".to_string(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("开始发送 HTTP POST 请求...");

    let client = reqwest::Client::new();

    // 示例 1: 从 JSON 文件读取数据并发送 POST 请求
    println!("\n=== 示例 1: 从 JSON 文件读取数据发送 POST 请求 ===");

    // 读取 JSON 文件内容
    let json_content = match fs::read_to_string("post_data.json") {
        Ok(content) => content,
        Err(_) => {
            // 如果文件不存在，创建示例 JSON 文件
            let example_json = r#"{
    "title": "从文件读取的 POST 数据",
    "body": "这是一个从 JSON 文件读取数据的示例",
    "userId": 1,
    "tags": ["rust", "http", "post"],
    "metadata": {
        "priority": "high",
        "category": "development"
    }
}"#;
            fs::write("post_data.json", example_json).expect("创建示例 JSON 文件失败");
            example_json.to_string()
        }
    };

    println!("从文件读取的 JSON 内容:");
    println!("{}", json_content);

    let response = client
        .post("https://jsonplaceholder.typicode.com/posts")
        .header("Content-Type", "application/json")
        .body(json_content.clone())
        .send()
        .await?;

    println!("状态码: {}", response.status());

    if response.status().is_success() {
        let post_response: Value = response.json().await?;
        println!("响应数据: {:#}", post_response);
    }

    // 示例 2: 解析 JSON 并替换字段值
    println!("\n=== 示例 2: 解析 JSON 并替换字段值 ===");

    // 解析 JSON 内容（使用 match 处理错误）
    let mut json_value: Value = match serde_json::from_str(&json_content) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("JSON 解析错误: {}", e);
            return Ok(()); // 简单返回，继续执行其他示例
        }
    };

    println!("原始 JSON 解析结果: {:#}", json_value);

    // 替换字段值
    if let Value::Object(ref mut map) = json_value {
        // 替换顶层字段
        map.insert(
            "title".to_string(),
            Value::String("解析后修改的标题".to_string()),
        );
        map.insert(
            "body".to_string(),
            Value::String("这是通过解析 JSON 修改的内容".to_string()),
        );
        map.insert(
            "userId".to_string(),
            Value::Number(serde_json::Number::from(999)),
        );

        // 添加新字段
        map.insert(
            "modified_at".to_string(),
            Value::String("2025-01-20T17:36:00Z".to_string()),
        );
        map.insert("status".to_string(), Value::String("processed".to_string()));

        // 修改嵌套字段
        if let Some(Value::Object(metadata)) = map.get_mut("metadata") {
            metadata.insert("priority".to_string(), Value::String("urgent".to_string()));
            metadata.insert("modified".to_string(), Value::Bool(true));
            metadata.insert("version".to_string(), Value::String("2.0.0".to_string()));
        }

        // 修改数组字段
        if let Some(Value::Array(tags)) = map.get_mut("tags") {
            tags.push(Value::String("json".to_string()));
            tags.push(Value::String("parsing".to_string()));
        }
    }

    // 重新生成 JSON 内容
    let modified_json = match serde_json::to_string_pretty(&json_value) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("JSON 序列化错误: {}", e);
            return Ok(()); // 简单返回，继续执行其他示例
        }
    };

    println!("修改后的 JSON 内容:");
    println!("{}", modified_json);

    let modified_response = client
        .post("https://jsonplaceholder.typicode.com/posts")
        .header("Content-Type", "application/json")
        .body(modified_json.clone())
        .send()
        .await?;

    println!("状态码: {}", modified_response.status());

    if modified_response.status().is_success() {
        let response_data: Value = modified_response.json().await?;
        println!("修改后的响应数据: {:#}", response_data);
    }

    // 保存修改后的 JSON 到文件
    fs::write("parsed_modified_data.json", &modified_json).expect("保存解析修改后的 JSON 文件失败");
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

    // 替换模板变量
    let final_json = template_json
        .replace("{{name}}", "张三")
        .replace("{{email}}", "zhangsan@example.com")
        .replace("{{age}}", "30")
        .replace("{{request_type}}", "create_user")
        .replace("{{timestamp}}", "2025-01-20T17:30:00Z");

    println!("模板替换后的 JSON:");
    println!("{}", final_json);

    let template_response = client
        .post("https://httpbin.org/post")
        .header("Content-Type", "application/json")
        .body(final_json)
        .send()
        .await?;

    println!("状态码: {}", template_response.status());

    if template_response.status().is_success() {
        let response_text = template_response.text().await?;
        println!("响应内容长度: {} 字符", response_text.len());
        if response_text.len() > 300 {
            println!("响应预览: {}...", &response_text[..300]);
        }
    }

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

    fs::write("modified_post_data.json", save_data).expect("保存修改后的 JSON 文件失败");
    println!("已保存修改后的报文到 modified_post_data.json 文件");

    let file_response = client
        .post("https://httpbin.org/post")
        .header("Content-Type", "application/json")
        .body(save_data)
        .send()
        .await?;

    println!("状态码: {}", file_response.status());

    // 示例 5: 并发重复请求
    println!("\n=== 示例 5: 并发重复请求测试 ===");

    // 配置并发请求参数
    let config = ConcurrencyConfig {
        requests_per_second: 10, // 测试时使用较小的并发数，避免对测试服务器造成过大压力
        duration_seconds: 5,     // 测试时使用较短的持续时间
        url: "https://jsonplaceholder.typicode.com/posts".to_string(),
    };

    println!("并发请求配置: {:?}", config);
    println!("开始并发请求测试...");

    // 执行并发请求测试
    concurrency_test(&client, &config, &modified_json).await?;

    println!("请求完成！所有示例执行完毕。");

    Ok(())
}

/// 请求结果详细记录
#[derive(Debug, serde::Serialize, Clone)]
struct RequestResult {
    request_id: usize,
    success: bool,
    status_code: Option<u16>,
    response_time_ms: u128,
    timestamp: String,
    error_message: Option<String>,
}

/// 执行并发请求测试（使用 JoinSet 实现，包含详细请求跟踪）
async fn concurrency_test(
    client: &reqwest::Client,
    config: &ConcurrencyConfig,
    request_body: &str,
) -> Result<(), Error> {
    let start_time = Instant::now();
    let mut total_requests = 0;
    let mut successful_requests = 0;
    let mut failed_requests = 0;
    let mut all_request_results = Vec::new();

    println!(
        "开始 {} 秒的并发测试，每秒 {} 个请求...",
        config.duration_seconds, config.requests_per_second
    );

    // 计算总共需要执行的请求数
    let total_expected_requests = config.requests_per_second * config.duration_seconds as usize;

    // 使用 JoinSet 来管理并发任务
    let mut join_set = JoinSet::new();

    while start_time.elapsed().as_secs() < config.duration_seconds {
        let second_start = Instant::now();
        let mut second_requests = 0;

        // 创建当前秒的所有并发任务并加入 JoinSet
        for i in 0..config.requests_per_second {
            let client = client.clone();
            let url = config.url.clone();
            let body = request_body.to_string();
            let request_id = total_requests + i + 1;

            join_set.spawn(async move {
                let request_start = Instant::now();
                let timestamp = chrono::Utc::now().to_rfc3339();

                // 在请求报文中添加唯一标识
                let mut modified_body =
                    if let Ok(mut json_value) = serde_json::from_str::<Value>(&body) {
                        if let Value::Object(ref mut map) = json_value {
                            map.insert(
                                "request_id".to_string(),
                                Value::Number(serde_json::Number::from(request_id)),
                            );
                            map.insert(
                                "request_timestamp".to_string(),
                                Value::String(timestamp.clone()),
                            );
                        }
                        serde_json::to_string(&json_value).unwrap_or(body)
                    } else {
                        body
                    };

                let result = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .header("X-Request-ID", request_id.to_string())
                    .body(modified_body)
                    .send()
                    .await;

                let response_time = request_start.elapsed();

                match result {
                    Ok(response) => {
                        let status = response.status();
                        let status_code = Some(status.as_u16());

                        if status.is_success() {
                            // 尝试获取响应体中的 request_id 进行验证
                            let response_body = match response.text().await {
                                Ok(text) => Some(text),
                                Err(_) => None,
                            };

                            RequestResult {
                                request_id,
                                success: true,
                                status_code,
                                response_time_ms: response_time.as_millis(),
                                timestamp,
                                error_message: None,
                            }
                        } else {
                            RequestResult {
                                request_id,
                                success: false,
                                status_code,
                                response_time_ms: response_time.as_millis(),
                                timestamp,
                                error_message: Some(format!("HTTP错误: {}", status)),
                            }
                        }
                    }
                    Err(e) => RequestResult {
                        request_id,
                        success: false,
                        status_code: None,
                        response_time_ms: response_time.as_millis(),
                        timestamp,
                        error_message: Some(format!("请求错误: {}", e)),
                    },
                }
            });

            second_requests += 1;
            total_requests += 1;
        }

        // 等待当前秒的所有请求完成
        let mut second_successful = 0;
        let mut second_failed = 0;
        let mut second_results = Vec::new();

        // 收集当前秒的请求结果
        for _ in 0..second_requests {
            if let Some(result) = join_set.join_next().await {
                match result {
                    Ok(request_result) => {
                        if request_result.success {
                            second_successful += 1;
                            successful_requests += 1;
                        } else {
                            second_failed += 1;
                            failed_requests += 1;
                        }

                        let result_clone = request_result.clone();
                        all_request_results.push(result_clone.clone());
                        second_results.push(result_clone.clone());

                        // 每10个请求打印一次详细结果
                        if request_result.request_id % 10 == 0 {
                            println!(
                                "请求 {}: {} - 响应时间: {}ms",
                                request_result.request_id,
                                if request_result.success {
                                    "成功"
                                } else {
                                    "失败"
                                },
                                request_result.response_time_ms
                            );
                        }
                    }
                    Err(_) => {
                        second_failed += 1;
                        failed_requests += 1;
                        // 创建错误结果记录
                        let error_result = RequestResult {
                            request_id: total_requests,
                            success: false,
                            status_code: None,
                            response_time_ms: 0,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            error_message: Some("任务执行错误".to_string()),
                        };
                        all_request_results.push(error_result);
                    }
                }
            }
        }

        // 打印当前秒的统计
        let avg_response_time: u128 = if !second_results.is_empty() {
            second_results
                .iter()
                .map(|r| r.response_time_ms)
                .sum::<u128>()
                / second_results.len() as u128
        } else {
            0
        };

        println!(
            "第 {} 秒: 成功 {} / 失败 {} 请求, 平均响应时间: {}ms",
            start_time.elapsed().as_secs() + 1,
            second_successful,
            second_failed,
            avg_response_time
        );

        // 等待到下一秒开始
        let elapsed = second_start.elapsed();
        if elapsed < Duration::from_secs(1) {
            sleep(Duration::from_secs(1) - elapsed).await;
        }
    }

    // 确保所有任务都已完成
    while let Some(result) = join_set.join_next().await {
        if let Ok(request_result) = result {
            if request_result.success {
                successful_requests += 1;
            } else {
                failed_requests += 1;
            }
            all_request_results.push(request_result);
        }
    }

    // 按请求ID排序结果
    all_request_results.sort_by_key(|r| r.request_id);

    // 计算统计信息
    let total_time = start_time.elapsed();
    let actual_rps = total_requests as f64 / total_time.as_secs_f64();

    let response_times: Vec<u128> = all_request_results
        .iter()
        .map(|r| r.response_time_ms)
        .collect();
    let avg_response_time = if !response_times.is_empty() {
        response_times.iter().sum::<u128>() as f64 / response_times.len() as f64
    } else {
        0.0
    };

    let min_response_time = response_times.iter().min().copied().unwrap_or(0);
    let max_response_time = response_times.iter().max().copied().unwrap_or(0);

    // 打印测试结果
    println!("\n=== 详细并发测试结果（JoinSet实现） ===");
    println!("总请求数: {}", total_requests);
    println!("成功请求: {}", successful_requests);
    println!("失败请求: {}", failed_requests);
    println!(
        "成功率: {:.2}%",
        (successful_requests as f64 / total_requests as f64) * 100.0
    );
    println!("总耗时: {:?}", total_time);
    println!("实际RPS: {:.2}", actual_rps);
    println!("平均响应时间: {:.2}ms", avg_response_time);
    println!("最小响应时间: {}ms", min_response_time);
    println!("最大响应时间: {}ms", max_response_time);
    println!("预期请求数: {}", total_expected_requests);
    println!(
        "完成率: {:.2}%",
        (total_requests as f64 / total_expected_requests as f64) * 100.0
    );

    // 打印前10个请求的详细结果作为示例
    println!("\n=== 前10个请求的详细结果 ===");
    for result in all_request_results.iter().take(10) {
        println!(
            "请求 {}: {} - 状态码: {:?} - 响应时间: {}ms",
            result.request_id,
            if result.success { "成功" } else { "失败" },
            result.status_code,
            result.response_time_ms
        );
    }

    // 保存详细测试结果到文件
    let detailed_results = serde_json::json!({
        "summary": {
            "implementation": "JoinSet",
            "total_requests": total_requests,
            "successful_requests": successful_requests,
            "failed_requests": failed_requests,
            "success_rate": (successful_requests as f64 / total_requests as f64) * 100.0,
            "total_duration_seconds": total_time.as_secs(),
            "actual_rps": actual_rps,
            "average_response_time_ms": avg_response_time,
            "min_response_time_ms": min_response_time,
            "max_response_time_ms": max_response_time,
            "expected_requests": total_expected_requests,
            "completion_rate": (total_requests as f64 / total_expected_requests as f64) * 100.0
        },
        "config": {
            "requests_per_second": config.requests_per_second,
            "duration_seconds": config.duration_seconds,
            "url": config.url
        },
        "detailed_results": all_request_results
    });

    let detailed_json = serde_json::to_string_pretty(&detailed_results).unwrap();
    fs::write("detailed_concurrency_results.json", &detailed_json)
        .expect("保存详细并发测试结果失败");
    println!("已保存详细并发测试结果到 detailed_concurrency_results.json");

    // 同时保存简版结果
    let test_result = format!(
        r#"{{
    "concurrency_test_results": {{
        "implementation": "JoinSet",
        "total_requests": {},
        "successful_requests": {},
        "failed_requests": {},
        "success_rate": {:.2},
        "total_duration_seconds": {},
        "actual_rps": {:.2},
        "average_response_time_ms": {:.2},
        "min_response_time_ms": {},
        "max_response_time_ms": {},
        "expected_requests": {},
        "completion_rate": {:.2}
    }},
    "config": {{
        "requests_per_second": {},
        "duration_seconds": {},
        "url": "{}"
    }}
}}"#,
        total_requests,
        successful_requests,
        failed_requests,
        (successful_requests as f64 / total_requests as f64) * 100.0,
        total_time.as_secs(),
        actual_rps,
        avg_response_time,
        min_response_time,
        max_response_time,
        total_expected_requests,
        (total_requests as f64 / total_expected_requests as f64) * 100.0,
        config.requests_per_second,
        config.duration_seconds,
        config.url
    );

    fs::write("concurrency_test_results.json", &test_result).expect("保存并发测试结果失败");
    println!("已保存并发测试结果到 concurrency_test_results.json");

    Ok(())
}
