use HMSClipTest_rs::concurrency_test::{ConcurrencyConfig, ConcurrencyTestService};
use HMSClipTest_rs::db_adapter::get_recorder;
use HMSClipTest_rs::http_client::{AuthConfig, AuthType, HttpClientConfig, HttpClientService};
use HMSClipTest_rs::init_logging;
use HMSClipTest_rs::message::{Message, MessageFormat, MessageProcessor, MessageSource};
use bytes::Bytes;
use quick_xml::de::from_str as xml_from_str;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;
use warp::Filter;
use warp::http::StatusCode;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestRequest {
    url: String,
    auth_type: String,
    username: String,
    password: String,
    requests_per_second: u64,
    duration_seconds: u64,
    enable_interval_balance: Option<bool>,
    json_data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestResult {
    success: bool,
    total_requests: usize,
    successful_requests: usize,
    failed_requests: usize,
    average_response_time: f64,
    throughput: f64,
    error_rate: f64,
    duration: u64,
    detailed_results: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    println!("启动简化的 HTTP 性能测试 Web UI...");
    println!("服务器将运行在 http://localhost:8081");
    println!("可用端点:");
    println!("  GET / - Web 界面");
    println!("  POST /api/start-test - 启动性能测试");
    println!("  POST /alert - 接收报警（JSON/XML格式）");
    println!("  GET /health - 健康检查");

    // 主页路由
    let index_route = warp::path::end().map(|| warp::reply::html(include_str!("index.html")));

    // API 路由 - 性能测试
    let api_routes = warp::path("api")
        .and(warp::path("start-test"))
        .and(warp::post())
        .and(warp::body::json())
        .and_then(handle_test_request);

    // 报警接收路由
    let alert_route = warp::path("alert")
        .and(warp::post())
        .and(warp::body::bytes())
        .and_then(handle_alert_request);

    // 健康检查路由
    let health_route = warp::path("health").and(warp::get()).map(|| {
        warp::reply::json(&serde_json::json!({
            "status": "healthy",
            "service": "simple_web_ui",
            "timestamp": chrono::Utc::now().to_rfc3339()
        }))
    });

    // 组合所有路由
    let routes = index_route
        .or(api_routes)
        .or(alert_route)
        .or(health_route)
        .with(warp::cors().allow_any_origin());

    warp::serve(routes).run(([0, 0, 0, 0], 8081)).await;

    Ok(())
}

async fn handle_test_request(
    test_request: TestRequest,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // 根据认证配置创建HTTP客户端
    let http_client = if test_request.auth_type != "none" && !test_request.username.is_empty() {
        // 创建认证配置
        let auth_config = AuthConfig {
            username: test_request.username.clone(),
            password: test_request.password.clone(),
            auth_type: match test_request.auth_type.as_str() {
                "basic" => AuthType::Basic,
                "digest" => AuthType::Digest,
                _ => AuthType::Basic,
            },
        };

        let config = HttpClientConfig {
            timeout: std::time::Duration::from_secs(30),
            user_agent: "Rust-HTTP-Client/1.0".to_string(),
            auth: Some(auth_config),
        };

        match HttpClientService::new(config) {
            Ok(client) => client,
            Err(e) => {
                let reply = warp::reply::with_status(
                    format!("创建认证HTTP客户端失败: {}", e),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                );
                return Ok(Box::new(reply));
            }
        }
    } else {
        // 创建无认证的HTTP客户端
        match HttpClientService::new_default() {
            Ok(client) => client,
            Err(e) => {
                let reply = warp::reply::with_status(
                    format!("创建HTTP客户端失败: {}", e),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                );
                return Ok(Box::new(reply));
            }
        }
    };

    // 创建测试服务
    let test_service: ConcurrencyTestService = ConcurrencyTestService::new(http_client);

    // 配置测试参数
    let config = ConcurrencyConfig {
        requests_per_second: test_request.requests_per_second as usize,
        duration_seconds: test_request.duration_seconds,
        url: test_request.url,
        enable_interval_balance: test_request.enable_interval_balance.unwrap_or(false),
    };

    // 执行测试
    match test_service
        .run_test(&config, &test_request.json_data)
        .await
    {
        Ok(result) => {
            let test_result = TestResult {
                success: true,
                total_requests: result.summary.total_requests,
                successful_requests: result.summary.successful_requests,
                failed_requests: result.summary.failed_requests,
                average_response_time: result.summary.average_response_time_ms,
                throughput: result.summary.actual_rps,
                error_rate: 100.0 - result.summary.success_rate,
                duration: result.summary.total_duration_seconds,
                detailed_results: Some(serde_json::to_string_pretty(&result).unwrap_or_default()),
            };
            let reply = warp::reply::json(&test_result);
            Ok(Box::new(reply))
        }
        Err(e) => {
            let reply = warp::reply::with_status(
                format!("测试执行失败: {}", e),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
            Ok(Box::new(reply))
        }
    }
}

async fn handle_alert_request(body: Bytes) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // UTF-8 解码
    let content = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            let json = warp::reply::json(&serde_json::json!({
                "status": "error",
                "message": format!("无效的 UTF-8 内容: {}", e)
            }));
            let reply = warp::reply::with_status(json, StatusCode::BAD_REQUEST);
            return Ok(Box::new(reply));
        }
    };

    // 根据内容判断并解析格式
    match detect_and_parse_format(&content) {
        Ok((payload, format)) => {
            // 构造 Message 并异步投递到消息处理器（包含数据库记录）
            let msg_format = match format.as_str() {
                "JSON" => MessageFormat::Json,
                "XML" => MessageFormat::Xml,
                _ => MessageFormat::Unknown,
            };

            let msg = Message {
                id: Uuid::new_v4().to_string(),
                source: MessageSource::Http,
                payload: payload,
                received_at: chrono::Utc::now(),
                format: Some(msg_format),
            };

            // 提前获取 DbRecorder 实例，避免在异步块内部 await
            let recorder = get_recorder().await;

            // 异步处理消息（不阻塞HTTP响应）
            tokio::spawn(async move {
                if let Err(e) = MessageProcessor::process_message_with_recorder(msg, recorder).await
                {
                    eprintln!("Message processing error: {}", e);
                }
            });

            let json = warp::reply::json(&serde_json::json!({
                "status": "success",
                "message": "报警已接收并转发到消息处理器",
                "format": match format.as_str() {
                    "JSON" => "JSON",
                    "XML" => "XML",
                    _ => "UNKNOWN",
                },
                "content_length": content.len()
            }));
            Ok(Box::new(warp::reply::with_status(json, StatusCode::OK)))
        }
        Err(e) => {
            let json = warp::reply::json(&serde_json::json!({
                "status": "error",
                "message": format!("解析失败: {}", e)
            }));
            let reply = warp::reply::with_status(json, StatusCode::BAD_REQUEST);
            Ok(Box::new(reply))
        }
    }
}

/// 检测并解析报文格式
fn detect_and_parse_format(
    content: &str,
) -> Result<(Value, String), Box<dyn std::error::Error + Send + Sync>> {
    // 去除前导空白
    let trimmed = content.trim_start();

    // 基于内容特征判断格式
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        // 尝试解析 JSON
        serde_json::from_str(trimmed)
            .map(|value| (value, "JSON".to_string()))
            .map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("JSON解析失败: {}", e),
                )) as Box<dyn std::error::Error + Send + Sync>
            })
    } else if trimmed.starts_with("<?xml") || trimmed.starts_with('<') {
        // 尝试解析 XML
        xml_from_str(trimmed)
            .map(|value| (value, "XML".to_string()))
            .map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("XML解析失败: {}", e),
                )) as Box<dyn std::error::Error + Send + Sync>
            })
    } else {
        // 如果没有明显特征，尝试按顺序解析
        if let Ok(value) = serde_json::from_str(trimmed) {
            Ok((value, "JSON".to_string()))
        } else if let Ok(value) = xml_from_str(trimmed) {
            Ok((value, "XML".to_string()))
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "无法识别的报文格式",
            )) as Box<dyn std::error::Error + Send + Sync>)
        }
    }
}
