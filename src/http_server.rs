use crate::message::{Message, MessageFormat, MessageProcessor, MessageSource};
use bytes::Bytes;
use quick_xml::de::from_str as xml_from_str;
use serde_json::Value;
use std::error::Error;
use uuid::Uuid;
use warp::Filter;
use warp::http::StatusCode;

#[derive(Debug)]
pub struct ParseError(String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for ParseError {}

/// HTTP 服务器服务
pub struct HttpServerService;

impl HttpServerService {
    /// 启动 HTTP 服务器接收报警
    pub async fn start_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
        println!("启动 HTTP 服务器在 http://localhost:{}", port);
        println!("可用端点:");
        println!("  POST /alert - 接收报警");
        println!("  GET /health - 健康检查");

        // POST /alert - 接收报警（根据内容自动识别 JSON 或 XML，直接转发到 MessageProcessor）
        let receive_alert = warp::path("alert")
            .and(warp::post())
            .and(warp::body::bytes())
            .and_then(|body: Bytes| async move {
                // UTF-8 解码
                let content = match String::from_utf8(body.to_vec()) {
                    Ok(s) => s,
                    Err(e) => {
                        let json = warp::reply::json(&serde_json::json!({
                            "status": "error",
                            "message": format!("无效的 UTF-8 内容: {}", e)
                        }));
                        return Ok::<_, warp::Rejection>(warp::reply::with_status(
                            json,
                            StatusCode::BAD_REQUEST,
                        ));
                    }
                };

                // 根据内容判断并解析格式
                match Self::detect_and_parse_format(&content) {
                    Ok((payload, format)) => Self::handle_raw_message(payload, format).await,
                    Err(e) => {
                        let json = warp::reply::json(&serde_json::json!({
                            "status": "error",
                            "message": format!("解析失败: {}", e)
                        }));
                        Ok(warp::reply::with_status(json, StatusCode::BAD_REQUEST))
                    }
                }
            });

        // 健康检查端点
        let health_check = warp::path("health").and(warp::get()).map(|| {
            warp::reply::json(&serde_json::json!({
                "status": "healthy",
                "service": "alert_receiver",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }))
        });

        // 组合所有路由
        let routes = receive_alert
            .or(health_check)
            .with(warp::cors().allow_any_origin());

        println!("HTTP 服务器已启动，监听端口 {}", port);
        println!("按 Ctrl+C 停止服务器");

        warp::serve(routes).run(([127, 0, 0, 1], port)).await;
        Ok(())
    }

    /// 检测并解析报文格式
    fn detect_and_parse_format(
        content: &str,
    ) -> Result<(Value, MessageFormat), Box<dyn Error + Send + Sync>> {
        // 去除前导空白
        let trimmed = content.trim_start();

        // 基于内容特征判断格式
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            // 尝试解析 JSON
            serde_json::from_str(trimmed)
                .map(|value| (value, MessageFormat::Json))
                .map_err(|e| {
                    Box::new(ParseError(format!("JSON解析失败: {}", e)))
                        as Box<dyn Error + Send + Sync>
                })
        } else if trimmed.starts_with("<?xml") || trimmed.starts_with('<') {
            // 尝试解析 XML
            xml_from_str(trimmed)
                .map(|value| (value, MessageFormat::Xml))
                .map_err(|e| {
                    Box::new(ParseError(format!("XML解析失败: {}", e)))
                        as Box<dyn Error + Send + Sync>
                })
        } else {
            // 如果没有明显特征，尝试按顺序解析
            if let Ok(value) = serde_json::from_str(trimmed) {
                Ok((value, MessageFormat::Json))
            } else if let Ok(value) = xml_from_str(trimmed) {
                Ok((value, MessageFormat::Xml))
            } else {
                Err(Box::new(ParseError("无法识别的报文格式".to_string())))
            }
        }
    }

    /// 处理原始报文：直接封装为 Message 并送入 MessageProcessor（非阻塞），不做任何内容解析或保存
    async fn handle_raw_message(
        body: Value,
        format: MessageFormat,
    ) -> Result<warp::reply::WithStatus<warp::reply::Json>, warp::Rejection> {
        // 构造 Message 并异步投递，不阻塞请求
        let msg_format = format.clone();
        let msg = Message {
            id: Uuid::new_v4().to_string(),
            source: MessageSource::Http,
            payload: body,
            received_at: chrono::Utc::now(),
            format: Some(msg_format),
        };
        tokio::spawn(async move {
            if let Err(e) = MessageProcessor::process_message(msg).await {
                eprintln!("Message processing error: {}", e);
            }
        });

        let json = warp::reply::json(&serde_json::json!({
            "status": "success",
            "message": "报文已接收并转发到消息处理器",
            "format": match format {
                MessageFormat::Json => "JSON",
                MessageFormat::Xml => "XML",
                MessageFormat::Unknown => "UNKNOWN",
            }
        }));
        Ok(warp::reply::with_status(json, StatusCode::OK))
    }
}

/// 在后台启动 HTTP 服务器（不阻塞主线程）
pub async fn start_http_server_background(
    port: u16,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error>> {
    let server_handle = tokio::spawn(async move {
        if let Err(e) = HttpServerService::start_server(port).await {
            eprintln!("HTTP 服务器运行错误: {}", e);
        }
    });

    // 等待一小段时间确保服务器正在启动
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(server_handle)
}

/// 简化的服务器启动函数（保持向后兼容）
pub async fn start_http_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    HttpServerService::start_server(port).await
}

/// 获取服务器状态（用于检查服务器是否正在运行）
pub async fn check_server_health(port: u16) -> Result<bool, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let url = format!("http://localhost:{}/health", port);

    match client
        .get(&url)
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await
    {
        Ok(response) => Ok(response.status().is_success()),
        Err(_) => Ok(false),
    }
}
