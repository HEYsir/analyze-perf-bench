use crate::concurrency_test::RequestResult;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use warp::Filter;

/// 报警数据结构
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct Alert {
    pub id: String,
    pub level: String, // "critical", "warning", "info"
    pub message: String,
    pub timestamp: String,
    pub source: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

/// 请求与报警关联记录
#[derive(Debug, serde::Serialize, Clone)]
pub struct RequestAlertCorrelation {
    pub request_id: usize,
    pub request_success: bool,
    pub request_time: String,
    pub alert_received_time: String,
    pub alert_message_summary: String,
}

/// HTTP 服务状态
#[derive(Clone)]
pub struct ServerState {
    pub alerts: Arc<Mutex<Vec<Alert>>>,
    pub alert_count: Arc<Mutex<usize>>,
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            alerts: Arc::new(Mutex::new(Vec::new())),
            alert_count: Arc::new(Mutex::new(0)),
        }
    }
}

/// 报警关联管理状态
#[derive(Clone)]
pub struct AlertCorrelationState {
    pub correlations: Arc<Mutex<Vec<RequestAlertCorrelation>>>,
    pub request_results: Arc<Mutex<Vec<RequestResult>>>,
}

impl AlertCorrelationState {
    pub fn new() -> Self {
        Self {
            correlations: Arc::new(Mutex::new(Vec::new())),
            request_results: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

/// HTTP 服务器服务
pub struct HttpServerService;

impl HttpServerService {
    /// 启动 HTTP 服务器接收报警
    pub async fn start_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let state = ServerState::new();
        let correlation_state = AlertCorrelationState::new();

        let state_filter = warp::any().map(move || state.clone());
        let correlation_filter = warp::any().map(move || correlation_state.clone());

        println!("启动 HTTP 服务器在 http://localhost:{}", port);
        println!("可用端点:");
        println!("  POST /alert - 接收报警");
        println!("  GET /alerts - 查看所有报警");
        println!("  GET /alerts/count - 查看报警数量");
        println!("  GET /alerts/level/{{level}} - 按级别查看报警");
        println!("  DELETE /alerts/{{id}} - 删除特定报警");
        println!("  DELETE /alerts - 清空所有报警");
        println!("  GET /health - 健康检查");

        // POST /alert - 接收报警
        let receive_alert = warp::path("alert")
            .and(warp::post())
            .and(warp::body::json())
            .and(state_filter.clone())
            .and(correlation_filter.clone())
            .and_then(|alert: Alert, state: ServerState, correlation_state: AlertCorrelationState| async move {
                Self::handle_alert(alert, state, correlation_state).await
            });

        // GET /alerts - 查看所有报警
        let get_alerts = warp::path("alerts")
            .and(warp::get())
            .and(state_filter.clone())
            .and_then(|state: ServerState| async move {
                let alerts = state.alerts.lock().await;
                Ok::<_, warp::Rejection>(warp::reply::json(&*alerts))
            });

        // GET /alerts/count - 查看报警数量
        let get_alert_count = warp::path!("alerts" / "count")
            .and(warp::get())
            .and(state_filter.clone())
            .and_then(|state: ServerState| async move {
                let count = state.alert_count.lock().await;
                Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
                    "total_alerts": *count
                })))
            });

        // GET /alerts/level/{level} - 按级别查看报警
        let get_alerts_by_level = warp::path!("alerts" / "level" / String)
            .and(warp::get())
            .and(state_filter.clone())
            .and_then(|level: String, state: ServerState| async move {
                let alerts = state.alerts.lock().await;
                let filtered: Vec<&Alert> = alerts
                    .iter()
                    .filter(|alert| alert.level.to_lowercase() == level.to_lowercase())
                    .collect();
                Ok::<_, warp::Rejection>(warp::reply::json(&filtered))
            });

        // DELETE /alerts/{id} - 删除特定报警
        let delete_alert = warp::path!("alerts" / String)
            .and(warp::delete())
            .and(state_filter.clone())
            .and_then(|id: String, state: ServerState| async move {
                Self::delete_alert_by_id(id, state).await
            });

        // DELETE /alerts - 清空所有报警
        let clear_alerts = warp::path("alerts")
            .and(warp::delete())
            .and(state_filter.clone())
            .and_then(|state: ServerState| async move { Self::clear_all_alerts(state).await });

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
            .or(get_alerts)
            .or(get_alert_count)
            .or(get_alerts_by_level)
            .or(delete_alert)
            .or(clear_alerts)
            .or(health_check)
            .with(warp::cors().allow_any_origin());

        println!("HTTP 服务器已启动，监听端口 {}", port);
        println!("按 Ctrl+C 停止服务器");

        warp::serve(routes).run(([127, 0, 0, 1], port)).await;
        Ok(())
    }

    /// 处理报警接收
    async fn handle_alert(
        alert: Alert,
        state: ServerState,
        correlation_state: AlertCorrelationState,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let mut alerts = state.alerts.lock().await;
        let mut count = state.alert_count.lock().await;

        alerts.push(alert.clone());
        *count += 1;

        // 解析报警内容中的请求ID
        let mut request_id: Option<usize> = None;
        let mut task_uuid: Option<String> = None;
        let alert_message_summary = if alert.message.len() > 50 {
            format!("{}...", &alert.message[..50])
        } else {
            alert.message.clone()
        };

        // 尝试从消息中提取请求ID
        if let Some(metadata) = &alert.metadata {
            if let Some(Value::Number(id)) = metadata.get("request_id") {
                request_id = id.as_u64().map(|id| id as usize);
            }
        }

        // 如果从metadata中没找到，尝试解析 message 字段为 response_data.json 格式
        if request_id.is_none() {
            // 尝试解析 message 字段为 JSON
            if let Ok(response_data) = serde_json::from_str::<Value>(&alert.message) {
                // 检查是否是 response_data.json 格式
                if response_data.get("analysisResult").is_some() {
                    // 从 analysisResult 中提取 taskID
                    if let Some(Value::Array(results)) = response_data.get("analysisResult") {
                        if !results.is_empty() {
                            if let Some(Value::String(task_id)) = results[0].get("taskID") {
                                task_uuid = Some(task_id.clone());
                                println!("从报警消息中提取到 taskID: {}", task_id);
                            }
                        }
                    }
                }
            }
        }

        // 如果从metadata中没找到，尝试从消息中解析请求ID
        if request_id.is_none() && task_uuid.is_none() {
            // 简单的正则匹配，查找 "request_id": 123 这样的模式
            let re = regex::Regex::new(r#""request_id"\s*:\s*(\d+)"#).unwrap();
            if let Some(caps) = re.captures(&alert.message) {
                if let Ok(id) = caps[1].parse::<usize>() {
                    request_id = Some(id);
                }
            }
        }

        // 如果找到了 task_uuid，从并发任务中查找匹配的请求
        if let Some(task_id) = &task_uuid {
            let mut correlations = correlation_state.correlations.lock().await;
            let request_results = correlation_state.request_results.lock().await;

            // 查找包含相同 task_uuid 的请求结果
            let matched_request = request_results
                .iter()
                .find(|result| {
                    // 检查请求结果中是否包含这个 task_uuid
                    // 这里假设 task_uuid 存储在请求体的某个字段中
                    // 在实际实现中，需要检查请求的具体内容
                    result
                        .error_message
                        .as_ref()
                        .map_or(false, |msg| msg.contains(task_id))
                })
                .cloned();

            if let Some(result) = matched_request {
                let correlation = RequestAlertCorrelation {
                    request_id: result.request_id,
                    request_success: result.success,
                    request_time: result.timestamp.clone(),
                    alert_received_time: alert.timestamp.clone(),
                    alert_message_summary: format!("关联 taskID: {}", task_id),
                };

                correlations.push(correlation);

                println!(
                    "收到报警并与请求关联 - 请求ID: {}, taskID: {}, 成功: {}, 请求时间: {}, 报警时间: {}",
                    result.request_id, task_id, result.success, result.timestamp, alert.timestamp
                );

                request_id = Some(result.request_id);
            } else {
                println!(
                    "收到报警但未找到对应的请求记录 - taskID: {}, 报警摘要: {}",
                    task_id, alert_message_summary
                );
            }
        }
        // 如果找到了请求ID，创建关联记录
        else if let Some(req_id) = request_id {
            let mut correlations = correlation_state.correlations.lock().await;
            let request_results = correlation_state.request_results.lock().await;

            // 查找对应的请求结果
            let request_result = request_results
                .iter()
                .find(|r| r.request_id == req_id)
                .cloned();

            if let Some(result) = request_result {
                let correlation = RequestAlertCorrelation {
                    request_id: req_id,
                    request_success: result.success,
                    request_time: result.timestamp.clone(),
                    alert_received_time: alert.timestamp.clone(),
                    alert_message_summary: alert_message_summary.clone(),
                };

                correlations.push(correlation);

                println!(
                    "收到报警并与请求关联 - 请求ID: {}, 成功: {}, 请求时间: {}, 报警时间: {}, 报警摘要: {}",
                    req_id,
                    result.success,
                    result.timestamp,
                    alert.timestamp,
                    alert_message_summary
                );
            } else {
                println!(
                    "收到报警但未找到对应的请求记录 - 请求ID: {}, 报警摘要: {}",
                    req_id, alert_message_summary
                );
            }
        } else {
            println!(
                "收到新报警（未关联请求）: {} - {}",
                alert.level, alert_message_summary
            );
        }

        Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
            "status": "success",
            "message": "报警接收成功",
            "alert_id": alert.id,
            "request_id_found": request_id.is_some(),
            "request_id": request_id,
            "task_uuid_found": task_uuid.is_some(),
            "task_uuid": task_uuid
        })))
    }

    /// 删除特定报警
    async fn delete_alert_by_id(
        id: String,
        state: ServerState,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let mut alerts = state.alerts.lock().await;
        let mut count = state.alert_count.lock().await;

        let original_len = alerts.len();
        alerts.retain(|alert| alert.id != id);

        if alerts.len() < original_len {
            *count = alerts.len();
            println!("删除报警: {}", id);
            Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
                "status": "success",
                "message": "报警删除成功"
            })))
        } else {
            Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
                "status": "error",
                "message": "报警不存在"
            })))
        }
    }

    /// 清空所有报警
    async fn clear_all_alerts(state: ServerState) -> Result<impl warp::Reply, warp::Rejection> {
        let mut alerts = state.alerts.lock().await;
        let mut count = state.alert_count.lock().await;

        let cleared_count = alerts.len();
        alerts.clear();
        *count = 0;

        println!("清空所有报警，共 {} 条", cleared_count);

        Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
            "status": "success",
            "message": format!("成功清空 {} 条报警", cleared_count)
        })))
    }
}

/// 简化的服务器启动函数
pub async fn start_http_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    HttpServerService::start_server(port).await
}
