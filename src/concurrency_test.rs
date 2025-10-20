use crate::http_client::HttpClientService;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;

/// 并发测试配置
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConcurrencyConfig {
    pub requests_per_second: usize,
    pub duration_seconds: u64,
    pub url: String,
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

/// 请求结果详细记录
#[derive(Debug, serde::Serialize, Clone)]
pub struct RequestResult {
    pub request_id: usize,
    pub success: bool,
    pub status_code: Option<u16>,
    pub response_time_ms: u128,
    pub timestamp: String,
    pub error_message: Option<String>,
}

/// 并发测试结果
#[derive(Debug, serde::Serialize)]
pub struct ConcurrencyTestResult {
    pub summary: TestSummary,
    pub config: ConcurrencyConfig,
    pub detailed_results: Vec<RequestResult>,
}

/// 测试摘要
#[derive(Debug, serde::Serialize)]
pub struct TestSummary {
    pub implementation: String,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub success_rate: f64,
    pub total_duration_seconds: u64,
    pub actual_rps: f64,
    pub average_response_time_ms: f64,
    pub min_response_time_ms: u128,
    pub max_response_time_ms: u128,
    pub expected_requests: usize,
    pub completion_rate: f64,
}

/// 并发测试服务
pub struct ConcurrencyTestService {
    http_client: HttpClientService,
}

impl ConcurrencyTestService {
    pub fn new(http_client: HttpClientService) -> Self {
        Self { http_client }
    }

    /// 执行并发测试
    pub async fn run_test(
        &self,
        config: &ConcurrencyConfig,
        request_body: &str,
    ) -> Result<ConcurrencyTestResult, Box<dyn std::error::Error>> {
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
                let client = self.http_client.clone();
                let url = config.url.clone();
                let body = request_body.to_string();
                let request_id = total_requests + i + 1;

                join_set.spawn(async move {
                    let request_start = Instant::now();
                    let timestamp = chrono::Utc::now().to_rfc3339();

                    // 在请求报文中添加唯一标识
                    let modified_body =
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
                        .post_json(
                            &url,
                            &modified_body,
                            Some(vec![("X-Request-ID", &request_id.to_string())]),
                        )
                        .await;

                    let response_time = request_start.elapsed();

                    match result {
                        Ok(response) => {
                            let status = response.status();
                            let status_code = Some(status.as_u16());

                            if status.is_success() {
                                // 尝试获取响应体中的 request_id 进行验证
                                let _response_body = match response.text().await {
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

        // 创建测试结果
        let summary = TestSummary {
            implementation: "JoinSet".to_string(),
            total_requests,
            successful_requests,
            failed_requests,
            success_rate: (successful_requests as f64 / total_requests as f64) * 100.0,
            total_duration_seconds: total_time.as_secs(),
            actual_rps,
            average_response_time_ms: avg_response_time,
            min_response_time_ms: min_response_time,
            max_response_time_ms: max_response_time,
            expected_requests: total_expected_requests,
            completion_rate: (total_requests as f64 / total_expected_requests as f64) * 100.0,
        };

        Ok(ConcurrencyTestResult {
            summary,
            config: config.clone(),
            detailed_results: all_request_results,
        })
    }

    /// 打印测试结果
    pub fn print_test_result(&self, result: &ConcurrencyTestResult) {
        println!("\n=== 详细并发测试结果（JoinSet实现） ===");
        println!("总请求数: {}", result.summary.total_requests);
        println!("成功请求: {}", result.summary.successful_requests);
        println!("失败请求: {}", result.summary.failed_requests);
        println!("成功率: {:.2}%", result.summary.success_rate);
        println!("总耗时: {}s", result.summary.total_duration_seconds);
        println!("实际RPS: {:.2}", result.summary.actual_rps);
        println!(
            "平均响应时间: {:.2}ms",
            result.summary.average_response_time_ms
        );
        println!("最小响应时间: {}ms", result.summary.min_response_time_ms);
        println!("最大响应时间: {}ms", result.summary.max_response_time_ms);
        println!("预期请求数: {}", result.summary.expected_requests);
        println!("完成率: {:.2}%", result.summary.completion_rate);

        // 打印前10个请求的详细结果作为示例
        println!("\n=== 前10个请求的详细结果 ===");
        for result in result.detailed_results.iter().take(10) {
            println!(
                "请求 {}: {} - 状态码: {:?} - 响应时间: {}ms",
                result.request_id,
                if result.success { "成功" } else { "失败" },
                result.status_code,
                result.response_time_ms
            );
        }
    }
}

/// 简化的并发测试函数
pub async fn run_simple_concurrency_test(
    url: &str,
    json_data: &str,
    requests_per_second: usize,
    duration_seconds: u64,
) -> Result<ConcurrencyTestResult, Box<dyn std::error::Error>> {
    let config = ConcurrencyConfig {
        url: url.to_string(),
        requests_per_second,
        duration_seconds,
    };

    let http_client = HttpClientService::new(Default::default())?;
    let test_service = ConcurrencyTestService::new(http_client);

    test_service.run_test(&config, json_data).await
}
