use crate::db::SqliteRecorder; // <-- 新增导入
use crate::http_client::HttpClientService;
use crate::json_processor::JsonProcessor;
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time::sleep;
use uuid::Uuid;

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
    pub task_uuid: Option<String>,
    pub request_id: usize,
    pub seq_in_second: usize,
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

    pub fn modify_json(
        request_id: i64,
        timestamp: String,
        task_uuid: String,
        json_clone: &mut Value,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // 替换字段值，添加 UUID（operate on the per-task clone）
        let modifications = vec![
            (
                "TaskInfo.picture.0.targetAttrs.request_timestamp".to_string(),
                Value::String(timestamp.clone()),
            ),
            (
                "TaskInfo.picture.0.targetAttrs.request_id".to_string(),
                Value::Number(serde_json::Number::from(request_id as i64)),
            ),
            (
                "TaskInfo.picture.0.targetAttrs.task_uuid".to_string(),
                Value::String(task_uuid.clone()),
            ),
        ];
        for (field_path, new_value) in modifications {
            if let Err(e) = JsonProcessor::modify_json_field(json_clone, &field_path, new_value) {
                eprintln!("修改字段 {} 失败: {}", field_path, e);
                return Err(format!("修改报文失败: {}", e).into());
            }
        }

        let body_out = match JsonProcessor::format_json_pretty(&json_clone) {
            Ok(v) => v,
            Err(e) => return Err(format!("json转字符串失败: {}", e).into()),
        };

        Ok(body_out)
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

        let body = request_body.to_string();
        // 解析一次基础 JSON 模板，后续每个并发任务各自克隆并修改，避免跨任务共享可变状态
        let base_json = match serde_json::from_str::<Value>(&body) {
            Ok(v) => v,
            Err(e) => return Err(format!("解析 JSON 失败: {}", e).into()),
        };
        while start_time.elapsed().as_secs() < config.duration_seconds {
            let second_start = Instant::now();

            // 创建当前秒的所有并发任务并加入 JoinSet
            for i in 0..config.requests_per_second {
                let client = self.http_client.clone();
                let url = config.url.clone();
                let request_id = total_requests + i + 1;
                let seq_in_second = i + 1;

                // 为每个任务准备要移动的值，避免在 async move 中多次移动同一值或共享可变数据
                let mut json_clone = base_json.clone();

                join_set.spawn(async move {
                    let timestamp = chrono::Utc::now().to_rfc3339();
                    let ts_seconds = chrono::Utc::now().timestamp();
                    // 为每个并发任务生成唯一的 UUID
                    let task_uuid = Uuid::new_v4().to_string();

                    let body_out = match ConcurrencyTestService::modify_json(
                        request_id as i64,
                        timestamp.clone(),
                        task_uuid.clone(),
                        &mut json_clone,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            panic!("{}", format!("修改报文失败: {}", e));
                        }
                    };

                    // 发送请求并记录结果
                    let request_start = Instant::now();
                    let result = client.post_json(&url, &body_out, None).await;
                    let response_time = request_start.elapsed();

                    let request_result = match result {
                        Ok(response) => {
                            let status = response.status();
                            let status_code = Some(status.as_u16());
                            RequestResult {
                                task_uuid: Some(task_uuid),
                                request_id,
                                seq_in_second,
                                success: status.is_success(),
                                status_code,
                                response_time_ms: response_time.as_millis(),
                                timestamp,
                                error_message: if status.is_success() {
                                    Some("success".to_string())
                                } else {
                                    Some(format!("HTTP错误: {}", status))
                                },
                            }
                        }
                        Err(e) => RequestResult {
                            task_uuid: Some(task_uuid),
                            request_id,
                            seq_in_second,
                            success: false,
                            status_code: None,
                            response_time_ms: response_time.as_millis(),
                            timestamp,
                            error_message: Some(format!("请求错误: {}", e)),
                        },
                    };

                    // 将记录写入 SQLite（非阻塞）
                    if let Err(e) = SqliteRecorder::instance()
                        .await
                        .insert_request(
                            ts_seconds,
                            request_result.task_uuid.clone(),
                            request_result.request_id,
                            request_result.seq_in_second,
                            request_result.success,
                            request_result.error_message.clone(),
                        )
                        .await
                    {
                        eprintln!("写入 SQLite 失败: {}", e);
                    }

                    // 任务返回 RequestResult
                    request_result
                });
            }
            total_requests += config.requests_per_second;

            // 等待到下一秒开始
            let elapsed = second_start.elapsed();
            if elapsed < Duration::from_secs(1) {
                sleep(Duration::from_secs(1) - elapsed).await;
            }
        }
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(request_result) => {
                    if request_result.success {
                        successful_requests += 1;
                    } else {
                        failed_requests += 1;
                    }
                    all_request_results.push(request_result);
                }
                Err(_) => {
                    println!("任务执行错误");
                }
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

        // 一次性输出所有请求的统计摘要
        println!("\n=== 请求统计摘要 ===");
        if !result.detailed_results.is_empty() {
            let success_count = result.detailed_results.iter().filter(|r| r.success).count();
            let failure_count = result.detailed_results.len() - success_count;
            println!(
                "总请求: {} (成功: {}, 失败: {})",
                result.detailed_results.len(),
                success_count,
                failure_count
            );

            // 按状态码分组统计
            use std::collections::HashMap;
            let mut status_counts: HashMap<Option<u16>, usize> = HashMap::new();
            for result in &result.detailed_results {
                *status_counts.entry(result.status_code).or_insert(0) += 1;
            }

            println!("状态码分布:");
            for (status_code, count) in status_counts {
                let status_str = match status_code {
                    Some(code) => format!("{}", code),
                    None => "无状态码".to_string(),
                };
                println!("  {}: {} 个请求", status_str, count);
            }

            // 响应时间分布
            let response_times: Vec<u128> = result
                .detailed_results
                .iter()
                .map(|r| r.response_time_ms)
                .collect();

            if !response_times.is_empty() {
                let p50 = Self::percentile(&response_times, 0.5);
                let p90 = Self::percentile(&response_times, 0.9);
                let p95 = Self::percentile(&response_times, 0.95);
                let p99 = Self::percentile(&response_times, 0.99);

                println!("响应时间分位数:");
                println!("  P50: {}ms", p50);
                println!("  P90: {}ms", p90);
                println!("  P95: {}ms", p95);
                println!("  P99: {}ms", p99);
            }
        }

        // 只显示前5个请求的详细结果作为示例
        println!("\n=== 前5个请求的详细结果（示例） ===");
        for result in result.detailed_results.iter().take(5) {
            println!(
                "请求 {}: {} - 状态码: {:?} - 响应时间: {}ms",
                result.request_id,
                if result.success { "成功" } else { "失败" },
                result.status_code,
                result.response_time_ms
            );
        }
    }

    /// 计算百分位数
    fn percentile(data: &[u128], percentile: f64) -> u128 {
        if data.is_empty() {
            return 0;
        }
        let mut sorted = data.to_vec();
        sorted.sort();
        let index = (percentile * (sorted.len() - 1) as f64).round() as usize;
        sorted[index]
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
