use reqwest::Error;
use serde_json::Value;
use std::time::Duration;

/// HTTP 客户端配置
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    pub timeout: Duration,
    pub user_agent: String,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            user_agent: "Rust-HTTP-Client/1.0".to_string(),
        }
    }
}

/// HTTP 客户端服务
#[derive(Clone)]
pub struct HttpClientService {
    client: reqwest::Client,
    config: HttpClientConfig,
}

impl HttpClientService {
    /// 创建新的 HTTP 客户端服务
    pub fn new(config: HttpClientConfig) -> Result<Self, Error> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .user_agent(&config.user_agent)
            .build()?;

        Ok(Self { client, config })
    }

    /// 发送 POST 请求
    pub async fn post_json(
        &self,
        url: &str,
        json_data: &str,
        headers: Option<Vec<(&str, &str)>>,
    ) -> Result<reqwest::Response, Error> {
        let mut request = self.client.post(url).body(json_data.to_string());

        // 添加默认的 Content-Type 头
        request = request.header("Content-Type", "application/json");

        // 添加自定义头部
        if let Some(headers) = headers {
            for (key, value) in headers {
                request = request.header(key, value);
            }
        }

        request.send().await
    }

    /// 发送 POST 请求并返回 JSON 响应
    pub async fn post_json_with_response<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        json_data: &str,
        headers: Option<Vec<(&str, &str)>>,
    ) -> Result<T, Error> {
        let response = self.post_json(url, json_data, headers).await?;

        if response.status().is_success() {
            response.json().await
        } else {
            Err(Error::from(response.error_for_status().unwrap_err()))
        }
    }

    /// 发送 POST 请求并返回文本响应
    pub async fn post_json_with_text(
        &self,
        url: &str,
        json_data: &str,
        headers: Option<Vec<(&str, &str)>>,
    ) -> Result<String, Error> {
        let response = self.post_json(url, json_data, headers).await?;
        response.text().await
    }
}

/// 简化的 HTTP 客户端函数
pub async fn send_post_request(url: &str, json_data: &str) -> Result<reqwest::Response, Error> {
    let client = reqwest::Client::new();
    client
        .post(url)
        .header("Content-Type", "application/json")
        .body(json_data.to_string())
        .send()
        .await
}
