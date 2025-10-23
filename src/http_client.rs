use anyhow::{Result, anyhow};
use base64::prelude::*;
use digest_auth::{AuthContext, HttpMethod, WwwAuthenticateHeader};
use reqwest::{Client, Response, cookie::Jar};
use std::sync::Arc;
use std::time::Duration;

/// HTTP 客户端配置
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    pub timeout: Duration,
    pub user_agent: String,
    pub auth: Option<AuthConfig>,
}

/// 认证配置
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub username: String,
    pub password: String,
    pub auth_type: AuthType,
}

/// 认证类型
#[derive(Debug, Clone)]
pub enum AuthType {
    Basic,
    Digest,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            user_agent: "Rust-HTTP-Client/1.0".to_string(),
            auth: None,
        }
    }
}

/// HTTP 客户端服务
#[derive(Clone)]
pub struct HttpClientService {
    client: Client,
    config: HttpClientConfig,
    cookie_jar: Arc<Jar>,
}

impl HttpClientService {
    /// 创建新的 HTTP 客户端服务
    pub fn new(config: HttpClientConfig) -> Result<Self> {
        let cookie_jar = Arc::new(Jar::default());

        let client = Client::builder()
            .timeout(config.timeout)
            .user_agent(&config.user_agent)
            .danger_accept_invalid_certs(true)
            .cookie_provider(Arc::clone(&cookie_jar))
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {}", e))?;

        Ok(Self {
            client,
            config,
            cookie_jar,
        })
    }

    /// 发送 POST 请求（支持智能认证）
    pub async fn post_json(
        &self,
        url: &str,
        json_data: &str,
        headers: Option<Vec<(&str, &str)>>,
    ) -> Result<Response> {
        let mut request = self.client.post(url).body(json_data.to_string());

        // 添加默认的 Content-Type 头
        request = request.header("Content-Type", "application/json");

        // 添加自定义头部
        if let Some(headers) = headers {
            for (key, value) in headers {
                request = request.header(key, value);
            }
        }

        // 智能认证处理：先尝试无认证请求，如果失败再添加认证
        let response = request.try_clone().unwrap().send().await?;

        // 检查是否需要认证
        if response.status().as_u16() != 401 {
            return Ok(response);
        }

        println!("检测到 401 认证失败，处理认证...");
        // 处理认证
        let auth_string = self.get_auth(response, url, "POST").await?;
        println!("生成的认证头: {}", auth_string);

        // 重新发送认证请求
        let request = request.header("Authorization", auth_string);
        request
            .send()
            .await
            .map_err(|e| anyhow!("HTTP request failed: {}", e))
    }

    /// 使用 digest_auth 库添加认证到请求
    async fn get_auth(
        &self,
        response: reqwest::Response,
        url: &str,
        method: &str,
    ) -> Result<String> {
        if let Some(auth_config) = &self.config.auth {
            match auth_config.auth_type {
                AuthType::Basic => {
                    // Basic 认证
                    let auth_value = format!("{}:{}", auth_config.username, auth_config.password);
                    let encoded = BASE64_STANDARD.encode(auth_value);
                    Ok(format!("Basic {}", encoded))
                }
                AuthType::Digest => {
                    // 完全使用 digest_auth 库实现 Digest 认证
                    self.get_digest_auth(response, url, method, auth_config)
                        .await
                }
            }
        } else {
            Err(anyhow!("no auth config"))
        }
    }

    /// 使用 digest_auth 库实现 Digest 认证
    async fn get_digest_auth(
        &self,
        response: reqwest::Response,
        url: &str,
        method: &str,
        auth_config: &AuthConfig,
    ) -> Result<String> {
        // 检查是否有 WWW-Authenticate 头
        let auth_header = match response.headers().get("WWW-Authenticate") {
            Some(header) => header,
            None => return Err(anyhow!("Missing WWW-Authenticate header")),
        };
        let header_str = auth_header.to_str().unwrap_or("");
        println!("收到认证挑战: {}", header_str);

        // 使用 digest_auth 库解析挑战头
        let mut auth_header = WwwAuthenticateHeader::parse(header_str).map_err(|e| {
            let error_msg = format!("Digest认证头解析错误: {}", e);
            println!("{}", error_msg);
            anyhow!(error_msg)
        })?;
        let _http_method = match method.to_uppercase().as_str() {
            "GET" => HttpMethod::GET,
            "POST" => HttpMethod::POST,
            "PUT" => HttpMethod::PUT,
            "DELETE" => HttpMethod::DELETE,
            _ => HttpMethod::GET, // 默认使用 GET
        };
        // 构建认证上下文 - 需要用户名、密码和URI
        let context = AuthContext::new_with_method(
            &auth_config.username,
            &auth_config.password,
            url,
            None::<Vec<u8>>,
            _http_method,
        );

        // 构建认证响应
        let auth_response = auth_header.respond(&context).map_err(|e| {
            let error_msg = format!("Digest认证响应错误: {}", e);
            println!("{}", error_msg);
            anyhow!(error_msg)
        })?;

        println!("生成的 Digest 认证头: {}", auth_response);
        Ok(auth_response.to_string())
    }
}
