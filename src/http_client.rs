use base64::prelude::*;
use md5::{Digest, Md5};
use reqwest::{Error, cookie::Jar};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
            auth: Some(AuthConfig {
                // 默认使用 Digest 认证
                username: "admin".to_string(),
                password: "password".to_string(),
                auth_type: AuthType::Digest,
            }),
        }
    }
}

/// Digest 认证挑战信息
#[derive(Debug, Clone)]
pub struct DigestChallenge {
    pub realm: String,
    pub nonce: String,
    pub qop: Option<String>,
    pub algorithm: Option<String>,
    pub opaque: Option<String>,
}

impl DigestChallenge {
    /// 从 WWW-Authenticate 头解析挑战信息
    pub fn from_header(header: &str) -> Option<Self> {
        if !header.to_lowercase().starts_with("digest") {
            return None;
        }

        let parts: Vec<&str> = header.splitn(2, ' ').collect();
        if parts.len() < 2 {
            return None;
        }

        let params_str = parts[1];
        let mut params = HashMap::new();

        for param in params_str.split(',') {
            let kv: Vec<&str> = param.splitn(2, '=').collect();
            if kv.len() == 2 {
                let key = kv[0].trim();
                let value = kv[1].trim().trim_matches('"');
                params.insert(key.to_string(), value.to_string());
            }
        }

        Some(Self {
            realm: params.get("realm").cloned().unwrap_or_default(),
            nonce: params.get("nonce").cloned().unwrap_or_default(),
            qop: params.get("qop").cloned(),
            algorithm: params.get("algorithm").cloned(),
            opaque: params.get("opaque").cloned(),
        })
    }
}

/// HTTP 客户端服务
#[derive(Clone)]
pub struct HttpClientService {
    client: reqwest::Client,
    config: HttpClientConfig,
    cookie_jar: Arc<Jar>, // Cookie 存储
}

impl HttpClientService {
    /// 创建新的 HTTP 客户端服务
    pub fn new(config: HttpClientConfig) -> Result<Self, Error> {
        // 创建共享的 Cookie Jar
        let cookie_jar = Arc::new(Jar::default());

        // 创建带有 Cookie 存储的客户端
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .user_agent(&config.user_agent)
            .danger_accept_invalid_certs(true) // 禁用 SSL 证书验证
            .cookie_provider(Arc::clone(&cookie_jar)) // 启用 Cookie 存储
            .build()?;

        Ok(Self {
            client,
            config,
            cookie_jar,
        })
    }

    /// 发送 POST 请求（支持智能认证和 Cookie 保持）
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

        // 智能认证处理：先尝试无认证请求，如果失败再添加认证
        let response = request.try_clone().unwrap().send().await?;

        // 检查是否需要认证
        if response.status().as_u16() == 401 {
            println!("检测到 401 认证失败，重新发送认证请求...");

            // 处理认证
            request = self.add_auth(request, url, "POST").await?;

            // 重新发送认证请求
            request.send().await
        } else {
            // 认证成功或不需要认证，直接返回响应
            Ok(response)
        }
    }

    /// 添加认证到请求
    async fn add_auth(
        &self,
        request: reqwest::RequestBuilder,
        url: &str,
        method: &str,
    ) -> Result<reqwest::RequestBuilder, Error> {
        if let Some(auth_config) = &self.config.auth {
            match auth_config.auth_type {
                AuthType::Basic => {
                    // Basic 认证
                    let auth_value = format!("{}:{}", auth_config.username, auth_config.password);
                    let encoded = BASE64_STANDARD.encode(auth_value);
                    Ok(request.header("Authorization", format!("Basic {}", encoded)))
                }
                AuthType::Digest => {
                    // Digest 认证 - 完整的实现
                    let auth_header = self
                        .build_digest_auth_header(url, method, auth_config)
                        .await?;
                    Ok(request.header("Authorization", auth_header))
                }
            }
        } else {
            Ok(request)
        }
    }

    /// 构建 Digest 认证头
    async fn build_digest_auth_header(
        &self,
        url: &str,
        method: &str,
        auth_config: &AuthConfig,
    ) -> Result<String, Error> {
        // 先发送挑战请求获取 nonce（不包含认证信息，故意触发 401 获取挑战）
        let challenge_response = self
            .client
            .get(url)
            .header("Content-Type", "application/json")
            .send()
            .await?;

        // 检查是否有 WWW-Authenticate 头
        let challenge =
            if let Some(auth_header) = challenge_response.headers().get("WWW-Authenticate") {
                let header_str = auth_header.to_str().unwrap_or("");
                println!("收到认证挑战: {}", header_str);

                if let Some(challenge) = DigestChallenge::from_header(header_str) {
                    challenge
                } else {
                    println!("无法解析 Digest 挑战，回退到 Basic 认证");
                    return self.build_basic_auth_header(auth_config);
                }
            } else {
                println!("服务器未返回 WWW-Authenticate 头，回退到 Basic 认证");
                return self.build_basic_auth_header(auth_config);
            };

        // 生成 Digest 认证响应
        let ha1 = self.calculate_ha1(
            &auth_config.username,
            &challenge.realm,
            &auth_config.password,
        );
        let ha2 = self.calculate_ha2(method, url);
        let response = self.calculate_response(&ha1, &challenge.nonce, &ha2);

        // 构建认证头
        let mut auth_header = format!(
            "Digest username=\"{}\", realm=\"{}\", nonce=\"{}\", uri=\"{}\", response=\"{}\"",
            auth_config.username, challenge.realm, challenge.nonce, url, response
        );

        if let Some(qop) = &challenge.qop {
            auth_header.push_str(&format!(", qop={}", qop));
        }
        if let Some(algorithm) = &challenge.algorithm {
            auth_header.push_str(&format!(", algorithm={}", algorithm));
        }
        if let Some(opaque) = &challenge.opaque {
            auth_header.push_str(&format!(", opaque=\"{}\"", opaque));
        }

        println!("生成的 Digest 认证头: {}", auth_header);
        Ok(auth_header)
    }

    /// 构建 Basic 认证头（回退方案）
    fn build_basic_auth_header(&self, auth_config: &AuthConfig) -> Result<String, Error> {
        let auth_value = format!("{}:{}", auth_config.username, auth_config.password);
        let encoded = BASE64_STANDARD.encode(auth_value);
        Ok(format!("Basic {}", encoded))
    }

    /// 计算 HA1 (username:realm:password)
    fn calculate_ha1(&self, username: &str, realm: &str, password: &str) -> String {
        let data = format!("{}:{}:{}", username, realm, password);
        let mut hasher = Md5::new();
        hasher.update(data.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// 计算 HA2 (method:uri)
    fn calculate_ha2(&self, method: &str, uri: &str) -> String {
        let data = format!("{}:{}", method, uri);
        let mut hasher = Md5::new();
        hasher.update(data.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// 计算响应 (HA1:nonce:HA2)
    fn calculate_response(&self, ha1: &str, nonce: &str, ha2: &str) -> String {
        let data = format!("{}:{}:{}", ha1, nonce, ha2);
        let mut hasher = Md5::new();
        hasher.update(data.as_bytes());
        hex::encode(hasher.finalize())
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

    /// 发送带有 Digest 认证的 POST 请求（专门方法）
    pub async fn post_json_with_digest_auth(
        &self,
        url: &str,
        json_data: &str,
        username: &str,
        password: &str,
        headers: Option<Vec<(&str, &str)>>,
    ) -> Result<reqwest::Response, Error> {
        let config = HttpClientConfig {
            timeout: self.config.timeout,
            user_agent: self.config.user_agent.clone(),
            auth: Some(AuthConfig {
                username: username.to_string(),
                password: password.to_string(),
                auth_type: AuthType::Digest,
            }),
        };

        let digest_client = HttpClientService::new(config)?;
        digest_client.post_json(url, json_data, headers).await
    }

    /// 发送带有 Basic 认证的 POST 请求（专门方法）
    pub async fn post_json_with_basic_auth(
        &self,
        url: &str,
        json_data: &str,
        username: &str,
        password: &str,
        headers: Option<Vec<(&str, &str)>>,
    ) -> Result<reqwest::Response, Error> {
        let config = HttpClientConfig {
            timeout: self.config.timeout,
            user_agent: self.config.user_agent.clone(),
            auth: Some(AuthConfig {
                username: username.to_string(),
                password: password.to_string(),
                auth_type: AuthType::Basic,
            }),
        };

        let basic_client = HttpClientService::new(config)?;
        basic_client.post_json(url, json_data, headers).await
    }
}

/// 简化的 HTTP 客户端函数
pub async fn send_post_request(url: &str, json_data: &str) -> Result<reqwest::Response, Error> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // 禁用 SSL 证书验证
        .build()?;
    client
        .post(url)
        .header("Content-Type", "application/json")
        .body(json_data.to_string())
        .send()
        .await
}

/// 使用 Digest 认证的简化函数
pub async fn send_post_request_with_digest_auth(
    url: &str,
    json_data: &str,
    username: &str,
    password: &str,
) -> Result<reqwest::Response, Error> {
    let config = HttpClientConfig {
        timeout: Duration::from_secs(30),
        user_agent: "Rust-HTTP-Client/1.0".to_string(),
        auth: Some(AuthConfig {
            username: username.to_string(),
            password: password.to_string(),
            auth_type: AuthType::Digest,
        }),
    };

    let client = HttpClientService::new(config)?;
    client.post_json(url, json_data, None).await
}

/// 使用 Basic 认证的简化函数
pub async fn send_post_request_with_basic_auth(
    url: &str,
    json_data: &str,
    username: &str,
    password: &str,
) -> Result<reqwest::Response, Error> {
    let config = HttpClientConfig {
        timeout: Duration::from_secs(30),
        user_agent: "Rust-HTTP-Client/1.0".to_string(),
        auth: Some(AuthConfig {
            username: username.to_string(),
            password: password.to_string(),
            auth_type: AuthType::Basic,
        }),
    };

    let client = HttpClientService::new(config)?;
    client.post_json(url, json_data, None).await
}
