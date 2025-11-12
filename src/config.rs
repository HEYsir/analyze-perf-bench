use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 应用配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppConfig {
    pub http_client: HttpClientConfig,
    pub concurrency_test: ConcurrencyTestConfig,
    pub http_server: HttpServerConfig,
    pub file_paths: FilePathsConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            http_client: HttpClientConfig::default(),
            concurrency_test: ConcurrencyTestConfig::default(),
            http_server: HttpServerConfig::default(),
            file_paths: FilePathsConfig::default(),
        }
    }
}

/// HTTP 客户端配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HttpClientConfig {
    pub timeout_seconds: u64,
    pub user_agent: String,
    pub default_content_type: String,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 30,
            user_agent: "Rust-HTTP-Client/1.0".to_string(),
            default_content_type: "application/json".to_string(),
        }
    }
}

/// 并发测试配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConcurrencyTestConfig {
    pub default_requests_per_second: usize,
    pub default_duration_seconds: u64,
    pub default_test_url: String,
    pub max_requests_per_second: usize,
}

impl Default for ConcurrencyTestConfig {
    fn default() -> Self {
        Self {
            default_requests_per_second: 10,
            default_duration_seconds: 5,
            default_test_url: "https://jsonplaceholder.typicode.com/posts".to_string(),
            max_requests_per_second: 1000,
        }
    }
}

/// HTTP 服务器配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HttpServerConfig {
    pub default_port: u16,
    pub bind_address: String,
    pub max_alerts: usize,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            default_port: 8080,
            bind_address: "127.0.0.1".to_string(),
            max_alerts: 10000,
        }
    }
}

/// 文件路径配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilePathsConfig {
    pub post_data_file: String,
    pub parsed_modified_file: String,
    pub modified_post_file: String,
    pub concurrency_results_file: String,
    pub detailed_results_file: String,
}

impl Default for FilePathsConfig {
    fn default() -> Self {
        Self {
            post_data_file: "post_data.json".to_string(),
            parsed_modified_file: "parsed_modified_data.json".to_string(),
            modified_post_file: "modified_post_data.json".to_string(),
            concurrency_results_file: "concurrency_test_results.json".to_string(),
            detailed_results_file: "detailed_concurrency_results.json".to_string(),
        }
    }
}

/// 配置管理器
pub struct ConfigManager {
    config: AppConfig,
}

impl ConfigManager {
    pub fn new() -> Self {
        Self {
            config: AppConfig::default(),
        }
    }

    pub fn with_config(config: AppConfig) -> Self {
        Self { config }
    }

    /// 从文件加载配置
    pub fn load_from_file(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(file_path)?;
        let config: AppConfig = serde_json::from_str(&content)?;
        Ok(Self { config })
    }

    /// 保存配置到文件
    pub fn save_to_file(&self, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let content = serde_json::to_string_pretty(&self.config)?;
        std::fs::write(file_path, content)?;
        Ok(())
    }

    /// 获取配置引用
    pub fn get_config(&self) -> &AppConfig {
        &self.config
    }

    /// 获取配置可变引用
    pub fn get_config_mut(&mut self) -> &mut AppConfig {
        &mut self.config
    }

    /// 更新配置
    pub fn update_config(&mut self, new_config: AppConfig) {
        self.config = new_config;
    }

    /// 获取 HTTP 客户端超时时间
    pub fn get_http_client_timeout(&self) -> Duration {
        Duration::from_secs(self.config.http_client.timeout_seconds)
    }

    /// 创建 HTTP 客户端配置
    pub fn create_http_client_config(&self) -> crate::http_client::HttpClientConfig {
        crate::http_client::HttpClientConfig {
            timeout: self.get_http_client_timeout(),
            user_agent: self.config.http_client.user_agent.clone(),
            auth: None, // 默认不设置认证，由调用方根据需要配置
        }
    }

    /// 创建带有认证的 HTTP 客户端配置
    pub fn create_http_client_config_with_auth(
        &self,
        username: Option<String>,
        password: Option<String>,
        auth_type: Option<crate::http_client::AuthType>,
    ) -> crate::http_client::HttpClientConfig {
        let auth_config = if let (Some(username), Some(password)) = (username, password) {
            Some(crate::http_client::AuthConfig {
                username,
                password,
                auth_type: auth_type.unwrap_or(crate::http_client::AuthType::Digest),
            })
        } else {
            None
        };

        crate::http_client::HttpClientConfig {
            timeout: self.get_http_client_timeout(),
            user_agent: self.config.http_client.user_agent.clone(),
            auth: auth_config,
        }
    }

    /// 创建默认的 Digest 认证 HTTP 客户端配置
    pub fn create_http_client_config_with_digest_auth(
        &self,
        username: String,
        password: String,
    ) -> crate::http_client::HttpClientConfig {
        self.create_http_client_config_with_auth(
            Some(username),
            Some(password),
            Some(crate::http_client::AuthType::Digest),
        )
    }

    /// 创建默认的 Basic 认证 HTTP 客户端配置
    pub fn create_http_client_config_with_basic_auth(
        &self,
        username: String,
        password: String,
    ) -> crate::http_client::HttpClientConfig {
        self.create_http_client_config_with_auth(
            Some(username),
            Some(password),
            Some(crate::http_client::AuthType::Basic),
        )
    }

    /// 创建并发测试配置
    pub fn create_concurrency_config(&self) -> crate::concurrency_test::ConcurrencyConfig {
        crate::concurrency_test::ConcurrencyConfig {
            requests_per_second: self.config.concurrency_test.default_requests_per_second,
            duration_seconds: self.config.concurrency_test.default_duration_seconds,
            url: self.config.concurrency_test.default_test_url.clone(),
        }
    }

    /// 验证配置
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // 验证 HTTP 客户端配置
        if self.config.http_client.timeout_seconds == 0 {
            errors.push("HTTP 客户端超时时间不能为0".to_string());
        }

        // 验证并发测试配置
        if self.config.concurrency_test.default_requests_per_second == 0 {
            errors.push("默认并发请求数不能为0".to_string());
        }

        if self.config.concurrency_test.default_duration_seconds == 0 {
            errors.push("默认测试持续时间不能为0".to_string());
        }

        if self.config.concurrency_test.default_requests_per_second
            > self.config.concurrency_test.max_requests_per_second
        {
            errors.push("默认并发请求数不能超过最大限制".to_string());
        }

        // 验证 HTTP 服务器配置
        if self.config.http_server.default_port == 0 {
            errors.push("HTTP 服务器端口不能为0".to_string());
        }

        if self.config.http_server.max_alerts == 0 {
            errors.push("最大报警数不能为0".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// 默认配置示例
pub fn create_default_config() -> AppConfig {
    AppConfig::default()
}

/// 创建测试配置
pub fn create_test_config() -> AppConfig {
    AppConfig {
        http_client: HttpClientConfig {
            timeout_seconds: 10,
            user_agent: "Test-Client/1.0".to_string(),
            default_content_type: "application/json".to_string(),
        },
        concurrency_test: ConcurrencyTestConfig {
            default_requests_per_second: 5,
            default_duration_seconds: 3,
            default_test_url: "https://jsonplaceholder.typicode.com/posts".to_string(),
            max_requests_per_second: 100,
        },
        http_server: HttpServerConfig {
            default_port: 8081,
            bind_address: "0.0.0.0".to_string(),
            max_alerts: 1000,
        },
        file_paths: FilePathsConfig::default(),
    }
}
