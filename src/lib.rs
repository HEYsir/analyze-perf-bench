//! HTTP POST 请求测试工具库
//!
//! 这是一个用于测试 HTTP POST 请求的 Rust 库，提供以下功能：
//! - HTTP 客户端功能
//! - JSON 数据处理
//! - 并发请求测试
//! - HTTP 服务器功能
//! - 配置管理

pub mod concurrency_test;
pub mod config;
pub mod http_client;
pub mod http_server;
pub mod json_processor;
pub mod message; // <-- 新增
pub mod db; // <-- 新增

// 重新导出主要类型，方便使用
pub use concurrency_test::{
    ConcurrencyConfig, ConcurrencyTestResult, ConcurrencyTestService, run_simple_concurrency_test,
};
pub use config::{AppConfig, ConfigManager, create_default_config, create_test_config};
pub use http_client::{HttpClientConfig, HttpClientService};
pub use http_server::{Alert, HttpServerService, ServerState, start_http_server};
pub use json_processor::{JsonProcessor, JsonProcessorError, parse_and_modify_json};
pub use message::{Message, MessageProcessor, MessageSource}; // <-- 新增导出
pub use db::SqliteRecorder; // <-- 导出记录器（可选）

/// 库版本信息
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const NAME: &str = env!("CARGO_PKG_NAME");
pub const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

/// 库信息结构体
#[derive(Debug)]
pub struct LibraryInfo {
    pub name: String,
    pub version: String,
    pub description: String,
}

impl Default for LibraryInfo {
    fn default() -> Self {
        Self {
            name: NAME.to_string(),
            version: VERSION.to_string(),
            description: DESCRIPTION.to_string(),
        }
    }
}

/// 获取库信息
pub fn get_library_info() -> LibraryInfo {
    LibraryInfo::default()
}

/// 初始化日志系统（简单实现）
pub fn init_logging() {
    // 这里可以集成更复杂的日志系统如 log4rs、tracing 等
    println!("{} v{} - {}", NAME, VERSION, DESCRIPTION);
    println!("HTTP POST 请求测试工具库已初始化");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_info() {
        let info = get_library_info();
        assert!(!info.name.is_empty());
        assert!(!info.version.is_empty());
        assert!(!info.description.is_empty());
    }

    #[test]
    fn test_config_creation() {
        let default_config = create_default_config();
        assert_eq!(default_config.http_client.timeout_seconds, 30);

        let test_config = create_test_config();
        assert_eq!(test_config.http_client.timeout_seconds, 10);
    }
}
