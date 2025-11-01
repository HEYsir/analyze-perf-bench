use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;

/// 消息来源，预留 RMQ / Kafka / 其它
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageSource {
    Http,
    Rmq,
    Kafka,
    Other(String),
}

/// 统一的消息结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub source: MessageSource,
    pub payload: Value,
    pub received_at: DateTime<Utc>,
}

/// 消息接收/处理抽象（可注册不同 sink）
#[async_trait]
pub trait MessageSink: Send + Sync {
    async fn send(&self, msg: &Message) -> Result<(), Box<dyn Error + Send + Sync>>;
}

/// 消息处理器：当前实现为简单日志 + 占位扩展点（RMQ/Kafka）
pub struct MessageProcessor;

impl MessageProcessor {
    /// 处理单条消息（异步）。未来可以在这里做路由、持久化、发送到 RMQ/Kafka 等。
    pub async fn process_message(msg: Message) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 简单记录
        println!(
            "MessageProcessor: received message id={} source={:?} at={}",
            msg.id, msg.source, msg.received_at
        );

        // 示例：根据来源做不同处理（占位）
        match msg.source {
            MessageSource::Http => {
                // 当前只是打印，未来可以转发到持久化/索引/匹配模块
                println!("HTTP 消息负载: {}", msg.payload);
            }
            MessageSource::Rmq => {
                // TODO: RMQ sink integration
                println!("RMQ 消息，预留处理");
            }
            MessageSource::Kafka => {
                // TODO: Kafka sink integration
                println!("Kafka 消息，预留处理");
            }
            MessageSource::Other(s) => {
                println!("Other source: {}", s);
            }
        }

        // 如果需要异步将消息推到其他模块 / 队列，可以在此调用已注册的 sinks。
        Ok(())
    }
}

// 占位：可以实现具体的 RMQ/Kafka sink，如下所示（示例接口，未实现）
// pub struct RmqSink { /* connection params */ }
// #[async_trait::async_trait]
// impl MessageSink for RmqSink {
//     async fn send(&self, msg: &Message) -> Result<(), Box<dyn Error + Send + Sync>> {
//         // 实现将 msg 发送到 RMQ
//         Ok(())
//     }
// }
