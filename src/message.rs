use crate::db::SqliteRecorder;
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

/// 消息格式，预留 JSON / Protobuf / 其它
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageFormat {
    Json,
    Xml,
    Unknown,
}

/// 统一的消息结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub source: MessageSource,
    pub payload: Value,
    pub received_at: DateTime<Utc>,
    pub format: Option<MessageFormat>, // 新增字段标识数据格式
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
        // 获取 SqliteRecorder 实例
        let recorder = SqliteRecorder::instance().await;
        Self::process_message_with_recorder(msg, &recorder).await
    }

    /// 使用预获取的 SqliteRecorder 实例处理消息，避免在异步块内部 await
    pub async fn process_message_with_recorder(
        msg: Message,
        recorder: &SqliteRecorder,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 简单记录
        println!(
            "MessageProcessor: received message id={} source={:?} at={}",
            msg.id, msg.source, msg.received_at
        );

        let payload = msg.payload;
        let request_id = payload
            .pointer("TaskInfo/picture/0/targetAttrs/request_id")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let task_uuid = payload
            .pointer("TaskInfo/picture/0/targetAttrs/task_uuid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let alarm_time = payload
            .pointer("TaskInfo/alarm_time")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        println!(
            "MessageProcessor: request_id={} task_uuid={}",
            request_id, task_uuid
        );

        // 克隆需要在异步块中使用的值
        let task_uuid_clone = task_uuid.clone();
        let msg_timestamp = msg.received_at.timestamp();

        let _ = recorder
            .update_alarm(
                Some(task_uuid_clone),
                Some(request_id as usize),
                true,
                msg_timestamp,
                alarm_time,
            )
            .await;
        Ok(())
    }
}
