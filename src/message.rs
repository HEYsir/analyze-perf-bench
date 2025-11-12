use crate::db_adapter::{DbRecorder, get_recorder}; // 使用批量写入适配器
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
        // 获取 DbRecorder 实例（使用批量写入）
        let recorder = get_recorder().await;
        Self::process_message_with_recorder(msg, recorder).await
    }

    /// 使用预获取的 DbRecorder 实例处理消息，避免在异步块内部 await
    pub async fn process_message_with_recorder(
        msg: Message,
        recorder: &DbRecorder,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 简单记录
        // println!(
        //     "MessageProcessor: received message id={} source={:?} at={}",
        //     msg.id, msg.source, msg.received_at
        // );

        let payload = msg.payload;
        // 获取 event_type 字段
        let event_type = payload
            .pointer("/eventType")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        if !["hawkResult", "TextToRuleArmingEvent"].contains(&event_type.as_str()) {
            return Ok(());
        }
        let is_hawk = event_type == "hawkResult";
        let (test_uuid_path, request_id_path, task_uuid_path, alarm_time_path) = if is_hawk {
            (
                "/analysisResult/0/targetAttrs/test_uuid",
                "/analysisResult/0/targetAttrs/request_id",
                "/analysisResult/0/targetAttrs/task_uuid",
                "/analysisResult/0/timeStamp",
            )
        } else {
            (
                "/targetAttr/test_uuid",
                "/targetAttr/request_id",
                "/targetAttr/task_uuid",
                "/TextToRuleArmingEvent/time",
            )
        };
        let test_uuid = payload
            .pointer(test_uuid_path)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let request_id = payload
            .pointer(request_id_path)
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let task_uuid = payload
            .pointer(task_uuid_path)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let alarm_time = payload
            .pointer(alarm_time_path)
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let msg_timestamp = msg.received_at.timestamp();

        // println!(
        //     "MessageProcessor: request_id={} task_uuid={} event_type={}",
        //     request_id, task_uuid, event_type
        // );

        // 将消息记录存储到新的数据表（使用批量写入）
        let _ = recorder
            .insert_message(
                Some(test_uuid.clone()),
                request_id,
                Some(task_uuid.clone()),
                event_type,
                msg_timestamp,
                alarm_time,
            )
            .await;
        Ok(())
    }
}
