use crate::db::SqliteRecorder as OriginalRecorder;
use crate::db_batch::SqliteBatchRecorder as BatchRecorder;
use std::error::Error;

/// 数据库写入适配器
/// 提供统一的接口，可以在原始写入和批量写入之间切换
#[derive(Clone)]
pub enum DbRecorder {
    Original(OriginalRecorder),
    Batch(BatchRecorder),
}

impl DbRecorder {
    /// 创建原始写入适配器
    pub fn original() -> Self {
        DbRecorder::Original(OriginalRecorder::new())
    }

    /// 创建批量写入适配器
    pub fn batch() -> Self {
        DbRecorder::Batch(BatchRecorder::new())
    }

    /// 获取全局单例实例（批量版本）
    pub async fn instance() -> &'static DbRecorder {
        static RECORDER: tokio::sync::OnceCell<DbRecorder> = tokio::sync::OnceCell::const_new();

        RECORDER
            .get_or_init(|| async {
                let recorder = DbRecorder::Batch(BatchRecorder::new());
                // 初始化数据库连接
                if let Err(e) = recorder.init("requests.db").await {
                    eprintln!("初始化批量数据库失败: {}", e);
                }
                recorder
            })
            .await
    }

    /// 初始化数据库连接
    pub async fn init(&self, path: &str) -> Result<(), Box<dyn Error>> {
        match self {
            DbRecorder::Original(recorder) => recorder.init(path).await,
            DbRecorder::Batch(recorder) => {
                // 批量写入的优化参数：缓冲区大小100，刷新间隔100ms
                recorder
                    .init(path, 100, std::time::Duration::from_millis(100))
                    .await
            }
        }
    }

    /// 插入请求记录
    pub async fn insert_request(
        &self,
        ts_seconds: i64,
        task_uuid: Option<String>,
        request_id: usize,
        seq_in_second: usize,
        success: bool,
        error_text: Option<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DbRecorder::Original(recorder) => {
                recorder
                    .insert_request(
                        ts_seconds,
                        task_uuid,
                        request_id,
                        seq_in_second,
                        success,
                        error_text,
                    )
                    .await
            }
            DbRecorder::Batch(recorder) => {
                recorder
                    .insert_request(
                        ts_seconds,
                        task_uuid,
                        request_id,
                        seq_in_second,
                        success,
                        error_text,
                    )
                    .await
            }
        }
    }

    /// 插入消息记录
    pub async fn insert_message(
        &self,
        request_id: u64,
        task_uuid: Option<String>,
        event_type: String,
        receive_time: i64,
        alarm_time: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DbRecorder::Original(recorder) => {
                recorder
                    .insert_message(request_id, task_uuid, event_type, receive_time, alarm_time)
                    .await
            }
            DbRecorder::Batch(recorder) => {
                recorder
                    .insert_message(request_id, task_uuid, event_type, receive_time, alarm_time)
                    .await
            }
        }
    }

    /// 更新报警记录
    pub async fn update_alarm(
        &self,
        task_uuid: Option<String>,
        request_id: Option<usize>,
        alarm_triggered: bool,
        receive_time: i64,
        alarm_time: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DbRecorder::Original(recorder) => {
                recorder
                    .update_alarm(
                        task_uuid,
                        request_id,
                        alarm_triggered,
                        receive_time,
                        alarm_time,
                    )
                    .await
            }
            DbRecorder::Batch(recorder) => {
                recorder
                    .update_alarm(
                        task_uuid,
                        request_id,
                        alarm_triggered,
                        receive_time,
                        alarm_time,
                    )
                    .await
            }
        }
    }

    /// 强制刷新缓冲区（仅批量写入有效）
    pub async fn flush(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DbRecorder::Original(_) => Ok(()), // 原始写入无需刷新
            DbRecorder::Batch(recorder) => recorder.flush().await,
        }
    }

    /// 关闭数据库连接
    pub async fn shutdown(&self) {
        match self {
            DbRecorder::Original(recorder) => recorder.shutdown().await,
            DbRecorder::Batch(recorder) => recorder.shutdown().await,
        }
    }

    /// 获取写入模式
    pub fn mode(&self) -> &'static str {
        match self {
            DbRecorder::Original(_) => "original",
            DbRecorder::Batch(_) => "batch",
        }
    }
}

/// 简化的全局访问函数，使用批量写入模式
pub async fn get_recorder() -> &'static DbRecorder {
    DbRecorder::instance().await
}
