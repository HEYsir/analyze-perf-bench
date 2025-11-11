use rusqlite::{Connection, params};
use std::collections::VecDeque;
use std::error::Error;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OnceCell, oneshot};

/// 批量写入的 SQLite 记录器
/// 支持批量收集写入请求并定期批量提交，提高高并发性能
#[derive(Clone)]
pub struct SqliteBatchRecorder {
    cmd_sender: Arc<Mutex<Option<Sender<BatchDbCommand>>>>,
    worker_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
}

/// 批量写入命令类型
enum BatchDbCommandType {
    InsertRequest {
        ts_seconds: i64,
        task_uuid: Option<String>,
        request_id: usize,
        seq_in_second: usize,
        success: bool,
        error_text: Option<String>,
    },
    InsertMessage {
        request_id: u64,
        task_uuid: Option<String>,
        event_type: String,
        receive_time: i64,
        alarm_time: i64,
    },
    UpdateAlarm {
        task_uuid: Option<String>,
        request_id: Option<usize>,
        alarm_triggered: bool,
        receive_time: i64,
        alarm_time: i64,
    },
    Flush, // 强制刷新缓冲区
}

/// 批量写入命令
struct BatchDbCommand {
    cmd_type: BatchDbCommandType,
    resp: Option<oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>>, // 可选，用于需要确认的操作
}

/// 批量写入缓冲区
struct BatchBuffer {
    requests: VecDeque<BatchDbCommandType>,
    messages: VecDeque<BatchDbCommandType>,
    updates: VecDeque<BatchDbCommandType>,
    last_flush: Instant,
    buffer_size: usize,
    flush_interval: Duration,
}

impl BatchBuffer {
    fn new(buffer_size: usize, flush_interval: Duration) -> Self {
        Self {
            requests: VecDeque::new(),
            messages: VecDeque::new(),
            updates: VecDeque::new(),
            last_flush: Instant::now(),
            buffer_size,
            flush_interval,
        }
    }

    fn add_command(&mut self, cmd_type: BatchDbCommandType) {
        match &cmd_type {
            BatchDbCommandType::InsertRequest { .. } => {
                self.requests.push_back(cmd_type);
            }
            BatchDbCommandType::InsertMessage { .. } => {
                self.messages.push_back(cmd_type);
            }
            BatchDbCommandType::UpdateAlarm { .. } => {
                self.updates.push_back(cmd_type);
            }
            BatchDbCommandType::Flush => {} // Flush 命令不加入缓冲区
        }
    }

    fn should_flush(&self) -> bool {
        self.requests.len() >= self.buffer_size
            || self.messages.len() >= self.buffer_size
            || self.updates.len() >= self.buffer_size
            || self.last_flush.elapsed() >= self.flush_interval
    }

    fn take_all(
        &mut self,
    ) -> (
        VecDeque<BatchDbCommandType>,
        VecDeque<BatchDbCommandType>,
        VecDeque<BatchDbCommandType>,
    ) {
        self.last_flush = Instant::now();
        (
            std::mem::take(&mut self.requests),
            std::mem::take(&mut self.messages),
            std::mem::take(&mut self.updates),
        )
    }

    fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.messages.is_empty() && self.updates.is_empty()
    }
}

// 全局单例
static BATCH_RECORDER: OnceCell<SqliteBatchRecorder> = OnceCell::const_new();

impl SqliteBatchRecorder {
    pub fn new() -> Self {
        Self {
            cmd_sender: Arc::new(Mutex::new(None)),
            worker_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// 获取全局单例实例
    pub async fn instance() -> &'static SqliteBatchRecorder {
        BATCH_RECORDER
            .get_or_init(|| async {
                let recorder = SqliteBatchRecorder::new();
                recorder
                    .init("requests.db", 100, Duration::from_millis(100))
                    .await
                    .unwrap();
                recorder
            })
            .await
    }

    /// 初始化并启动后台线程
    pub async fn init(
        &self,
        path: &str,
        buffer_size: usize,
        flush_interval: Duration,
    ) -> Result<(), Box<dyn Error>> {
        let mut guard = self.cmd_sender.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        let (tx, rx): (Sender<BatchDbCommand>, Receiver<BatchDbCommand>) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
        let db_path = path.to_string();

        let handle = thread::spawn(move || {
            match Connection::open(&db_path) {
                Ok(mut conn) => {
                    let _ = ready_tx.send(Ok(()));

                    // 初始化数据库表
                    if let Err(e) = conn.execute_batch(
                        r#"
                        PRAGMA journal_mode = WAL;
                        PRAGMA synchronous = NORMAL;
                        PRAGMA cache_size = -64000; -- 64MB cache
                        
                        CREATE TABLE IF NOT EXISTS request_records (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            ts_seconds INTEGER NOT NULL,
                            task_uuid TEXT,
                            request_id INTEGER NOT NULL,
                            seq_in_second INTEGER NOT NULL,
                            success INTEGER NOT NULL,
                            error_text TEXT,
                            alarm_triggered INTEGER NOT NULL DEFAULT 0,
                            receive_time INTEGER,
                            alarm_time INTEGER
                        );
                        
                        CREATE TABLE IF NOT EXISTS message_records (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            request_id INTEGER NOT NULL,
                            task_uuid TEXT,
                            event_type TEXT NOT NULL,
                            receive_time INTEGER NOT NULL,
                            alarm_time INTEGER NOT NULL,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                        );
                        "#,
                    ) {
                        eprintln!("初始化 SQLite 表失败: {}", e);
                    }

                    let mut buffer = BatchBuffer::new(buffer_size, flush_interval);

                    // 主循环：处理命令和批量写入
                    for cmd in rx {
                        match cmd.cmd_type {
                            BatchDbCommandType::Flush => {
                                // 强制刷新缓冲区
                                if let Err(e) = Self::flush_buffer(&mut conn, &mut buffer) {
                                    eprintln!("批量写入失败: {}", e);
                                }
                            }
                            cmd_type => {
                                buffer.add_command(cmd_type);

                                // 检查是否需要刷新缓冲区
                                if buffer.should_flush() {
                                    if let Err(e) = Self::flush_buffer(&mut conn, &mut buffer) {
                                        eprintln!("批量写入失败: {}", e);
                                    }
                                }
                            }
                        }

                        // 发送响应（如果需要）
                        if let Some(resp) = cmd.resp {
                            let _ = resp.send(Ok(()));
                        }
                    }

                    // 退出前刷新剩余数据
                    if !buffer.is_empty() {
                        if let Err(e) = Self::flush_buffer(&mut conn, &mut buffer) {
                            eprintln!("最终批量写入失败: {}", e);
                        }
                    }
                }
                Err(e) => {
                    let err_msg = format!("打开 SQLite 连接失败: {}", e);
                    eprintln!("{}", err_msg);
                    let _ = ready_tx.send(Err(err_msg.clone()));

                    for cmd in rx {
                        if let Some(resp) = cmd.resp {
                            let _ = resp.send(Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                err_msg.clone(),
                            ))));
                        }
                    }
                }
            }
        });

        // 等待后台线程启动
        let recv_res =
            tokio::task::spawn_blocking(move || ready_rx.recv_timeout(Duration::from_secs(2)))
                .await
                .map_err(|e| format!("等待后台线程启动失败: {}", e))?;

        match recv_res {
            Ok(Ok(())) => {
                *guard = Some(tx);
                let mut hguard = self.worker_handle.lock().await;
                *hguard = Some(handle);
                Ok(())
            }
            Ok(Err(msg)) => {
                let _ = tokio::task::spawn_blocking(move || handle.join()).await;
                Err(msg.into())
            }
            Err(_) => {
                let _ = tokio::task::spawn_blocking(move || handle.join()).await;
                Err("等待后台 DB 线程启动超时".into())
            }
        }
    }

    /// 批量刷新缓冲区到数据库
    fn flush_buffer(
        conn: &mut Connection,
        buffer: &mut BatchBuffer,
    ) -> Result<(), rusqlite::Error> {
        let (requests, messages, updates) = buffer.take_all();

        if requests.is_empty() && messages.is_empty() && updates.is_empty() {
            return Ok(());
        }

        // 开始事务
        let tx = conn.transaction()?;

        // 批量插入请求记录
        if !requests.is_empty() {
            let mut stmt = tx.prepare(
                "INSERT INTO request_records (ts_seconds, task_uuid, request_id, seq_in_second, success, error_text) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            )?;

            for cmd in requests {
                if let BatchDbCommandType::InsertRequest {
                    ts_seconds,
                    task_uuid,
                    request_id,
                    seq_in_second,
                    success,
                    error_text,
                } = cmd
                {
                    stmt.execute(params![
                        ts_seconds,
                        task_uuid,
                        request_id as i64,
                        seq_in_second as i64,
                        if success { 1 } else { 0 },
                        error_text
                    ])?;
                }
            }
        }

        // 批量插入消息记录
        if !messages.is_empty() {
            let mut stmt = tx.prepare(
                "INSERT INTO message_records (request_id, task_uuid, event_type, receive_time, alarm_time) 
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            )?;

            for cmd in messages {
                if let BatchDbCommandType::InsertMessage {
                    request_id,
                    task_uuid,
                    event_type,
                    receive_time,
                    alarm_time,
                } = cmd
                {
                    stmt.execute(params![
                        request_id as i64,
                        task_uuid,
                        event_type,
                        receive_time,
                        alarm_time
                    ])?;
                }
            }
        }

        // 批量更新记录
        if !updates.is_empty() {
            for cmd in updates {
                if let BatchDbCommandType::UpdateAlarm {
                    task_uuid,
                    request_id,
                    alarm_triggered,
                    receive_time,
                    alarm_time,
                } = cmd
                {
                    let mut sql = String::from("UPDATE request_records SET alarm_triggered = ?");
                    let alarm_flag = if alarm_triggered { 1i32 } else { 0i32 };
                    let mut params: Vec<&dyn rusqlite::ToSql> = vec![&alarm_flag];

                    sql.push_str(", receive_time = ?");
                    params.push(&receive_time);
                    sql.push_str(", alarm_time = ?");
                    params.push(&alarm_time);

                    sql.push_str(" WHERE ");
                    let mut conditions = Vec::new();
                    if let Some(uuid) = &task_uuid {
                        conditions.push("task_uuid = ?");
                        params.push(uuid);
                    }
                    if let Some(rid) = &request_id {
                        conditions.push("request_id = ?");
                        params.push(rid);
                    }
                    sql.push_str(&conditions.join(" AND "));

                    tx.execute(&sql, rusqlite::params_from_iter(params))?;
                }
            }
        }

        // 提交事务
        tx.commit()?;
        Ok(())
    }

    /// 异步插入请求记录（批量版本）
    pub async fn insert_request(
        &self,
        ts_seconds: i64,
        task_uuid: Option<String>,
        request_id: usize,
        seq_in_second: usize,
        success: bool,
        error_text: Option<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.send_command(
            BatchDbCommandType::InsertRequest {
                ts_seconds,
                task_uuid,
                request_id,
                seq_in_second,
                success,
                error_text,
            },
            false,
        )
        .await
    }

    /// 异步插入消息记录（批量版本）
    pub async fn insert_message(
        &self,
        request_id: u64,
        task_uuid: Option<String>,
        event_type: String,
        receive_time: i64,
        alarm_time: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.send_command(
            BatchDbCommandType::InsertMessage {
                request_id,
                task_uuid,
                event_type,
                receive_time,
                alarm_time,
            },
            false,
        )
        .await
    }

    /// 异步更新报警记录（批量版本）
    pub async fn update_alarm(
        &self,
        task_uuid: Option<String>,
        request_id: Option<usize>,
        alarm_triggered: bool,
        receive_time: i64,
        alarm_time: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if task_uuid.is_none() && request_id.is_none() {
            return Err("必须提供 task_uuid 或 request_id 其中之一".into());
        }

        self.send_command(
            BatchDbCommandType::UpdateAlarm {
                task_uuid,
                request_id,
                alarm_triggered,
                receive_time,
                alarm_time,
            },
            false,
        )
        .await
    }

    /// 强制刷新缓冲区
    pub async fn flush(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.send_command(BatchDbCommandType::Flush, true).await
    }

    /// 发送命令到后台线程
    async fn send_command(
        &self,
        cmd_type: BatchDbCommandType,
        wait_response: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let sender_opt = { self.cmd_sender.lock().await.clone() };
        let sender = match sender_opt {
            Some(s) => s,
            None => return Err("SqliteBatchRecorder 未初始化".into()),
        };

        let (resp_tx, resp_rx) = if wait_response {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let cmd = BatchDbCommand {
            cmd_type,
            resp: resp_tx,
        };

        let send_result = tokio::task::spawn_blocking(move || sender.send(cmd)).await;
        match send_result {
            Ok(Ok(_)) => {
                if let Some(resp_rx) = resp_rx {
                    match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
                        Ok(Ok(r)) => r,
                        Ok(Err(_)) => Err("后台线程响应失败".into()),
                        Err(_) => Err("等待后台 DB 响应超时".into()),
                    }
                } else {
                    Ok(())
                }
            }
            Ok(Err(e)) => Err(format!("发送命令失败: {}", e).into()),
            Err(e) => Err(format!("spawn_blocking 出错: {}", e).into()),
        }
    }

    /// 关闭后台线程
    pub async fn shutdown(&self) {
        println!("正在关闭批量数据库连接...");

        // 先刷新所有数据
        if let Err(e) = self.flush().await {
            eprintln!("刷新数据失败: {}", e);
        }

        let mut guard = self.cmd_sender.lock().await;
        if let Some(tx) = guard.take() {
            drop(tx);
        }

        let mut hguard = self.worker_handle.lock().await;
        if let Some(handle) = hguard.take() {
            println!("等待批量数据库工作线程退出...");
            if let Err(e) = tokio::task::spawn_blocking(move || handle.join()).await {
                eprintln!("工作线程退出错误: {:?}", e);
            }
        }
        println!("批量数据库连接已关闭");
    }
}

impl Drop for SqliteBatchRecorder {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.cmd_sender.try_lock() {
            if let Some(tx) = guard.take() {
                drop(tx);
            }
        }

        if let Ok(mut hguard) = self.worker_handle.try_lock() {
            if let Some(handle) = hguard.take() {
                let _ = std::thread::spawn(move || {
                    if let Err(_) = handle.join() {
                        // 忽略错误
                    }
                })
                .join();
            }
        }
        println!("批量数据库资源清理完成");
    }
}
