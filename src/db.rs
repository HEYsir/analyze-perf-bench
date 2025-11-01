use rusqlite::{params, Connection};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use tokio::sync::oneshot;
use std::sync::mpsc::{self, Sender, Receiver};

/// 单一长连接的 SQLite 记录器：在单独的阻塞线程中持有 rusqlite::Connection，
/// 通过 std::sync::mpsc 与该线程通信，insert_request 为异步接口通过 oneshot 等待结果。
#[derive(Clone)]
pub struct SqliteRecorder {
    cmd_sender: Arc<Mutex<Option<Sender<DbCommand>>>>,
}

struct DbCommand {
    ts_seconds: i64,
    task_uuid: Option<String>,
    request_id: usize,
    seq_in_second: usize,
    success: bool,
    error_text: Option<String>,
    resp: oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>,
}

impl SqliteRecorder {
    /// new 不会立即启动线程；调用 init() 来启动持久连接后台线程
    pub fn new(_path: &str) -> Self {
        Self {
            cmd_sender: Arc::new(Mutex::new(None)),
        }
    }

    /// 初始化并启动后台线程（创建 DB 文件与表）
    pub async fn init(&self, path: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut guard = self.cmd_sender.lock().unwrap();
        if guard.is_some() {
            // 已经初始化
            return Ok(());
        }

        // 创建 channel 与 worker 线程
        let (tx, rx): (Sender<DbCommand>, Receiver<DbCommand>) = mpsc::channel();
        let db_path = path.to_string();

        // spawn blocking thread that owns rusqlite::Connection
        thread::spawn(move || {
            // 建立连接并创建表
            match Connection::open(&db_path) {
                Ok(conn) => {
                    if let Err(e) = conn.execute_batch(
                        r#"
                        PRAGMA journal_mode = WAL;
                        CREATE TABLE IF NOT EXISTS request_records (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            ts_seconds INTEGER NOT NULL,
                            task_uuid TEXT,
                            request_id INTEGER NOT NULL,
                            seq_in_second INTEGER NOT NULL,
                            success INTEGER NOT NULL,
                            error_text TEXT
                        );
                        "#,
                    ) {
                        eprintln!("初始化 SQLite 表失败: {}", e);
                        // still continue to process incoming commands
                    }

                    // 主循环：阻塞接收命令并在同一连接上执行插入
                    for cmd in rx {
                        let res = conn.execute(
                            "INSERT INTO request_records (ts_seconds, task_uuid, request_id, seq_in_second, success, error_text)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![
                                cmd.ts_seconds,
                                cmd.task_uuid,
                                cmd.request_id as i64,
                                cmd.seq_in_second as i64,
                                if cmd.success { 1 } else { 0 },
                                cmd.error_text
                            ],
                        );
                        let send_res = match res {
                            Ok(_) => cmd.resp.send(Ok(())),
                            Err(e) => cmd.resp.send(Err(Box::new(e))),
                        };
                        if send_res.is_err() {
                            // receiver dropped, nothing to do
                        }
                    }

                    // channel closed -> 线程退出
                }
                Err(e) => {
                    eprintln!("打开 SQLite 连接失败: {}", e);
                    // drain incoming commands and reply with error
                    for cmd in rx {
                        let _ = cmd.resp.send(Err(Box::new(e.clone())));
                    }
                }
            }
        });

        *guard = Some(tx);
        Ok(())
    }

    /// 异步插入一条请求记录，使用后台线程的单一连接执行
    pub async fn insert_request(
        &self,
        ts_seconds: i64,
        task_uuid: Option<String>,
        request_id: usize,
        seq_in_second: usize,
        success: bool,
        error_text: Option<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 获取 sender
        let sender_opt = { self.cmd_sender.lock().unwrap().clone() };
        let sender = match sender_opt {
            Some(s) => s,
            None => {
                return Err(format!("SqliteRecorder 未初始化，请先调用 init(path)").into());
            }
        };

        // 准备 oneshot 用于等待结果
        let (resp_tx, resp_rx) = oneshot::channel::<Result<(), Box<dyn Error + Send + Sync>>>();

        // 构造命令
        let cmd = DbCommand {
            ts_seconds,
            task_uuid,
            request_id,
            seq_in_second,
            success,
            error_text,
            resp: resp_tx,
        };

        // sender.send 是阻塞的短操作：放入 spawn_blocking 避免在 async runtime 中阻塞
        let send_result = tokio::task::spawn_blocking(move || sender.send(cmd)).await;
        match send_result {
            Ok(Ok(_)) => {
                // 等待 worker 执行并通过 oneshot 返回结果
                match resp_rx.await {
                    Ok(r) => r,
                    Err(e) => Err(format!("后台线程响应失败: {}", e).into()),
                }
            }
            Ok(Err(e)) => Err(format!("向后台线程发送命令失败: {}", e).into()),
            Err(e) => Err(format!("发送命令时 spawn_blocking 出错: {}", e).into()),
        }
    }

    /// 关闭后台线程（通过丢弃 sender 来让线程退出）
    pub async fn shutdown(&self) {
        let mut guard = self.cmd_sender.lock().unwrap();
        if let Some(tx) = guard.take() {
            // dropping sender will close channel and worker will exit
            drop(tx);
            // small sleep to let background thread finish cleanly
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}