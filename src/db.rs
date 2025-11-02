use rusqlite::{Connection, params};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use std::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

/// 单一长连接的 SQLite 记录器：在单独的阻塞线程中持有 rusqlite::Connection，
/// 通过 std::sync::mpsc 与该线程通信，insert_request 为异步接口通过 oneshot 等待结果。
#[derive(Clone)]
pub struct SqliteRecorder {
    cmd_sender: Arc<Mutex<Option<Sender<DbCommand>>>>,
    // 保存 worker 的 JoinHandle，以便在 shutdown 时 join，保证线程已退出
    worker_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
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
            worker_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// 初始化并启动后台线程（创建 DB 文件与表）
    /// 确保后台线程成功打开 DB，否则返回错误；线程启动使用一个短超时的 ready 通道确认。
    pub async fn init(&self, path: &str) -> Result<(), Box<dyn Error>> {
        // 更稳健地处理 mutex 中毒
        let mut guard = self
            .cmd_sender
            .lock()
            .map_err(|_| "cmd_sender mutex 被破坏")?;
        if guard.is_some() {
            // 已经初始化
            return Ok(());
        }

        // 创建 channel 与 worker 线程
        let (tx, rx): (Sender<DbCommand>, Receiver<DbCommand>) = mpsc::channel();
        // ready 通道用于告知 init 后台线程是否成功打开 DB（避免 init 返回但线程失败）
        let (ready_tx, ready_rx): (Sender<Result<(), String>>, Receiver<Result<(), String>>) =
            mpsc::channel();
        let db_path = path.to_string();

        // spawn blocking thread that owns rusqlite::Connection
        let handle = thread::spawn(move || {
            // 建立连接并创建表
            match Connection::open(&db_path) {
                Ok(conn) => {
                    // 通知启动成功（忽略发送错误）
                    let _ = ready_tx.send(Ok(()));
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
                    let err_msg = format!("打开 SQLite 连接失败: {}", e);
                    eprintln!("{}", err_msg);
                    // 通知 init 失败
                    let _ = ready_tx.send(Err(err_msg.clone()));
                    // drain incoming commands and reply with error
                    for cmd in rx {
                        let _ = cmd.resp.send(Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            err_msg.clone(),
                        ))));
                    }
                }
            }
        });

        // 等待后台线程报告启动结果（使用 spawn_blocking 避免阻塞异步 runtime）
        let recv_res =
            tokio::task::spawn_blocking(move || ready_rx.recv_timeout(Duration::from_secs(2)))
                .await
                .map_err(|e| format!("等待后台线程 ready 信号 spawn_blocking 出错: {}", e))?;

        match recv_res {
            Ok(Ok(())) => {
                // 后台线程已成功打开 DB，将 sender 和 handle 存入结构体以供后续使用与 shutdown
                *guard = Some(tx);
                let mut hguard = self
                    .worker_handle
                    .lock()
                    .map_err(|_| "worker_handle mutex 被破坏")?;
                *hguard = Some(handle);
                Ok(())
            }
            Ok(Err(msg)) => {
                // 后台线程启动失败，join 清理并返回错误
                let _ = tokio::task::spawn_blocking(move || handle.join()).await;
                Err(msg.into())
            }
            Err(_) => {
                // ready 超时，join 线程并返回错误
                let _ = tokio::task::spawn_blocking(move || handle.join()).await;
                Err("等待后台 DB 线程启动超时".into())
            }
        }
    }

    /// 异步插入一条请求记录，使用后台线程的单一连接执行
    /// 增加了对 mutex 中毒的保护和对后台响应的超时检测（默认 5 秒）。
    pub async fn insert_request(
        &self,
        ts_seconds: i64,
        task_uuid: Option<String>,
        request_id: usize,
        seq_in_second: usize,
        success: bool,
        error_text: Option<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 获取 sender（处理 mutex 中毒）
        let sender_opt = {
            self.cmd_sender
                .lock()
                .map_err(|_| "cmd_sender mutex 被破坏")?
                .clone()
        };
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
                // 等待 worker 执行并通过 oneshot 返回结果，添加超时以避免永久等待
                match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => Err(format!("后台线程响应失败: {}", e).into()),
                    Err(_) => Err("等待后台 DB 响应超时".into()),
                }
            }
            Ok(Err(e)) => Err(format!("向后台线程发送命令失败: {}", e).into()),
            Err(e) => Err(format!("发送命令时 spawn_blocking 出错: {}", e).into()),
        }
    }

    /// 关闭后台线程（通过丢弃 sender 来让线程退出），并 join worker，确保后台线程已退出
    pub async fn shutdown(&self) {
        // 先取走 sender
        let mut guard = match self.cmd_sender.lock() {
            Ok(g) => g,
            Err(_) => {
                // 若 mutex 被污染，仍尝试取 worker handle
                let mut hguard = self.worker_handle.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(handle) = hguard.take() {
                    let _ = tokio::task::spawn_blocking(move || handle.join()).await;
                }
                return;
            }
        };
        if let Some(tx) = guard.take() {
            drop(tx); // closing channel -> worker 线程会在 rx 迭代结束后退出
        }

        // join worker thread（在 spawn_blocking 中执行 join）
        let mut hguard = match self.worker_handle.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        if let Some(handle) = hguard.take() {
            let _ = tokio::task::spawn_blocking(move || {
                let _ = handle.join();
            })
            .await;
        }
        // small sleep is unnecessary now because we joined the thread
    }
}
