use rusqlite::{Connection, params};
use std::error::Error;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell, oneshot};

/// 单一长连接的 SQLite 记录器：在单独的阻塞线程中持有 rusqlite::Connection，
/// 通过 std::sync::mpsc 与该线程通信，insert_request 为异步接口通过 oneshot 等待结果。
#[derive(Clone)]
pub struct SqliteRecorder {
    cmd_sender: Arc<Mutex<Option<Sender<DbCommand>>>>,
    // 保存 worker 的 JoinHandle，以便在 shutdown 时 join，保证线程已退出
    worker_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
}

enum DbCommandType {
    Insert {
        ts_seconds: i64,
        task_uuid: Option<String>,
        request_id: usize,
        seq_in_second: usize,
        success: bool,
        error_text: Option<String>,
    },
    UpdateAlarm {
        task_uuid: Option<String>,
        request_id: Option<usize>,
        alarm_triggered: bool,
        receive_time: i64,
        alarm_time: i64,
    },
}

struct DbCommand {
    cmd_type: DbCommandType,
    resp: oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>,
}

// 全局单例
static RECORDER: OnceCell<SqliteRecorder> = OnceCell::const_new();

impl SqliteRecorder {
    /// new 不会立即启动线程；调用 init() 来启动持久连接后台线程
    pub fn new() -> Self {
        Self {
            cmd_sender: Arc::new(Mutex::new(None)),
            worker_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// 获取全局单例实例
    pub async fn instance() -> &'static SqliteRecorder {
        RECORDER
            .get_or_init(|| {
                let recorder = SqliteRecorder::new();
                async move {
                    recorder.init("requests.db").await.unwrap(); // 确保初始化
                    recorder
                }
            })
            .await
    }

    /// 初始化并启动后台线程（创建 DB 文件与表）
    pub async fn init(&self, path: &str) -> Result<(), Box<dyn Error>> {
        // 使用异步锁
        let mut guard = self.cmd_sender.lock().await;
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
                            error_text TEXT,
                            alarm_triggered INTEGER NOT NULL DEFAULT 0,
                            receive_time INTEGER,
                            alarm_time INTEGER
                        );
                        "#,
                    ) {
                        eprintln!("初始化 SQLite 表失败: {}", e);
                        // still continue to process incoming commands
                    }

                    // 主循环：阻塞接收命令并在同一连接上执行插入
                    for cmd in rx {
                        let res = match cmd.cmd_type {
                            DbCommandType::Insert {
                                ts_seconds,
                                task_uuid,
                                request_id,
                                seq_in_second,
                                success,
                                error_text,
                            } => conn.execute(
                                "INSERT INTO request_records (ts_seconds, task_uuid, request_id, seq_in_second, success, error_text)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                params![
                                    ts_seconds,
                                    task_uuid,
                                    request_id as i64,
                                    seq_in_second as i64,
                                    if success { 1 } else { 0 },
                                    error_text
                                ],
                            ),
                            DbCommandType::UpdateAlarm {
                                task_uuid,
                                request_id,
                                alarm_triggered,
                                receive_time,
                                alarm_time,
                            } => {
                                let mut sql = String::from("UPDATE request_records SET alarm_triggered = ?");
                                // Create owned values that live long enough
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

                                conn.execute(&sql, rusqlite::params_from_iter(params))
                            }
                        };
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
                let mut hguard = self.worker_handle.lock().await;
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
    pub async fn insert_request(
        &self,
        ts_seconds: i64,
        task_uuid: Option<String>,
        request_id: usize,
        seq_in_second: usize,
        success: bool,
        error_text: Option<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 获取 sender（使用异步锁）
        let sender_opt = { self.cmd_sender.lock().await.clone() };
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
            cmd_type: DbCommandType::Insert {
                ts_seconds,
                task_uuid,
                request_id,
                seq_in_second,
                success,
                error_text,
            },
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

    /// 更新记录的报警相关字段
    /// 可以通过 task_uuid 或 request_id 查找记录(至少需要提供一个)
    /// 所有更新字段都是可选的，只更新提供的字段
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

        // 获取 sender（使用异步锁）
        let sender_opt = { self.cmd_sender.lock().await.clone() };
        let sender = match sender_opt {
            Some(s) => s,
            None => return Err("SqliteRecorder 未初始化，请先调用 init(path)".into()),
        };

        // 准备 oneshot 用于等待结果
        let (resp_tx, resp_rx) = oneshot::channel();

        // 构造更新命令
        let cmd = DbCommand {
            cmd_type: DbCommandType::UpdateAlarm {
                task_uuid,
                request_id,
                alarm_triggered,
                receive_time,
                alarm_time,
            },
            resp: resp_tx,
        };

        // 发送命令并等待结果
        let send_result = tokio::task::spawn_blocking(move || sender.send(cmd)).await;
        match send_result {
            Ok(Ok(_)) => match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => Err(format!("后台线程响应失败: {}", e).into()),
                Err(_) => Err("等待后台 DB 响应超时".into()),
            },
            Ok(Err(e)) => Err(format!("向后台线程发送命令失败: {}", e).into()),
            Err(e) => Err(format!("发送命令时 spawn_blocking 出错: {}", e).into()),
        }
    }

    /// 关闭后台线程（通过丢弃 sender 来让线程退出），并 join worker，确保后台线程已退出
    pub async fn shutdown(&self) {
        println!("正在关闭数据库连接...");
        // 使用异步锁获取 sender
        let mut guard = self.cmd_sender.lock().await;
        if let Some(tx) = guard.take() {
            drop(tx); // closing channel -> worker 线程会在 rx 迭代结束后退出
        }

        // join worker thread（在 spawn_blocking 中执行 join）
        let mut hguard = self.worker_handle.lock().await;
        if let Some(handle) = hguard.take() {
            println!("等待数据库工作线程完成所有操作并退出...");
            if let Err(e) = tokio::task::spawn_blocking(move || handle.join()).await {
                eprintln!("工作线程退出时发生错误: {:?}", e);
            }
        }
        println!("数据库连接已关闭");
    }
}

impl Drop for SqliteRecorder {
    fn drop(&mut self) {
        // 在Drop中，我们无法使用异步操作，所以只进行基本的清理
        // 主要的清理应该在shutdown方法中完成
        println!("SqliteRecorder 正在被释放...");
        // 尝试获取锁，如果失败则忽略（在Drop中不能阻塞）
        if let Ok(mut guard) = self.cmd_sender.try_lock() {
            if let Some(tx) = guard.take() {
                drop(tx); // 关闭通道，通知工作线程退出
            }
        }

        // 尝试获取工作线程句柄，如果失败则忽略
        if let Ok(mut hguard) = self.worker_handle.try_lock() {
            if let Some(handle) = hguard.take() {
                // 尝试加入线程，但设置超时避免阻塞
                let _ = std::thread::spawn(move || {
                    if let Err(_) = handle.join() {
                        // 忽略加入错误
                    }
                })
                .join();
            }
        }
        println!("数据库资源清理完成");
    }
}
