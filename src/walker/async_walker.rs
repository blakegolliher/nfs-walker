//! Async NFS Walker - READDIR + Pipelined GETATTR
//!
//! High-performance implementation that:
//! 1. Uses fast READDIR (names only) to get directory contents
//! 2. Chunks names and distributes to workers
//! 3. Workers use async pipelined GETATTR (128 concurrent per connection)
//!
//! Architecture:
//! ```text
//! Directory Queue
//! │
//! ├── Worker 0: pop dir → READDIR → chunk names → async GETATTR x128
//! ├── Worker 1: pop dir → READDIR → chunk names → async GETATTR x128
//! └── Worker N: pop dir → READDIR → chunk names → async GETATTR x128
//! │
//! └── Writer Thread: recv entries → batch insert to SQLite
//! ```

use crate::config::WalkConfig;
use crate::db::schema::{create_database, create_indexes, keys, optimize_for_reads, set_walk_info};
use crate::error::{NfsError, NfsResult, Result, WalkerError};
use crate::nfs::{NfsConnection, NfsConnectionBuilder};
use crate::nfs::types::{DbEntry, EntryType, NfsStat};
use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Injector, Stealer, Worker as DequeWorker};
use rusqlite::{params, Connection};
use std::collections::VecDeque;
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

// Import the FFI module
use crate::nfs::ffi;

/// Maximum concurrent in-flight stat requests per connection
const MAX_IN_FLIGHT: usize = 128;

/// Directory work item
#[derive(Debug, Clone)]
struct DirWork {
    path: String,
    depth: u32,
}

/// Result from walk operation
#[derive(Debug, Clone, Default)]
pub struct WalkStats {
    pub dirs: u64,
    pub files: u64,
    pub bytes: u64,
    pub errors: u64,
    pub duration: Duration,
    pub completed: bool,
}

/// Progress information for display
#[derive(Debug, Clone, Default)]
pub struct WalkProgress {
    pub dirs: u64,
    pub files: u64,
    pub bytes: u64,
    pub errors: u64,
    pub queue_size: usize,
    pub active_workers: usize,
    pub total_workers: usize,
    pub elapsed: Duration,
}

impl WalkProgress {
    pub fn files_per_second(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs > 0.0 {
            (self.files + self.dirs) as f64 / secs
        } else {
            0.0
        }
    }
}

/// Async walker using READDIR + pipelined GETATTR
pub struct AsyncWalker {
    config: WalkConfig,
    shutdown: Arc<AtomicBool>,
    dirs_count: Arc<AtomicU64>,
    files_count: Arc<AtomicU64>,
    bytes_count: Arc<AtomicU64>,
    errors_count: Arc<AtomicU64>,
}

impl AsyncWalker {
    pub fn new(config: WalkConfig) -> Self {
        Self {
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
            dirs_count: Arc::new(AtomicU64::new(0)),
            files_count: Arc::new(AtomicU64::new(0)),
            bytes_count: Arc::new(AtomicU64::new(0)),
            errors_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    pub fn progress(&self, elapsed: Duration) -> WalkProgress {
        WalkProgress {
            dirs: self.dirs_count.load(Ordering::Relaxed),
            files: self.files_count.load(Ordering::Relaxed),
            bytes: self.bytes_count.load(Ordering::Relaxed),
            errors: self.errors_count.load(Ordering::Relaxed),
            queue_size: 0,
            active_workers: 0,
            total_workers: self.config.worker_count,
            elapsed,
        }
    }

    pub fn run(&self) -> Result<WalkStats> {
        let start = Instant::now();

        info!("Opening database: {}", self.config.output_path.display());
        let db = self.open_database()?;

        // Channel for entries to write
        let (entry_tx, entry_rx) = bounded::<Vec<DbEntry>>(100);

        // Spawn writer thread
        let writer_handle = self.spawn_writer(db, entry_rx);

        // Work-stealing deque
        let injector: Arc<Injector<DirWork>> = Arc::new(Injector::new());
        let active_workers = Arc::new(AtomicUsize::new(0));
        let pending_work = Arc::new(AtomicU64::new(1));

        // Push root
        let start_path = self.config.nfs_url.walk_start_path();
        injector.push(DirWork {
            path: start_path.clone(),
            depth: 0,
        });

        // Create worker queues
        let mut workers_local: Vec<DequeWorker<DirWork>> = Vec::new();
        let mut stealers: Vec<Stealer<DirWork>> = Vec::new();

        for _ in 0..self.config.worker_count {
            let w = DequeWorker::new_fifo();
            stealers.push(w.stealer());
            workers_local.push(w);
        }

        let stealers = Arc::new(stealers);

        // Spawn workers
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for (id, local) in workers_local.into_iter().enumerate() {
            let nfs = self.create_connection()?;
            info!("Worker {} connected to {}", id, nfs.server());

            let injector = Arc::clone(&injector);
            let stealers = Arc::clone(&stealers);
            let entry_tx = entry_tx.clone();
            let shutdown = Arc::clone(&self.shutdown);
            let dirs_count = Arc::clone(&self.dirs_count);
            let files_count = Arc::clone(&self.files_count);
            let bytes_count = Arc::clone(&self.bytes_count);
            let errors_count = Arc::clone(&self.errors_count);
            let active_workers = Arc::clone(&active_workers);
            let pending_work = Arc::clone(&pending_work);
            let max_depth = self.config.max_depth;
            let dirs_only = self.config.dirs_only;

            let handle = thread::Builder::new()
                .name(format!("async-walker-{}", id))
                .spawn(move || {
                    async_worker_loop(
                        id,
                        nfs,
                        local,
                        injector,
                        stealers,
                        entry_tx,
                        shutdown,
                        dirs_count,
                        files_count,
                        bytes_count,
                        errors_count,
                        active_workers,
                        pending_work,
                        max_depth,
                        dirs_only,
                    );
                })
                .expect("Failed to spawn worker thread");

            handles.push(handle);
        }

        drop(entry_tx);

        for handle in handles {
            let _ = handle.join();
        }

        let db = writer_handle.join().expect("Writer thread panicked");

        info!("Finalizing database...");
        self.finalize_database(&db, start.elapsed(), !self.shutdown.load(Ordering::Relaxed))?;

        let stats = WalkStats {
            dirs: self.dirs_count.load(Ordering::Relaxed),
            files: self.files_count.load(Ordering::Relaxed),
            bytes: self.bytes_count.load(Ordering::Relaxed),
            errors: self.errors_count.load(Ordering::Relaxed),
            duration: start.elapsed(),
            completed: !self.shutdown.load(Ordering::Relaxed),
        };

        Ok(stats)
    }

    pub fn run_with_progress<F>(&self, progress_callback: F) -> Result<WalkStats>
    where
        F: Fn(WalkProgress) + Send + 'static,
    {
        let start = Instant::now();
        let shutdown = Arc::clone(&self.shutdown);
        let dirs = Arc::clone(&self.dirs_count);
        let files = Arc::clone(&self.files_count);
        let bytes = Arc::clone(&self.bytes_count);
        let errors = Arc::clone(&self.errors_count);
        let total_workers = self.config.worker_count;

        let progress_handle = thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                let progress = WalkProgress {
                    dirs: dirs.load(Ordering::Relaxed),
                    files: files.load(Ordering::Relaxed),
                    bytes: bytes.load(Ordering::Relaxed),
                    errors: errors.load(Ordering::Relaxed),
                    queue_size: 0,
                    active_workers: 0,
                    total_workers,
                    elapsed: start.elapsed(),
                };
                progress_callback(progress);
                thread::sleep(Duration::from_millis(100));
            }
        });

        let result = self.run();

        self.shutdown.store(true, Ordering::SeqCst);
        let _ = progress_handle.join();

        result
    }

    fn open_database(&self) -> Result<Connection> {
        let path = &self.config.output_path;

        if path.exists() {
            std::fs::remove_file(path).map_err(|e| WalkerError::Io(e))?;
        }

        let conn = Connection::open(path)
            .map_err(|e| WalkerError::Database(e.into()))?;

        create_database(&conn).map_err(|e| WalkerError::Database(e))?;

        set_walk_info(&conn, keys::SOURCE_URL, &self.config.nfs_url.to_display_string())
            .map_err(|e| WalkerError::Database(e))?;
        set_walk_info(&conn, keys::START_TIME, &chrono::Utc::now().to_rfc3339())
            .map_err(|e| WalkerError::Database(e))?;
        set_walk_info(&conn, keys::STATUS, "running")
            .map_err(|e| WalkerError::Database(e))?;
        set_walk_info(&conn, keys::WORKER_COUNT, &self.config.worker_count.to_string())
            .map_err(|e| WalkerError::Database(e))?;

        Ok(conn)
    }

    fn finalize_database(&self, conn: &Connection, duration: Duration, completed: bool) -> Result<()> {
        info!("Creating indexes...");
        create_indexes(conn).map_err(|e| WalkerError::Database(e))?;

        set_walk_info(conn, keys::END_TIME, &chrono::Utc::now().to_rfc3339())
            .map_err(|e| WalkerError::Database(e))?;
        set_walk_info(conn, keys::DURATION_SECS, &duration.as_secs().to_string())
            .map_err(|e| WalkerError::Database(e))?;
        set_walk_info(conn, keys::TOTAL_DIRS, &self.dirs_count.load(Ordering::Relaxed).to_string())
            .map_err(|e| WalkerError::Database(e))?;
        set_walk_info(conn, keys::TOTAL_FILES, &self.files_count.load(Ordering::Relaxed).to_string())
            .map_err(|e| WalkerError::Database(e))?;
        set_walk_info(conn, keys::TOTAL_BYTES, &self.bytes_count.load(Ordering::Relaxed).to_string())
            .map_err(|e| WalkerError::Database(e))?;
        set_walk_info(conn, keys::ERROR_COUNT, &self.errors_count.load(Ordering::Relaxed).to_string())
            .map_err(|e| WalkerError::Database(e))?;
        set_walk_info(conn, keys::STATUS, if completed { "completed" } else { "interrupted" })
            .map_err(|e| WalkerError::Database(e))?;

        optimize_for_reads(conn).map_err(|e| WalkerError::Database(e))?;

        Ok(())
    }

    fn create_connection(&self) -> Result<NfsConnection> {
        let timeout = Duration::from_secs(self.config.timeout_secs as u64);
        NfsConnectionBuilder::new(self.config.nfs_url.clone())
            .timeout(timeout)
            .retries(self.config.retry_count)
            .connect()
            .map_err(|e| WalkerError::Nfs(e))
    }

    fn spawn_writer(&self, conn: Connection, entry_rx: Receiver<Vec<DbEntry>>) -> JoinHandle<Connection> {
        thread::Builder::new()
            .name("db-writer".to_string())
            .spawn(move || {
                writer_loop(conn, entry_rx)
            })
            .expect("Failed to spawn writer thread")
    }
}

/// Stat result from async operation
struct StatResult {
    name: String,
    path: String,
    stat: Option<NfsStat>,
    error: Option<String>,
}

/// Context for async stat callback
struct CallbackContext {
    index: usize,
    results_ptr: *mut Vec<StatResult>,
    completed: Arc<AtomicU64>,
}

unsafe impl Send for CallbackContext {}

/// Async stat engine that pipelines GETATTR operations
struct AsyncStatEngine {
    nfs: *mut ffi::nfs_context,
    pending: VecDeque<(String, String)>,
    in_flight: usize,
    completed: Arc<AtomicU64>,
    results: Vec<StatResult>,
    total: usize,
}

impl AsyncStatEngine {
    unsafe fn new(nfs: *mut ffi::nfs_context) -> Self {
        Self {
            nfs,
            pending: VecDeque::new(),
            in_flight: 0,
            completed: Arc::new(AtomicU64::new(0)),
            results: Vec::new(),
            total: 0,
        }
    }

    fn add_paths(&mut self, names: Vec<String>, parent_path: &str) {
        for name in names {
            let full_path = if parent_path == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", parent_path, name)
            };
            self.pending.push_back((name, full_path));
        }
        self.total = self.pending.len();

        // Pre-allocate results
        self.results = Vec::with_capacity(self.total);
        for (name, path) in self.pending.iter() {
            self.results.push(StatResult {
                name: name.clone(),
                path: path.clone(),
                stat: None,
                error: None,
            });
        }
    }

    fn run(&mut self) -> NfsResult<Vec<StatResult>> {
        // Fire initial batch
        self.fire_requests()?;

        // Poll loop
        while self.in_flight > 0 || !self.pending.is_empty() {
            self.poll_and_service()?;

            if self.in_flight < MAX_IN_FLIGHT && !self.pending.is_empty() {
                self.fire_requests()?;
            }
        }

        Ok(std::mem::take(&mut self.results))
    }

    fn fire_requests(&mut self) -> NfsResult<()> {
        while self.in_flight < MAX_IN_FLIGHT {
            let result_index = self.total - self.pending.len();

            let (_name, path) = match self.pending.pop_front() {
                Some(item) => item,
                None => break,
            };

            let c_path = CString::new(path.as_bytes()).map_err(|_| NfsError::StatFailed {
                path: path.clone(),
                reason: "Path contains null bytes".to_string(),
            })?;

            let ctx = Box::new(CallbackContext {
                index: result_index,
                results_ptr: &mut self.results as *mut Vec<StatResult>,
                completed: Arc::clone(&self.completed),
            });

            let ret = unsafe {
                ffi::nfs_stat64_async(
                    self.nfs,
                    c_path.as_ptr(),
                    Some(stat_callback),
                    Box::into_raw(ctx) as *mut std::ffi::c_void,
                )
            };

            if ret != 0 {
                if let Some(result) = self.results.get_mut(result_index) {
                    result.error = Some("Failed to start async stat".to_string());
                }
                self.completed.fetch_add(1, Ordering::Relaxed);
            } else {
                self.in_flight += 1;
            }
        }

        Ok(())
    }

    fn poll_and_service(&mut self) -> NfsResult<()> {
        let fd = unsafe { ffi::nfs_get_fd(self.nfs) };
        let events = unsafe { ffi::nfs_which_events(self.nfs) };

        let mut pfd = libc::pollfd {
            fd,
            events: events as i16,
            revents: 0,
        };

        let ret = unsafe { libc::poll(&mut pfd, 1, 100) };

        if ret < 0 {
            return Err(NfsError::Protocol {
                code: ret,
                message: "poll() failed".to_string(),
            });
        }

        if ret > 0 {
            let old_completed = self.completed.load(Ordering::Relaxed);

            let service_ret = unsafe { ffi::nfs_service(self.nfs, pfd.revents as i32) };

            if service_ret < 0 {
                return Err(NfsError::Protocol {
                    code: service_ret,
                    message: "nfs_service() failed".to_string(),
                });
            }

            let new_completed = self.completed.load(Ordering::Relaxed);
            let just_completed = (new_completed - old_completed) as usize;
            self.in_flight = self.in_flight.saturating_sub(just_completed);
        }

        Ok(())
    }
}

/// C callback for nfs_stat64_async
unsafe extern "C" fn stat_callback(
    status: i32,
    _nfs: *mut ffi::nfs_context,
    data: *mut std::ffi::c_void,
    private_data: *mut std::ffi::c_void,
) {
    let ctx = Box::from_raw(private_data as *mut CallbackContext);
    let results = &mut *ctx.results_ptr;

    if status < 0 {
        let error_msg = if !data.is_null() {
            let c_str = std::ffi::CStr::from_ptr(data as *const i8);
            c_str.to_string_lossy().to_string()
        } else {
            "Unknown error".to_string()
        };

        if let Some(result) = results.get_mut(ctx.index) {
            result.error = Some(error_msg);
        }
    } else {
        let stat_ptr = data as *const ffi::nfs_stat_64;
        if !stat_ptr.is_null() {
            let stat = &*stat_ptr;
            let nfs_stat = NfsStat {
                size: stat.nfs_size,
                inode: stat.nfs_ino,
                nlink: stat.nfs_nlink,
                uid: stat.nfs_uid as u32,
                gid: stat.nfs_gid as u32,
                mode: stat.nfs_mode as u32,
                atime: Some(stat.nfs_atime as i64),
                mtime: Some(stat.nfs_mtime as i64),
                ctime: Some(stat.nfs_ctime as i64),
                blksize: stat.nfs_blksize,
                blocks: stat.nfs_blocks,
            };

            if let Some(result) = results.get_mut(ctx.index) {
                result.stat = Some(nfs_stat);
            }
        }
    }

    ctx.completed.fetch_add(1, Ordering::Relaxed);
}

/// Worker loop using async pipelined GETATTR
fn async_worker_loop(
    id: usize,
    nfs: NfsConnection,
    local: DequeWorker<DirWork>,
    injector: Arc<Injector<DirWork>>,
    stealers: Arc<Vec<Stealer<DirWork>>>,
    entry_tx: Sender<Vec<DbEntry>>,
    shutdown: Arc<AtomicBool>,
    dirs_count: Arc<AtomicU64>,
    files_count: Arc<AtomicU64>,
    bytes_count: Arc<AtomicU64>,
    errors_count: Arc<AtomicU64>,
    active_workers: Arc<AtomicUsize>,
    pending_work: Arc<AtomicU64>,
    max_depth: Option<usize>,
    dirs_only: bool,
) {
    debug!("Async worker {} started", id);

    let mut batch: Vec<DbEntry> = Vec::with_capacity(1000);
    let mut idle_spins = 0;
    const MAX_IDLE_SPINS: u32 = 1000;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Get work
        let work = local.pop().or_else(|| {
            loop {
                match injector.steal() {
                    crossbeam_deque::Steal::Success(w) => return Some(w),
                    crossbeam_deque::Steal::Empty => break,
                    crossbeam_deque::Steal::Retry => continue,
                }
            }
            for (i, stealer) in stealers.iter().enumerate() {
                if i == id { continue; }
                loop {
                    match stealer.steal() {
                        crossbeam_deque::Steal::Success(w) => return Some(w),
                        crossbeam_deque::Steal::Empty => break,
                        crossbeam_deque::Steal::Retry => continue,
                    }
                }
            }
            None
        });

        let work = match work {
            Some(w) => {
                idle_spins = 0;
                active_workers.fetch_add(1, Ordering::Relaxed);
                w
            }
            None => {
                idle_spins += 1;

                if pending_work.load(Ordering::SeqCst) == 0
                    && active_workers.load(Ordering::SeqCst) == 0
                {
                    break;
                }

                if idle_spins > MAX_IDLE_SPINS {
                    thread::sleep(Duration::from_micros(100));
                    idle_spins = 0;
                }
                continue;
            }
        };

        // Check depth
        if let Some(max) = max_depth {
            if work.depth > max as u32 {
                pending_work.fetch_sub(1, Ordering::SeqCst);
                active_workers.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
        }

        debug!("Worker {} READDIR: {}", id, work.path);

        // Phase 1: Fast READDIR to get names only
        let names: Vec<String> = match nfs.opendir_names_only(&work.path) {
            Ok(handle) => {
                let mut names = Vec::new();
                while let Some(entry) = handle.readdir() {
                    if !entry.is_special() {
                        names.push(entry.name);
                    }
                }
                names
            }
            Err(e) => {
                errors_count.fetch_add(1, Ordering::Relaxed);
                // Not found errors are common on active filesystems (race condition)
                if e.to_string().contains("not found") || e.to_string().contains("No such file") {
                    debug!("Worker {} READDIR not found: {}", id, work.path);
                } else {
                    warn!("Worker {} READDIR failed: {} -> {}", id, work.path, e);
                }
                pending_work.fetch_sub(1, Ordering::SeqCst);
                active_workers.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
        };

        let name_count = names.len();
        debug!("Worker {} READDIR complete: {} -> {} names", id, work.path, name_count);

        if names.is_empty() {
            dirs_count.fetch_add(1, Ordering::Relaxed);
            pending_work.fetch_sub(1, Ordering::SeqCst);
            active_workers.fetch_sub(1, Ordering::Relaxed);
            continue;
        }

        // Phase 2: Async pipelined GETATTR
        let raw_ctx = unsafe { nfs.raw_context() };
        let mut engine = unsafe { AsyncStatEngine::new(raw_ctx) };
        engine.add_paths(names, &work.path);

        let results = match engine.run() {
            Ok(r) => r,
            Err(e) => {
                errors_count.fetch_add(1, Ordering::Relaxed);
                if e.to_string().contains("not found") || e.to_string().contains("No such file") {
                    debug!("Worker {} async stat not found: {}", id, work.path);
                } else {
                    warn!("Worker {} async stat failed: {} -> {}", id, work.path, e);
                }
                pending_work.fetch_sub(1, Ordering::SeqCst);
                active_workers.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
        };

        // Process results
        let mut subdir_count = 0u64;
        let mut file_count = 0u64;
        let mut byte_count = 0u64;
        let mut error_count = 0u64;

        for result in results {
            if let Some(ref err) = result.error {
                error_count += 1;
                debug!("Worker {} stat error: {} -> {}", id, result.path, err);
                continue;
            }

            let stat = match result.stat {
                Some(s) => s,
                None => {
                    error_count += 1;
                    continue;
                }
            };

            let entry_type = stat.entry_type();
            let is_dir = entry_type == EntryType::Directory;

            if dirs_only && !is_dir {
                continue;
            }

            let db_entry = DbEntry {
                parent_path: Some(work.path.clone()),
                name: result.name,
                path: result.path.clone(),
                entry_type,
                size: stat.size,
                mtime: stat.mtime,
                atime: stat.atime,
                ctime: stat.ctime,
                mode: Some(stat.mode),
                uid: Some(stat.uid),
                gid: Some(stat.gid),
                nlink: Some(stat.nlink as u64),
                inode: stat.inode,
                depth: work.depth + 1,
            };

            batch.push(db_entry);

            if is_dir {
                subdir_count += 1;
                pending_work.fetch_add(1, Ordering::SeqCst);
                local.push(DirWork {
                    path: result.path,
                    depth: work.depth + 1,
                });
            } else {
                file_count += 1;
                byte_count += stat.size;
            }

            if batch.len() >= 1000 {
                if entry_tx.send(std::mem::replace(&mut batch, Vec::with_capacity(1000))).is_err() {
                    break;
                }
            }
        }

        dirs_count.fetch_add(1, Ordering::Relaxed);
        files_count.fetch_add(file_count, Ordering::Relaxed);
        bytes_count.fetch_add(byte_count, Ordering::Relaxed);
        errors_count.fetch_add(error_count, Ordering::Relaxed);

        debug!(
            "Worker {} complete: {} -> {} entries ({} dirs, {} files, {} errors)",
            id, work.path, name_count, subdir_count, file_count, error_count
        );

        pending_work.fetch_sub(1, Ordering::SeqCst);
        active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    if !batch.is_empty() {
        let _ = entry_tx.send(batch);
    }

    debug!("Async worker {} finished", id);
}

/// Writer thread
fn writer_loop(mut conn: Connection, entry_rx: Receiver<Vec<DbEntry>>) -> Connection {
    debug!("Writer thread started");

    conn.execute_batch(
        "PRAGMA synchronous = OFF;
         PRAGMA journal_mode = OFF;
         PRAGMA cache_size = -64000;
         PRAGMA temp_store = MEMORY;"
    ).expect("Failed to set bulk load pragmas");

    let mut total_written = 0u64;
    let mut pending: Vec<DbEntry> = Vec::with_capacity(20000);
    let batch_size = 10000;

    while let Ok(entries) = entry_rx.recv() {
        pending.extend(entries);

        if pending.len() >= batch_size {
            if let Err(e) = write_batch_fast(&mut conn, &pending) {
                error!("Database write failed: {}", e);
            } else {
                total_written += pending.len() as u64;
            }
            pending.clear();
        }
    }

    if !pending.is_empty() {
        if let Err(e) = write_batch_fast(&mut conn, &pending) {
            error!("Final database write failed: {}", e);
        } else {
            total_written += pending.len() as u64;
        }
    }

    conn.execute_batch("PRAGMA synchronous = NORMAL;").ok();

    debug!("Writer thread finished, wrote {} entries", total_written);
    conn
}

fn write_batch_fast(conn: &mut Connection, entries: &[DbEntry]) -> Result<()> {
    let tx = conn.transaction()
        .map_err(|e| WalkerError::Database(e.into()))?;

    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO entries (parent_id, name, path, entry_type, size, mtime, atime, ctime, mode, uid, gid, nlink, inode, depth)
             VALUES (NULL, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
        ).map_err(|e| WalkerError::Database(e.into()))?;

        for entry in entries {
            stmt.execute(params![
                entry.name,
                entry.path,
                entry.entry_type.as_db_int(),
                entry.size as i64,
                entry.mtime,
                entry.atime,
                entry.ctime,
                entry.mode.map(|m| m as i64),
                entry.uid.map(|u| u as i64),
                entry.gid.map(|g| g as i64),
                entry.nlink.map(|n| n as i64),
                entry.inode as i64,
                entry.depth as i64,
            ]).map_err(|e| WalkerError::Database(e.into()))?;
        }
    }

    tx.commit().map_err(|e| WalkerError::Database(e.into()))?;
    Ok(())
}
