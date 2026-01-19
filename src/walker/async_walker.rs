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
use super::simple::{WalkStats, WalkProgress};
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
const MAX_IN_FLIGHT: usize = 512;

/// Work item - either a directory to scan or a chunk of names to stat
#[derive(Debug, Clone)]
enum WorkItem {
    /// Directory to READDIR
    Directory { path: String, depth: u32 },
    /// Chunk of names to GETATTR (parent_path, names, depth)
    NameChunk { parent_path: String, names: Vec<String>, depth: u32 },
}

/// Batch size for streaming READDIR (dispatch chunks as they arrive)
/// Smaller = faster dispatch to workers, more overlap with GETATTR
/// 10K entries ≈ 66 RPCs at 8KB buffer, dispatches every ~0.5s
const READDIR_STREAM_BATCH: u32 = 10_000;

/// READDIR RPC buffer size in bytes
/// Note: Larger values (32KB+) caused hangs with some servers. Stick with default.
const READDIR_BUFFER_SIZE: u32 = 8192; // 8KB default


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
        let injector: Arc<Injector<WorkItem>> = Arc::new(Injector::new());
        let active_workers = Arc::new(AtomicUsize::new(0));
        let pending_work = Arc::new(AtomicU64::new(1));

        // Push root
        let start_path = self.config.nfs_url.walk_start_path();
        injector.push(WorkItem::Directory {
            path: start_path.clone(),
            depth: 0,
        });

        // Create worker queues
        let mut workers_local: Vec<DequeWorker<WorkItem>> = Vec::new();
        let mut stealers: Vec<Stealer<WorkItem>> = Vec::new();

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
            // Note: Don't set readdir_buffer_size - causes hangs with some servers
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

/// Context for async stat callback - uses indexed results like the proven working implementation
struct CallbackContext {
    index: usize,
    results_ptr: *mut Vec<StatResult>,
    completed: Arc<AtomicU64>,
}

unsafe impl Send for CallbackContext {}

/// Async stat engine that pipelines GETATTR operations
/// Uses pre-allocated indexed Vec (proven working pattern from previous/async_stat.rs)
struct AsyncStatEngine {
    nfs: *mut ffi::nfs_context,
    pending: VecDeque<(String, String)>,  // (name, full_path)
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

    /// Add paths to stat - must be called before run()
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

        // CRITICAL: Pre-allocate results to exact size to prevent reallocation.
        // The callback contexts store raw pointers to self.results, so if the Vec
        // reallocates during fire_requests(), those pointers become dangling.
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

    /// Run all stats and return results
    fn run(&mut self) -> NfsResult<Vec<StatResult>> {
        // Fire initial batch of requests
        self.fire_requests()?;

        // Poll loop
        while self.in_flight > 0 || !self.pending.is_empty() {
            self.poll_and_service()?;

            // Fire more requests if we have capacity
            if self.in_flight < MAX_IN_FLIGHT && !self.pending.is_empty() {
                self.fire_requests()?;
            }
        }

        Ok(std::mem::take(&mut self.results))
    }

    fn fire_requests(&mut self) -> NfsResult<()> {
        while self.in_flight < MAX_IN_FLIGHT {
            // Calculate index BEFORE popping - this is the index into pre-allocated results
            let result_index = self.total - self.pending.len();

            let (_name, path) = match self.pending.pop_front() {
                Some(item) => item,
                None => break,
            };

            let c_path = CString::new(path.as_bytes()).map_err(|_| NfsError::StatFailed {
                path: path.clone(),
                reason: "Path contains null bytes".to_string(),
            })?;

            // Create callback context with index into pre-allocated results
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
                // Mark as error in pre-allocated results slot
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

        let ret = unsafe { libc::poll(&mut pfd, 1, 1) };

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

            // Update in_flight count based on completions
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

/// Worker loop using pipelined GETATTR after READDIR completes
fn async_worker_loop(
    id: usize,
    nfs: NfsConnection,
    local: DequeWorker<WorkItem>,
    injector: Arc<Injector<WorkItem>>,
    stealers: Arc<Vec<Stealer<WorkItem>>>,
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

        // Get work from local queue, injector, or steal from others
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

        match work {
            WorkItem::Directory { path, depth } => {
                process_directory(
                    id, &nfs, &path, depth, max_depth, dirs_only,
                    &local, &injector, &entry_tx, &mut batch,
                    &dirs_count, &files_count, &bytes_count, &errors_count,
                    &pending_work, &active_workers,
                );
            }
            WorkItem::NameChunk { parent_path, names, depth } => {
                process_name_chunk(
                    id, &nfs, &parent_path, names, depth, dirs_only,
                    &local, &entry_tx, &mut batch,
                    &dirs_count, &files_count, &bytes_count, &errors_count,
                    &pending_work, &active_workers,
                );
            }
        }
    }

    // Send any remaining entries
    if !batch.is_empty() {
        let _ = entry_tx.send(batch);
    }

    debug!("Async worker {} finished", id);
}

/// Process a directory: Streaming READDIR with concurrent GETATTR dispatch
///
/// Uses streaming batched READDIR to overlap directory listing with GETATTR processing:
/// 1. Read first batch of names (READDIR_STREAM_BATCH entries)
/// 2. Dispatch batch to workers for GETATTR while reading next batch
/// 3. Repeat until EOF
///
/// This overlaps READDIR I/O with GETATTR processing for better throughput on large directories.
fn process_directory(
    id: usize,
    nfs: &NfsConnection,
    path: &str,
    depth: u32,
    max_depth: Option<usize>,
    dirs_only: bool,
    local: &DequeWorker<WorkItem>,
    injector: &Arc<Injector<WorkItem>>,
    entry_tx: &Sender<Vec<DbEntry>>,
    batch: &mut Vec<DbEntry>,
    dirs_count: &Arc<AtomicU64>,
    files_count: &Arc<AtomicU64>,
    bytes_count: &Arc<AtomicU64>,
    errors_count: &Arc<AtomicU64>,
    pending_work: &Arc<AtomicU64>,
    active_workers: &Arc<AtomicUsize>,
) {
    // Check depth limit
    if let Some(max) = max_depth {
        if depth > max as u32 {
            pending_work.fetch_sub(1, Ordering::SeqCst);
            active_workers.fetch_sub(1, Ordering::Relaxed);
            return;
        }
    }

    info!("Worker {} processing dir (streaming): {}", id, path);

    let readdir_start = Instant::now();
    let mut cookie: u64 = 0;
    let mut cookieverf: u64 = 0;
    let mut total_names: usize = 0;
    let mut batch_num: usize = 0;
    let mut first_chunk_for_self: Option<Vec<String>> = None;

    // Streaming READDIR loop - dispatch batches as they arrive
    loop {
        info!("Worker {} READDIR RPC: path={} cookie={} verf={} max={}",
            id, path, cookie, cookieverf, READDIR_STREAM_BATCH);

        // Read a batch of names
        let dir_handle = match nfs.opendir_names_only_at_cookie(
            path,
            cookie,
            cookieverf,
            READDIR_STREAM_BATCH,
        ) {
            Ok(h) => h,
            Err(e) => {
                errors_count.fetch_add(1, Ordering::Relaxed);
                if e.to_string().contains("not found") || e.to_string().contains("No such file") {
                    debug!("Worker {} READDIR not found: {}", id, path);
                } else {
                    warn!("Worker {} READDIR failed: {} -> {}", id, path, e);
                }
                pending_work.fetch_sub(1, Ordering::SeqCst);
                active_workers.fetch_sub(1, Ordering::Relaxed);
                return;
            }
        };

        // Collect names from this batch
        let mut names = Vec::new();
        while let Some(entry) = dir_handle.readdir() {
            if !entry.is_special() {
                names.push(entry.name);
            }
        }

        // Get continuation cookie, verifier, and EOF status for next iteration
        // IMPORTANT: Use get_continuation_cookie(), NOT the last-iterated entry's cookie!
        // The entries list is in reverse order, so the last-iterated entry has the
        // SMALLEST cookie. Using it would cause re-reads of the same entries.
        cookie = dir_handle.get_continuation_cookie();
        cookieverf = dir_handle.get_cookieverf();
        let libnfs_eof = dir_handle.is_eof();

        let batch_size = names.len();
        total_names += batch_size;
        batch_num += 1;

        // EOF detection: libnfs flag OR we got fewer entries than requested
        // (if server had more, it would return at least max_entries)
        let is_eof = libnfs_eof || (batch_size < READDIR_STREAM_BATCH as usize);

        // Debug: log each batch's EOF status
        info!("Worker {} READDIR batch {}: {} names, total={}, cookie={}, eof={} (libnfs={}, underflow={})",
            id, batch_num, batch_size, total_names, cookie, is_eof, libnfs_eof,
            batch_size < READDIR_STREAM_BATCH as usize);

        if batch_size > 0 {
            if batch_num == 1 && is_eof {
                // Small directory - single batch, process locally
                debug!("Worker {} READDIR: {} -> {} names (single batch)", id, path, total_names);
                do_getattr_chunk(
                    id, nfs, path, names, depth, dirs_only,
                    local, entry_tx, batch,
                    &dirs_count, &files_count, &bytes_count, &errors_count,
                    &pending_work,
                );
            } else if first_chunk_for_self.is_none() {
                // First batch of a large directory - save for ourselves to process after dispatching
                first_chunk_for_self = Some(names);
            } else {
                // Subsequent batches - dispatch to other workers immediately
                pending_work.fetch_add(1, Ordering::SeqCst);
                injector.push(WorkItem::NameChunk {
                    parent_path: path.to_string(),
                    names,
                    depth,
                });
            }
        }

        if is_eof {
            break;
        }
    }

    let readdir_elapsed = readdir_start.elapsed();

    if total_names > 1000 {
        info!("Worker {} READDIR: {} -> {} names in {} batches, {:.2}s ({:.0} names/sec)",
            id, path, total_names, batch_num, readdir_elapsed.as_secs_f64(),
            total_names as f64 / readdir_elapsed.as_secs_f64());
    }

    // Process the first chunk ourselves (after all others are dispatched)
    if let Some(my_chunk) = first_chunk_for_self {
        do_getattr_chunk(
            id, nfs, path, my_chunk, depth, dirs_only,
            local, entry_tx, batch,
            &dirs_count, &files_count, &bytes_count, &errors_count,
            &pending_work,
        );
    }

    // This directory is done
    dirs_count.fetch_add(1, Ordering::Relaxed);
    pending_work.fetch_sub(1, Ordering::SeqCst);
    active_workers.fetch_sub(1, Ordering::Relaxed);
}

/// Process a chunk of names (just GETATTR, no READDIR)
fn process_name_chunk(
    id: usize,
    nfs: &NfsConnection,
    parent_path: &str,
    names: Vec<String>,
    depth: u32,
    dirs_only: bool,
    local: &DequeWorker<WorkItem>,
    entry_tx: &Sender<Vec<DbEntry>>,
    batch: &mut Vec<DbEntry>,
    _dirs_count: &Arc<AtomicU64>,
    files_count: &Arc<AtomicU64>,
    bytes_count: &Arc<AtomicU64>,
    errors_count: &Arc<AtomicU64>,
    pending_work: &Arc<AtomicU64>,
    active_workers: &Arc<AtomicUsize>,
) {
    debug!("Worker {} processing chunk: {} names in {}", id, names.len(), parent_path);

    let dummy_dirs = Arc::new(AtomicU64::new(0));
    do_getattr_chunk(
        id, nfs, parent_path, names, depth, dirs_only,
        local, entry_tx, batch,
        &dummy_dirs, // Don't count dirs for chunks
        files_count, bytes_count, errors_count,
        pending_work,
    );

    pending_work.fetch_sub(1, Ordering::SeqCst);
    active_workers.fetch_sub(1, Ordering::Relaxed);
}

/// Do pipelined GETATTR for a list of names
fn do_getattr_chunk(
    id: usize,
    nfs: &NfsConnection,
    parent_path: &str,
    names: Vec<String>,
    depth: u32,
    dirs_only: bool,
    local: &DequeWorker<WorkItem>,
    entry_tx: &Sender<Vec<DbEntry>>,
    batch: &mut Vec<DbEntry>,
    dirs_count: &Arc<AtomicU64>,
    files_count: &Arc<AtomicU64>,
    bytes_count: &Arc<AtomicU64>,
    errors_count: &Arc<AtomicU64>,
    pending_work: &Arc<AtomicU64>,
) {
    let name_count = names.len();
    let getattr_start = Instant::now();
    let raw_ctx = unsafe { nfs.raw_context() };
    let mut engine = unsafe { AsyncStatEngine::new(raw_ctx) };
    engine.add_paths(names, parent_path);

    let results = match engine.run() {
        Ok(r) => r,
        Err(e) => {
            errors_count.fetch_add(name_count as u64, Ordering::Relaxed);
            warn!("Worker {} async stat failed: {} -> {}", id, parent_path, e);
            return;
        }
    };
    let getattr_elapsed = getattr_start.elapsed();

    if name_count > 1000 {
        info!("Worker {} GETATTR: {} ({} names) in {:.2}s ({:.0} stats/sec)",
            id, parent_path, name_count, getattr_elapsed.as_secs_f64(),
            name_count as f64 / getattr_elapsed.as_secs_f64());
    }

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
            parent_path: Some(parent_path.to_string()),
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
            depth: depth + 1,
        };

        batch.push(db_entry);

        if is_dir {
            subdir_count += 1;
            pending_work.fetch_add(1, Ordering::SeqCst);
            local.push(WorkItem::Directory {
                path: result.path,
                depth: depth + 1,
            });
        } else {
            file_count += 1;
            byte_count += stat.size;
        }

        // Send batch if large enough
        if batch.len() >= 1000 {
            if entry_tx.send(std::mem::replace(batch, Vec::with_capacity(1000))).is_err() {
                return;
            }
        }
    }

    // Update global counters
    if subdir_count > 0 {
        dirs_count.fetch_add(subdir_count, Ordering::Relaxed);
    }
    files_count.fetch_add(file_count, Ordering::Relaxed);
    bytes_count.fetch_add(byte_count, Ordering::Relaxed);
    errors_count.fetch_add(error_count, Ordering::Relaxed);
}

/// Number of columns per row (excluding auto-generated id)
const DB_COLUMNS: usize = 13;
/// Max rows per multi-row INSERT (conservative: 100 * 13 = 1300 params)
/// Smaller chunks = less memory pressure from String clones
const ROWS_PER_INSERT: usize = 100;
/// Batch size before writing to DB - reduced to avoid memory pressure
const WRITER_BATCH_SIZE: usize = 10_000;

/// Writer thread
fn writer_loop(mut conn: Connection, entry_rx: Receiver<Vec<DbEntry>>) -> Connection {
    debug!("Writer thread started");

    conn.execute_batch(
        "PRAGMA synchronous = OFF;
         PRAGMA journal_mode = OFF;
         PRAGMA cache_size = -256000;
         PRAGMA temp_store = MEMORY;
         PRAGMA mmap_size = 1073741824;"
    ).expect("Failed to set bulk load pragmas");

    let mut total_written = 0u64;
    let mut pending: Vec<DbEntry> = Vec::with_capacity(WRITER_BATCH_SIZE + 10000);

    while let Ok(entries) = entry_rx.recv() {
        pending.extend(entries);

        if pending.len() >= WRITER_BATCH_SIZE {
            if let Err(e) = write_batch_multirow(&mut conn, &pending) {
                error!("Database write failed: {}", e);
            } else {
                total_written += pending.len() as u64;
            }
            pending.clear();
        }
    }

    if !pending.is_empty() {
        if let Err(e) = write_batch_multirow(&mut conn, &pending) {
            error!("Final database write failed: {}", e);
        } else {
            total_written += pending.len() as u64;
        }
    }

    conn.execute_batch("PRAGMA synchronous = NORMAL;").ok();

    debug!("Writer thread finished, wrote {} entries", total_written);
    conn
}

/// Build a multi-row INSERT statement for N rows
fn build_multirow_insert(num_rows: usize) -> String {
    let mut sql = String::with_capacity(100 + num_rows * 60);
    sql.push_str(
        "INSERT INTO entries (parent_id, name, path, entry_type, size, mtime, atime, ctime, mode, uid, gid, nlink, inode, depth) VALUES "
    );

    for i in 0..num_rows {
        if i > 0 {
            sql.push(',');
        }
        let base = i * DB_COLUMNS;
        sql.push_str(&format!(
            "(NULL,?{},?{},?{},?{},?{},?{},?{},?{},?{},?{},?{},?{},?{})",
            base + 1, base + 2, base + 3, base + 4, base + 5, base + 6, base + 7,
            base + 8, base + 9, base + 10, base + 11, base + 12, base + 13
        ));
    }
    sql
}

/// Write entries using multi-row INSERT statements (much faster than row-by-row)
/// Uses Box<dyn ToSql> to avoid cloning strings while supporting nullable fields
fn write_batch_multirow(conn: &mut Connection, entries: &[DbEntry]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let tx = conn.transaction()
        .map_err(|e| WalkerError::Database(e.into()))?;

    // Process in chunks of ROWS_PER_INSERT
    for chunk in entries.chunks(ROWS_PER_INSERT) {
        let sql = build_multirow_insert(chunk.len());

        // Build params as boxed trait objects - avoids cloning strings
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(chunk.len() * DB_COLUMNS);

        for entry in chunk {
            params.push(Box::new(entry.name.as_str()));
            params.push(Box::new(entry.path.as_str()));
            params.push(Box::new(entry.entry_type.as_db_int() as i64));
            params.push(Box::new(entry.size as i64));
            params.push(Box::new(entry.mtime.map(|v| v as i64)));
            params.push(Box::new(entry.atime.map(|v| v as i64)));
            params.push(Box::new(entry.ctime.map(|v| v as i64)));
            params.push(Box::new(entry.mode.map(|m| m as i64)));
            params.push(Box::new(entry.uid.map(|u| u as i64)));
            params.push(Box::new(entry.gid.map(|g| g as i64)));
            params.push(Box::new(entry.nlink.map(|n| n as i64)));
            params.push(Box::new(entry.inode as i64));
            params.push(Box::new(entry.depth as i64));
        }

        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter()
            .map(|p| p.as_ref())
            .collect();

        tx.execute(&sql, param_refs.as_slice())
            .map_err(|e| WalkerError::Database(e.into()))?;
    }

    tx.commit().map_err(|e| WalkerError::Database(e.into()))?;
    Ok(())
}
