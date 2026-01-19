//! Fast NFS Walker - Parallel READDIRPLUS
//!
//! A high-performance implementation that:
//! 1. Uses READDIRPLUS to get names AND attributes in one RPC call
//! 2. All workers read directories in parallel (no single coordinator)
//! 3. Dedicated writer thread handles all DB writes (no mutex contention)
//!
//! Architecture:
//! ```text
//! Directory Queue (crossbeam deque - work stealing)
//! │
//! ├── Worker 0: pop dir → READDIRPLUS → send entries → push subdirs
//! ├── Worker 1: pop dir → READDIRPLUS → send entries → push subdirs
//! └── Worker N: pop dir → READDIRPLUS → send entries → push subdirs
//! │
//! └── Writer Thread: recv entries → batch insert to SQLite
//! ```

use crate::config::WalkConfig;
use crate::db::schema::{create_database, create_indexes, keys, optimize_for_reads, set_walk_info};
use crate::error::{Result, WalkerError};
use crate::nfs::{resolve_dns, NfsConnection, NfsConnectionBuilder};
use crate::nfs::types::{DbEntry, EntryType};
use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Injector, Stealer, Worker as DequeWorker};
use rusqlite::{params, Connection};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

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

/// Fast parallel walker using READDIRPLUS
pub struct SimpleWalker {
    config: WalkConfig,
    shutdown: Arc<AtomicBool>,
    dirs_count: Arc<AtomicU64>,
    files_count: Arc<AtomicU64>,
    bytes_count: Arc<AtomicU64>,
    errors_count: Arc<AtomicU64>,
}

impl SimpleWalker {
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

        // Open SQLite database
        info!("Opening database: {}", self.config.output_path.display());
        let db = self.open_database()?;

        // Channel for entries to write (workers -> writer)
        let (entry_tx, entry_rx) = bounded::<Vec<DbEntry>>(100);

        // Spawn dedicated writer thread
        let writer_handle = self.spawn_writer(db, entry_rx);

        // Work-stealing deque for directories
        let injector: Arc<Injector<DirWork>> = Arc::new(Injector::new());

        // Track active workers and pending work
        let active_workers = Arc::new(AtomicUsize::new(0));
        let pending_work = Arc::new(AtomicU64::new(1)); // Start with 1 for root

        // Push root directory
        let start_path = self.config.nfs_url.walk_start_path();
        injector.push(DirWork {
            path: start_path.clone(),
            depth: 0,
        });

        // Create worker local queues and stealers
        let mut workers_local: Vec<DequeWorker<DirWork>> = Vec::new();
        let mut stealers: Vec<Stealer<DirWork>> = Vec::new();

        for _ in 0..self.config.worker_count {
            let w = DequeWorker::new_fifo();
            stealers.push(w.stealer());
            workers_local.push(w);
        }

        let stealers = Arc::new(stealers);

        // Resolve DNS to get all server IPs for round-robin load balancing
        let server_ips = resolve_dns(&self.config.nfs_url.server);
        if server_ips.len() > 1 {
            info!("DNS resolved {} to {} IPs: {:?}",
                  self.config.nfs_url.server, server_ips.len(), server_ips);
        }

        // Spawn workers
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for (id, local) in workers_local.into_iter().enumerate() {
            // Round-robin across server IPs
            let ip = if !server_ips.is_empty() {
                Some(server_ips[id % server_ips.len()].clone())
            } else {
                None
            };
            let nfs = self.create_connection_with_ip(ip.as_deref())?;
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
            let worker_count = self.config.worker_count;

            let handle = thread::Builder::new()
                .name(format!("walker-{}", id))
                .spawn(move || {
                    worker_loop(
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
                        worker_count,
                    );
                })
                .expect("Failed to spawn worker thread");

            handles.push(handle);
        }

        // Drop our sender so writer knows when to stop
        drop(entry_tx);

        // Wait for workers
        for handle in handles {
            let _ = handle.join();
        }

        // Wait for writer to finish
        let db = writer_handle.join().expect("Writer thread panicked");

        // Finalize database
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
        self.create_connection_with_ip(None)
    }

    fn create_connection_with_ip(&self, ip: Option<&str>) -> Result<NfsConnection> {
        let timeout = Duration::from_secs(self.config.timeout_secs as u64);
        let mut builder = NfsConnectionBuilder::new(self.config.nfs_url.clone())
            .timeout(timeout)
            .retries(self.config.retry_count);

        if let Some(ip) = ip {
            builder = builder.with_ip(ip.to_string());
        }

        builder.connect().map_err(|e| WalkerError::Nfs(e))
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

/// Worker thread - processes directories using READDIRPLUS
fn worker_loop(
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
    worker_count: usize,
) {
    debug!("Worker {} started", id);

    let mut batch: Vec<DbEntry> = Vec::with_capacity(1000);
    let mut idle_spins = 0;
    const MAX_IDLE_SPINS: u32 = 1000;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Try to get work: local queue first, then injector, then steal
        let work = local.pop().or_else(|| {
            // Try injector
            loop {
                match injector.steal() {
                    crossbeam_deque::Steal::Success(w) => return Some(w),
                    crossbeam_deque::Steal::Empty => break,
                    crossbeam_deque::Steal::Retry => continue,
                }
            }
            // Try stealing from other workers
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
                // No work found - check if we should exit
                idle_spins += 1;

                if pending_work.load(Ordering::SeqCst) == 0
                    && active_workers.load(Ordering::SeqCst) == 0
                {
                    // No pending work and no active workers - we're done
                    break;
                }

                if idle_spins > MAX_IDLE_SPINS {
                    // Yield to avoid busy spinning
                    thread::sleep(Duration::from_micros(100));
                    idle_spins = 0;
                }
                continue;
            }
        };

        // Check max depth
        if let Some(max) = max_depth {
            if work.depth > max as u32 {
                pending_work.fetch_sub(1, Ordering::SeqCst);
                active_workers.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
        }

        debug!("Worker {} READDIRPLUS: {}", id, work.path);

        // Read directory with READDIRPLUS (gets attributes in same RPC!)
        match nfs.readdir_plus(&work.path) {
            Ok(entries) => {
                let entry_count = entries.len();
                let mut subdir_count = 0;
                let mut file_count = 0;
                let mut byte_count = 0u64;

                for nfs_entry in entries {
                    // Skip . and ..
                    if nfs_entry.name == "." || nfs_entry.name == ".." {
                        continue;
                    }

                    let full_path = if work.path == "/" {
                        format!("/{}", nfs_entry.name)
                    } else {
                        format!("{}/{}", work.path, nfs_entry.name)
                    };

                    let is_dir = nfs_entry.entry_type == EntryType::Directory;

                    // Skip files if dirs_only mode
                    if dirs_only && !is_dir {
                        continue;
                    }

                    // Create DB entry from READDIRPLUS attributes
                    let db_entry = DbEntry {
                        parent_path: Some(work.path.clone()),
                        name: nfs_entry.name.clone(),
                        path: full_path.clone(),
                        entry_type: nfs_entry.entry_type,
                        size: nfs_entry.size(),
                        mtime: nfs_entry.mtime(),
                        atime: nfs_entry.atime(),
                        ctime: nfs_entry.ctime(),
                        mode: nfs_entry.mode(),
                        uid: nfs_entry.uid(),
                        gid: nfs_entry.gid(),
                        nlink: nfs_entry.nlink(),
                        inode: nfs_entry.inode,
                        depth: work.depth + 1,
                    };

                    batch.push(db_entry);

                    if is_dir {
                        // Queue subdirectory for processing
                        subdir_count += 1;
                        pending_work.fetch_add(1, Ordering::SeqCst);
                        local.push(DirWork {
                            path: full_path,
                            depth: work.depth + 1,
                        });
                    } else {
                        file_count += 1;
                        byte_count += nfs_entry.size();
                    }

                    // Send batch if full
                    if batch.len() >= 1000 {
                        if entry_tx.send(std::mem::replace(&mut batch, Vec::with_capacity(1000))).is_err() {
                            break;
                        }
                    }
                }

                dirs_count.fetch_add(1, Ordering::Relaxed);
                files_count.fetch_add(file_count, Ordering::Relaxed);
                bytes_count.fetch_add(byte_count, Ordering::Relaxed);

                debug!(
                    "Worker {} READDIRPLUS complete: {} -> {} entries ({} dirs, {} files)",
                    id, work.path, entry_count, subdir_count, file_count
                );
            }
            Err(e) => {
                errors_count.fetch_add(1, Ordering::Relaxed);
                // Not found errors are common on active filesystems (race condition)
                // Log them at debug level to reduce noise
                if e.to_string().contains("not found") || e.to_string().contains("No such file") {
                    debug!("Worker {} READDIRPLUS not found: {}", id, work.path);
                } else {
                    warn!("Worker {} READDIRPLUS failed: {} -> {}", id, work.path, e);
                }
            }
        }

        // Mark this work item as done
        pending_work.fetch_sub(1, Ordering::SeqCst);
        active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    // Send remaining batch
    if !batch.is_empty() {
        let _ = entry_tx.send(batch);
    }

    debug!("Worker {} finished", id);
}

/// Writer thread - handles all database writes with optimized bulk loading
fn writer_loop(mut conn: Connection, entry_rx: Receiver<Vec<DbEntry>>) -> Connection {
    debug!("Writer thread started");

    // Optimize for bulk loading - these settings dramatically improve write performance
    conn.execute_batch(
        "PRAGMA synchronous = OFF;
         PRAGMA journal_mode = OFF;
         PRAGMA cache_size = -64000;
         PRAGMA temp_store = MEMORY;"
    ).expect("Failed to set bulk load pragmas");

    let mut total_written = 0u64;
    let mut pending: Vec<DbEntry> = Vec::with_capacity(20000);
    let batch_size = 10000;

    // Receive batches and write to DB
    while let Ok(entries) = entry_rx.recv() {
        pending.extend(entries);

        // Write when we have enough
        if pending.len() >= batch_size {
            if let Err(e) = write_batch_fast(&mut conn, &pending) {
                error!("Database write failed: {}", e);
            } else {
                total_written += pending.len() as u64;
            }
            pending.clear();
        }
    }

    // Write remaining entries
    if !pending.is_empty() {
        if let Err(e) = write_batch_fast(&mut conn, &pending) {
            error!("Final database write failed: {}", e);
        } else {
            total_written += pending.len() as u64;
        }
    }

    // Re-enable safety for final operations
    conn.execute_batch("PRAGMA synchronous = NORMAL;").ok();

    debug!("Writer thread finished, wrote {} entries", total_written);
    conn
}

/// Write a batch of entries to the database using prepared statement
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_walk_stats_default() {
        let stats = WalkStats::default();
        assert_eq!(stats.dirs, 0);
        assert_eq!(stats.files, 0);
        assert!(!stats.completed);
    }

    #[test]
    fn test_walk_progress_rate() {
        let mut progress = WalkProgress::default();
        progress.files = 1000;
        progress.dirs = 100;
        progress.elapsed = Duration::from_secs(10);
        assert!((progress.files_per_second() - 110.0).abs() < 0.1);
    }
}
