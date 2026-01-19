//! Async walk coordinator - orchestrates parallel filesystem walks using tokio
//!
//! This coordinator uses a simple worker-based model (like the sync version):
//! - Fixed number of worker tasks pull from a shared channel
//! - Workers process directories and push results back
//! - No complex semaphore juggling or per-directory task spawning
//!
//! Termination: Walk is complete when all workers are idle and channel is empty.

use crate::config::WalkConfig;
use crate::db::{UnifiedWriter, UnifiedWriterHandle};
use crate::error::{Result, WalkerError};
use crate::nfs::pool::NfsConnectionPool;
use crate::nfs::types::{DbEntry, DirStats, EntryType};
use crate::nfs::DnsResolver;
use crate::walker::queue::DirTask;
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Statistics collected during the walk
#[derive(Debug, Default)]
pub struct AsyncWalkStats {
    pub dirs_processed: AtomicU64,
    pub files_found: AtomicU64,
    pub bytes_found: AtomicU64,
    pub errors: AtomicU64,
    pub skipped: AtomicU64,
    pub entries_queued: AtomicU64,
}

impl AsyncWalkStats {
    pub fn record_dir(&self) {
        self.dirs_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_files(&self, count: u64) {
        self.files_found.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_found.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_skip(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_queued(&self, count: u64) {
        self.entries_queued.fetch_add(count, Ordering::Relaxed);
    }
}

/// Result of a completed async walk
#[derive(Debug)]
pub struct AsyncWalkResult {
    pub total_dirs: u64,
    pub total_files: u64,
    pub total_bytes: u64,
    pub entries_written: u64,
    pub errors: u64,
    pub skipped: u64,
    pub duration: Duration,
    pub completed: bool,
}

/// Async walk coordinator using tokio
pub struct AsyncWalkCoordinator {
    config: Arc<WalkConfig>,
    pool: Arc<NfsConnectionPool>,
    shutdown: Arc<AtomicBool>,
    stats: Arc<AsyncWalkStats>,
    #[allow(dead_code)]
    dns_resolver: Arc<DnsResolver>,
}

impl AsyncWalkCoordinator {
    /// Create a new async coordinator
    pub fn new(config: WalkConfig, num_connections: usize) -> Result<Self> {
        let config = Arc::new(config);

        let dns_resolver = DnsResolver::new(
            &config.nfs_url.server,
            config.dns_refresh_secs,
            !config.disable_dns_lb,
        );

        let pool = Arc::new(NfsConnectionPool::with_dns_resolver(
            Arc::clone(&config),
            num_connections,
            Arc::clone(&dns_resolver),
        ));

        let shutdown = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(AsyncWalkStats::default());

        Ok(Self {
            config,
            pool,
            shutdown,
            stats,
            dns_resolver,
        })
    }

    /// Get shutdown flag for signal handlers
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Get a reference to the stats for external progress reporting
    pub fn stats(&self) -> &Arc<AsyncWalkStats> {
        &self.stats
    }

    /// Run the async filesystem walk
    pub async fn run(self) -> Result<AsyncWalkResult> {
        let start_time = Instant::now();
        let start_datetime: DateTime<Utc> = Utc::now();

        info!(
            url = %self.config.nfs_url.to_display_string(),
            connections = self.pool.max_connections(),
            "Starting async filesystem walk"
        );

        // Create output writer
        let writer = UnifiedWriter::new(
            &self.config.output_path,
            self.config.batch_size,
            self.config.batch_size * 2,
            self.config.output_format,
        )?;
        let writer_handle = writer.handle();

        debug!(start_time = %start_datetime.to_rfc3339(), "Walk started");

        // Work queue - unbounded sender, bounded receiver isn't needed since workers self-limit
        let (task_tx, task_rx) = async_channel::unbounded::<DirTask>();

        // Track active workers (workers currently processing, not idle)
        let active_workers = Arc::new(AtomicU64::new(0));

        // Track pending work (directories queued but not yet processed)
        let pending_count = Arc::new(AtomicU64::new(0));

        // Number of workers = number of connections (each worker needs a connection)
        let num_workers = self.pool.max_connections();

        info!(num_workers = num_workers, "Starting async workers");

        // Seed with root directory
        let root_path = self.config.nfs_url.walk_start_path();
        let root_entry = DbEntry::root(&root_path);
        writer_handle.send_entry(root_entry)?;
        self.stats.record_queued(1);

        let root_task = DirTask::new(root_path.clone(), None, 0);
        pending_count.fetch_add(1, Ordering::SeqCst);
        task_tx.send(root_task).await
            .map_err(|_| WalkerError::ChannelClosed)?;

        // Spawn worker tasks
        let mut worker_handles = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let pool = Arc::clone(&self.pool);
            let config = Arc::clone(&self.config);
            let stats = Arc::clone(&self.stats);
            let writer = writer_handle.clone();
            let shutdown = Arc::clone(&self.shutdown);
            let tx = task_tx.clone();
            let rx = task_rx.clone();
            let active = Arc::clone(&active_workers);
            let pending = Arc::clone(&pending_count);

            let handle = tokio::spawn(async move {
                worker_loop(
                    worker_id,
                    pool,
                    config,
                    stats,
                    writer,
                    shutdown,
                    tx,
                    rx,
                    active,
                    pending,
                ).await
            });

            worker_handles.push(handle);
        }

        // Drop our sender so workers can detect completion
        drop(task_tx);

        // Wait for all workers to complete
        for handle in worker_handles {
            let _ = handle.await;
        }

        info!("All workers completed");

        // Get final stats
        let dirs = self.stats.dirs_processed.load(Ordering::Relaxed);
        let files = self.stats.files_found.load(Ordering::Relaxed);
        let bytes = self.stats.bytes_found.load(Ordering::Relaxed);
        let errors = self.stats.errors.load(Ordering::Relaxed);
        let skipped = self.stats.skipped.load(Ordering::Relaxed);
        let entries_written = writer_handle.entries_written();

        // Finalize writer
        drop(writer_handle);
        info!("Finalizing database");
        writer.finish()?;

        let duration = start_time.elapsed();

        info!(
            dirs = dirs,
            files = files,
            bytes = bytes,
            errors = errors,
            duration_secs = duration.as_secs(),
            "Async walk completed"
        );

        Ok(AsyncWalkResult {
            total_dirs: dirs,
            total_files: files,
            total_bytes: bytes,
            entries_written,
            errors,
            skipped,
            duration,
            completed: !self.shutdown.load(Ordering::Relaxed),
        })
    }
}

/// Worker loop - pulls tasks from channel, processes them, pushes results back
async fn worker_loop(
    worker_id: usize,
    pool: Arc<NfsConnectionPool>,
    config: Arc<WalkConfig>,
    stats: Arc<AsyncWalkStats>,
    writer: UnifiedWriterHandle,
    shutdown: Arc<AtomicBool>,
    tx: async_channel::Sender<DirTask>,
    rx: async_channel::Receiver<DirTask>,
    active_workers: Arc<AtomicU64>,
    pending_count: Arc<AtomicU64>,
) {
    debug!(worker_id = worker_id, "Worker starting");

    loop {
        // Check shutdown
        if shutdown.load(Ordering::Relaxed) {
            debug!(worker_id = worker_id, "Worker shutting down");
            break;
        }

        // Try to receive a task with timeout
        let task = match tokio::time::timeout(
            Duration::from_millis(100),
            rx.recv()
        ).await {
            Ok(Ok(task)) => task,
            Ok(Err(_)) => {
                // Channel closed - check if we should exit
                if pending_count.load(Ordering::SeqCst) == 0 {
                    debug!(worker_id = worker_id, "Channel closed, no pending work, exiting");
                    break;
                }
                continue;
            }
            Err(_) => {
                // Timeout - check if all work is done
                let pending = pending_count.load(Ordering::SeqCst);
                let active = active_workers.load(Ordering::SeqCst);
                if pending == 0 && active == 0 {
                    debug!(worker_id = worker_id, "No pending work and no active workers, exiting");
                    break;
                }
                continue;
            }
        };

        // Mark as active
        active_workers.fetch_add(1, Ordering::SeqCst);

        // Process the directory
        let new_tasks = process_directory(
            &pool,
            &config,
            &stats,
            &writer,
            task,
            worker_id,
        ).await;

        // Update pending count: -1 for completed, +N for new directories
        let new_count = new_tasks.len();
        if new_count > 0 {
            pending_count.fetch_add(new_count as u64, Ordering::SeqCst);
        }
        pending_count.fetch_sub(1, Ordering::SeqCst);

        // Queue new tasks
        for new_task in new_tasks {
            if tx.send(new_task).await.is_err() {
                // Channel closed
                pending_count.fetch_sub(1, Ordering::SeqCst);
            }
        }

        // Mark as inactive
        active_workers.fetch_sub(1, Ordering::SeqCst);
    }

    debug!(worker_id = worker_id, "Worker exited");
}

/// Process a single directory
async fn process_directory(
    pool: &NfsConnectionPool,
    config: &WalkConfig,
    stats: &AsyncWalkStats,
    writer: &UnifiedWriterHandle,
    task: DirTask,
    worker_id: usize,
) -> Vec<DirTask> {
    // Check depth limit
    if let Some(max_depth) = config.max_depth {
        if task.depth as usize > max_depth {
            stats.record_skip();
            return vec![];
        }
    }

    // Check exclusions
    if config.is_excluded(&task.path) {
        stats.record_skip();
        return vec![];
    }

    // Acquire a connection from the pool
    let pooled_conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            stats.record_error();
            warn!(worker_id = worker_id, error = %e, path = %task.path, "Failed to acquire connection");
            return vec![];
        }
    };

    // Read directory in blocking task
    let path = task.path.clone();
    let conn = pooled_conn.take();

    let (conn, entries_result) = tokio::task::spawn_blocking(move || {
        let result = conn.readdir_plus(&path);
        (conn, result)
    }).await.expect("Blocking task panicked");

    // Return connection to pool
    pool.return_connection(conn).await;

    // Handle result
    let entries = match entries_result {
        Ok(entries) => entries,
        Err(e) => {
            stats.record_error();
            if !e.is_recoverable() {
                warn!(worker_id = worker_id, error = %e, path = %task.path, "Directory read failed");
            }
            return vec![];
        }
    };

    stats.record_dir();

    let mut dir_stats = DirStats::default();
    let mut new_tasks = Vec::new();

    for entry in entries {
        if entry.is_special() {
            continue;
        }

        let db_entry = DbEntry::from_nfs_entry(&entry, &task.path, task.depth + 1);
        dir_stats.add_entry(&entry);

        // Queue subdirectories
        if entry.entry_type == EntryType::Directory {
            let should_queue = config
                .max_depth
                .map(|max| (task.depth + 1) as usize <= max)
                .unwrap_or(true);

            if should_queue && !config.is_excluded(&db_entry.path) {
                let subtask = DirTask::new(db_entry.path.clone(), None, task.depth + 1);
                new_tasks.push(subtask);
            }
        }

        // Send to writer
        if !config.dirs_only || entry.entry_type == EntryType::Directory {
            if let Err(e) = writer.send_entry(db_entry) {
                debug!(error = %e, "Failed to send entry to writer");
            } else {
                stats.record_queued(1);
            }
        }
    }

    // Record stats
    stats.record_files(dir_stats.file_count);
    stats.record_bytes(dir_stats.total_bytes);

    // Send directory stats
    if let Err(e) = writer.send_dir_stats(task.path.clone(), dir_stats) {
        debug!(error = %e, "Failed to send dir stats");
    }

    new_tasks
}
