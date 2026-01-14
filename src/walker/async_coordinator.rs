//! Async walk coordinator - orchestrates parallel filesystem walks using tokio
//!
//! This coordinator uses an async architecture with a connection pool for
//! better resource utilization and higher concurrency.

use crate::config::WalkConfig;
use crate::db::{UnifiedWriter, UnifiedWriterHandle};
use crate::error::{Result, WalkerError, WorkerError};
use crate::nfs::pool::NfsConnectionPool;
use crate::nfs::types::{DbEntry, DirStats, EntryType};
use crate::walker::queue::DirTask;
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, error, info, warn};

/// Statistics collected during the walk
#[derive(Debug, Default)]
pub struct AsyncWalkStats {
    pub dirs_processed: AtomicU64,
    pub files_found: AtomicU64,
    pub bytes_found: AtomicU64,
    pub errors: AtomicU64,
    pub skipped: AtomicU64,
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
}

impl AsyncWalkCoordinator {
    /// Create a new async coordinator
    pub fn new(config: WalkConfig, num_connections: usize) -> Result<Self> {
        let config = Arc::new(config);
        let pool = Arc::new(NfsConnectionPool::new(Arc::clone(&config), num_connections));
        let shutdown = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(AsyncWalkStats::default());

        Ok(Self {
            config,
            pool,
            shutdown,
            stats,
        })
    }

    /// Get shutdown flag for signal handlers
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
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

        // Create output writer (SQLite or Parquet based on config)
        let writer = UnifiedWriter::new(
            &self.config.output_path,
            self.config.batch_size,
            self.config.batch_size * 2,
            self.config.output_format,
        )?;
        let writer_handle = writer.handle();

        debug!(start_time = %start_datetime.to_rfc3339(), "Walk started");

        // Create work queue channel - larger buffer for better throughput
        let (task_tx, mut task_rx) = mpsc::channel::<DirTask>(self.config.queue_size);

        // Semaphore to limit concurrent directory processing
        let max_concurrent = self.config.worker_count.max(self.pool.max_connections() * 2);
        let semaphore = Arc::new(Semaphore::new(max_concurrent));

        // Track in-flight tasks
        let in_flight = Arc::new(AtomicU64::new(0));

        info!(max_concurrent = max_concurrent, "Starting async dispatcher");

        // Seed with root directory
        let root_path = self.config.nfs_url.walk_start_path();
        let root_entry = DbEntry::root(&root_path);
        writer_handle.send_entry(root_entry)?;

        let root_task = DirTask::new(root_path.clone(), None, 0);
        in_flight.fetch_add(1, Ordering::SeqCst);
        task_tx.send(root_task).await
            .map_err(|_| WalkerError::Worker(WorkerError::QueueSendFailed))?;

        // Main dispatch loop - receives tasks and spawns processing tasks
        while !self.shutdown.load(Ordering::Relaxed) {
            // Check if we're done (no tasks in queue and none in flight)
            let current_in_flight = in_flight.load(Ordering::SeqCst);

            // Try to receive with a timeout
            let task = match tokio::time::timeout(
                Duration::from_millis(50),
                task_rx.recv()
            ).await {
                Ok(Some(task)) => task,
                Ok(None) => break, // Channel closed
                Err(_) => {
                    // Timeout - check if we're done
                    if current_in_flight == 0 {
                        break;
                    }
                    continue;
                }
            };

            // Acquire semaphore permit to limit concurrency
            let permit = semaphore.clone().acquire_owned().await
                .expect("Semaphore closed");

            // Clone everything needed for the spawned task
            let pool = Arc::clone(&self.pool);
            let config = Arc::clone(&self.config);
            let stats = Arc::clone(&self.stats);
            let writer = writer_handle.clone();
            let tx = task_tx.clone();
            let in_flight_clone = Arc::clone(&in_flight);

            // Spawn task to process this directory
            tokio::spawn(async move {
                let result = process_directory_task(&pool, &config, &stats, &writer, task).await;

                match result {
                    Ok(new_tasks) => {
                        // Queue new directory tasks
                        let new_count = new_tasks.len() as u64;
                        if new_count > 0 {
                            in_flight_clone.fetch_add(new_count, Ordering::SeqCst);
                        }

                        for new_task in new_tasks {
                            if tx.send(new_task).await.is_err() {
                                // Channel closed, decrement
                                in_flight_clone.fetch_sub(1, Ordering::SeqCst);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Task failed");
                    }
                }

                // Mark this task as complete
                in_flight_clone.fetch_sub(1, Ordering::SeqCst);

                // Drop permit to release semaphore slot
                drop(permit);
            });
        }

        // Wait for all in-flight tasks to complete
        debug!("Waiting for in-flight tasks to complete");
        while in_flight.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait for semaphore to be fully available (all tasks done)
        let _ = semaphore.acquire_many(max_concurrent as u32).await;

        // Get final stats
        let dirs = self.stats.dirs_processed.load(Ordering::Relaxed);
        let files = self.stats.files_found.load(Ordering::Relaxed);
        let bytes = self.stats.bytes_found.load(Ordering::Relaxed);
        let errors = self.stats.errors.load(Ordering::Relaxed);
        let skipped = self.stats.skipped.load(Ordering::Relaxed);
        let entries_written = writer_handle.entries_written();

        // Finish database
        drop(writer_handle);
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

/// Process a single directory task
async fn process_directory_task(
    pool: &NfsConnectionPool,
    config: &WalkConfig,
    stats: &AsyncWalkStats,
    writer: &UnifiedWriterHandle,
    task: DirTask,
) -> Result<Vec<DirTask>> {
    // Check depth limit
    if let Some(max_depth) = config.max_depth {
        if task.depth as usize > max_depth {
            stats.record_skip();
            return Ok(vec![]);
        }
    }

    // Check exclusions
    if config.is_excluded(&task.path) {
        stats.record_skip();
        return Ok(vec![]);
    }

    // Acquire a connection from the pool
    let pooled_conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            stats.record_error();
            return Err(WalkerError::Nfs(e));
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
            if e.is_recoverable() {
                stats.record_skip();
                return Ok(vec![]);
            }
            return Err(WalkerError::Nfs(e));
        }
    };

    stats.record_dir();

    let mut dir_stats = DirStats::default();
    let mut new_tasks = Vec::new();

    for entry in entries {
        if entry.is_special() {
            continue;
        }

        // Create database entry
        let db_entry = DbEntry::from_nfs_entry(&entry, &task.path, task.depth + 1);

        // Track directory stats
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
                error!(error = %e, "Failed to send entry to writer");
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

    Ok(new_tasks)
}
