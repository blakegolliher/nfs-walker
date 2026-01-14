//! Worker thread logic for parallel directory walking
//!
//! Each worker:
//! - Has its own NFS connection (libnfs is not thread-safe)
//! - Pulls directory tasks from the work queue
//! - Reads directory contents using READDIRPLUS
//! - Sends entries to the database writer
//! - Pushes subdirectories back to the work queue

use crate::config::WalkConfig;
use crate::db::WriterHandle;
use crate::error::{WalkOutcome, WorkerError};
use crate::nfs::types::{DbEntry, DirStats, EntryType};
use crate::nfs::{NfsConnection, NfsConnectionBuilder};
use crate::walker::queue::{DirTask, WorkGuard, WorkQueueReceiver, WorkQueueSender};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

/// Statistics collected by a worker
#[derive(Debug, Default)]
pub struct WorkerStats {
    /// Directories processed
    pub dirs_processed: AtomicU64,

    /// Files found
    pub files_found: AtomicU64,

    /// Bytes found (sum of file sizes)
    pub bytes_found: AtomicU64,

    /// Errors encountered
    pub errors: AtomicU64,

    /// Directories skipped (permission denied, etc.)
    pub skipped: AtomicU64,
}

impl WorkerStats {
    fn record_dir(&self) {
        self.dirs_processed.fetch_add(1, Ordering::Relaxed);
    }

    fn record_files(&self, count: u64) {
        self.files_found.fetch_add(count, Ordering::Relaxed);
    }

    fn record_bytes(&self, bytes: u64) {
        self.bytes_found.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_skip(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
    }
}

/// A worker thread that processes directory tasks
pub struct Worker {
    /// Worker ID
    id: usize,

    /// Thread handle
    handle: Option<JoinHandle<Result<(), WorkerError>>>,

    /// Worker statistics
    stats: Arc<WorkerStats>,
}

impl Worker {
    /// Spawn a new worker thread
    pub fn spawn(
        id: usize,
        config: Arc<WalkConfig>,
        queue_rx: WorkQueueReceiver,
        queue_tx: WorkQueueSender,
        writer: WriterHandle,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, WorkerError> {
        let stats = Arc::new(WorkerStats::default());
        let stats_clone = Arc::clone(&stats);

        let handle = thread::Builder::new()
            .name(format!("walker-{}", id))
            .spawn(move || {
                worker_loop(id, config, queue_rx, queue_tx, writer, shutdown, stats_clone)
            })
            .map_err(|e| WorkerError::InitFailed {
                id,
                reason: e.to_string(),
            })?;

        Ok(Self {
            id,
            handle: Some(handle),
            stats,
        })
    }

    /// Get worker ID
    pub fn id(&self) -> usize {
        self.id
    }

    /// Get worker statistics
    pub fn stats(&self) -> &WorkerStats {
        &self.stats
    }

    /// Wait for the worker to finish
    pub fn join(mut self) -> Result<(), WorkerError> {
        if let Some(handle) = self.handle.take() {
            match handle.join() {
                Ok(result) => result,
                Err(_) => Err(WorkerError::Panicked {
                    id: self.id,
                    message: "Worker thread panicked".into(),
                }),
            }
        } else {
            Ok(())
        }
    }
}

/// Main worker loop
fn worker_loop(
    id: usize,
    config: Arc<WalkConfig>,
    queue_rx: WorkQueueReceiver,
    queue_tx: WorkQueueSender,
    writer: WriterHandle,
    shutdown: Arc<AtomicBool>,
    stats: Arc<WorkerStats>,
) -> Result<(), WorkerError> {
    info!(worker = id, "Worker starting");

    // Create NFS connection for this worker
    let mut nfs = match NfsConnectionBuilder::new(config.nfs_url.clone())
        .timeout(Duration::from_secs(config.timeout_secs as u64))
        .retries(config.retry_count)
        .connect()
    {
        Ok(conn) => conn,
        Err(e) => {
            error!(worker = id, error = %e, "Failed to connect to NFS");
            return Err(WorkerError::InitFailed {
                id,
                reason: e.to_string(),
            });
        }
    };

    info!(
        worker = id,
        server = nfs.server(),
        export = nfs.export(),
        "NFS connection established"
    );

    // Process tasks until shutdown
    while !shutdown.load(Ordering::Relaxed) {
        // Try to get a task with timeout
        let task = match queue_rx.recv_timeout(Duration::from_millis(100)) {
            Some(task) => task,
            None => continue, // Timeout - check shutdown and retry
        };

        // Mark as actively working
        let _guard = WorkGuard::new(&queue_rx);

        // Process the directory
        let outcome = process_directory(
            id,
            &task,
            &config,
            &mut nfs,
            &queue_tx,
            &writer,
            &stats,
        );

        match &outcome {
            WalkOutcome::Success { entries, .. } => {
                trace!(worker = id, path = %task.path, entries = entries, "Directory processed");
            }
            WalkOutcome::Skipped { path, reason } => {
                debug!(worker = id, path = %path, reason = %reason, "Directory skipped");
            }
            WalkOutcome::Failed { path, error } => {
                warn!(worker = id, path = %path, error = %error, "Directory failed");
            }
        }
    }

    info!(
        worker = id,
        dirs = stats.dirs_processed.load(Ordering::Relaxed),
        files = stats.files_found.load(Ordering::Relaxed),
        "Worker shutting down"
    );

    Ok(())
}

/// Process a single directory
fn process_directory(
    worker_id: usize,
    task: &DirTask,
    config: &WalkConfig,
    nfs: &mut NfsConnection,
    queue_tx: &WorkQueueSender,
    writer: &WriterHandle,
    stats: &WorkerStats,
) -> WalkOutcome {
    // Check depth limit
    if let Some(max_depth) = config.max_depth {
        if task.depth as usize > max_depth {
            stats.record_skip();
            return WalkOutcome::Skipped {
                path: task.path.clone(),
                reason: format!("Exceeded max depth {}", max_depth),
            };
        }
    }

    // Check exclusions
    if config.is_excluded(&task.path) {
        stats.record_skip();
        return WalkOutcome::Skipped {
            path: task.path.clone(),
            reason: "Matched exclusion pattern".into(),
        };
    }

    // Read directory contents
    let entries = match nfs.readdir_plus(&task.path) {
        Ok(entries) => entries,
        Err(e) => {
            stats.record_error();

            // Handle recoverable errors
            if e.is_recoverable() {
                stats.record_skip();
                return WalkOutcome::Skipped {
                    path: task.path.clone(),
                    reason: e.to_string(),
                };
            }

            return WalkOutcome::Failed {
                path: task.path.clone(),
                error: e,
            };
        }
    };

    stats.record_dir();

    let mut dir_stats = DirStats::default();
    let mut entry_count = 0;
    let mut subdir_count = 0;

    for entry in entries {
        // Skip . and ..
        if entry.is_special() {
            continue;
        }

        entry_count += 1;

        // Create database entry
        let db_entry = DbEntry::from_nfs_entry(
            &entry,
            &task.path,
            task.depth + 1,
        );

        // Track directory stats
        dir_stats.add_entry(&entry);

        // If this is a directory, queue it for processing
        if entry.entry_type == EntryType::Directory {
            subdir_count += 1;

            // Check depth before queueing
            let should_queue = config
                .max_depth
                .map(|max| (task.depth + 1) as usize <= max)
                .unwrap_or(true);

            if should_queue && !config.is_excluded(&db_entry.path) {
                let subtask = DirTask::new(
                    db_entry.path.clone(),
                    None, // We don't have the ID yet - will be assigned by writer
                    task.depth + 1,
                );

                // Try to queue - if backpressure, we'll process inline later
                if !queue_tx.try_send(subtask.clone()).unwrap_or(false) {
                    // Backpressure - log but don't process inline here
                    // The coordinator will handle requeuing
                    queue_tx.record_inline();
                    trace!(
                        worker = worker_id,
                        path = %subtask.path,
                        "Backpressure - task will be retried"
                    );
                }
            }
        }

        // Send to writer (unless dirs_only and this is a file)
        if !config.dirs_only || entry.entry_type == EntryType::Directory {
            if let Err(e) = writer.send_entry(db_entry) {
                error!(worker = worker_id, error = %e, "Failed to send entry to writer");
            }
        }
    }

    // Record file stats
    stats.record_files(dir_stats.file_count);
    stats.record_bytes(dir_stats.total_bytes);

    // Send directory stats to writer (using path for lookup)
    if let Err(e) = writer.send_dir_stats(task.path.clone(), dir_stats) {
        debug!(worker = worker_id, error = %e, "Failed to send dir stats");
    }

    WalkOutcome::Success {
        path: task.path.clone(),
        entries: entry_count,
        subdirs: subdir_count,
    }
}

/// Aggregate statistics from multiple workers
pub fn aggregate_stats(workers: &[Worker]) -> (u64, u64, u64, u64, u64) {
    let mut dirs = 0u64;
    let mut files = 0u64;
    let mut bytes = 0u64;
    let mut errors = 0u64;
    let mut skipped = 0u64;

    for worker in workers {
        dirs += worker.stats.dirs_processed.load(Ordering::Relaxed);
        files += worker.stats.files_found.load(Ordering::Relaxed);
        bytes += worker.stats.bytes_found.load(Ordering::Relaxed);
        errors += worker.stats.errors.load(Ordering::Relaxed);
        skipped += worker.stats.skipped.load(Ordering::Relaxed);
    }

    (dirs, files, bytes, errors, skipped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_stats() {
        let stats = WorkerStats::default();

        stats.record_dir();
        stats.record_files(10);
        stats.record_bytes(1024);
        stats.record_error();
        stats.record_skip();

        assert_eq!(stats.dirs_processed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.files_found.load(Ordering::Relaxed), 10);
        assert_eq!(stats.bytes_found.load(Ordering::Relaxed), 1024);
        assert_eq!(stats.errors.load(Ordering::Relaxed), 1);
        assert_eq!(stats.skipped.load(Ordering::Relaxed), 1);
    }
}
