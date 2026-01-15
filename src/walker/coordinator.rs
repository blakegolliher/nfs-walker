//! Walk coordinator - orchestrates the parallel filesystem walk
//!
//! The coordinator is responsible for:
//! - Setting up the work queue and workers
//! - Starting and stopping the walk
//! - Progress reporting
//! - Signal handling (graceful shutdown)
//! - Final statistics and cleanup

use crate::config::WalkConfig;
use crate::db::BatchedWriter;
use crate::error::{Result, WalkerError, WorkerError};
use crate::nfs::types::DbEntry;
use crate::walker::queue::{EntryQueue, WorkQueue};
use crate::walker::worker::{aggregate_stats, Worker};
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Result of a completed walk
#[derive(Debug)]
pub struct WalkResult {
    /// Total directories scanned
    pub total_dirs: u64,

    /// Total files found
    pub total_files: u64,

    /// Total bytes (sum of file sizes)
    pub total_bytes: u64,

    /// Total entries written to database
    pub entries_written: u64,

    /// Number of errors encountered
    pub errors: u64,

    /// Number of directories skipped
    pub skipped: u64,

    /// Time taken for the walk
    pub duration: Duration,

    /// Whether the walk completed (vs was interrupted)
    pub completed: bool,
}

/// Coordinates the parallel filesystem walk
pub struct WalkCoordinator {
    /// Configuration
    config: Arc<WalkConfig>,

    /// Work queue for directory tasks
    queue: WorkQueue,

    /// Entry queue for distributing file entries from large directories
    entry_queue: EntryQueue,

    /// Database writer
    writer: BatchedWriter,

    /// Worker threads
    workers: Vec<Worker>,

    /// Shutdown signal
    shutdown: Arc<AtomicBool>,

    /// Walk start time
    start_time: Option<Instant>,
}

impl WalkCoordinator {
    /// Create a new walk coordinator
    pub fn new(config: WalkConfig) -> Result<Self> {
        let config = Arc::new(config);

        // Create work queue for directory tasks
        let queue = WorkQueue::new(config.queue_size);

        // Create entry queue for distributing entries from large directories
        // Size it large enough to buffer entries while workers process them
        let entry_queue = EntryQueue::new(config.batch_size * 4);

        // Create database writer
        let writer = BatchedWriter::new(
            &config.output_path,
            config.batch_size,
            config.batch_size * 2, // Channel size = 2x batch size
        )?;

        // Shutdown signal
        let shutdown = Arc::new(AtomicBool::new(false));

        Ok(Self {
            config,
            queue,
            entry_queue,
            writer,
            workers: Vec::new(),
            shutdown,
            start_time: None,
        })
    }

    /// Get a clone of the shutdown flag (for signal handlers)
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Run the filesystem walk
    pub fn run(mut self) -> Result<WalkResult> {
        let start_time = Instant::now();
        let start_datetime: DateTime<Utc> = Utc::now();
        self.start_time = Some(start_time);

        info!(
            url = %self.config.nfs_url.to_display_string(),
            workers = self.config.worker_count,
            "Starting filesystem walk"
        );

        // Record walk start in database
        let writer_handle = self.writer.handle();
        self.record_walk_start(&start_datetime)?;

        // Seed the queue with the root directory
        // Note: After mounting, paths are relative to mount root, so "/" not "/export"
        // We use walk_start_path consistently for both the root entry and queue seed
        // to ensure parent_id lookups work correctly
        let root_path = self.config.nfs_url.walk_start_path();
        let root_entry = DbEntry::root(&root_path);

        // Send root entry to writer
        writer_handle.send_entry(root_entry)?;

        // Seed the queue
        self.queue
            .seed(root_path.clone())
            .map_err(|_| WalkerError::Worker(WorkerError::QueueSendFailed))?;

        // Spawn workers
        self.spawn_workers()?;

        // Wait for completion
        let completed = self.wait_for_completion();

        // Signal shutdown
        self.shutdown.store(true, Ordering::SeqCst);

        // Wait for workers to finish
        let (dirs, files, bytes, errors, skipped) = self.join_workers()?;

        // Finish database writer
        let entries_written = writer_handle.stats().entries_written();
        drop(writer_handle); // Release handle before finishing

        self.writer.finish()?;

        // Record walk end
        let duration = start_time.elapsed();

        info!(
            dirs = dirs,
            files = files,
            bytes = bytes,
            errors = errors,
            duration_secs = duration.as_secs(),
            "Walk completed"
        );

        Ok(WalkResult {
            total_dirs: dirs,
            total_files: files,
            total_bytes: bytes,
            entries_written,
            errors,
            skipped,
            duration,
            completed,
        })
    }

    /// Spawn worker threads
    fn spawn_workers(&mut self) -> Result<()> {
        let writer_handle = self.writer.handle();

        for id in 0..self.config.worker_count {
            let worker = Worker::spawn(
                id,
                Arc::clone(&self.config),
                self.queue.receiver(),
                self.queue.sender(),
                self.entry_queue.sender(),
                self.entry_queue.receiver(),
                writer_handle.clone(),
                Arc::clone(&self.shutdown),
            )?;

            self.workers.push(worker);
        }

        info!(count = self.workers.len(), "Workers spawned");
        Ok(())
    }

    /// Wait for the walk to complete or be interrupted
    fn wait_for_completion(&self) -> bool {
        let check_interval = Duration::from_millis(100);
        let stable_checks_required = 3; // Must be complete for 3 consecutive checks
        let mut stable_count = 0;

        loop {
            // Check for shutdown signal
            if self.shutdown.load(Ordering::Relaxed) {
                info!("Shutdown signal received");
                return false;
            }

            // Check if both queues are complete (no pending work)
            let dir_queue_complete = self.queue.is_complete();
            let entry_queue_empty = self.entry_queue.is_empty();

            if dir_queue_complete && entry_queue_empty {
                stable_count += 1;
                if stable_count >= stable_checks_required {
                    return true;
                }
            } else {
                stable_count = 0;
            }

            thread::sleep(check_interval);
        }
    }

    /// Join all worker threads and collect final stats
    fn join_workers(&mut self) -> Result<(u64, u64, u64, u64, u64)> {
        // Get stats before joining
        let stats = aggregate_stats(&self.workers);

        // Join all workers
        let workers = std::mem::take(&mut self.workers);
        for worker in workers {
            if let Err(e) = worker.join() {
                warn!(error = %e, "Worker failed to join cleanly");
            }
        }

        Ok(stats)
    }

    /// Record walk metadata at start
    fn record_walk_start(&self, start_time: &DateTime<Utc>) -> Result<()> {
        // We'll update the database directly after the walk completes
        // For now, just log the start
        debug!(
            start_time = %start_time.to_rfc3339(),
            "Walk started"
        );
        Ok(())
    }
}

/// Progress information for display
#[derive(Debug, Clone)]
pub struct WalkProgress {
    /// Directories processed
    pub dirs: u64,

    /// Files found
    pub files: u64,

    /// Bytes found
    pub bytes: u64,

    /// Current queue size
    pub queue_size: usize,

    /// Active workers
    pub active_workers: usize,

    /// Total workers
    pub total_workers: usize,

    /// Errors encountered
    pub errors: u64,

    /// Elapsed time
    pub elapsed: Duration,
}

impl WalkProgress {
    /// Calculate files per second rate
    pub fn files_per_second(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs > 0.0 {
            self.files as f64 / secs
        } else {
            0.0
        }
    }

    /// Calculate dirs per second rate
    pub fn dirs_per_second(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs > 0.0 {
            self.dirs as f64 / secs
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_walk_progress_rates() {
        let progress = WalkProgress {
            dirs: 1000,
            files: 10000,
            bytes: 1024 * 1024 * 100,
            queue_size: 500,
            active_workers: 4,
            total_workers: 8,
            errors: 5,
            elapsed: Duration::from_secs(10),
        };

        assert!((progress.files_per_second() - 1000.0).abs() < 0.1);
        assert!((progress.dirs_per_second() - 100.0).abs() < 0.1);
    }
}
