//! Fast walker using names-only readdir + parallel stat
//!
//! Simple, high-performance approach:
//! 1. opendir_names_only() to get all filenames (no stat, no cookies to manage)
//! 2. Batch names into chunks of ~10k
//! 3. Spawn tokio tasks, each with its own NFS connection doing parallel stats
//!
//! Two modes:
//! - HFC (High File Count): Scan single directory, no recursion
//! - Normal: Recursive walk using same pattern

use crate::config::WalkConfig;
use crate::db::WriterHandle;
use crate::error::NfsResult;
use crate::nfs::types::{DbEntry, EntryType};
use crate::nfs::{resolve_dns, AsyncStatEngine, LiveProgress, NfsConnectionBuilder};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Maximum number of connections/workers for parallel async stat
/// Each connection pipelines 128 concurrent requests
const MAX_ASYNC_CONNECTIONS: usize = 128;

/// Batch size for work-stealing queue
/// Small enough to allow good load balancing, large enough to amortize overhead
const WORK_BATCH_SIZE: usize = 10_000;

/// Result from a fast walk operation
#[derive(Debug)]
pub struct FastWalkResult {
    pub total_entries: u64,
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_bytes: u64,
    pub errors: u64,
    pub duration: Duration,
    pub completed: bool,
}

/// Live progress counters for HFC mode
#[derive(Clone)]
pub struct HfcProgress {
    pub phase: Arc<AtomicU64>,      // 0 = collecting names, 1 = stat phase
    pub total_names: Arc<AtomicU64>, // Total names to stat
    pub processed: Arc<AtomicU64>,   // Names processed so far
    pub files: Arc<AtomicU64>,
    pub dirs: Arc<AtomicU64>,
    pub bytes: Arc<AtomicU64>,
}

impl HfcProgress {
    pub fn new() -> Self {
        Self {
            phase: Arc::new(AtomicU64::new(0)),
            total_names: Arc::new(AtomicU64::new(0)),
            processed: Arc::new(AtomicU64::new(0)),
            files: Arc::new(AtomicU64::new(0)),
            dirs: Arc::new(AtomicU64::new(0)),
            bytes: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for HfcProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Fast walker for high file count directories
pub struct FastWalker {
    config: Arc<WalkConfig>,
    shutdown: Arc<AtomicBool>,
    /// Resolved IP addresses for DNS round-robin
    resolved_ips: Vec<String>,
    /// Live progress counters
    progress: HfcProgress,
}

impl FastWalker {
    pub fn new(config: WalkConfig) -> Self {
        // Resolve DNS to get all IPs for load balancing
        let resolved_ips = resolve_dns(&config.nfs_url.server);

        info!(
            server = %config.nfs_url.server,
            ips = ?resolved_ips,
            "Resolved DNS for NFS server"
        );

        Self {
            config: Arc::new(config),
            shutdown: Arc::new(AtomicBool::new(false)),
            resolved_ips,
            progress: HfcProgress::new(),
        }
    }

    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Get a clone of the progress counters for monitoring
    pub fn progress(&self) -> HfcProgress {
        self.progress.clone()
    }

    /// Run HFC mode: scan single directory without recursion
    pub async fn run_hfc(&self, writer: &WriterHandle) -> NfsResult<FastWalkResult> {
        let start = Instant::now();
        let path = self.config.nfs_url.walk_start_path();

        info!(path = %path, workers = self.config.worker_count, "Starting HFC scan");

        // Phase 1: Collect all names
        self.progress.phase.store(0, Ordering::Relaxed);
        let names = self.collect_names(&path).await?;
        let total_names = names.len();

        if total_names == 0 {
            return Ok(FastWalkResult {
                total_entries: 0,
                total_files: 0,
                total_dirs: 0,
                total_bytes: 0,
                errors: 0,
                duration: start.elapsed(),
                completed: true,
            });
        }

        info!(
            entries = total_names,
            phase1_ms = start.elapsed().as_millis(),
            "Phase 1 complete: collected names"
        );

        // Phase 2: Parallel stat
        self.progress.phase.store(1, Ordering::Relaxed);
        self.progress.total_names.store(total_names as u64, Ordering::Relaxed);
        let stats = self.parallel_stat(&path, names, writer, 1).await;

        let duration = start.elapsed();
        let rate = stats.processed as f64 / duration.as_secs_f64();

        info!(
            entries = stats.processed,
            files = stats.files,
            dirs = stats.dirs,
            bytes = stats.bytes,
            errors = stats.errors,
            duration_ms = duration.as_millis(),
            rate = format!("{:.0}/s", rate),
            "HFC scan complete"
        );

        Ok(FastWalkResult {
            total_entries: stats.processed,
            total_files: stats.files,
            total_dirs: stats.dirs,
            total_bytes: stats.bytes,
            errors: stats.errors,
            duration,
            completed: !self.shutdown.load(Ordering::Relaxed),
        })
    }

    /// Run normal recursive walk using fast pattern
    pub async fn run_recursive(&self, writer: &WriterHandle) -> NfsResult<FastWalkResult> {
        let start = Instant::now();
        let root_path = self.config.nfs_url.walk_start_path();

        info!(path = %root_path, workers = self.config.worker_count, "Starting recursive walk");

        // Channel for directories to process
        let (dir_tx, mut dir_rx) = mpsc::channel::<(String, u32)>(10_000);

        // Stats
        let total_entries = Arc::new(AtomicU64::new(0));
        let total_files = Arc::new(AtomicU64::new(0));
        let total_dirs = Arc::new(AtomicU64::new(0));
        let total_bytes = Arc::new(AtomicU64::new(0));
        let total_errors = Arc::new(AtomicU64::new(0));

        // Seed with root
        dir_tx.send((root_path.clone(), 0)).await.ok();

        // Record root directory
        let root_entry = DbEntry::root(&root_path);
        let _ = writer.send_entry(root_entry);
        total_dirs.fetch_add(1, Ordering::Relaxed);

        // Process directories
        while let Some((path, depth)) = dir_rx.recv().await {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            // Check depth limit
            if let Some(max_depth) = self.config.max_depth {
                if depth as usize > max_depth {
                    continue;
                }
            }

            // Check exclusions
            if self.config.is_excluded(&path) {
                continue;
            }

            debug!(path = %path, depth = depth, "Processing directory");

            // Collect names for this directory
            match self.collect_names(&path).await {
                Ok(names) => {
                    if names.is_empty() {
                        continue;
                    }

                    // Parallel stat this directory
                    let stats = self.parallel_stat_with_dirs(
                        &path,
                        names,
                        writer,
                        depth + 1,
                        &dir_tx,
                    ).await;

                    total_entries.fetch_add(stats.processed, Ordering::Relaxed);
                    total_files.fetch_add(stats.files, Ordering::Relaxed);
                    total_dirs.fetch_add(stats.dirs, Ordering::Relaxed);
                    total_bytes.fetch_add(stats.bytes, Ordering::Relaxed);
                    total_errors.fetch_add(stats.errors, Ordering::Relaxed);
                }
                Err(e) => {
                    warn!(path = %path, error = %e, "Failed to read directory");
                    total_errors.fetch_add(1, Ordering::Relaxed);
                }
            }

            // Check if more work (peek without blocking)
            if dir_rx.is_empty() {
                // Give pending sends a moment to arrive
                tokio::time::sleep(Duration::from_millis(10)).await;
                if dir_rx.is_empty() {
                    break;
                }
            }
        }

        let duration = start.elapsed();
        let entries = total_entries.load(Ordering::Relaxed);
        let files = total_files.load(Ordering::Relaxed);
        let dirs = total_dirs.load(Ordering::Relaxed);
        let bytes = total_bytes.load(Ordering::Relaxed);
        let errors = total_errors.load(Ordering::Relaxed);

        info!(
            entries = entries,
            files = files,
            dirs = dirs,
            bytes = bytes,
            errors = errors,
            duration_ms = duration.as_millis(),
            rate = format!("{:.0}/s", entries as f64 / duration.as_secs_f64()),
            "Recursive walk complete"
        );

        Ok(FastWalkResult {
            total_entries: entries,
            total_files: files,
            total_dirs: dirs,
            total_bytes: bytes,
            errors,
            duration,
            completed: !self.shutdown.load(Ordering::Relaxed),
        })
    }

    /// Collect all names from a directory using names-only readdir
    async fn collect_names(&self, path: &str) -> NfsResult<Vec<String>> {
        let config = Arc::clone(&self.config);
        let path_owned = path.to_string();
        let path_for_log = path.to_string();
        // Use first resolved IP for name collection
        let target_ip = self.resolved_ips[0].clone();

        debug!(path = %path, ip = %target_ip, "Collecting names with opendir_names_only");
        let start = Instant::now();

        // Run blocking NFS operation in dedicated thread
        let result = tokio::task::spawn_blocking(move || {
            let nfs = NfsConnectionBuilder::new(config.nfs_url.clone())
                .with_ip(target_ip.clone())
                .timeout(Duration::from_secs(config.timeout_secs as u64))
                .retries(config.retry_count)
                .connect()?;

            debug!(server = %nfs.server(), ip = %target_ip, "Connected for names-only readdir");

            let handle = nfs.opendir_names_only(&path_owned)?;
            let mut names = Vec::new();

            while let Some(entry) = handle.readdir() {
                if entry.is_special() {
                    continue;
                }
                names.push(entry.name);

                // Log progress for large directories
                if names.len() % 100_000 == 0 {
                    debug!(count = names.len(), "Collecting names...");
                }
            }

            Ok(names)
        })
        .await
        .map_err(|e| crate::error::NfsError::Protocol {
            code: -1,
            message: format!("Task join error: {}", e),
        })??;

        let elapsed = start.elapsed();
        let count = result.len();
        let rate = count as f64 / elapsed.as_secs_f64();

        info!(
            path = %path_for_log,
            entries = count,
            ms = elapsed.as_millis(),
            rate = format!("{:.0}/s", rate),
            "Names collected"
        );

        Ok(result)
    }

    /// Parallel stat without directory queuing (HFC mode)
    async fn parallel_stat(
        &self,
        parent_path: &str,
        names: Vec<String>,
        writer: &WriterHandle,
        depth: u32,
    ) -> StatStats {
        self.parallel_stat_inner(parent_path, names, writer, depth, None).await
    }

    /// Parallel stat with directory queuing (recursive mode)
    async fn parallel_stat_with_dirs(
        &self,
        parent_path: &str,
        names: Vec<String>,
        writer: &WriterHandle,
        depth: u32,
        dir_tx: &mpsc::Sender<(String, u32)>,
    ) -> StatStats {
        self.parallel_stat_inner(parent_path, names, writer, depth, Some(dir_tx)).await
    }

    /// Core parallel stat implementation using work-stealing queue
    ///
    /// Workers pull batches from a shared queue, allowing fast workers to
    /// process more work. Each connection pipelines 128 concurrent stat requests.
    async fn parallel_stat_inner(
        &self,
        parent_path: &str,
        names: Vec<String>,
        writer: &WriterHandle,
        depth: u32,
        dir_tx: Option<&mpsc::Sender<(String, u32)>>,
    ) -> StatStats {
        let total = names.len();
        let num_workers = self.config.worker_count.min(MAX_ASYNC_CONNECTIONS);

        // Create batches for the work queue
        let num_batches = (total + WORK_BATCH_SIZE - 1) / WORK_BATCH_SIZE;

        info!(
            parent = %parent_path,
            total_entries = total,
            workers = num_workers,
            batches = num_batches,
            batch_size = WORK_BATCH_SIZE,
            pipeline_depth = 128,
            ips = ?self.resolved_ips,
            "Starting work-stealing stat phase"
        );

        // Create bounded channel for work batches
        // Buffer size = number of batches (all work queued upfront)
        let (work_tx, work_rx): (Sender<Vec<String>>, Receiver<Vec<String>>) =
            bounded(num_batches.max(1));

        // Queue all batches
        for chunk in names.chunks(WORK_BATCH_SIZE) {
            let _ = work_tx.send(chunk.to_vec());
        }
        // Drop sender so workers know when queue is exhausted
        drop(work_tx);

        // Shared stats counters
        let stats_processed = Arc::new(AtomicU64::new(0));
        let stats_files = Arc::new(AtomicU64::new(0));
        let stats_dirs = Arc::new(AtomicU64::new(0));
        let stats_bytes = Arc::new(AtomicU64::new(0));
        let stats_errors = Arc::new(AtomicU64::new(0));

        // Spawn workers
        let mut handles = Vec::new();

        for worker_id in 0..num_workers {
            let config = Arc::clone(&self.config);
            let parent = parent_path.to_string();
            let writer = writer.clone();
            let dir_tx = dir_tx.cloned();
            let progress = self.progress.clone();
            let work_rx = work_rx.clone();

            // Round-robin IP assignment across workers
            let ip = self.resolved_ips[worker_id % self.resolved_ips.len()].clone();

            // Clone stats counters for this worker
            let w_processed = Arc::clone(&stats_processed);
            let w_files = Arc::clone(&stats_files);
            let w_dirs = Arc::clone(&stats_dirs);
            let w_bytes = Arc::clone(&stats_bytes);
            let w_errors = Arc::clone(&stats_errors);

            debug!(
                worker = worker_id,
                ip = %ip,
                "Spawning work-stealing stat worker"
            );

            let handle = tokio::task::spawn_blocking(move || {
                work_stealing_stat_worker(
                    config, parent, work_rx, writer, depth, dir_tx,
                    worker_id, ip, progress,
                    w_processed, w_files, w_dirs, w_bytes, w_errors
                )
            });

            handles.push(handle);
        }

        // Wait for all workers to complete
        for (worker_id, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(batches_processed)) => {
                    debug!(
                        worker = worker_id,
                        batches = batches_processed,
                        "Worker completed"
                    );
                }
                Ok(Err(e)) => {
                    warn!(worker = worker_id, error = %e, "Worker failed");
                    stats_errors.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    warn!(worker = worker_id, error = %e, "Worker task panicked");
                    stats_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let stats = StatStats {
            processed: stats_processed.load(Ordering::Relaxed),
            files: stats_files.load(Ordering::Relaxed),
            dirs: stats_dirs.load(Ordering::Relaxed),
            bytes: stats_bytes.load(Ordering::Relaxed),
            errors: stats_errors.load(Ordering::Relaxed),
        };

        info!(
            parent = %parent_path,
            processed = stats.processed,
            files = stats.files,
            dirs = stats.dirs,
            bytes = stats.bytes,
            errors = stats.errors,
            "Work-stealing stat phase complete"
        );

        stats
    }
}

/// Stats from a stat operation
#[derive(Debug, Default)]
struct StatStats {
    processed: u64,
    files: u64,
    dirs: u64,
    bytes: u64,
    errors: u64,
}

/// Work-stealing stat worker
///
/// Creates one NFS connection and processes batches from a shared queue.
/// Fast workers automatically get more work, ensuring good load balancing.
fn work_stealing_stat_worker(
    config: Arc<WalkConfig>,
    parent_path: String,
    work_rx: Receiver<Vec<String>>,
    writer: WriterHandle,
    depth: u32,
    dir_tx: Option<mpsc::Sender<(String, u32)>>,
    worker_id: usize,
    target_ip: String,
    progress: HfcProgress,
    stats_processed: Arc<AtomicU64>,
    stats_files: Arc<AtomicU64>,
    stats_dirs: Arc<AtomicU64>,
    stats_bytes: Arc<AtomicU64>,
    stats_errors: Arc<AtomicU64>,
) -> NfsResult<usize> {
    let start = Instant::now();

    // Create NFS connection to specific IP (reused for all batches)
    let nfs = NfsConnectionBuilder::new(config.nfs_url.clone())
        .with_ip(target_ip.clone())
        .timeout(Duration::from_secs(config.timeout_secs as u64))
        .retries(config.retry_count)
        .connect()?;

    debug!(
        worker = worker_id,
        server = %nfs.server(),
        ip = %target_ip,
        "Work-stealing worker connected"
    );

    // Create live progress for real-time updates
    let live_progress = LiveProgress {
        processed: Arc::clone(&progress.processed),
        files: Arc::clone(&progress.files),
        dirs: Arc::clone(&progress.dirs),
        bytes: Arc::clone(&progress.bytes),
    };

    let mut batches_processed = 0;
    let mut total_entries = 0u64;

    // Process batches until queue is exhausted
    while let Ok(names) = work_rx.recv() {
        let batch_size = names.len();

        // Run async stat engine on this batch
        let (results, _async_stats) = unsafe {
            let raw_ctx = nfs.raw_context();
            let mut engine = AsyncStatEngine::new(raw_ctx);
            engine.set_live_progress(live_progress.clone());
            engine.add_paths(names, &parent_path);
            engine.run()?
        };

        // Process results - send to writer and queue directories
        let mut batch_files = 0u64;
        let mut batch_dirs = 0u64;
        let mut batch_bytes = 0u64;
        let mut batch_errors = 0u64;

        for result in results {
            if let Some(ref nfs_stat) = result.stat {
                let entry_type = nfs_stat.entry_type();

                if entry_type == EntryType::Directory {
                    batch_dirs += 1;

                    // Queue directories for recursive processing
                    if let Some(ref tx) = dir_tx {
                        let should_queue = config
                            .max_depth
                            .map(|max| (depth) as usize <= max)
                            .unwrap_or(true);

                        if should_queue && !config.is_excluded(&result.path) {
                            let _ = tx.try_send((result.path.clone(), depth));
                        }
                    }
                } else {
                    batch_files += 1;
                    batch_bytes += nfs_stat.size;
                }

                // Create and send DB entry
                let db_entry = DbEntry::from_stat(&result.path, &result.name, nfs_stat, depth);

                if !config.dirs_only || entry_type == EntryType::Directory {
                    if let Err(e) = writer.send_entry(db_entry) {
                        warn!(path = %result.path, error = %e, "Failed to send entry to writer");
                    }
                }
            } else {
                batch_errors += 1;
            }
        }

        // Update shared stats
        stats_processed.fetch_add(batch_size as u64, Ordering::Relaxed);
        stats_files.fetch_add(batch_files, Ordering::Relaxed);
        stats_dirs.fetch_add(batch_dirs, Ordering::Relaxed);
        stats_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
        stats_errors.fetch_add(batch_errors, Ordering::Relaxed);

        batches_processed += 1;
        total_entries += batch_size as u64;
    }

    let elapsed = start.elapsed();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        total_entries as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    info!(
        worker = worker_id,
        batches = batches_processed,
        entries = total_entries,
        ms = elapsed.as_millis(),
        rate = format!("{:.0}/s", rate),
        "Work-stealing worker complete"
    );

    Ok(batches_processed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stat_stats_default() {
        let stats = StatStats::default();
        assert_eq!(stats.processed, 0);
        assert_eq!(stats.files, 0);
        assert_eq!(stats.dirs, 0);
    }
}
