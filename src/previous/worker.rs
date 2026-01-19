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
use crate::error::{NfsError, WalkOutcome, WorkerError};
use crate::nfs::types::{DbEntry, DirStats, EntryType, NfsDirEntry};
use crate::nfs::{DnsResolver, NfsConnection, NfsConnectionBuilder};
use crate::walker::parallel_stat::process_directory_parallel;
use crate::walker::queue::{
    DirTask, EntryQueueReceiver, EntryQueueSender, StatQueueReceiver, StatQueueSender, StatTask,
    WorkGuard, WorkQueueReceiver, WorkQueueSender, LARGE_DIR_THRESHOLD,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

/// Batch size for skinny streaming reads (entries per RPC call)
const SKINNY_BATCH_SIZE: u32 = 10_000;

/// Number of entries to probe with READDIRPLUS before deciding to switch to skinny
const SKINNY_PROBE_SIZE: usize = 1_000;

/// Maximum directory ratio to allow skinny mode (e.g., 0.01 = 1% directories)
/// If more than this fraction are directories, stick with READDIRPLUS
const SKINNY_MAX_DIR_RATIO: f64 = 0.01;

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
        entry_tx: EntryQueueSender,
        entry_rx: EntryQueueReceiver,
        stat_tx: StatQueueSender,
        stat_rx: StatQueueReceiver,
        writer: WriterHandle,
        shutdown: Arc<AtomicBool>,
        dns_resolver: Arc<DnsResolver>,
    ) -> Result<Self, WorkerError> {
        let stats = Arc::new(WorkerStats::default());
        let stats_clone = Arc::clone(&stats);

        let handle = thread::Builder::new()
            .name(format!("walker-{}", id))
            .spawn(move || {
                worker_loop(
                    id, config, queue_rx, queue_tx, entry_tx, entry_rx, stat_tx, stat_rx,
                    writer, shutdown, stats_clone, dns_resolver,
                )
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

/// Create an NFS connection for a worker, using DNS resolver for IP assignment
fn create_worker_connection(
    id: usize,
    config: &WalkConfig,
    dns_resolver: &DnsResolver,
) -> Result<(NfsConnection, Option<String>), NfsError> {
    // Get assigned IP from DNS resolver
    let target_ip = dns_resolver.get_ip_for_worker(id);

    let mut builder = NfsConnectionBuilder::new(config.nfs_url.clone())
        .timeout(Duration::from_secs(config.timeout_secs as u64))
        .retries(config.retry_count);

    if let Some(ref ip) = target_ip {
        debug!(worker = id, ip = %ip, "Connecting to assigned IP");
        builder = builder.with_ip(ip.clone());
    }

    let conn = builder.connect()?;
    Ok((conn, target_ip))
}

/// Main worker loop
#[allow(clippy::too_many_arguments)]
fn worker_loop(
    id: usize,
    config: Arc<WalkConfig>,
    queue_rx: WorkQueueReceiver,
    queue_tx: WorkQueueSender,
    entry_tx: EntryQueueSender,
    entry_rx: EntryQueueReceiver,
    stat_tx: StatQueueSender,
    stat_rx: StatQueueReceiver,
    writer: WriterHandle,
    shutdown: Arc<AtomicBool>,
    stats: Arc<WorkerStats>,
    dns_resolver: Arc<DnsResolver>,
) -> Result<(), WorkerError> {
    debug!(worker = id, "Worker starting");

    // Create NFS connection for this worker using DNS resolver
    let (mut nfs, mut current_ip) = match create_worker_connection(id, &config, &dns_resolver) {
        Ok(result) => result,
        Err(e) => {
            error!(worker = id, error = %e, "Failed to connect to NFS");
            return Err(WorkerError::InitFailed {
                id,
                reason: e.to_string(),
            });
        }
    };

    debug!(
        worker = id,
        server = nfs.server(),
        export = nfs.export(),
        ip = ?current_ip,
        "NFS connection established"
    );

    // Process tasks until shutdown
    while !shutdown.load(Ordering::Relaxed) {
        // First, try to get a directory task (highest priority)
        let task = match queue_rx.recv_timeout(Duration::from_millis(5)) {
            Some(task) => Some(task),
            None => None,
        };

        if let Some(task) = task {
            // Mark as actively working
            let _guard = WorkGuard::new(&queue_rx);

            // Process the directory
            let outcome = process_directory(
                id,
                &task,
                &config,
                &mut nfs,
                &queue_tx,
                &entry_tx,
                &stat_tx,
                &writer,
                &stats,
                &shutdown,
            );

            match &outcome {
                WalkOutcome::Success { entries, .. } => {
                    trace!(worker = id, path = %task.path, entries = entries, "Directory processed");
                    // Report success to DNS resolver
                    if let Some(ref ip) = current_ip {
                        dns_resolver.report_success(ip);
                    }
                }
                WalkOutcome::Skipped { path, reason } => {
                    debug!(worker = id, path = %path, reason = %reason, "Directory skipped");
                }
                WalkOutcome::Failed { path, error } => {
                    warn!(worker = id, path = %path, error = %error, "Directory failed");

                    // Check if this error should trigger a reconnect
                    if error.should_reconnect() {
                        // Report failure to DNS resolver
                        if let Some(ref ip) = current_ip {
                            dns_resolver.report_failure(ip, id);
                        }

                        // Try to reconnect to a (potentially different) IP
                        info!(worker = id, "Attempting reconnect after connection failure");
                        match create_worker_connection(id, &config, &dns_resolver) {
                            Ok((new_conn, new_ip)) => {
                                nfs = new_conn;
                                current_ip = new_ip;
                                info!(
                                    worker = id,
                                    ip = ?current_ip,
                                    "Reconnected successfully"
                                );

                                // Re-queue the failed task for retry
                                let retry_task = DirTask::new(
                                    task.path.clone(),
                                    task.entry_id,
                                    task.depth,
                                );
                                if queue_tx.try_send(retry_task).is_err() {
                                    warn!(
                                        worker = id,
                                        path = %task.path,
                                        "Failed to re-queue task after reconnect"
                                    );
                                }
                            }
                            Err(e) => {
                                error!(
                                    worker = id,
                                    error = %e,
                                    "Failed to reconnect, worker will exit"
                                );
                                return Err(WorkerError::NfsError {
                                    id,
                                    source: e,
                                });
                            }
                        }
                    }
                }
            }
            continue;
        }

        // No directory tasks - try processing stat tasks (parallel GETATTR)
        let stat_tasks = stat_rx.drain(50); // Process up to 50 stat tasks at a time
        if !stat_tasks.is_empty() {
            let _guard = WorkGuard::new(&queue_rx); // Mark as working
            process_stat_tasks(id, &mut nfs, &config, &queue_tx, &writer, &stats, stat_tasks);
            continue;
        }

        // No stat tasks - try processing db entry tasks
        let entries = entry_rx.drain(100);
        if !entries.is_empty() {
            for entry in entries {
                if let Err(e) = writer.send_entry(entry) {
                    error!(worker = id, error = %e, "Failed to send entry to writer");
                }
            }
            continue;
        }

        // Nothing to do, wait a bit
        std::thread::sleep(Duration::from_millis(5));
    }

    debug!(
        worker = id,
        dirs = stats.dirs_processed.load(Ordering::Relaxed),
        files = stats.files_found.load(Ordering::Relaxed),
        "Worker shutting down"
    );

    Ok(())
}

/// Process stat tasks - issue GETATTR calls for parallel stat resolution
fn process_stat_tasks(
    worker_id: usize,
    nfs: &mut NfsConnection,
    config: &WalkConfig,
    queue_tx: &WorkQueueSender,
    writer: &WriterHandle,
    stats: &WorkerStats,
    tasks: Vec<StatTask>,
) {
    let task_count = tasks.len();
    let mut dir_count = 0u64;
    let mut file_count = 0u64;

    for task in tasks {
        let full_path = if task.parent_path == "/" {
            format!("/{}", task.name)
        } else {
            format!("{}/{}", task.parent_path, task.name)
        };

        match nfs.stat(&full_path) {
            Ok(stat_result) => {
                let entry_type = stat_result.entry_type();

                if entry_type == EntryType::Directory {
                    dir_count += 1;

                    // Queue directory for processing
                    let should_queue = config
                        .max_depth
                        .map(|max| (task.depth) as usize <= max)
                        .unwrap_or(true);

                    if should_queue && !config.is_excluded(&full_path) {
                        let subtask = DirTask::new(full_path.clone(), None, task.depth);
                        if let Err(()) = queue_tx.try_send(subtask) {
                            warn!(worker = worker_id, path = %full_path, "Failed to queue directory from stat");
                        }
                    }
                } else {
                    file_count += 1;
                }

                // Create db entry with full attributes
                let db_entry = DbEntry::from_stat(&full_path, &task.name, &stat_result, task.depth);

                if !config.dirs_only || entry_type == EntryType::Directory {
                    if let Err(e) = writer.send_entry(db_entry) {
                        error!(worker = worker_id, error = %e, "Failed to send stat entry to writer");
                    }
                }
            }
            Err(e) => {
                // Non-fatal: entry might have been deleted or we lack permissions
                trace!(worker = worker_id, path = %full_path, error = %e, "GETATTR failed");
                stats.record_error();
            }
        }
    }

    // Update stats
    stats.record_files(file_count);

    trace!(
        worker = worker_id,
        tasks = task_count,
        files = file_count,
        dirs = dir_count,
        "Processed stat tasks"
    );
}

/// Process a single directory using hybrid READDIRPLUS/skinny approach
///
/// Strategy:
/// 1. Probe first batch with READDIRPLUS to analyze directory composition
/// 2. If large flat directory (>100K entries), use parallel READDIRPLUS
/// 3. If mostly files (< 1% directories), switch to skinny streaming for speed
/// 4. If many subdirectories, continue with READDIRPLUS for complete type info
/// 5. For skinny mode, push Unknown entries to stat queue for parallel GETATTR
#[allow(clippy::too_many_arguments)]
fn process_directory(
    worker_id: usize,
    task: &DirTask,
    config: &WalkConfig,
    nfs: &mut NfsConnection,
    queue_tx: &WorkQueueSender,
    _entry_tx: &EntryQueueSender,
    _stat_tx: &StatQueueSender,
    writer: &WriterHandle,
    stats: &WorkerStats,
    shutdown: &Arc<AtomicBool>,
) -> WalkOutcome {
    // Use simple READDIRPLUS-only path if skinny mode is disabled
    if config.disable_skinny {
        return process_directory_simple(worker_id, task, config, nfs, queue_tx, _entry_tx, writer, stats);
    }
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

    // Phase 1: Probe with READDIRPLUS to analyze directory composition
    // Use opendir_at_cookie with a limit to avoid reading entire huge directories
    debug!(worker = worker_id, path = %task.path, "Starting directory probe");
    let probe_handle = match nfs.opendir_at_cookie(&task.path, 0, (SKINNY_PROBE_SIZE + 10) as u32) {
        Ok(h) => h,
        Err(e) => {
            stats.record_error();
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

    // Collect probe entries and analyze (read up to SKINNY_PROBE_SIZE entries)
    let mut probe_entries: Vec<NfsDirEntry> = Vec::with_capacity(SKINNY_PROBE_SIZE);
    let mut probe_dir_count = 0usize;

    while probe_entries.len() < SKINNY_PROBE_SIZE {
        match probe_handle.readdir() {
            Some(entry) => {
                if !entry.is_special() {
                    if entry.entry_type == EntryType::Directory {
                        probe_dir_count += 1;
                    }
                    probe_entries.push(entry);
                }
            }
            None => break, // End of directory
        }
    }

    let probe_size = probe_entries.len();
    let probe_full = probe_size >= SKINNY_PROBE_SIZE;

    debug!(
        worker = worker_id,
        path = %task.path,
        probe_size = probe_size,
        probe_full = probe_full,
        "Probe complete"
    );

    let dir_ratio = if probe_size > 0 {
        probe_dir_count as f64 / probe_size as f64
    } else {
        0.0
    };

    // Decide: use parallel mode if probe is full AND directory ratio is low (flat dir)
    let use_parallel = probe_full && dir_ratio <= SKINNY_MAX_DIR_RATIO;

    // For large flat directories, use simple parallel READDIR + GETATTR
    if use_parallel {
        drop(probe_handle);

        debug!(
            worker = worker_id,
            path = %task.path,
            probe_size = probe_size,
            dir_ratio = %format!("{:.2}%", dir_ratio * 100.0),
            "Using parallel READDIR+GETATTR for flat directory"
        );

        match process_directory_parallel(
            nfs,
            &task.path,
            config,
            queue_tx,
            writer,
            task.depth,
            shutdown,
        ) {
            Ok((entries, subdirs, _duration)) => {
                stats.record_dir();
                stats.record_files(entries);
                return WalkOutcome::Success {
                    path: task.path.clone(),
                    entries: entries as usize,
                    subdirs: subdirs as usize,
                };
            }
            Err(e) => {
                stats.record_error();
                return WalkOutcome::Failed {
                    path: task.path.clone(),
                    error: e,
                };
            }
        }
    }

    // Standard READDIRPLUS path for small/mixed directories
    // (Large flat directories are handled by process_directory_parallel above)
    stats.record_dir();

    let mut dir_stats = DirStats::default();
    let mut entry_count = 0usize;
    let mut subdir_count = 0usize;

    // Process probe entries first
    for entry in probe_entries {
        entry_count += 1;
        let db_entry = DbEntry::from_nfs_entry(&entry, &task.path, task.depth + 1);
        dir_stats.add_entry(&entry);

        if entry.entry_type == EntryType::Directory {
            subdir_count += 1;
            queue_subdir(worker_id, &db_entry, task, config, queue_tx);
        }

        if !config.dirs_only || entry.entry_type == EntryType::Directory {
            if let Err(e) = writer.send_entry(db_entry) {
                error!(worker = worker_id, error = %e, "Failed to send entry to writer");
            }
        }
    }

    // Continue reading from where probe left off
    while let Some(entry) = probe_handle.readdir() {
        if entry.is_special() {
            continue;
        }

        entry_count += 1;
        let db_entry = DbEntry::from_nfs_entry(&entry, &task.path, task.depth + 1);
        dir_stats.add_entry(&entry);

        if entry.entry_type == EntryType::Directory {
            subdir_count += 1;
            queue_subdir(worker_id, &db_entry, task, config, queue_tx);
        }

        if !config.dirs_only || entry.entry_type == EntryType::Directory {
            if let Err(e) = writer.send_entry(db_entry) {
                error!(worker = worker_id, error = %e, "Failed to send entry to writer");
            }
        }
    }

    // Log large directories
    if entry_count > LARGE_DIR_THRESHOLD {
        debug!(
            worker = worker_id,
            path = %task.path,
            entries = entry_count,
            "Processed large directory"
        );
    }

    stats.record_files(dir_stats.file_count);
    stats.record_bytes(dir_stats.total_bytes);

    // Send directory stats to writer
    if let Err(e) = writer.send_dir_stats(task.path.clone(), dir_stats) {
        debug!(worker = worker_id, error = %e, "Failed to send dir stats");
    }

    WalkOutcome::Success {
        path: task.path.clone(),
        entries: entry_count,
        subdirs: subdir_count,
    }
}

/// Queue a subdirectory for processing
fn queue_subdir(
    worker_id: usize,
    db_entry: &DbEntry,
    task: &DirTask,
    config: &WalkConfig,
    queue_tx: &WorkQueueSender,
) {
    let should_queue = config
        .max_depth
        .map(|max| (task.depth + 1) as usize <= max)
        .unwrap_or(true);

    if should_queue && !config.is_excluded(&db_entry.path) {
        let subtask = DirTask::new(db_entry.path.clone(), None, task.depth + 1);
        if !queue_tx.try_send(subtask.clone()).unwrap_or(false) {
            queue_tx.record_inline();
            trace!(worker = worker_id, path = %subtask.path, "Backpressure - task will be retried");
        }
    }
}

/// Continue reading directory with skinny streaming after probe (unused - kept for reference)
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
fn continue_with_skinny(
    worker_id: usize,
    task: &DirTask,
    config: &WalkConfig,
    nfs: &mut NfsConnection,
    queue_tx: &WorkQueueSender,
    writer: &WriterHandle,
    dir_stats: &mut DirStats,
    entry_count: &mut usize,
    subdir_count: &mut usize,
    unknown_entries: &mut Vec<(String, u64)>,
    mut cookie: u64,
    mut cookieverf: u64,
) -> Result<(), crate::error::NfsError> {
    loop {
        let dir_handle = nfs.opendir_names_only_at_cookie(
            &task.path,
            cookie,
            cookieverf,
            SKINNY_BATCH_SIZE,
        )?;

        let mut batch_count = 0u32;

        while let Some(entry) = dir_handle.readdir() {
            if entry.is_special() {
                continue;
            }

            batch_count += 1;
            *entry_count += 1;
            cookie = entry.cookie;

            // Build path for this entry
            let entry_path = if task.path == "/" {
                format!("/{}", entry.name)
            } else {
                format!("{}/{}", task.path, entry.name)
            };

            // Skinny entries have Unknown type - track for later stat resolution
            // We optimistically treat them as files for stats (most likely case)
            if entry.entry_type == EntryType::Unknown {
                unknown_entries.push((entry_path.clone(), entry.inode));
                dir_stats.file_count += 1; // Assume file, will correct if wrong
            } else {
                dir_stats.add_entry(&entry);
                if entry.entry_type == EntryType::Directory {
                    *subdir_count += 1;
                    let db_entry = DbEntry::from_nfs_entry(&entry, &task.path, task.depth + 1);
                    queue_subdir(worker_id, &db_entry, task, config, queue_tx);
                }
            }

            // Create and send DB entry (with potentially Unknown type)
            let db_entry = DbEntry::from_nfs_entry(&entry, &task.path, task.depth + 1);

            if !config.dirs_only || entry.entry_type == EntryType::Directory {
                if let Err(e) = writer.send_entry(db_entry) {
                    error!(worker = worker_id, error = %e, "Failed to send entry to writer");
                }
            }
        }

        // Update verifier for next batch
        cookieverf = dir_handle.get_cookieverf();

        // EOF when we get an empty batch
        if batch_count == 0 {
            break;
        }
    }

    Ok(())
}

/// Resolve Unknown entries by stat'ing them to find directories
///
/// Returns the paths of entries that are actually directories.
/// NOTE: Kept for reference - replaced by parallel stat via StatQueue
#[allow(dead_code)]
fn resolve_unknown_entries(
    worker_id: usize,
    nfs: &mut NfsConnection,
    unknown_entries: &[(String, u64)],
) -> Vec<String> {
    let mut directories = Vec::new();

    // TODO: This is currently sequential - Phase 4 will add parallel stat via connection pool
    for (path, _inode) in unknown_entries {
        match nfs.stat(path) {
            Ok(stat) => {
                if stat.entry_type() == EntryType::Directory {
                    directories.push(path.clone());
                }
            }
            Err(e) => {
                // Non-fatal: entry might have been deleted or we lack permissions
                trace!(worker = worker_id, path = %path, error = %e, "Failed to stat unknown entry");
            }
        }
    }

    if !directories.is_empty() {
        debug!(
            worker = worker_id,
            total_unknown = unknown_entries.len(),
            resolved_dirs = directories.len(),
            "Resolved unknown entries to find directories"
        );
    }

    directories
}

/// Simple directory processing using only READDIRPLUS (no skinny optimization)
///
/// This is the original implementation, used when --no-skinny flag is set.
#[allow(clippy::too_many_arguments)]
fn process_directory_simple(
    worker_id: usize,
    task: &DirTask,
    config: &WalkConfig,
    nfs: &mut NfsConnection,
    queue_tx: &WorkQueueSender,
    _entry_tx: &EntryQueueSender,
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

    // Read directory contents using simple readdir_plus
    let entries = match nfs.readdir_plus(&task.path) {
        Ok(entries) => entries,
        Err(e) => {
            stats.record_error();
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

    // Log large directories for visibility
    if entries.len() > LARGE_DIR_THRESHOLD {
        debug!(
            worker = worker_id,
            path = %task.path,
            entries = entries.len(),
            "Processing large directory (READDIRPLUS mode)"
        );
    }

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
        let db_entry = DbEntry::from_nfs_entry(&entry, &task.path, task.depth + 1);

        // Track directory stats
        dir_stats.add_entry(&entry);

        // If this is a directory, queue it for processing
        if entry.entry_type == EntryType::Directory {
            subdir_count += 1;
            queue_subdir(worker_id, &db_entry, task, config, queue_tx);
        }

        // Send directly to writer - no intermediate queue
        if !config.dirs_only || entry.entry_type == EntryType::Directory {
            if let Err(e) = writer.send_entry(db_entry) {
                error!(worker = worker_id, error = %e, "Failed to send entry to writer");
            }
        }
    }

    // Record file stats
    stats.record_files(dir_stats.file_count);
    stats.record_bytes(dir_stats.total_bytes);

    // Send directory stats to writer
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
