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
use crate::nfs::types::{DbEntry, DirStats, EntryType, NfsDirEntry};
use crate::nfs::{NfsConnection, NfsConnectionBuilder};
use crate::walker::queue::{
    DirTask, EntryQueueReceiver, EntryQueueSender, WorkGuard, WorkQueueReceiver, WorkQueueSender,
    LARGE_DIR_THRESHOLD,
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
        writer: WriterHandle,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, WorkerError> {
        let stats = Arc::new(WorkerStats::default());
        let stats_clone = Arc::clone(&stats);

        let handle = thread::Builder::new()
            .name(format!("walker-{}", id))
            .spawn(move || {
                worker_loop(
                    id, config, queue_rx, queue_tx, entry_tx, entry_rx, writer, shutdown, stats_clone,
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

/// Main worker loop
fn worker_loop(
    id: usize,
    config: Arc<WalkConfig>,
    queue_rx: WorkQueueReceiver,
    queue_tx: WorkQueueSender,
    entry_tx: EntryQueueSender,
    entry_rx: EntryQueueReceiver,
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
        // First, try to get a directory task
        let task = match queue_rx.recv_timeout(Duration::from_millis(10)) {
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
        } else {
            // No directory tasks available - help process entries from large directories
            // This is how workers share the load when one directory has many files
            let entries = entry_rx.drain(100); // Process up to 100 entries at a time
            if !entries.is_empty() {
                for entry in entries {
                    if let Err(e) = writer.send_entry(entry) {
                        error!(worker = id, error = %e, "Failed to send entry to writer");
                    }
                }
            } else {
                // No entries to process either, wait a bit
                std::thread::sleep(Duration::from_millis(10));
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

/// Process a single directory using hybrid READDIRPLUS/skinny approach
///
/// Strategy:
/// 1. Probe first batch with READDIRPLUS to analyze directory composition
/// 2. If mostly files (< 1% directories), switch to skinny streaming for speed
/// 3. If many subdirectories, continue with READDIRPLUS for complete type info
/// 4. For skinny mode, stat Unknown entries at the end to find any subdirectories
fn process_directory(
    worker_id: usize,
    task: &DirTask,
    config: &WalkConfig,
    nfs: &mut NfsConnection,
    queue_tx: &WorkQueueSender,
    entry_tx: &EntryQueueSender,
    writer: &WriterHandle,
    stats: &WorkerStats,
) -> WalkOutcome {
    // Use simple READDIRPLUS-only path if skinny mode is disabled
    if config.disable_skinny {
        return process_directory_simple(worker_id, task, config, nfs, queue_tx, entry_tx, writer, stats);
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
    // Use regular opendir() which is known to work reliably
    info!(worker = worker_id, path = %task.path, "Starting directory probe");
    let probe_handle = match nfs.opendir(&task.path) {
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

    info!(
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

    // Decide: use skinny mode if probe is full AND directory ratio is low
    let use_skinny = probe_full && dir_ratio <= SKINNY_MAX_DIR_RATIO;

    if use_skinny {
        info!(
            worker = worker_id,
            path = %task.path,
            probe_size = probe_size,
            dir_ratio = %format!("{:.2}%", dir_ratio * 100.0),
            "Switching to skinny mode for large flat directory"
        );
    }

    stats.record_dir();

    // Track all results
    let mut dir_stats = DirStats::default();
    let mut entry_count = 0usize;
    let mut subdir_count = 0usize;
    let mut unknown_entries: Vec<(String, u64)> = Vec::new(); // (path, inode) for later stat

    // Phase 2: Continue reading the rest of the directory
    if use_skinny {
        // For skinny mode, we restart from the beginning with READDIR
        // because READDIRPLUS cookies may not be compatible with READDIR
        // The probe entries are NOT written to DB - skinny will read everything fresh
        drop(probe_handle);

        match continue_with_skinny_full(
            worker_id,
            task,
            config,
            nfs,
            queue_tx,
            writer,
            &mut dir_stats,
            &mut entry_count,
            &mut subdir_count,
            &mut unknown_entries,
        ) {
            Ok(()) => {}
            Err(e) => {
                warn!(worker = worker_id, path = %task.path, error = %e, "Skinny scan failed");
            }
        }
    } else {
        // READDIRPLUS mode: process probe entries first, then continue reading
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
    }

    // Phase 3: Resolve Unknown entries (from skinny mode) to find directories
    if !unknown_entries.is_empty() {
        let resolved_dirs = resolve_unknown_entries(
            worker_id,
            nfs,
            &unknown_entries,
        );

        for dir_path in resolved_dirs {
            subdir_count += 1;

            // Queue the directory for processing
            let should_queue = config
                .max_depth
                .map(|max| (task.depth + 1) as usize <= max)
                .unwrap_or(true);

            if should_queue && !config.is_excluded(&dir_path) {
                let subtask = DirTask::new(dir_path, None, task.depth + 1);
                if !queue_tx.try_send(subtask.clone()).unwrap_or(false) {
                    queue_tx.record_inline();
                    trace!(worker = worker_id, path = %subtask.path, "Backpressure - task will be retried");
                }
            }
        }
    }

    // Log large directories
    if entry_count > LARGE_DIR_THRESHOLD {
        info!(
            worker = worker_id,
            path = %task.path,
            entries = entry_count,
            mode = if use_skinny { "skinny" } else { "readdirplus" },
            "Processed large directory"
        );
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

/// Full skinny scan of a directory from the beginning
///
/// Used when we detect a large flat directory - reads everything with READDIR
/// instead of READDIRPLUS for better performance.
#[allow(clippy::too_many_arguments)]
fn continue_with_skinny_full(
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
) -> Result<(), crate::error::NfsError> {
    use std::collections::HashSet;

    let mut cookie: u64 = 0;
    let mut cookieverf: u64 = 0;
    let mut batch_num = 0u32;

    // Track first batch inodes to detect wrap-around
    let mut first_batch_inodes: HashSet<u64> = HashSet::new();
    let mut is_first_batch = true;

    info!(
        worker = worker_id,
        path = %task.path,
        "Starting skinny full scan"
    );

    // Safety limit - 100 batches of 10k = 1M entries max
    const MAX_BATCHES: u32 = 100;

    loop {
        info!(
            worker = worker_id,
            batch = batch_num,
            cookie = cookie,
            cookieverf = cookieverf,
            "Opening skinny batch"
        );

        let dir_handle = nfs.opendir_names_only_at_cookie(
            &task.path,
            cookie,
            cookieverf,
            SKINNY_BATCH_SIZE,
        )?;

        let mut batch_count = 0u32;
        let mut last_cookie = cookie;
        let mut wrap_detected = false;

        while let Some(entry) = dir_handle.readdir() {
            if entry.is_special() {
                continue;
            }

            // Check for wrap-around by seeing if we encounter an inode from first batch
            if !is_first_batch && first_batch_inodes.contains(&entry.inode) {
                warn!(
                    worker = worker_id,
                    batch = batch_num,
                    inode = entry.inode,
                    name = %entry.name,
                    total = *entry_count,
                    "Wrap-around detected - same inode seen again"
                );
                wrap_detected = true;
                break;
            }

            // Record first batch inodes for wrap detection
            if is_first_batch {
                first_batch_inodes.insert(entry.inode);
            }

            batch_count += 1;
            *entry_count += 1;
            last_cookie = entry.cookie;

            // Build path for this entry
            let entry_path = if task.path == "/" {
                format!("/{}", entry.name)
            } else {
                format!("{}/{}", task.path, entry.name)
            };

            // Skinny entries have Unknown type - track for later stat resolution
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

            // Create and send DB entry
            let db_entry = DbEntry::from_nfs_entry(&entry, &task.path, task.depth + 1);

            if !config.dirs_only || entry.entry_type == EntryType::Directory {
                if let Err(e) = writer.send_entry(db_entry) {
                    error!(worker = worker_id, error = %e, "Failed to send entry to writer");
                }
            }
        }

        is_first_batch = false;

        // Update cookie and verifier for next batch
        let new_verifier = dir_handle.get_cookieverf();

        // Log batch progress
        info!(
            worker = worker_id,
            batch = batch_num,
            entries = batch_count,
            total = *entry_count,
            cookie = last_cookie,
            new_verifier = new_verifier,
            "Skinny batch complete"
        );

        batch_num += 1;

        // Exit if wrap-around detected
        if wrap_detected {
            info!(
                worker = worker_id,
                total = *entry_count,
                "Exiting skinny scan due to wrap-around"
            );
            break;
        }

        // EOF when we get an empty batch
        if batch_count == 0 {
            info!(
                worker = worker_id,
                total = *entry_count,
                "Skinny scan complete - EOF reached"
            );
            break;
        }

        // Safety limit to prevent infinite loops
        if batch_num >= MAX_BATCHES {
            warn!(
                worker = worker_id,
                path = %task.path,
                batches = batch_num,
                entries = *entry_count,
                "Hit safety limit on skinny batches - stopping"
            );
            break;
        }

        // Update for next batch
        cookie = last_cookie;
        cookieverf = new_verifier;
    }

    Ok(())
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
        info!(
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
