//! Fast NFS Walker - Parallel READDIRPLUS
//!
//! A high-performance implementation that:
//! 1. Uses READDIRPLUS to get names AND attributes in one RPC call
//! 2. All workers read directories in parallel (no single coordinator)
//! 3. Sharded Parquet writers handle output (no single-writer contention)
//!
//! Architecture:
//! ```text
//! Directory Queue (crossbeam deque - work stealing)
//! │
//! ├── Worker 0: pop dir → READDIRPLUS → ShardedSender(entry) → push subdirs
//! ├── Worker 1: pop dir → READDIRPLUS → ShardedSender(entry) → push subdirs
//! └── Worker N: pop dir → READDIRPLUS → ShardedSender(entry) → push subdirs
//! │
//! └── N Parquet Writer Threads: recv batch → row group → part file
//! ```

use crate::config::WalkConfig;
use crate::error::{DirFailure, FailureKind, Result, WalkerError};
use crate::nfs::types::{display_path, extract_extension_bytes, DbEntry, EntryType};
use crate::nfs::{resolve_dns, NfsConnection, NfsConnectionBuilder};
use crate::parquet::direct_writer::{
    spawn_direct_parquet_writers, write_metadata_json as write_direct_metadata_json,
    DirectWriteConfig,
};
use crate::walker::resolve::{EntryResolver, NfsOps, ResolveStats};
use crate::walker::retry::{self, DirError, FailureLog, RetryPolicy};
use crate::walker::sharding::path_to_shard;
use crossbeam_channel::Sender;
use crossbeam_deque::{Injector, Stealer, Worker as DequeWorker};
use regex::Regex;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// Directory work item
#[derive(Debug, Clone)]
struct DirWork {
    path: Vec<u8>,
    depth: u32,
    /// Cached file handle from parent's READDIRPLUS response
    /// When set, we can skip LOOKUP RPCs and use this handle directly
    file_handle: Option<Vec<u8>>,
    /// Set only on continuation items produced by `worker_loop_pipelined`
    /// when a directory crosses `--big-dir-split-after` mid-enumeration.
    /// Carries the NFS cookie/cookieverf from the last completed page so
    /// the stealing worker resumes reading at exactly the next page,
    /// avoiding both double-reads and gaps.
    resume: Option<DirResume>,
}

/// Resume hint embedded in a continuation `DirWork`.
#[derive(Debug, Clone, Copy)]
struct DirResume {
    cookie: u64,
    cookieverf: [i8; 8],
}

impl DirWork {
    /// Construct a fresh work item — full enumeration from page 0.
    fn fresh(path: Vec<u8>, depth: u32, file_handle: Option<Vec<u8>>) -> Self {
        Self {
            path,
            depth,
            file_handle,
            resume: None,
        }
    }

    /// Construct a continuation produced by an in-flight pipelined slot
    /// that bailed at a page boundary. The file handle is mandatory
    /// (mid-dir resume requires it; a path-LOOKUP would not be safe).
    fn continuation(
        path: Vec<u8>,
        depth: u32,
        file_handle: Vec<u8>,
        cookie: u64,
        cookieverf: [i8; 8],
    ) -> Self {
        Self {
            path,
            depth,
            file_handle: Some(file_handle),
            resume: Some(DirResume { cookie, cookieverf }),
        }
    }
}

/// Join an absolute export path and one NFS directory-entry name without ever
/// interpreting either as UTF-8.
fn join_nfs_path(parent: &[u8], name: &[u8]) -> Vec<u8> {
    let mut path = Vec::with_capacity(parent.len() + name.len() + 1);
    if parent == b"/" {
        path.push(b'/');
    } else {
        path.extend_from_slice(parent);
        path.push(b'/');
    }
    path.extend_from_slice(name);
    path
}

/// Per-worker fan-out helper. Each entry is routed to its owning
/// path-shard channel by `path_to_shard(entry.path, shards)`. Workers
/// hold N partial batches in parallel; shards == 1 collapses to a
/// single channel and matches the legacy behavior bit-for-bit.
struct ShardedSender {
    senders: Vec<Sender<Vec<DbEntry>>>,
    batches: Vec<Vec<DbEntry>>,
    batch_size: usize,
    shards: usize,
}

impl ShardedSender {
    fn new(senders: Vec<Sender<Vec<DbEntry>>>, batch_size: usize) -> Self {
        let shards = senders.len();
        let batches = (0..shards)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        Self {
            senders,
            batches,
            batch_size,
            shards,
        }
    }

    /// Push one entry into its shard's pending batch. If that batch
    /// reaches `batch_size`, it's drained and shipped to its writer's
    /// channel; returns Err(()) when the channel is closed (writer
    /// gone, propagate as "shutdown").
    fn push(&mut self, entry: DbEntry) -> std::result::Result<(), ()> {
        let shard = path_to_shard(&entry.path, self.shards);
        let batch = &mut self.batches[shard];
        batch.push(entry);
        if batch.len() >= self.batch_size {
            let full = std::mem::replace(batch, Vec::with_capacity(self.batch_size));
            self.senders[shard].send(full).map_err(|_| ())?;
        }
        Ok(())
    }

    /// Drain residual partial batches at end-of-walk.
    fn flush_all(&mut self) {
        for (shard, batch) in self.batches.iter_mut().enumerate() {
            if !batch.is_empty() {
                let full = std::mem::take(batch);
                let _ = self.senders[shard].send(full);
            }
        }
    }
}

/// Result from walk operation
#[derive(Debug, Clone, Default)]
pub struct WalkStats {
    pub dirs: u64,
    pub files: u64,
    pub bytes: u64,
    /// Directories that could not be read, plus entries whose type
    /// could not be established (`unresolved_entries`), after the retry
    /// policy was exhausted. Any nonzero value makes the scan
    /// [`WalkerError::ScanIncomplete`]; a caller never sees a
    /// successful `WalkStats` with `errors > 0`.
    pub errors: u64,
    /// Directories that disappeared between their parent's listing and
    /// their own read, plus entries that disappeared before their
    /// attributes could be read (`vanished_entries`). Confirmed by
    /// LOOKUP. A race on a live tree, not a hole.
    pub vanished: u64,
    /// Entries READDIRPLUS returned without attributes that a GETATTR
    /// on their file handle resolved.
    pub resolved_by_getattr: u64,
    /// Entries READDIRPLUS returned without attributes or a usable
    /// handle that a LOOKUP resolved. The Linux server lists every
    /// mountpoint this way.
    pub resolved_by_lookup: u64,
    /// The part of `errors` that is entries whose type could not be
    /// established. They have no row and were not descended into.
    pub unresolved_entries: u64,
    /// The part of `vanished` that is entries, which have no row
    /// (a vanished directory's row was emitted by its parent).
    pub vanished_entries: u64,
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
    vanished_count: Arc<AtomicU64>,
    resolve_stats: ResolveStats,
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
            vanished_count: Arc::new(AtomicU64::new(0)),
            resolve_stats: ResolveStats::default(),
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
            total_workers: self.config.worker_count,
            elapsed,
        }
    }

    /// Drive the scan to completion.
    ///
    /// Fans out to `writer_shards` independent streaming Parquet writers
    /// (see `parquet::direct_writer`). Returns when every worker and
    /// every writer thread has joined, or with the first error
    /// encountered.
    pub fn run(&self) -> Result<WalkStats> {
        let start = Instant::now();
        let shards = self.config.writer_shards.max(1);

        let scan_id = uuid::Uuid::new_v4().to_string();
        let scan_timestamp_us = chrono::Utc::now().timestamp_micros();

        info!(
            "Opening direct-write Parquet output: {} (scan_id={}, shards={})",
            self.config.output_path.display(),
            scan_id,
            shards
        );

        let metrics = self.build_metrics(shards);
        metrics.set_output_path(self.config.output_path.clone());

        // Logger is started early so progress is visible before workers
        // come up. It's joined in the unconditional cleanup block below.
        let logger_handle = self.maybe_start_logger(metrics.clone(), start);

        // Run the body inside an IIFE so any `?` lands in `result`
        // without skipping the logger join below.
        let result = (|| -> Result<WalkStats> {
            let direct_cfg = DirectWriteConfig {
                output_dir: self.config.output_path.clone(),
                scan_id: scan_id.clone(),
                scan_timestamp_us,
                shards,
                row_group_size: crate::parquet::direct_writer::DEFAULT_ROW_GROUP_SIZE,
                target_file_size: crate::parquet::direct_writer::DEFAULT_TARGET_FILE_SIZE,
                compression: self.config.parquet_compression.to_direct_writer(),
                channel_depth: crate::parquet::direct_writer::DEFAULT_CHANNEL_DEPTH,
            };

            let pool = spawn_direct_parquet_writers(direct_cfg, metrics.clone())?;

            // Register clones for queue-depth observation. MUST be paired
            // with `release_write_senders()` before joining writers.
            for tx in &pool.senders {
                metrics.register_write_sender(tx.clone());
            }

            let scan_dir = pool.scan_dir.clone();
            // Every directory failure and vanished directory of this
            // scan, beside the part files it belongs with.
            let failures = Arc::new(
                FailureLog::open(&scan_dir.join(retry::FAILURE_LOG_NAME))
                    .map_err(WalkerError::Io)?,
            );
            let workers_result =
                self.run_workers(pool.senders, metrics.clone(), Arc::clone(&failures));

            // ALWAYS release the observability clones and join the
            // writer threads, regardless of whether the workers
            // succeeded. Skipping this step (e.g. by `?`-propagating
            // workers_result here) would leave the writer threads
            // blocked on recv() with the channels held open via the
            // metrics-registered sender clones, and the main thread
            // would exit before they finish flushing footers — yielding
            // truncated `.parquet` files on disk.
            metrics.release_write_senders();
            let mut summaries = Vec::with_capacity(pool.joins.len());
            let mut writer_err: Option<WalkerError> = None;
            for (idx, h) in pool.joins.into_iter().enumerate() {
                match h.join() {
                    Ok(Ok(s)) => summaries.push(s),
                    Ok(Err(e)) => {
                        if writer_err.is_none() {
                            writer_err = Some(e);
                        }
                        warn!("parquet writer shard {} failed", idx);
                    }
                    Err(_) => {
                        if writer_err.is_none() {
                            writer_err =
                                Some(WalkerError::Parquet(crate::error::ParquetError::Other(
                                    format!("parquet writer shard {} panicked", idx),
                                )));
                        }
                    }
                }
            }

            // Workers' error wins (it's the upstream cause). Only if
            // workers succeeded does a writer error become the failure.
            workers_result?;
            if let Some(e) = writer_err {
                return Err(e);
            }

            let source_url = self.config.nfs_url.to_display_string();
            let (total_entries, _total_bytes, _files) = write_direct_metadata_json(
                &scan_dir,
                &scan_id,
                scan_timestamp_us,
                &source_url,
                &summaries,
            )?;

            let dirs = self.dirs_count.load(Ordering::Relaxed);
            let files = self.files_count.load(Ordering::Relaxed);
            let bytes = self.bytes_count.load(Ordering::Relaxed);
            let errors = self.errors_count.load(Ordering::Relaxed);
            let vanished = self.vanished_count.load(Ordering::Relaxed);
            failures.flush();

            // Walker counters and parquet row count measure different
            // things and can't be compared directly:
            //   - `dirs_count` counts directories we READ (called
            //     readdirplus on), not directories we EMITTED.
            //   - `files_count` counts files we saw inside those reads.
            //   - parquet rows = every non-dot entry returned by any
            //     readdir that we completed (subdirs are emitted by their
            //     parent's readdir even when we don't recurse into them).
            //
            // The relation with no depth limit, no dirs-only mode, and
            // no exclusions is `parquet_rows == files + dirs - 1` (minus
            // one because the root directory is never emitted as a child
            // of its parent). `dirs_only` makes the worker drop file
            // entries before they reach the writer, which breaks the
            // relation. `max_depth` adds a "seen but skipped" term we
            // don't track separately; `--exclude` drops matched entries
            // before both counting and emission — either disables the
            // check.
            if self.config.max_depth.is_none()
                && !self.config.dirs_only
                && self.config.exclude_patterns.is_empty()
                && self.config.exclude_dirs.is_empty()
            {
                let expected = files.saturating_add(dirs.saturating_sub(1));
                if total_entries != expected {
                    warn!(
                        "parquet row count {} != expected {} (files {} + dirs {} - 1) — \
                         entries may have been dropped",
                        total_entries, expected, files, dirs
                    );
                }
            }

            info!(
                "Direct-write Parquet scan complete: {} rows in {} part files",
                total_entries,
                summaries.iter().map(|s| s.part_files.len()).sum::<usize>()
            );

            let resolved_by_getattr = self.resolve_stats.by_getattr.load(Ordering::Relaxed);
            let resolved_by_lookup = self.resolve_stats.by_lookup.load(Ordering::Relaxed);
            if resolved_by_getattr + resolved_by_lookup > 0 {
                info!(
                    "READDIRPLUS returned {} entries without attributes: {} resolved by GETATTR, \
                     {} by LOOKUP",
                    resolved_by_getattr + resolved_by_lookup,
                    resolved_by_getattr,
                    resolved_by_lookup
                );
            }

            let stats = WalkStats {
                dirs,
                files,
                bytes,
                errors,
                vanished,
                resolved_by_getattr,
                resolved_by_lookup,
                unresolved_entries: self.resolve_stats.unresolved.load(Ordering::Relaxed),
                vanished_entries: self.resolve_stats.vanished.load(Ordering::Relaxed),
                duration: start.elapsed(),
                completed: !self.shutdown.load(Ordering::Relaxed),
            };
            if errors > 0 {
                // The part files are intact for diagnosis, but a tree
                // with unread directories is not an index of the tree.
                return Err(WalkerError::ScanIncomplete {
                    stats: Box::new(stats),
                    failures: failures.samples(),
                    failure_log: failures.path().map(Path::to_path_buf),
                });
            }
            Ok(stats)
        })();

        // ALWAYS stop the logger and join its thread — must run on both
        // success and failure paths.
        metrics.signal_shutdown();
        if let Some(h) = logger_handle {
            let _ = h.join();
        }

        result
    }

    /// Build a fresh `ScanMetrics` populated with references to the walker's
    /// existing scan counters.
    fn build_metrics(&self, shards: usize) -> Arc<crate::scanlog::ScanMetrics> {
        let counters = crate::scanlog::CounterRefs {
            dirs: Arc::clone(&self.dirs_count),
            files: Arc::clone(&self.files_count),
            bytes: Arc::clone(&self.bytes_count),
            errors: Arc::clone(&self.errors_count),
            // Active-worker tracking is currently per-run inside the worker
            // pool. Hand the snapshot thread a fresh atomic — it'll show 0
            // until the per-run instrumentation lands. (Phase-2 follow-up.)
            active_workers: Arc::new(AtomicUsize::new(0)),
        };
        crate::scanlog::ScanMetrics::new(self.config.worker_count, shards, counters)
    }

    /// Spawn the per-scan progress logger if `--log` is enabled. Returns
    /// `None` when the user passed `--no-log`.
    fn maybe_start_logger(
        &self,
        metrics: Arc<crate::scanlog::ScanMetrics>,
        started_at: Instant,
    ) -> Option<JoinHandle<()>> {
        let cfg = self.config.log.as_ref()?;
        let log_cfg = crate::scanlog::LogConfig::new(cfg.path.clone(), cfg.format, cfg.interval);
        match crate::scanlog::start_logger(metrics, log_cfg, started_at) {
            Ok(h) => Some(h),
            Err(e) => {
                warn!("Failed to start progress logfile: {}", e);
                None
            }
        }
    }

    /// Run worker threads.
    ///
    /// `entry_txs` carries one sender per parquet writer shard. Each
    /// worker routes per-entry via `gxhash(path) % N` into the matching
    /// writer's channel.
    fn run_workers(
        &self,
        entry_txs: Vec<Sender<Vec<DbEntry>>>,
        metrics: Arc<crate::scanlog::ScanMetrics>,
        failures: Arc<FailureLog>,
    ) -> Result<()> {
        // Work-stealing deque for directories
        let injector: Arc<Injector<DirWork>> = Arc::new(Injector::new());

        // Track active workers and pending work. Sharing the
        // active_workers atomic with `metrics` lets the snapshot thread
        // read the same value without double-bookkeeping.
        let active_workers = metrics.active_workers();
        let pending_work = Arc::new(AtomicU64::new(1)); // Start with 1 for root

        // Push root directory (no cached file handle - will do path lookup)
        let start_path = self.config.nfs_url.walk_start_path().into_bytes();
        injector.push(DirWork::fresh(start_path.clone(), 0, None));

        // Create worker local queues and stealers
        let mut workers_local: Vec<DequeWorker<DirWork>> = Vec::new();
        let mut stealers: Vec<Stealer<DirWork>> = Vec::new();

        for _ in 0..self.config.worker_count {
            let w = DequeWorker::new_fifo();
            stealers.push(w.stealer());
            workers_local.push(w);
        }

        let stealers = Arc::new(stealers);

        // Get server IPs. If --server-ips was passed, use that list verbatim
        // (bypasses DNS — required when the auth server returns a single A
        // record per query and the local resolver caches it). Otherwise
        // resolve DNS for round-robin load balancing.
        let server_ips = if !self.config.server_ips.is_empty() {
            info!(
                "Using {} explicit server VIPs (--server-ips), skipping DNS: {:?}",
                self.config.server_ips.len(),
                self.config.server_ips
            );
            self.config.server_ips.clone()
        } else {
            let ips = resolve_dns(&self.config.nfs_url.server);
            if ips.len() > 1 {
                info!(
                    "DNS resolved {} to {} IPs: {:?}",
                    self.config.nfs_url.server,
                    ips.len(),
                    ips
                );
            } else if ips.len() == 1 {
                info!(
                    "DNS resolved {} to a single IP ({}). If the server actually has more VIPs, \
                     pass --server-ips IP1,IP2,... to use them all.",
                    self.config.nfs_url.server, ips[0]
                );
            }
            ips
        };

        // Per-VIP consecutive-failure counts. An IP is skipped once it
        // reaches FAIL_THRESHOLD. Any successful mount on any IP clears
        // all counts (VIPs come back; transient flakes shouldn't retire
        // them). With the builder's own --retries (default 3) baked into
        // each `create_connection_with_ip` attempt, threshold=3 means a
        // truly-dead VIP costs ~3 × (timeout × retries) before the rest
        // of the spawn loop stops touching it.
        const FAIL_THRESHOLD: u32 = 3;
        let mut fail_counts: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();

        // Spawn workers. A connection failure must NOT short-circuit out
        // of this function via `?` — already-spawned workers below would
        // be detached, their senders would stay alive (cloned into each
        // thread), and the caller would never get to join the writer
        // threads. Instead, capture the error, signal shutdown so any
        // already-spawned worker exits its loop, then fall through to
        // the drop+join cleanup at the end.
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        let mut spawn_err: Option<WalkerError> = None;

        // Shared compiled --exclude patterns; workers skip matched paths
        // (no emission, no descent). Empty for the common case.
        let exclude: Arc<Vec<Regex>> = Arc::new(self.config.exclude_patterns.clone());
        // Shared compiled --exclude-dir globs; a matching directory name
        // is dropped at the entry, so its subtree is never visited.
        let exclude_dirs: Arc<Vec<Regex>> = Arc::new(self.config.exclude_dirs.clone());

        'spawn: for (id, local) in workers_local.into_iter().enumerate() {
            // Pick an IP with failover. The round-robin position is the
            // first choice; on failure, walk the rest of the pool in
            // order. An IP whose consecutive-failure count is at or
            // above FAIL_THRESHOLD is skipped. Fatal only if EVERY IP is
            // either at threshold or fails this worker's attempt.
            //
            // `server_ips` is guaranteed non-empty: either `--server-ips`
            // populated it (config validation rejects empty) or
            // `resolve_dns` returned at least the hostname-as-passthrough
            // fallback. Asserting here so a future contract break is
            // loud in dev rather than silent in release.
            debug_assert!(
                !server_ips.is_empty(),
                "server_ips must be non-empty: --server-ips validation and resolve_dns fallback both guarantee it"
            );
            let nfs = {
                let n = server_ips.len();
                let primary_idx = id % n;
                let mut connected: Option<NfsConnection> = None;
                // Lazy: only allocate when a failover actually happens.
                // The common success-on-first-try path never touches the
                // Vec at all.
                let mut tried: Option<Vec<String>> = None;
                let mut last_err: Option<WalkerError> = None;
                for offset in 0..n {
                    let ip = &server_ips[(primary_idx + offset) % n];
                    if fail_counts.get(ip).copied().unwrap_or(0) >= FAIL_THRESHOLD {
                        continue;
                    }
                    match self.create_connection_with_ip(Some(ip)) {
                        Ok(c) => {
                            if let Some(t) = &tried {
                                info!(
                                    "Worker {} mounted via {} after failover from {:?}",
                                    id, ip, t
                                );
                            }
                            if fail_counts.values().any(|v| *v > 0) {
                                info!(
                                    "Worker {} mounted successfully on {}; clearing failure counts (VIP recovery)",
                                    id, ip
                                );
                                fail_counts.clear();
                            }
                            connected = Some(c);
                            break;
                        }
                        Err(e) => {
                            let count = fail_counts.entry(ip.clone()).or_insert(0);
                            *count = count.saturating_add(1);
                            warn!(
                                "Worker {} mount on VIP {} failed: {} (consecutive failures: {}/{})",
                                id, ip, e, count, FAIL_THRESHOLD
                            );
                            tried.get_or_insert_with(Vec::new).push(ip.clone());
                            last_err = Some(e);
                        }
                    }
                }
                match connected {
                    Some(c) => c,
                    None => {
                        let blacklisted = fail_counts
                            .values()
                            .filter(|v| **v >= FAIL_THRESHOLD)
                            .count();
                        error!(
                            "Worker {} could not mount on any of {} VIPs ({} at fail-threshold). Aborting.",
                            id, n, blacklisted
                        );
                        self.shutdown.store(true, Ordering::SeqCst);
                        spawn_err = Some(last_err.unwrap_or_else(|| {
                            WalkerError::Nfs(crate::error::NfsError::ConnectionFailed {
                                server: self.config.nfs_url.server.clone(),
                                reason: format!(
                                    "worker {} could not mount on any of {} VIPs (all at fail-threshold from earlier workers)",
                                    id, n
                                ),
                            })
                        }));
                        break 'spawn;
                    }
                }
            };
            info!("Worker {} connected to {}", id, nfs.server());

            let injector = Arc::clone(&injector);
            let stealers = Arc::clone(&stealers);
            let entry_txs = entry_txs.clone();
            let shutdown = Arc::clone(&self.shutdown);
            let dirs_count = Arc::clone(&self.dirs_count);
            let files_count = Arc::clone(&self.files_count);
            let bytes_count = Arc::clone(&self.bytes_count);
            let errors_count = Arc::clone(&self.errors_count);
            let failures = Arc::clone(&failures);
            let vanished_count = Arc::clone(&self.vanished_count);
            let resolve_stats = self.resolve_stats.clone();
            let retry_policy = RetryPolicy::from_retries(self.config.retry_count);
            let active_workers = Arc::clone(&active_workers);
            let pending_work = Arc::clone(&pending_work);
            let metrics = Arc::clone(&metrics);
            let max_depth = self.config.max_depth;
            let dirs_only = self.config.dirs_only;
            let batch_size = self.config.batch_size;
            let pipeline_depth = self.config.pipeline_depth;
            let big_dir_split_after = self.config.big_dir_split_after;
            let exclude = Arc::clone(&exclude);
            let exclude_dirs = Arc::clone(&exclude_dirs);

            let handle = thread::Builder::new()
                .name(format!("walker-{}", id))
                .spawn(move || {
                    if pipeline_depth > 0 {
                        worker_loop_pipelined(
                            id,
                            nfs,
                            local,
                            injector,
                            stealers,
                            entry_txs,
                            shutdown,
                            dirs_count,
                            files_count,
                            bytes_count,
                            errors_count,
                            failures,
                            vanished_count,
                            resolve_stats,
                            retry_policy,
                            active_workers,
                            pending_work,
                            max_depth,
                            dirs_only,
                            exclude,
                            exclude_dirs,
                            batch_size,
                            pipeline_depth,
                            big_dir_split_after,
                            metrics,
                        );
                    } else {
                        worker_loop(
                            id,
                            nfs,
                            local,
                            injector,
                            stealers,
                            entry_txs,
                            shutdown,
                            dirs_count,
                            files_count,
                            bytes_count,
                            errors_count,
                            failures,
                            vanished_count,
                            resolve_stats,
                            retry_policy,
                            active_workers,
                            pending_work,
                            max_depth,
                            dirs_only,
                            exclude,
                            exclude_dirs,
                            batch_size,
                            metrics,
                        );
                    }
                })
                .expect("Failed to spawn worker thread");

            handles.push(handle);
        }

        // Drop our senders so writers know when to stop. (Per-worker
        // sender clones still live inside each thread and drop when the
        // thread returns from its loop.)
        drop(entry_txs);

        // Wait for all already-spawned workers. ALWAYS run this — even
        // when spawn_err is Some, we must drain the threads we already
        // launched before returning, otherwise our caller's writer-join
        // step deadlocks or partially-completes.
        for handle in handles {
            let _ = handle.join();
        }

        match spawn_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
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

    fn create_connection_with_ip(&self, ip: Option<&str>) -> Result<NfsConnection> {
        let timeout = Duration::from_secs(self.config.timeout_secs as u64);
        let mut builder = NfsConnectionBuilder::new(self.config.nfs_url.clone())
            .timeout(timeout)
            .retries(self.config.retry_count);

        if let Some(ip) = ip {
            builder = builder.with_ip(ip.to_string());
        }

        builder.connect().map_err(WalkerError::Nfs)
    }
}

/// Worker thread - processes directories using READDIRPLUS.
///
/// `entry_txs` carries one sender per writer shard. The worker holds an
/// equal number of pending `batch` slots inside `ShardedSender`; entries
/// route per-path via `gxhash % shards`. With shards == 1 this collapses
/// to one batch + one channel — bit-identical to the legacy worker.
#[allow(clippy::too_many_arguments)]
fn worker_loop<N: NfsOps>(
    id: usize,
    nfs: N,
    local: DequeWorker<DirWork>,
    injector: Arc<Injector<DirWork>>,
    stealers: Arc<Vec<Stealer<DirWork>>>,
    entry_txs: Vec<Sender<Vec<DbEntry>>>,
    shutdown: Arc<AtomicBool>,
    dirs_count: Arc<AtomicU64>,
    files_count: Arc<AtomicU64>,
    bytes_count: Arc<AtomicU64>,
    errors_count: Arc<AtomicU64>,
    failures: Arc<FailureLog>,
    vanished_count: Arc<AtomicU64>,
    resolve_stats: ResolveStats,
    retry_policy: RetryPolicy,
    active_workers: Arc<AtomicUsize>,
    pending_work: Arc<AtomicU64>,
    max_depth: Option<usize>,
    dirs_only: bool,
    exclude: Arc<Vec<Regex>>,
    exclude_dirs: Arc<Vec<Regex>>,
    batch_size: usize,
    metrics: Arc<crate::scanlog::ScanMetrics>,
) {
    debug!("Worker {} started", id);

    let resolver = EntryResolver {
        worker: id,
        policy: &retry_policy,
        max_sleep: Duration::MAX,
        failures: &failures,
        errors: &errors_count,
        vanished: &vanished_count,
        stats: &resolve_stats,
    };
    let mut sender = ShardedSender::new(entry_txs, batch_size);
    let mut idle_spins = 0;
    // Steal-sweep budget before yielding. Each idle spin scans the
    // injector plus every peer's stealer; a large budget just burns
    // cores and cache-line traffic at the scan tail with many workers.
    const MAX_IDLE_SPINS: u32 = 64;

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
                if i == id {
                    continue;
                }
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
                // Continuations are produced only by
                // worker_loop_pipelined; pipeline_depth is fixed per
                // run so this branch should never see one. Refuse
                // explicitly rather than silently re-reading from
                // cookie 0 (which would double-count every page the
                // producing worker already emitted).
                if w.resume.is_some() {
                    error!(
                        "Worker {} (legacy) refusing continuation work for {} — \
                         continuations require --pipeline-depth > 0",
                        id,
                        display_path(&w.path)
                    );
                    pending_work.fetch_sub(1, Ordering::SeqCst);
                    active_workers.fetch_sub(1, Ordering::Relaxed);
                    errors_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                // Legacy worker only has one in-flight at a time, so a
                // fixed tag uniquely identifies its slot.
                metrics.enter_dir(id, 0, display_path(&w.path).into_owned());
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
                metrics.exit_dir(id, 0);
                continue;
            }
        }

        // Log whether we're using cached file handle or path
        if work.file_handle.is_some() {
            debug!(
                "Worker {} READDIRPLUS (cached FH): {}",
                id,
                display_path(&work.path)
            );
        } else {
            debug!(
                "Worker {} READDIRPLUS (path lookup): {}",
                id,
                display_path(&work.path)
            );
        }

        // Read directory with READDIRPLUS in chunks for immediate processing
        // This ensures entries start flowing to the DB immediately, even for
        // directories with millions of files. Progress counters are updated
        // incrementally so the UI shows real-time progress.
        //
        // OPTIMIZATION: When we have a cached file handle from the parent's
        // READDIRPLUS response, we use it directly to avoid LOOKUP RPCs.
        // This is critical for narrow-deep trees where path resolution
        // would cause O(n²) LOOKUPs.
        let mut subdir_count = 0usize;
        let mut chunk_file_count = 0u64;
        let mut chunk_byte_count = 0u64;
        let mut channel_broken = false;

        // Define the callback that processes directory entries
        // This is used by both readdir_plus_by_fh and readdir_plus_with_fh.
        // "." / ".." never reach here — readdirplus_full_callback strips
        // them at the FFI boundary.
        let metrics_for_chunks = Arc::clone(&metrics);
        // Once any page of this directory has reached the writers a
        // retry would duplicate rows; the retry loop checks this.
        let emitted = std::cell::Cell::new(false);
        // The handle this directory is being read through: the parent
        // for a LOOKUP of an entry READDIRPLUS returned without one.
        // `None` while the directory is read by path; resolved on
        // first use.
        let dir_fh = std::cell::RefCell::new(work.file_handle.clone());
        let mut process_entries = |chunk: Vec<crate::nfs::types::NfsDirEntry>| -> bool {
            if !chunk.is_empty() {
                emitted.set(true);
            }
            metrics_for_chunks.record_entries(id, 0, chunk.len() as u64);
            for mut nfs_entry in chunk {
                let full_path = join_nfs_path(&work.path, &nfs_entry.name);

                // READDIRPLUS may leave out an entry's attributes. Fetch
                // them before the entry is classified: without a type it
                // can be neither emitted nor descended into. A path
                // exclusion applies to every type, so it needs none.
                if nfs_entry.entry_type == EntryType::Unknown
                    && (crate::config::excluded_entry_bytes(
                        &nfs_entry.name,
                        false,
                        &full_path,
                        &exclude,
                        &[],
                    ) || !resolver.settle(
                        &nfs,
                        &mut nfs_entry,
                        &full_path,
                        &work.path,
                        &dir_fh,
                    ))
                {
                    continue;
                }

                let is_dir = nfs_entry.entry_type == EntryType::Directory;

                // Skip files if dirs_only mode
                if dirs_only && !is_dir {
                    continue;
                }

                // --exclude (path regex) and --exclude-dir (name glob):
                // a match is neither emitted nor descended into, so an
                // excluded directory's whole subtree is absent. Empty in
                // the common case.
                if crate::config::excluded_entry_bytes(
                    &nfs_entry.name,
                    is_dir,
                    &full_path,
                    &exclude,
                    &exclude_dirs,
                ) {
                    continue;
                }

                let size = nfs_entry.size();

                if is_dir {
                    // Queue subdirectory for processing with cached file
                    // handle. The only per-entry clone left in this loop:
                    // the path is needed by both the DirWork and the
                    // DbEntry, and only for directories.
                    subdir_count += 1;
                    pending_work.fetch_add(1, Ordering::SeqCst);
                    local.push(DirWork::fresh(
                        full_path.clone(),
                        work.depth + 1,
                        nfs_entry.file_handle.take(),
                    ));
                } else {
                    chunk_file_count += 1;
                    chunk_byte_count += size;
                }

                // Create DB entry from READDIRPLUS attributes. `name` is
                // moved (the loop owns nfs_entry), `path` is moved, and
                // parent_path stays None — the writer derives it from
                // `path` zero-copy — so files cost zero extra allocations
                // here.
                let db_entry = DbEntry {
                    parent_path: None,
                    path: full_path,
                    entry_type: nfs_entry.entry_type,
                    size,
                    mtime_sec: nfs_entry.mtime_sec(),
                    mtime_nsec: nfs_entry.mtime_nsec(),
                    atime_sec: nfs_entry.atime_sec(),
                    atime_nsec: nfs_entry.atime_nsec(),
                    ctime_sec: nfs_entry.ctime_sec(),
                    ctime_nsec: nfs_entry.ctime_nsec(),
                    mode: nfs_entry.mode(),
                    uid: nfs_entry.uid(),
                    gid: nfs_entry.gid(),
                    nlink: nfs_entry.nlink(),
                    inode: nfs_entry.inode,
                    fsid: nfs_entry.fsid(),
                    depth: work.depth + 1,
                    extension: if nfs_entry.entry_type == EntryType::File {
                        extract_extension_bytes(&nfs_entry.name)
                    } else {
                        None
                    },
                    blocks: nfs_entry.blocks(),
                    name: nfs_entry.name,
                };

                // Push straight into the ShardedSender; it auto-flushes
                // the per-shard batch when full.
                if sender.push(db_entry).is_err() {
                    channel_broken = true;
                    return false;
                }
                // Update progress counters incrementally for real-time
                // display (matters on flat dirs with millions of entries).
                if chunk_file_count >= batch_size as u64 {
                    files_count.fetch_add(chunk_file_count, Ordering::Relaxed);
                    bytes_count.fetch_add(chunk_byte_count, Ordering::Relaxed);
                    chunk_file_count = 0;
                    chunk_byte_count = 0;
                }
            }
            !channel_broken // Continue reading if channel is OK
        };

        // Use cached file handle if available, otherwise resolve path.
        // Time the RPC for the per-scan progress logfile.
        let rpc_start = Instant::now();
        let result = retry::read_dir_with_retry(
            &work.path,
            work.file_handle.clone(),
            &retry_policy,
            |fh| match fh {
                Some(fh) => {
                    // A retry may come with a re-resolved handle.
                    if dir_fh.borrow().as_deref() != Some(fh) {
                        *dir_fh.borrow_mut() = Some(fh.to_vec());
                    }
                    nfs.readdir_plus_by_fh(fh, batch_size, &mut process_entries)
                }
                None => nfs.readdir_plus_with_fh(&work.path, batch_size, &mut process_entries),
            },
            || nfs.resolve_path_to_fh(&work.path),
            || emitted.get(),
            thread::sleep,
        );
        metrics.record_nfs_latency(id, rpc_start.elapsed());

        match result {
            Ok(entry_count) => {
                // Update directory count and any remaining files from final partial batch
                dirs_count.fetch_add(1, Ordering::Relaxed);
                files_count.fetch_add(chunk_file_count, Ordering::Relaxed);
                bytes_count.fetch_add(chunk_byte_count, Ordering::Relaxed);

                debug!(
                    "Worker {} READDIRPLUS complete: {} -> {} entries ({} subdirs)",
                    id,
                    display_path(&work.path),
                    entry_count,
                    subdir_count
                );
            }
            Err(DirError::Vanished { attempts }) => {
                // Deleted or renamed between its parent's listing and
                // now: a race on a live tree, recorded, not an error.
                vanished_count.fetch_add(1, Ordering::Relaxed);
                failures.record_vanished(&work.path, attempts);
                debug!(
                    "Worker {} directory vanished during the scan: {} (after {} attempts)",
                    id,
                    display_path(&work.path),
                    attempts
                );
            }
            Err(DirError::Failed(f)) => {
                errors_count.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Worker {} could not read {}: {} ({}, {} attempts) — the scan will be reported incomplete",
                    id, f.path, f.error, f.kind, f.attempts
                );
                failures.record(&f);

                // An RPC timeout poisons the connection (see
                // NfsConnection::poison) — it can never be serviced
                // again, and the retry policy already declined to
                // retry on it. Exit so the remaining queue is stolen
                // by workers with live connections.
                if !nfs.is_connected() {
                    error!(
                        "Worker {} connection poisoned after RPC failure; exiting \
                         (peers steal its remaining queue)",
                        id
                    );
                    pending_work.fetch_sub(1, Ordering::SeqCst);
                    active_workers.fetch_sub(1, Ordering::Relaxed);
                    metrics.exit_dir(id, 0);
                    break;
                }
            }
        }

        // Mark this work item as done
        pending_work.fetch_sub(1, Ordering::SeqCst);
        active_workers.fetch_sub(1, Ordering::Relaxed);
        metrics.exit_dir(id, 0);
    }

    // Ship any partial per-shard batches left in the ShardedSender.
    sender.flush_all();

    debug!("Worker {} finished", id);
}

// ============================================================
// Pipelined worker loop
// ============================================================
//
// Selected when `--pipeline-depth N > 0`. Holds up to N READDIRPLUS
// RPCs in flight on this worker's single libnfs context, demuxing
// completions as they arrive. See `docs/PIPELINED_READDIRPLUS_DESIGN.md`.
//
// The legacy `worker_loop` above must remain bit-for-bit identical to
// its pre-pipelining behavior — error logging and counter ordering
// live there. The pipelined worker duplicates the entry-emission logic
// inline rather than refactoring the legacy closure (intentional
// duplication; revisit once pipelining is the default).

/// Per-slot state tracked alongside an in-flight READDIRPLUS.
struct DirState {
    /// The original work item (path, depth). `work.file_handle` is
    /// informational; the authoritative handle lives in `file_handle`.
    work: DirWork,
    /// The directory file handle used for every submit in this dir
    /// (set on first submit, reused for cookie-chain re-submits).
    file_handle: Vec<u8>,
    cookie: u64,
    cookieverf: [i8; 8],
    /// Wall-clock at last submit. Reset on every cookie-chain re-submit;
    /// used to compute per-RPC NFS latency on completion.
    submitted_at: Instant,
    /// Tag of the most recent submit. Updated on every cookie-chain
    /// re-submit. Used to key the per-slot HotDir entry in scanlog so
    /// concurrent in-flight slots in the same worker don't clobber
    /// each other's tracking state.
    tag: u64,
    /// Cumulative entries returned by READDIRPLUS pages for this slot
    /// (across cookie-chain re-submits). Compared against
    /// `--big-dir-split-after` to decide when to push a continuation.
    entries_seen: u64,
    /// Attempts made on this directory so far (first-page retries).
    attempts: u32,
}

/// What to do with a pipelined directory whose page errored.
enum Redo {
    Resubmit(Vec<u8>),
    Vanished,
    Fail,
}

/// Should the current dir bail at the next page boundary and push a
/// continuation? Pure helper so the truth table is unit-testable.
///
/// `threshold == 0` disables splitting entirely. `eof` always wins —
/// once the server reports EOF there's nothing left to hand off.
#[inline]
fn should_split_now(entries_seen: u64, threshold: u64, eof: bool) -> bool {
    threshold > 0 && !eof && entries_seen >= threshold
}

/// Try to grab a work item from local / injector / stealers (mirrors
/// the legacy worker's lookup logic).
fn try_get_work(
    local: &DequeWorker<DirWork>,
    injector: &Injector<DirWork>,
    stealers: &[Stealer<DirWork>],
    self_id: usize,
) -> Option<DirWork> {
    if let Some(w) = local.pop() {
        return Some(w);
    }
    loop {
        match injector.steal() {
            crossbeam_deque::Steal::Success(w) => return Some(w),
            crossbeam_deque::Steal::Empty => break,
            crossbeam_deque::Steal::Retry => continue,
        }
    }
    for (i, stealer) in stealers.iter().enumerate() {
        if i == self_id {
            continue;
        }
        loop {
            match stealer.steal() {
                crossbeam_deque::Steal::Success(w) => return Some(w),
                crossbeam_deque::Steal::Empty => break,
                crossbeam_deque::Steal::Retry => continue,
            }
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn worker_loop_pipelined<N: NfsOps>(
    id: usize,
    nfs: N,
    local: DequeWorker<DirWork>,
    injector: Arc<Injector<DirWork>>,
    stealers: Arc<Vec<Stealer<DirWork>>>,
    entry_txs: Vec<Sender<Vec<DbEntry>>>,
    shutdown: Arc<AtomicBool>,
    dirs_count: Arc<AtomicU64>,
    files_count: Arc<AtomicU64>,
    bytes_count: Arc<AtomicU64>,
    errors_count: Arc<AtomicU64>,
    failures: Arc<FailureLog>,
    vanished_count: Arc<AtomicU64>,
    resolve_stats: ResolveStats,
    retry_policy: RetryPolicy,
    active_workers: Arc<AtomicUsize>,
    pending_work: Arc<AtomicU64>,
    max_depth: Option<usize>,
    dirs_only: bool,
    exclude: Arc<Vec<Regex>>,
    exclude_dirs: Arc<Vec<Regex>>,
    batch_size: usize,
    pipeline_depth: usize,
    big_dir_split_after: u64,
    metrics: Arc<crate::scanlog::ScanMetrics>,
) {
    debug!("Worker {} (pipelined depth={}) started", id, pipeline_depth);

    let resolver = EntryResolver {
        worker: id,
        policy: &retry_policy,
        // Short, so the in-flight READDIRPLUS slots stay serviced.
        max_sleep: Duration::from_millis(250),
        failures: &failures,
        errors: &errors_count,
        vanished: &vanished_count,
        stats: &resolve_stats,
    };

    // Window the libnfs poll relatively tightly so a worker holding a
    // few slow in-flight slots can still return promptly to refill
    // empty slots from its deque or notice shutdown.
    const POLL_STEP_MS: i32 = 10;

    let mut slots: Vec<crate::nfs::connection::InflightReaddir> =
        Vec::with_capacity(pipeline_depth);
    let mut states: Vec<DirState> = Vec::with_capacity(pipeline_depth);
    let mut sender = ShardedSender::new(entry_txs, batch_size);
    let mut next_tag: u64 = (id as u64) << 48;
    // Local active-flag mirrors the legacy active_workers semantics:
    // counts as "active" while this worker holds at least one in-flight
    // slot. Used for the (pending_work==0 && active_workers==0)
    // termination check.
    let mut active_flag = false;

    'outer: loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // A poisoned connection (RPC timeout in a sync LOOKUP) can never
        // be serviced again. Fail the in-flight slots and exit so peers
        // steal the remaining queue — without this check the worker
        // would drain the global queue, erroring every item.
        if !nfs.is_connected() {
            error!(
                "Worker {} connection poisoned; exiting (dropping {} in-flight slots)",
                id,
                slots.len()
            );
            let n = slots.len() as u64;
            errors_count.fetch_add(n, Ordering::Relaxed);
            pending_work.fetch_sub(n, Ordering::SeqCst);
            for s in &states {
                metrics.exit_dir(id, s.tag);
            }
            slots.clear();
            states.clear();
            break;
        }

        // -------- 1. Refill empty slots. --------
        while slots.len() < pipeline_depth {
            let Some(work) = try_get_work(&local, &injector, &stealers, id) else {
                break;
            };

            // Honor max_depth identically to the legacy worker.
            if let Some(max) = max_depth {
                if work.depth > max as u32 {
                    pending_work.fetch_sub(1, Ordering::SeqCst);
                    continue;
                }
            }

            // Resolve fh: cached if present (steady state), else
            // synchronous LOOKUP chain (root dir, or externally
            // injected dirs without a cached fh).
            let fh = match work.file_handle.clone() {
                Some(fh) => fh,
                None => {
                    // Bounded retry on the LOOKUP; a short cap on the
                    // backoff keeps in-flight slots serviced.
                    let path = work.path.clone();
                    let resolved = retry::read_dir_with_retry(
                        &path,
                        None,
                        &retry_policy,
                        |_| nfs.resolve_path_to_fh(&path),
                        || nfs.resolve_path_to_fh(&path),
                        || false,
                        |d| thread::sleep(d.min(Duration::from_millis(250))),
                    );
                    match resolved {
                        Ok(fh) => fh,
                        Err(DirError::Vanished { attempts }) => {
                            vanished_count.fetch_add(1, Ordering::Relaxed);
                            failures.record_vanished(&path, attempts);
                            debug!(
                                "Worker {} directory vanished before LOOKUP: {}",
                                id,
                                display_path(&path)
                            );
                            pending_work.fetch_sub(1, Ordering::SeqCst);
                            continue;
                        }
                        Err(DirError::Failed(f)) => {
                            errors_count.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                "Worker {} pipelined LOOKUP failed: {} -> {} ({}, {} attempts)",
                                id, f.path, f.error, f.kind, f.attempts
                            );
                            failures.record(&f);
                            pending_work.fetch_sub(1, Ordering::SeqCst);
                            continue;
                        }
                    }
                }
            };

            let tag = next_tag;
            next_tag = next_tag.wrapping_add(1);

            // Resume cookie/cookieverf when this is a continuation
            // produced by a prior split; (0, [0; 8]) for fresh items.
            let (start_cookie, start_cookieverf) = work
                .resume
                .map_or((0u64, [0i8; 8]), |r| (r.cookie, r.cookieverf));

            match nfs.submit_readdirplus_by_fh(&fh, start_cookie, start_cookieverf, tag) {
                Ok(slot) => {
                    if start_cookie == 0 {
                        debug!(
                            "Worker {} pipelined submit: tag={:#x} {} (depth={})",
                            id,
                            tag,
                            display_path(&work.path),
                            work.depth
                        );
                    } else {
                        debug!(
                            "Worker {} pipelined submit (resume): tag={:#x} {} cookie={:#x}",
                            id,
                            tag,
                            display_path(&work.path),
                            start_cookie
                        );
                    }
                    // Track this dir under the initial tag for the
                    // entire dir lifetime (across cookie-chain
                    // re-submits) so accumulated entries don't reset.
                    // Continuations get a fresh tag — a giant flat dir
                    // being read by N workers concurrently will appear
                    // as N separate (worker, tag) entries in scanlog,
                    // all pointing at the same path. That's the
                    // diagnostic signal we want.
                    metrics.enter_dir(id, tag, display_path(&work.path).into_owned());
                    slots.push(slot);
                    states.push(DirState {
                        work,
                        file_handle: fh,
                        cookie: start_cookie,
                        cookieverf: start_cookieverf,
                        submitted_at: Instant::now(),
                        tag,
                        entries_seen: 0,
                        attempts: 1,
                    });
                }
                Err(e) => {
                    errors_count.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "Worker {} pipelined submit failed: {} -> {}",
                        id,
                        display_path(&work.path),
                        e
                    );
                    failures.record(&DirFailure {
                        path: display_path(&work.path).into_owned(),
                        kind: e.failure_kind(),
                        error: e.to_string(),
                        attempts: 1,
                    });
                    pending_work.fetch_sub(1, Ordering::SeqCst);
                }
            }
        }

        // Update active-worker bookkeeping based on slot occupancy.
        let now_active = !slots.is_empty();
        if now_active && !active_flag {
            active_workers.fetch_add(1, Ordering::Relaxed);
            active_flag = true;
        } else if !now_active && active_flag {
            active_workers.fetch_sub(1, Ordering::Relaxed);
            active_flag = false;
        }

        if slots.is_empty() {
            // Idle. Apply the legacy termination check: if no work is
            // pending anywhere and no other worker is active, exit.
            if pending_work.load(Ordering::SeqCst) == 0
                && active_workers.load(Ordering::SeqCst) == 0
            {
                break;
            }
            thread::sleep(Duration::from_micros(100));
            continue;
        }

        // -------- 2. Drive RPCs. --------
        // Block up to ~50 ms for at least one completion. Returning
        // early on timeout lets us refill from new work and re-check
        // shutdown.
        let pump_result = nfs.pump(&slots, 1, POLL_STEP_MS * 5);
        match pump_result {
            Ok(0) => {
                // Timeout, no progress. Loop back to refill / shutdown check.
                continue;
            }
            Ok(_) => { /* fall through to drain */ }
            Err(e) => {
                // fd-level error: fail every in-flight slot and abandon
                // this worker. The connection is likely unrecoverable.
                error!(
                    "Worker {} pipelined pump failed: {} (dropping {} in-flight slots)",
                    id,
                    e,
                    slots.len()
                );
                let n = slots.len() as u64;
                errors_count.fetch_add(n, Ordering::Relaxed);
                pending_work.fetch_sub(n, Ordering::SeqCst);
                for s in &states {
                    metrics.exit_dir(id, s.tag);
                    failures.record(&DirFailure {
                        path: display_path(&s.work.path).into_owned(),
                        kind: FailureKind::Connection,
                        error: format!("pipelined pump failed: {}", e),
                        attempts: s.attempts.max(1),
                    });
                }
                slots.clear();
                states.clear();
                if active_flag {
                    active_workers.fetch_sub(1, Ordering::Relaxed);
                    active_flag = false;
                }
                break 'outer;
            }
        }

        // -------- 3. Drain completed slots (reverse iter for swap_remove). --------
        let mut i = slots.len();
        while i > 0 {
            i -= 1;
            if !slots[i].is_completed() {
                continue;
            }

            let mut slot = slots.swap_remove(i);
            let mut state = states.swap_remove(i);
            let result = slot.take_result();
            // Drop slot before potentially submitting the next page so
            // libnfs's internal PDU bookkeeping for this completed PDU
            // is released first.
            drop(slot);

            // Per-RPC NFS latency: time from last submit to this completion.
            metrics.record_nfs_latency(id, state.submitted_at.elapsed());

            // Successful response (matches legacy: status SUCCESS path).
            if result.status == ffi_rpc_status_success() {
                let mut subdir_count = 0usize;
                let mut chunk_file_count = 0u64;
                let mut chunk_byte_count = 0u64;
                let mut channel_broken = false;
                // Capture page entry count up front — `result.entries` is
                // moved into the for loop below. "." / ".." never appear
                // here (readdirplus_full_callback strips them), so this
                // counts exactly the emittable entries of the page.
                let entries_in_page = result.entries.len() as u64;
                metrics.record_entries(id, state.tag, entries_in_page);
                // The parent handle for a LOOKUP of an entry READDIRPLUS
                // returned without one; filled on first use.
                let dir_fh = std::cell::RefCell::new(None);

                for mut nfs_entry in result.entries {
                    let full_path = join_nfs_path(&state.work.path, &nfs_entry.name);

                    // Same rule as the legacy worker: an entry without
                    // attributes is resolved before it is classified.
                    // The GETATTR/LOOKUP is synchronous and shares the
                    // context with the in-flight READDIRPLUS slots, like
                    // the LOOKUP chain above.
                    if nfs_entry.entry_type == EntryType::Unknown {
                        if crate::config::excluded_entry_bytes(
                            &nfs_entry.name,
                            false,
                            &full_path,
                            &exclude,
                            &[],
                        ) {
                            continue;
                        }
                        if dir_fh.borrow().is_none() {
                            *dir_fh.borrow_mut() = Some(state.file_handle.clone());
                        }
                        if !resolver.settle(
                            &nfs,
                            &mut nfs_entry,
                            &full_path,
                            &state.work.path,
                            &dir_fh,
                        ) {
                            continue;
                        }
                    }

                    let is_dir = nfs_entry.entry_type == EntryType::Directory;

                    if dirs_only && !is_dir {
                        continue;
                    }

                    // --exclude / --exclude-dir, mirroring the legacy
                    // worker: a match is neither emitted nor descended
                    // into.
                    if crate::config::excluded_entry_bytes(
                        &nfs_entry.name,
                        is_dir,
                        &full_path,
                        &exclude,
                        &exclude_dirs,
                    ) {
                        continue;
                    }

                    let size = nfs_entry.size();

                    if is_dir {
                        subdir_count += 1;
                        pending_work.fetch_add(1, Ordering::SeqCst);
                        local.push(DirWork::fresh(
                            full_path.clone(),
                            state.work.depth + 1,
                            nfs_entry.file_handle.take(),
                        ));
                    } else {
                        chunk_file_count += 1;
                        chunk_byte_count += size;
                    }

                    // Same zero-copy shape as the legacy worker: move
                    // name + path, let the writer derive parent_path.
                    let db_entry = DbEntry {
                        parent_path: None,
                        path: full_path,
                        entry_type: nfs_entry.entry_type,
                        size,
                        mtime_sec: nfs_entry.mtime_sec(),
                        mtime_nsec: nfs_entry.mtime_nsec(),
                        atime_sec: nfs_entry.atime_sec(),
                        atime_nsec: nfs_entry.atime_nsec(),
                        ctime_sec: nfs_entry.ctime_sec(),
                        ctime_nsec: nfs_entry.ctime_nsec(),
                        mode: nfs_entry.mode(),
                        uid: nfs_entry.uid(),
                        gid: nfs_entry.gid(),
                        nlink: nfs_entry.nlink(),
                        inode: nfs_entry.inode,
                        fsid: nfs_entry.fsid(),
                        depth: state.work.depth + 1,
                        extension: if nfs_entry.entry_type == EntryType::File {
                            extract_extension_bytes(&nfs_entry.name)
                        } else {
                            None
                        },
                        blocks: nfs_entry.blocks(),
                        name: nfs_entry.name,
                    };

                    if sender.push(db_entry).is_err() {
                        channel_broken = true;
                        break;
                    }

                    // Periodic counter flush to keep the live
                    // entries/sec readout responsive on huge dirs.
                    if chunk_file_count >= batch_size as u64 {
                        files_count.fetch_add(chunk_file_count, Ordering::Relaxed);
                        bytes_count.fetch_add(chunk_byte_count, Ordering::Relaxed);
                        chunk_file_count = 0;
                        chunk_byte_count = 0;
                    }
                }

                files_count.fetch_add(chunk_file_count, Ordering::Relaxed);
                bytes_count.fetch_add(chunk_byte_count, Ordering::Relaxed);

                if channel_broken {
                    // Writer is gone; we're shutting down. Drop the
                    // remaining slots and exit.
                    debug!("Worker {} entry channel broken, exiting", id);
                    let n_remaining = slots.len() as u64;
                    pending_work.fetch_sub(n_remaining + 1, Ordering::SeqCst);
                    metrics.exit_dir(id, state.tag);
                    for s in &states {
                        metrics.exit_dir(id, s.tag);
                    }
                    slots.clear();
                    states.clear();
                    if active_flag {
                        active_workers.fetch_sub(1, Ordering::Relaxed);
                        active_flag = false;
                    }
                    break 'outer;
                }

                state.entries_seen = state.entries_seen.saturating_add(entries_in_page);

                if should_split_now(state.entries_seen, big_dir_split_after, result.eof) {
                    // SPLIT: hand the rest of this directory to the
                    // deque so another worker (or this worker, later)
                    // can resume from the saved cookie. dirs_count is
                    // NOT incremented — the directory is not yet
                    // exhausted; the worker that eventually hits EOF
                    // for this dir is the one that bumps the counter.
                    //
                    // pending_work accounting is net-zero: +1 for the
                    // continuation push, -1 for this slot's
                    // abandonment. Done as two ops so the +/- model
                    // stays grep-auditable alongside the EOF/error
                    // sites that already pair with their pushes.
                    //
                    // BAD_COOKIE: if the directory is mutated between
                    // now and the resume, the server returns an error
                    // status; that lands on the existing error path
                    // below (errors_count++, no retry).
                    debug!(
                        "Worker {} pipelined SPLIT: {} entries_seen={} cookie={:#x}",
                        id,
                        display_path(&state.work.path),
                        state.entries_seen,
                        result.next_cookie
                    );
                    let cont = DirWork::continuation(
                        state.work.path.clone(),
                        state.work.depth,
                        state.file_handle.clone(),
                        result.next_cookie,
                        result.next_cookieverf,
                    );
                    pending_work.fetch_add(1, Ordering::SeqCst);
                    local.push(cont);
                    pending_work.fetch_sub(1, Ordering::SeqCst);
                    metrics.exit_dir(id, state.tag);
                    // state + slot dropped here.
                } else if result.eof {
                    debug!(
                        "Worker {} pipelined EOF: {} ({} subdirs in this page)",
                        id,
                        display_path(&state.work.path),
                        subdir_count
                    );
                    // dirs_count is bumped exactly once per directory —
                    // here, by whichever worker hits EOF. Continuations
                    // (split branch above) deliberately do NOT increment.
                    dirs_count.fetch_add(1, Ordering::Relaxed);
                    pending_work.fetch_sub(1, Ordering::SeqCst);
                    metrics.exit_dir(id, state.tag);
                    // state + slot dropped here.
                } else {
                    // More pages for the same dir — advance cookie and
                    // re-submit. The libnfs `tag` parameter is per-RPC
                    // so it advances; `state.tag` (the scanlog tracking
                    // key) stays fixed for the dir's lifetime so
                    // `entries_seen` accumulates correctly.
                    state.cookie = result.next_cookie;
                    state.cookieverf = result.next_cookieverf;
                    let rpc_tag = next_tag;
                    next_tag = next_tag.wrapping_add(1);
                    match nfs.submit_readdirplus_by_fh(
                        &state.file_handle,
                        state.cookie,
                        state.cookieverf,
                        rpc_tag,
                    ) {
                        Ok(new_slot) => {
                            slots.push(new_slot);
                            state.submitted_at = Instant::now();
                            states.push(state);
                        }
                        Err(e) => {
                            // The directory is abandoned mid-read: rows
                            // from its earlier pages were already
                            // emitted but the dir counts as an error,
                            // so the run(-level) row-count
                            // reconciliation warning may fire. That's
                            // the honest signal — entries were dropped.
                            errors_count.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                "Worker {} pipelined re-submit failed: {} -> {}",
                                id,
                                display_path(&state.work.path),
                                e
                            );
                            failures.record(&DirFailure {
                                path: display_path(&state.work.path).into_owned(),
                                kind: e.failure_kind(),
                                error: e.to_string(),
                                attempts: state.attempts.max(1),
                            });
                            pending_work.fetch_sub(1, Ordering::SeqCst);
                            metrics.exit_dir(id, state.tag);
                        }
                    }
                }
            } else {
                // RPC- or NFS3-level error on this page. Same policy
                // as the legacy worker, re-submitted immediately (the
                // round trip is the spacing): retry only while no page
                // of this directory has been emitted.
                let s = result.status;
                let err = crate::nfs::connection::nfs3_status_to_nfs_error(s, &state.work.path);
                let first_page = state.cookie == 0 && state.entries_seen == 0;
                state.attempts = state.attempts.saturating_add(1);
                let mut redo = Redo::Fail;
                if first_page {
                    match retry::plan(&err, state.attempts, &retry_policy) {
                        retry::Action::Fail(_) => {}
                        retry::Action::Retry {
                            refresh_fh: false, ..
                        } => {
                            redo = Redo::Resubmit(state.file_handle.clone());
                        }
                        retry::Action::Retry {
                            refresh_fh: true, ..
                        }
                        | retry::Action::CheckGone => {
                            match nfs.resolve_path_to_fh(&state.work.path) {
                                Ok(fh) => {
                                    if state.attempts < retry_policy.max_attempts {
                                        redo = Redo::Resubmit(fh);
                                    }
                                }
                                Err(e) if e.failure_kind() == FailureKind::NotFound => {
                                    redo = Redo::Vanished;
                                }
                                Err(_) => {
                                    if state.attempts < retry_policy.max_attempts {
                                        redo = Redo::Resubmit(state.file_handle.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                match redo {
                    Redo::Resubmit(fh) => {
                        let rpc_tag = next_tag;
                        next_tag = next_tag.wrapping_add(1);
                        match nfs.submit_readdirplus_by_fh(&fh, 0, [0i8; 8], rpc_tag) {
                            Ok(new_slot) => {
                                debug!(
                                    "Worker {} pipelined retry {} of {}: {} ({})",
                                    id,
                                    state.attempts + 1,
                                    retry_policy.max_attempts,
                                    display_path(&state.work.path),
                                    err
                                );
                                state.file_handle = fh;
                                state.cookie = 0;
                                state.cookieverf = [0i8; 8];
                                state.submitted_at = Instant::now();
                                slots.push(new_slot);
                                states.push(state);
                            }
                            Err(e) => {
                                errors_count.fetch_add(1, Ordering::Relaxed);
                                warn!(
                                    "Worker {} pipelined retry submit failed: {} -> {}",
                                    id,
                                    display_path(&state.work.path),
                                    e
                                );
                                failures.record(&DirFailure {
                                    path: display_path(&state.work.path).into_owned(),
                                    kind: e.failure_kind(),
                                    error: e.to_string(),
                                    attempts: state.attempts,
                                });
                                pending_work.fetch_sub(1, Ordering::SeqCst);
                                metrics.exit_dir(id, state.tag);
                            }
                        }
                    }
                    Redo::Vanished => {
                        vanished_count.fetch_add(1, Ordering::Relaxed);
                        failures.record_vanished(&state.work.path, state.attempts);
                        debug!(
                            "Worker {} directory vanished during the scan: {}",
                            id,
                            display_path(&state.work.path)
                        );
                        pending_work.fetch_sub(1, Ordering::SeqCst);
                        metrics.exit_dir(id, state.tag);
                    }
                    Redo::Fail => {
                        errors_count.fetch_add(1, Ordering::Relaxed);
                        let f = DirFailure {
                            path: display_path(&state.work.path).into_owned(),
                            kind: err.failure_kind(),
                            error: err.to_string(),
                            attempts: state.attempts,
                        };
                        warn!(
                            "Worker {} could not read {}: {} ({}, {} attempts, status={}) — the scan will be reported incomplete",
                            id, f.path, f.error, f.kind, f.attempts, s
                        );
                        failures.record(&f);
                        pending_work.fetch_sub(1, Ordering::SeqCst);
                        metrics.exit_dir(id, state.tag);
                    }
                }
            }
        }
    }

    // Final cleanup. Drop slots first (must happen before nfs drops to
    // satisfy the FFI lifetime contract — stack-frame drop order does
    // this automatically since slots/states are declared after nfs is
    // bound as a parameter).
    if active_flag {
        active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    sender.flush_all();

    debug!("Worker {} (pipelined) finished", id);
}

/// Wrapper around the FFI constant so we don't need an `unsafe` import
/// every time we want to compare an RPC status to "success".
#[inline]
fn ffi_rpc_status_success() -> i32 {
    crate::nfs::connection::ffi::RPC_STATUS_SUCCESS as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_nfs_path_preserves_raw_name_bytes() {
        assert_eq!(join_nfs_path(b"/", b"bad-\xff"), b"/bad-\xff");
        assert_eq!(
            join_nfs_path(b"/parent-\xfe", b"child-\xff"),
            b"/parent-\xfe/child-\xff"
        );
    }

    #[test]
    fn test_walk_stats_default() {
        let stats = WalkStats::default();
        assert_eq!(stats.dirs, 0);
        assert_eq!(stats.files, 0);
        assert!(!stats.completed);
    }

    #[test]
    fn test_walk_progress_rate() {
        let progress = WalkProgress {
            files: 1000,
            dirs: 100,
            elapsed: Duration::from_secs(10),
            ..WalkProgress::default()
        };
        assert!((progress.files_per_second() - 110.0).abs() < 0.1);
    }

    // ------------------------------------------------------------------
    // Big-dir continuation: split decision + entry conservation.
    //
    // These tests exercise the dispatch state machine without touching
    // libnfs. The split decision is a pure function; the conservation
    // test simulates a sequence of READDIRPLUS pages with the same
    // cookie-handoff logic the real worker uses.
    // ------------------------------------------------------------------

    #[test]
    fn test_should_split_now_truth_table() {
        // (entries_seen, threshold, eof) -> expected
        let cases = [
            (0u64, 1_000_000u64, false, false),
            (999_999, 1_000_000, false, false),
            (1_000_000, 1_000_000, false, true),
            (5_000_000, 1_000_000, false, true),
            (5_000_000, 1_000_000, true, false), // EOF wins
            (5_000_000, 0, false, false),        // disabled
            (0, 0, false, false),                // disabled, empty
            (0, 0, true, false),                 // disabled + EOF
            (1, 1, false, true),                 // exactly at threshold
        ];
        for (entries_seen, threshold, eof, expected) in cases {
            let got = should_split_now(entries_seen, threshold, eof);
            assert_eq!(
                got, expected,
                "should_split_now({entries_seen}, {threshold}, {eof}) = {got}, expected {expected}"
            );
        }
    }

    /// Synthetic READDIRPLUS reply.
    #[derive(Clone)]
    struct MockPage {
        entries: u64,
        next_cookie: u64,
        eof: bool,
    }

    /// One DirWork in the simulation. Carries the cookie at which the
    /// next page should be read (0 for a fresh dir, otherwise the
    /// continuation cookie produced by a prior split).
    #[derive(Debug, Clone)]
    struct SimWork {
        start_cookie: u64,
    }

    /// Drive the split-dispatch state machine end-to-end against a
    /// canned page sequence. Returns `(total_entries_seen,
    /// continuation_count)`. Asserts internally that no page is
    /// re-read or skipped at any split boundary.
    fn run_split_simulation(pages: &[MockPage], threshold: u64) -> (u64, u64) {
        // Page lookup keyed by start cookie. A worker resuming at
        // cookie K reads page index `cookie_index[&K]`.
        let mut cookie_index = std::collections::HashMap::new();
        cookie_index.insert(0u64, 0usize);
        for (i, page) in pages.iter().enumerate() {
            // Only register the next page's start cookie when there is
            // a next page (skip for terminal eof page).
            if !page.eof && i + 1 < pages.len() {
                cookie_index.insert(page.next_cookie, i + 1);
            }
        }

        let mut deque: Vec<SimWork> = vec![SimWork { start_cookie: 0 }];
        let mut total_entries: u64 = 0;
        let mut continuations: u64 = 0;
        // Visited page indices — flag any re-read or skip.
        let mut visited: Vec<bool> = vec![false; pages.len()];

        while let Some(work) = deque.pop() {
            // entries_seen is per-DirWork (not cumulative across
            // continuations), matching the real DirState semantics.
            let mut entries_seen: u64 = 0;
            let mut idx = *cookie_index
                .get(&work.start_cookie)
                .expect("simulation: unknown resume cookie");

            loop {
                let page = &pages[idx];
                assert!(
                    !visited[idx],
                    "page index {idx} read twice — cookie handoff is wrong"
                );
                visited[idx] = true;

                let entries_in_page = page.entries;
                total_entries += entries_in_page;
                entries_seen = entries_seen.saturating_add(entries_in_page);

                if should_split_now(entries_seen, threshold, page.eof) {
                    // Push continuation; abandon this slot.
                    continuations += 1;
                    deque.push(SimWork {
                        start_cookie: page.next_cookie,
                    });
                    break;
                } else if page.eof {
                    break;
                } else {
                    // Cookie-chain re-submit: same DirWork, advance to
                    // the next page index.
                    idx += 1;
                    assert!(
                        idx < pages.len(),
                        "non-EOF page with no successor — bad fixture"
                    );
                }
            }
        }

        // Conservation: every page must have been read exactly once.
        for (i, v) in visited.iter().enumerate() {
            assert!(
                *v,
                "page {i} was never read — split dispatch dropped a page"
            );
        }

        (total_entries, continuations)
    }

    #[test]
    fn test_split_dispatch_conserves_entries_no_threshold() {
        // Threshold disabled: original DirWork chains through every page,
        // produces zero continuations, sees all entries exactly once.
        let pages: Vec<MockPage> = (0..10)
            .map(|i| MockPage {
                entries: 1_000,
                next_cookie: (i + 1) as u64 * 100,
                eof: i == 9,
            })
            .collect();
        let (total, conts) = run_split_simulation(&pages, 0);
        assert_eq!(total, 10_000);
        assert_eq!(conts, 0);
    }

    #[test]
    fn test_split_dispatch_conserves_entries_with_threshold() {
        // 10 pages × 1000 entries each, threshold 2500. Expect a split
        // after page 3 (3000 ≥ 2500), after page 6 of the continuation
        // (3000 ≥ 2500), and no split at the EOF page even if over
        // threshold.
        let pages: Vec<MockPage> = (0..10)
            .map(|i| MockPage {
                entries: 1_000,
                next_cookie: (i + 1) as u64 * 100,
                eof: i == 9,
            })
            .collect();
        let (total, conts) = run_split_simulation(&pages, 2_500);
        assert_eq!(total, 10_000, "must read every page exactly once");
        // 10 pages of 1000 each, splitting at every 2500 cumulative
        // boundary (per-DirWork): 3 + 3 + 3 + 1(EOF) = 4 DirWorks =>
        // 3 continuations from the 4 segments (original + 3 conts).
        assert_eq!(conts, 3, "expected 3 split events for this fixture");
    }

    #[test]
    fn test_split_dispatch_eof_at_threshold_does_not_split() {
        // Single page, eof, exactly at threshold — must not split.
        let pages = vec![MockPage {
            entries: 5_000,
            next_cookie: 0,
            eof: true,
        }];
        let (total, conts) = run_split_simulation(&pages, 5_000);
        assert_eq!(total, 5_000);
        assert_eq!(conts, 0, "EOF wins over threshold");
    }

    #[test]
    fn test_split_dispatch_recursive_resplit() {
        // Tiny threshold forces a split on every page boundary.
        // 5 pages × 100 entries with threshold 50: every page crosses
        // threshold, every non-EOF page produces a continuation.
        let pages: Vec<MockPage> = (0..5)
            .map(|i| MockPage {
                entries: 100,
                next_cookie: (i + 1) as u64 * 100,
                eof: i == 4,
            })
            .collect();
        let (total, conts) = run_split_simulation(&pages, 50);
        assert_eq!(total, 500);
        // 4 non-EOF pages, each splits → 4 continuations.
        assert_eq!(conts, 4);
    }

    #[test]
    fn test_dirwork_continuation_carries_resume() {
        let dw = DirWork::continuation(b"/a/b".to_vec(), 3, vec![1, 2, 3, 4], 42, [9; 8]);
        assert!(dw.resume.is_some());
        let r = dw.resume.unwrap();
        assert_eq!(r.cookie, 42);
        assert_eq!(r.cookieverf, [9i8; 8]);
        assert_eq!(dw.file_handle.as_deref(), Some(&[1, 2, 3, 4][..]));
    }

    #[test]
    fn test_dirwork_fresh_has_no_resume() {
        let dw = DirWork::fresh(b"/a".to_vec(), 0, None);
        assert!(dw.resume.is_none());
        assert!(dw.file_handle.is_none());
    }

    // ============================================================
    // The real worker loops against a scripted tree
    // ============================================================
    //
    // `FakeNfs` answers the calls a worker makes from tables, so both
    // loops run end to end without a server. The tests below cover the
    // entries READDIRPLUS returns without attributes.

    use crate::error::{NfsError, NfsResult};
    use crate::nfs::connection::InflightReaddir;
    use crate::nfs::types::{EntryAttrs, LookupReply, NfsDirEntry, NfsStat};
    use std::cell::RefCell;
    use std::collections::{HashMap, VecDeque};

    #[derive(Default)]
    struct FakeNfs {
        /// Directory handle -> what READDIRPLUS returns for it.
        dirs: HashMap<Vec<u8>, Vec<NfsDirEntry>>,
        /// Path -> handle, for path LOOKUPs from the export root.
        paths: HashMap<Vec<u8>, Vec<u8>>,
        /// Handle -> GETATTR replies, taken front to back; the last one
        /// repeats.
        getattrs: RefCell<HashMap<Vec<u8>, VecDeque<NfsResult<EntryAttrs>>>>,
        /// (directory handle, name) -> LOOKUP reply.
        lookups: HashMap<(Vec<u8>, Vec<u8>), NfsResult<LookupReply>>,
        /// Every call, in order.
        calls: RefCell<Vec<String>>,
    }

    fn show(bytes: &[u8]) -> String {
        display_path(bytes).into_owned()
    }

    impl FakeNfs {
        fn log(&self, call: String) {
            self.calls.borrow_mut().push(call);
        }

        fn dir(&mut self, path: &[u8], fh: &[u8], entries: Vec<NfsDirEntry>) {
            self.paths.insert(path.to_vec(), fh.to_vec());
            self.dirs.insert(fh.to_vec(), entries);
        }

        fn getattr(&mut self, fh: &[u8], replies: Vec<NfsResult<EntryAttrs>>) {
            self.getattrs
                .borrow_mut()
                .insert(fh.to_vec(), replies.into());
        }

        fn lookup(&mut self, dir_fh: &[u8], name: &[u8], reply: NfsResult<LookupReply>) {
            self.lookups.insert((dir_fh.to_vec(), name.to_vec()), reply);
        }

        fn calls_matching(&self, prefix: &str) -> Vec<String> {
            self.calls
                .borrow()
                .iter()
                .filter(|c| c.starts_with(prefix))
                .cloned()
                .collect()
        }
    }

    impl NfsOps for &FakeNfs {
        fn is_connected(&self) -> bool {
            true
        }

        fn resolve_path_to_fh(&self, path: &[u8]) -> NfsResult<Vec<u8>> {
            self.log(format!("PATH {}", show(path)));
            self.paths
                .get(path)
                .cloned()
                .ok_or(NfsError::NotFound { path: show(path) })
        }

        fn readdir_plus_by_fh(
            &self,
            file_handle: &[u8],
            chunk_size: usize,
            callback: &mut dyn FnMut(Vec<NfsDirEntry>) -> bool,
        ) -> NfsResult<usize> {
            self.log(format!("READDIRPLUS {}", show(file_handle)));
            let entries = self.dirs.get(file_handle).ok_or(NfsError::StaleHandle {
                path: show(file_handle),
            })?;
            for chunk in entries.chunks(chunk_size.max(1)) {
                if !callback(chunk.to_vec()) {
                    break;
                }
            }
            Ok(entries.len())
        }

        fn readdir_plus_with_fh(
            &self,
            path: &[u8],
            chunk_size: usize,
            callback: &mut dyn FnMut(Vec<NfsDirEntry>) -> bool,
        ) -> NfsResult<usize> {
            let fh = self.resolve_path_to_fh(path)?;
            self.readdir_plus_by_fh(&fh, chunk_size, callback)
        }

        fn submit_readdirplus_by_fh(
            &self,
            file_handle: &[u8],
            _cookie: u64,
            _cookieverf: [i8; 8],
            tag: u64,
        ) -> NfsResult<InflightReaddir> {
            self.log(format!("READDIRPLUS {}", show(file_handle)));
            Ok(match self.dirs.get(file_handle) {
                // One page holds the whole directory.
                Some(entries) => InflightReaddir::completed(
                    entries.clone(),
                    true,
                    0,
                    ffi_rpc_status_success(),
                    tag,
                ),
                None => InflightReaddir::completed(
                    Vec::new(),
                    false,
                    0,
                    -(crate::nfs::connection::ffi::nfsstat3_NFS3ERR_STALE as i32),
                    tag,
                ),
            })
        }

        fn pump(
            &self,
            slots: &[InflightReaddir],
            _min_completions: usize,
            _timeout_ms: i32,
        ) -> NfsResult<usize> {
            Ok(slots.iter().filter(|s| s.is_completed()).count())
        }

        fn getattr_by_fh(&self, file_handle: &[u8], path: &[u8]) -> NfsResult<EntryAttrs> {
            self.log(format!("GETATTR {}", show(file_handle)));
            let mut scripted = self.getattrs.borrow_mut();
            let replies = scripted
                .get_mut(file_handle)
                .ok_or_else(|| NfsError::AttrFailed {
                    path: show(path),
                    reason: "GETATTR: not scripted".into(),
                })?;
            if replies.len() > 1 {
                replies.pop_front().unwrap()
            } else {
                replies.front().cloned().unwrap()
            }
        }

        fn lookup_name(&self, dir_fh: &[u8], name: &[u8], path: &[u8]) -> NfsResult<LookupReply> {
            self.log(format!("LOOKUP {} {}", show(dir_fh), show(name)));
            self.lookups
                .get(&(dir_fh.to_vec(), name.to_vec()))
                .cloned()
                .unwrap_or(Err(NfsError::NotFound { path: show(path) }))
        }
    }

    fn stat(inode: u64) -> NfsStat {
        NfsStat {
            inode,
            fsid: 1,
            size: 100,
            mode: 0o644,
            uid: 10,
            gid: 20,
            nlink: 1,
            ..NfsStat::default()
        }
    }

    fn attrs_of(entry_type: EntryType, stat: NfsStat) -> EntryAttrs {
        EntryAttrs {
            entry_type,
            nfs_type: 0, // only read for an unrecognized type
            stat,
        }
    }

    /// An entry as READDIRPLUS returns it with attributes.
    fn listed(name: &[u8], entry_type: EntryType, inode: u64, fh: Option<&[u8]>) -> NfsDirEntry {
        NfsDirEntry {
            name: name.to_vec(),
            entry_type,
            stat: Some(stat(inode)),
            inode,
            file_handle: fh.map(<[u8]>::to_vec),
        }
    }

    /// An entry as READDIRPLUS returns it without attributes.
    fn bare(name: &[u8], inode: u64, fh: Option<&[u8]>) -> NfsDirEntry {
        NfsDirEntry {
            name: name.to_vec(),
            entry_type: EntryType::Unknown,
            stat: None,
            inode,
            file_handle: fh.map(<[u8]>::to_vec),
        }
    }

    struct Walked {
        rows: Vec<DbEntry>,
        dirs: u64,
        files: u64,
        errors: u64,
        vanished: u64,
        stats: ResolveStats,
        failures: Vec<DirFailure>,
    }

    impl Walked {
        fn row(&self, path: &[u8]) -> Option<&DbEntry> {
            self.rows.iter().find(|r| r.path == path)
        }

        fn paths(&self) -> Vec<String> {
            let mut paths: Vec<String> = self.rows.iter().map(|r| show(&r.path)).collect();
            paths.sort();
            paths
        }

        fn load(counter: &Arc<AtomicU64>) -> u64 {
            counter.load(Ordering::Relaxed)
        }
    }

    /// Walk the scripted tree from `/` with one worker: the legacy loop
    /// for `pipeline_depth == 0`, the pipelined loop otherwise.
    fn walk(nfs: &FakeNfs, pipeline_depth: usize, exclude: Vec<Regex>) -> Walked {
        let (tx, rx) = crossbeam_channel::unbounded();
        let injector = Arc::new(Injector::new());
        injector.push(DirWork::fresh(b"/".to_vec(), 0, None));
        let local = DequeWorker::new_fifo();
        let stealers = Arc::new(vec![local.stealer()]);
        let counter = || Arc::new(AtomicU64::new(0));
        let (dirs, files, bytes, errors, vanished) =
            (counter(), counter(), counter(), counter(), counter());
        let failures = Arc::new(FailureLog::in_memory());
        let stats = ResolveStats::default();
        // No retries: a scripted transient error would otherwise sleep.
        let policy = RetryPolicy::from_retries(0);
        let metrics =
            crate::scanlog::ScanMetrics::new(1, 1, crate::scanlog::CounterRefs::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let pending = Arc::new(AtomicU64::new(1));
        let exclude = Arc::new(exclude);
        let exclude_dirs = Arc::new(Vec::new());

        if pipeline_depth == 0 {
            worker_loop(
                0,
                nfs,
                local,
                injector,
                stealers,
                vec![tx],
                shutdown,
                Arc::clone(&dirs),
                Arc::clone(&files),
                bytes,
                Arc::clone(&errors),
                Arc::clone(&failures),
                Arc::clone(&vanished),
                stats.clone(),
                policy,
                active,
                Arc::clone(&pending),
                None,
                false,
                exclude,
                exclude_dirs,
                1000,
                metrics,
            );
        } else {
            worker_loop_pipelined(
                0,
                nfs,
                local,
                injector,
                stealers,
                vec![tx],
                shutdown,
                Arc::clone(&dirs),
                Arc::clone(&files),
                bytes,
                Arc::clone(&errors),
                Arc::clone(&failures),
                Arc::clone(&vanished),
                stats.clone(),
                policy,
                active,
                Arc::clone(&pending),
                None,
                false,
                exclude,
                exclude_dirs,
                1000,
                pipeline_depth,
                0,
                metrics,
            );
        }
        assert_eq!(pending.load(Ordering::SeqCst), 0, "every work item settled");

        Walked {
            rows: rx.try_iter().flatten().collect(),
            dirs: Walked::load(&dirs),
            files: Walked::load(&files),
            errors: Walked::load(&errors),
            vanished: Walked::load(&vanished),
            stats,
            failures: failures.samples(),
        }
    }

    /// Both loops must treat every case the same way.
    const LOOPS: [usize; 2] = [0, 4];

    /// The regression this change exists for. A directory READDIRPLUS
    /// returns without attributes used to be written as `unknown` and
    /// never read, so its whole subtree was missing from the scan.
    #[test]
    fn directory_without_readdirplus_attributes_is_resolved_and_traversed() {
        for depth in LOOPS {
            let mut nfs = FakeNfs::default();
            nfs.dir(
                b"/",
                b"R",
                vec![
                    listed(b"plain.txt", EntryType::File, 10, None),
                    bare(b"nostat", 11, Some(b"H1")),
                ],
            );
            nfs.getattr(b"H1", vec![Ok(attrs_of(EntryType::Directory, stat(11)))]);
            nfs.dir(
                b"/nostat",
                b"H1",
                vec![
                    listed(b"inner.txt", EntryType::File, 12, None),
                    listed(b"deep", EntryType::Directory, 13, Some(b"H3")),
                ],
            );
            nfs.dir(
                b"/nostat/deep",
                b"H3",
                vec![listed(b"leaf", EntryType::File, 14, None)],
            );

            let w = walk(&nfs, depth, vec![]);

            assert_eq!(
                w.paths(),
                [
                    "/nostat",
                    "/nostat/deep",
                    "/nostat/deep/leaf",
                    "/nostat/inner.txt",
                    "/plain.txt"
                ],
                "depth {depth}: the subtree under the attribute-less directory is in the scan"
            );
            let dir = w.row(b"/nostat").unwrap();
            assert_eq!(dir.entry_type, EntryType::Directory, "depth {depth}");
            assert_eq!(dir.mode, Some(0o644), "depth {depth}");
            assert_eq!(dir.fsid, Some(1), "depth {depth}");
            assert_eq!(w.errors, 0, "depth {depth}");
            assert_eq!(w.dirs, 3, "depth {depth}: /, /nostat, /nostat/deep");
            assert_eq!(w.files, 3, "depth {depth}");
            assert_eq!(Walked::load(&w.stats.by_getattr), 1, "depth {depth}");
            assert_eq!(Walked::load(&w.stats.by_lookup), 0, "depth {depth}");
            assert_eq!(
                nfs.calls_matching("GETATTR"),
                ["GETATTR H1"],
                "depth {depth}"
            );
            assert_eq!(
                nfs.calls_matching("READDIRPLUS H1"),
                ["READDIRPLUS H1"],
                "depth {depth}: read through the handle READDIRPLUS supplied"
            );
        }
    }

    /// The same directory with the handle missing too, which is how the
    /// Linux server lists a mountpoint: a LOOKUP of the raw name in the
    /// parent finds it. Its identity is the resolved object's.
    #[test]
    fn directory_without_attributes_or_handle_is_resolved_by_lookup_and_traversed() {
        for depth in LOOPS {
            let mut nfs = FakeNfs::default();
            nfs.dir(b"/", b"R", vec![bare(b"mnt-\xff", 11, None)]);
            nfs.lookup(
                b"R",
                b"mnt-\xff",
                Ok(LookupReply {
                    file_handle: b"H2".to_vec(),
                    attrs: Some(attrs_of(
                        EntryType::Directory,
                        NfsStat {
                            fsid: 99,
                            mode: 0o755,
                            ..stat(2)
                        },
                    )),
                }),
            );
            nfs.dir(
                b"/mnt-\xff",
                b"H2",
                vec![listed(b"child.txt", EntryType::File, 20, None)],
            );

            let w = walk(&nfs, depth, vec![]);

            assert_eq!(
                w.paths(),
                [r"/mnt-\xff", r"/mnt-\xff/child.txt"],
                "depth {depth}"
            );
            let dir = w.row(b"/mnt-\xff").unwrap();
            assert_eq!(dir.name, b"mnt-\xff", "depth {depth}: raw name bytes");
            assert_eq!(dir.entry_type, EntryType::Directory, "depth {depth}");
            assert_eq!(
                dir.inode, 2,
                "depth {depth}: the resolved object, not the listing's 11"
            );
            assert_eq!(dir.fsid, Some(99), "depth {depth}");
            assert_eq!(dir.mode, Some(0o755), "depth {depth}");
            assert_eq!(w.errors, 0, "depth {depth}");
            assert_eq!(Walked::load(&w.stats.by_lookup), 1, "depth {depth}");
            assert_eq!(Walked::load(&w.stats.by_getattr), 0, "depth {depth}");
            assert_eq!(
                nfs.calls_matching("LOOKUP"),
                [r"LOOKUP R mnt-\xff"],
                "depth {depth}: by the parent's handle and the raw name"
            );
            assert_eq!(
                nfs.calls_matching("READDIRPLUS H2"),
                ["READDIRPLUS H2"],
                "depth {depth}: read through the handle the LOOKUP returned"
            );
        }
    }

    /// A non-directory resolved through the fallback carries the
    /// attributes of the reply that resolved it.
    #[test]
    fn special_file_without_attributes_gets_its_type_and_attributes() {
        for depth in LOOPS {
            let mut nfs = FakeNfs::default();
            nfs.dir(b"/", b"R", vec![bare(b"pipe", 30, Some(b"P"))]);
            nfs.getattr(
                b"P",
                vec![Ok(attrs_of(
                    EntryType::Fifo,
                    NfsStat {
                        inode: 30,
                        fsid: 7,
                        size: 0,
                        mode: 0o600,
                        uid: 1001,
                        gid: 1002,
                        nlink: 1,
                        ..NfsStat::default()
                    },
                ))],
            );

            let w = walk(&nfs, depth, vec![]);

            let pipe = w.row(b"/pipe").expect("emitted");
            assert_eq!(pipe.entry_type, EntryType::Fifo, "depth {depth}");
            assert_eq!(pipe.size, 0, "depth {depth}");
            assert_eq!(pipe.mode, Some(0o600), "depth {depth}");
            assert_eq!(pipe.uid, Some(1001), "depth {depth}");
            assert_eq!(pipe.gid, Some(1002), "depth {depth}");
            assert_eq!(pipe.fsid, Some(7), "depth {depth}");
            assert_eq!(pipe.inode, 30, "depth {depth}");
            assert_eq!(w.files, 1, "depth {depth}");
            assert_eq!(w.errors, 0, "depth {depth}");
        }
    }

    /// An entry whose type cannot be established has no row, is not
    /// descended into, and fails the scan. Its siblings are unaffected.
    #[test]
    fn unresolved_entry_has_no_row_is_not_descended_and_fails_the_scan() {
        for depth in LOOPS {
            let mut nfs = FakeNfs::default();
            nfs.dir(
                b"/",
                b"R",
                vec![
                    bare(b"locked", 40, Some(b"L")),
                    listed(b"ok.txt", EntryType::File, 41, None),
                ],
            );
            nfs.getattr(
                b"L",
                vec![Err(NfsError::PermissionDenied {
                    path: "/locked".into(),
                })],
            );
            // If the worker descended anyway, it would find this.
            nfs.dir(
                b"/locked",
                b"L",
                vec![listed(b"secret", EntryType::File, 42, None)],
            );

            let w = walk(&nfs, depth, vec![]);

            assert_eq!(w.paths(), ["/ok.txt"], "depth {depth}");
            assert!(
                w.rows.iter().all(|r| r.entry_type != EntryType::Unknown),
                "depth {depth}: no row without a type"
            );
            assert_eq!(
                w.errors, 1,
                "depth {depth}: nonzero errors make the scan incomplete"
            );
            assert_eq!(Walked::load(&w.stats.unresolved), 1, "depth {depth}");
            assert_eq!(w.vanished, 0, "depth {depth}");
            assert_eq!(w.failures.len(), 1, "depth {depth}");
            assert_eq!(w.failures[0].path, "/locked", "depth {depth}");
            assert_eq!(
                w.failures[0].kind,
                FailureKind::PermissionDenied,
                "depth {depth}"
            );
            assert!(
                nfs.calls_matching("READDIRPLUS L").is_empty(),
                "depth {depth}: never read as a directory"
            );
        }
    }

    /// Deleted between the listing and the resolution, and confirmed
    /// gone by a path LOOKUP: recorded as vanished, no row, and the
    /// scan stays complete. The rule for a vanished directory.
    #[test]
    fn entry_that_vanishes_before_resolution_is_not_a_failure() {
        for depth in LOOPS {
            let mut nfs = FakeNfs::default();
            nfs.dir(
                b"/",
                b"R",
                vec![
                    bare(b"gone", 50, None),
                    listed(b"kept.txt", EntryType::File, 51, None),
                ],
            );
            // No LOOKUP reply and no path for `/gone`: both say NOENT.

            let w = walk(&nfs, depth, vec![]);

            assert_eq!(w.paths(), ["/kept.txt"], "depth {depth}");
            assert_eq!(w.errors, 0, "depth {depth}: the scan is complete");
            assert_eq!(w.vanished, 1, "depth {depth}");
            assert_eq!(Walked::load(&w.stats.vanished), 1, "depth {depth}");
            assert_eq!(Walked::load(&w.stats.unresolved), 0, "depth {depth}");
            assert!(w.failures.is_empty(), "depth {depth}");
            assert_eq!(
                nfs.calls_matching("PATH /gone"),
                ["PATH /gone"],
                "depth {depth}: confirmed by the path LOOKUP"
            );
            assert_eq!(w.files, 1, "depth {depth}: a vanished entry is not counted");
        }
    }

    /// The server says NOENT for the name, but the path still resolves:
    /// that is not a disappearance. The entry is unresolved.
    #[test]
    fn not_found_contradicted_by_the_path_lookup_fails_the_scan() {
        for depth in LOOPS {
            let mut nfs = FakeNfs::default();
            nfs.dir(b"/", b"R", vec![bare(b"odd", 60, None)]);
            nfs.paths.insert(b"/odd".to_vec(), b"O".to_vec());
            // LOOKUP(R, "odd") is unscripted: NOENT.

            let w = walk(&nfs, depth, vec![]);

            assert!(w.rows.is_empty(), "depth {depth}");
            assert_eq!(w.errors, 1, "depth {depth}");
            assert_eq!(w.vanished, 0, "depth {depth}");
            assert_eq!(w.failures[0].kind, FailureKind::NotFound, "depth {depth}");
        }
    }

    /// `--exclude` drops a path whatever its type, so an excluded entry
    /// costs no RPC and cannot fail the scan.
    #[test]
    fn path_excluded_entry_without_attributes_is_not_resolved() {
        let skipme = Regex::new("^/skipme$").unwrap();
        for depth in LOOPS {
            let mut nfs = FakeNfs::default();
            nfs.dir(
                b"/",
                b"R",
                vec![
                    bare(b"skipme", 70, Some(b"S")),
                    listed(b"kept.txt", EntryType::File, 71, None),
                ],
            );
            // GETATTR for S is unscripted: asking would fail the scan.

            let w = walk(&nfs, depth, vec![skipme.clone()]);

            assert_eq!(w.paths(), ["/kept.txt"], "depth {depth}");
            assert_eq!(w.errors, 0, "depth {depth}");
            assert!(nfs.calls_matching("GETATTR").is_empty(), "depth {depth}");
        }
    }
}
