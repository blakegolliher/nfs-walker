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
use crate::content::{checksum::compute_gxhash, filetype::detect_file_type as detect_mime_type};
use crate::error::{Result, WalkerError};
use crate::nfs::types::{DbEntry, EntryType};
use crate::nfs::{resolve_dns, NfsConnection, NfsConnectionBuilder};
use crate::parquet::direct_writer::{
    spawn_direct_parquet_writers, write_metadata_json as write_direct_metadata_json,
    DirectWriteConfig,
};
use crate::walker::sharding::path_to_shard;
use crossbeam_channel::Sender;
use crossbeam_deque::{Injector, Stealer, Worker as DequeWorker};
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
    fn fresh(path: String, depth: u32, file_handle: Option<Vec<u8>>) -> Self {
        Self { path, depth, file_handle, resume: None }
    }

    /// Construct a continuation produced by an in-flight pipelined slot
    /// that bailed at a page boundary. The file handle is mandatory
    /// (mid-dir resume requires it; a path-LOOKUP would not be safe).
    fn continuation(
        path: String,
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
            let full =
                std::mem::replace(batch, Vec::with_capacity(self.batch_size));
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
        let scan_timestamp_us =
            chrono::Utc::now().timestamp_micros();

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
                row_group_size: self.config.parquet_row_group_size,
                target_file_size: self.config.parquet_file_size_bytes,
                compression: self.config.parquet_compression.to_direct_writer(),
                channel_depth: self.config.parquet_channel_depth,
            };

            let pool = spawn_direct_parquet_writers(direct_cfg, metrics.clone())?;

            // Register clones for queue-depth observation. MUST be paired
            // with `release_write_senders()` before joining writers.
            for tx in &pool.senders {
                metrics.register_write_sender(tx.clone());
            }

            let scan_dir = pool.scan_dir.clone();
            let workers_result = self.run_workers(pool.senders, metrics.clone());

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
                            writer_err = Some(WalkerError::Parquet(
                                crate::error::ParquetError::Other(format!(
                                    "parquet writer shard {} panicked",
                                    idx
                                )),
                            ));
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

            // Walker counters and parquet row count measure different
            // things and can't be compared directly:
            //   - `dirs_count` counts directories we READ (called
            //     readdirplus on), not directories we EMITTED.
            //   - `files_count` counts files we saw inside those reads.
            //   - parquet rows = every non-dot entry returned by any
            //     readdir that we completed (subdirs are emitted by their
            //     parent's readdir even when we don't recurse into them).
            //
            // The relation with no depth limit and no dirs-only mode is
            // `parquet_rows == files + dirs - 1` (minus one because the
            // root directory is never emitted as a child of its parent).
            // `dirs_only` makes the worker drop file entries before they
            // reach the writer, which breaks the relation. `max_depth`
            // adds a "seen but skipped" term we don't track separately,
            // also breaking it. `exclude_patterns` is gathered into the
            // config but never consulted by the worker today, so it
            // doesn't perturb the count — no carve-out needed.
            if self.config.max_depth.is_none() && !self.config.dirs_only {
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

            Ok(WalkStats {
                dirs,
                files,
                bytes,
                errors,
                duration: start.elapsed(),
                completed: !self.shutdown.load(Ordering::Relaxed),
            })
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
        let log_cfg = crate::scanlog::LogConfig::new(
            cfg.path.clone(),
            cfg.format,
            cfg.interval,
        );
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
    ) -> Result<()> {
        // Pipelined mode currently does not support per-file content
        // analysis (checksum / file-type detection). Warn loudly if the
        // combination is requested; the pipelined worker silently skips
        // those reads.
        if self.config.pipeline_depth > 0
            && (self.config.compute_checksum || self.config.detect_file_type)
        {
            warn!(
                "--pipeline-depth {} ignores --checksum and --file-type; \
                 content analysis is not yet wired into the pipelined worker. \
                 Drop --pipeline-depth (or set it to 0) for full content metadata.",
                self.config.pipeline_depth
            );
        }

        // Work-stealing deque for directories
        let injector: Arc<Injector<DirWork>> = Arc::new(Injector::new());

        // Track active workers and pending work. Sharing the
        // active_workers atomic with `metrics` lets the snapshot thread
        // read the same value without double-bookkeeping.
        let active_workers = metrics.active_workers();
        let pending_work = Arc::new(AtomicU64::new(1)); // Start with 1 for root

        // Push root directory (no cached file handle - will do path lookup)
        let start_path = self.config.nfs_url.walk_start_path();
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
            info!("Using {} explicit server VIPs (--server-ips), skipping DNS: {:?}",
                  self.config.server_ips.len(), self.config.server_ips);
            self.config.server_ips.clone()
        } else {
            let ips = resolve_dns(&self.config.nfs_url.server);
            if ips.len() > 1 {
                info!("DNS resolved {} to {} IPs: {:?}",
                      self.config.nfs_url.server, ips.len(), ips);
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
            let active_workers = Arc::clone(&active_workers);
            let pending_work = Arc::clone(&pending_work);
            let metrics = Arc::clone(&metrics);
            let max_depth = self.config.max_depth;
            let dirs_only = self.config.dirs_only;
            let worker_count = self.config.worker_count;
            let batch_size = self.config.batch_size;
            let compute_checksum = self.config.compute_checksum;
            let detect_file_type = self.config.detect_file_type;
            let max_checksum_size = self.config.max_checksum_size;
            let pipeline_depth = self.config.pipeline_depth;
            let big_dir_split_after = self.config.big_dir_split_after;

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
                            active_workers,
                            pending_work,
                            max_depth,
                            dirs_only,
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
                            active_workers,
                            pending_work,
                            max_depth,
                            dirs_only,
                            worker_count,
                            batch_size,
                            compute_checksum,
                            detect_file_type,
                            max_checksum_size,
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

}

/// Worker thread - processes directories using READDIRPLUS.
///
/// `entry_txs` carries one sender per writer shard. The worker holds an
/// equal number of pending `batch` slots inside `ShardedSender`; entries
/// route per-path via `gxhash % shards`. With shards == 1 this collapses
/// to one batch + one channel — bit-identical to the legacy worker.
#[allow(clippy::too_many_arguments)]
fn worker_loop(
    id: usize,
    nfs: NfsConnection,
    local: DequeWorker<DirWork>,
    injector: Arc<Injector<DirWork>>,
    stealers: Arc<Vec<Stealer<DirWork>>>,
    entry_txs: Vec<Sender<Vec<DbEntry>>>,
    shutdown: Arc<AtomicBool>,
    dirs_count: Arc<AtomicU64>,
    files_count: Arc<AtomicU64>,
    bytes_count: Arc<AtomicU64>,
    errors_count: Arc<AtomicU64>,
    active_workers: Arc<AtomicUsize>,
    pending_work: Arc<AtomicU64>,
    max_depth: Option<usize>,
    dirs_only: bool,
    _worker_count: usize,
    batch_size: usize,
    compute_checksum: bool,
    detect_file_type: bool,
    max_checksum_size: u64,
    metrics: Arc<crate::scanlog::ScanMetrics>,
) {
    debug!("Worker {} started", id);

    let shards = entry_txs.len().max(1);
    // Local in-progress staging: when content analysis is active we need
    // a flat buffer so the post-readdir pass can patch checksum/file_type
    // by index. We push to staging first, then drain into the
    // ShardedSender once analysis is done.
    let mut staging: Vec<DbEntry> = Vec::with_capacity(batch_size);
    let mut sender = ShardedSender::new(entry_txs, batch_size);
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
                        id, w.path
                    );
                    pending_work.fetch_sub(1, Ordering::SeqCst);
                    active_workers.fetch_sub(1, Ordering::Relaxed);
                    errors_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                // Legacy worker only has one in-flight at a time, so a
                // fixed tag uniquely identifies its slot.
                metrics.enter_dir(id, 0, w.path.clone());
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
            debug!("Worker {} READDIRPLUS (cached FH): {}", id, work.path);
        } else {
            debug!("Worker {} READDIRPLUS (path lookup): {}", id, work.path);
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

        // Track files that need content analysis (path, batch_index, size)
        // We'll process them after the directory walk completes
        let needs_content = compute_checksum || detect_file_type;
        let mut files_for_content: Vec<(String, usize, u64)> = Vec::new();

        // Define the callback that processes directory entries
        // This is used by both readdir_plus_by_fh and readdir_plus_with_fh
        let metrics_for_chunks = Arc::clone(&metrics);
        let mut process_entries = |chunk: Vec<crate::nfs::types::NfsDirEntry>| -> bool {
            metrics_for_chunks.record_entries(id, 0, chunk.len() as u64);
            for nfs_entry in chunk {
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

                // Extract extension from filename (for files only)
                let extension = if nfs_entry.entry_type == EntryType::File {
                    nfs_entry.name.rsplit('.').next()
                        .filter(|ext| ext.len() < 10 && !ext.contains('/'))
                        .map(|s| s.to_lowercase())
                } else {
                    None
                };

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
                    depth: work.depth + 1,
                    extension,
                    blocks: nfs_entry.blocks(),
                    checksum: None,
                    file_type: None,
                };

                if is_dir {
                    // Queue subdirectory for processing with cached file handle
                    subdir_count += 1;
                    pending_work.fetch_add(1, Ordering::SeqCst);
                    local.push(DirWork::fresh(
                        full_path.clone(),
                        work.depth + 1,
                        nfs_entry.file_handle.clone(),
                    ));
                } else {
                    chunk_file_count += 1;
                    chunk_byte_count += nfs_entry.size();
                }

                if needs_content {
                    // Stage flat so the post-readdir patch step can
                    // address by index; track files needing analysis.
                    let entry_idx = staging.len();
                    if !is_dir {
                        files_for_content.push((full_path, entry_idx, nfs_entry.size()));
                    }
                    staging.push(db_entry);
                } else {
                    // Push straight into the ShardedSender; it auto-
                    // flushes the per-shard batch when full.
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
            }
            !channel_broken // Continue reading if channel is OK
        };

        // Use cached file handle if available, otherwise resolve path.
        // Time the RPC for the per-scan progress logfile.
        let rpc_start = Instant::now();
        let result = if let Some(ref fh) = work.file_handle {
            nfs.readdir_plus_by_fh(fh, batch_size, &mut process_entries)
        } else {
            nfs.readdir_plus_with_fh(&work.path, batch_size, &mut process_entries)
        };
        metrics.record_nfs_latency(id, rpc_start.elapsed());

        match result {
            Ok(entry_count) => {
                // Process content analysis for files if enabled
                // This happens AFTER the directory walk completes so nfs is no longer borrowed
                if needs_content && !files_for_content.is_empty() {
                    for (path, idx, size) in files_for_content.drain(..) {
                        if idx >= staging.len() {
                            continue; // Safety check
                        }

                        // Determine what content we need to read
                        let need_full_file = compute_checksum && size <= max_checksum_size;
                        let need_header = detect_file_type && !need_full_file;

                        // Read content
                        let content = if need_full_file {
                            // Read entire file for checksum (also use for file type)
                            match nfs.read_file_content(&path, max_checksum_size) {
                                Ok(Some(data)) => Some(data),
                                Ok(None) => None, // File too large
                                Err(e) => {
                                    debug!("Failed to read file content {}: {}", path, e);
                                    None
                                }
                            }
                        } else if need_header {
                            // Only read header for file type detection
                            match nfs.read_file_header(&path, 8192) {
                                Ok(data) => Some(data),
                                Err(e) => {
                                    debug!("Failed to read file header {}: {}", path, e);
                                    None
                                }
                            }
                        } else {
                            None
                        };

                        // Compute checksum and/or file type
                        if let Some(data) = content {
                            if compute_checksum && data.len() as u64 == size {
                                // Only set checksum if we read the full file
                                staging[idx].checksum = Some(compute_gxhash(&data));
                            }
                            if detect_file_type {
                                let header_len = std::cmp::min(data.len(), 8192);
                                staging[idx].file_type = detect_mime_type(&data[..header_len]);
                            }
                        }
                    }
                }

                // Update directory count and any remaining files from final partial batch
                dirs_count.fetch_add(1, Ordering::Relaxed);
                files_count.fetch_add(chunk_file_count, Ordering::Relaxed);
                bytes_count.fetch_add(chunk_byte_count, Ordering::Relaxed);

                debug!(
                    "Worker {} READDIRPLUS complete: {} -> {} entries ({} subdirs)",
                    id, work.path, entry_count, subdir_count
                );

                // Drain content-analysis staging into the ShardedSender.
                // (The non-content path already pushed directly inside
                // the readdir callback.)
                if needs_content && !staging.is_empty() {
                    for entry in staging.drain(..) {
                        if sender.push(entry).is_err() {
                            debug!("Worker {} channel closed during content drain", id);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                errors_count.fetch_add(1, Ordering::Relaxed);
                // Not found errors are common on active filesystems (race condition)
                // Log them at debug level to reduce noise
                if e.to_string().contains("not found") || e.to_string().contains("No such file")
                    || e.to_string().contains("Permission denied") {
                    debug!("Worker {} READDIRPLUS error: {} -> {}", id, work.path, e);
                } else {
                    warn!("Worker {} READDIRPLUS failed: {} -> {}", id, work.path, e);
                }
            }
        }

        // Mark this work item as done
        pending_work.fetch_sub(1, Ordering::SeqCst);
        active_workers.fetch_sub(1, Ordering::Relaxed);
        metrics.exit_dir(id, 0);
    }

    // Drain residual staging entries (content-analysis path may have
    // queued some between the last iteration and shutdown) and any
    // partial per-shard batches in the ShardedSender.
    for entry in staging.drain(..) {
        if sender.push(entry).is_err() {
            break;
        }
    }
    sender.flush_all();
    let _ = shards;

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
// its pre-pipelining behavior — content-analysis, error logging, and
// counter ordering all live there. The pipelined worker duplicates the
// entry-emission logic inline rather than refactoring the legacy
// closure (intentional duplication; revisit once pipelining is the
// default).

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
fn worker_loop_pipelined(
    id: usize,
    nfs: crate::nfs::NfsConnection,
    local: DequeWorker<DirWork>,
    injector: Arc<Injector<DirWork>>,
    stealers: Arc<Vec<Stealer<DirWork>>>,
    entry_txs: Vec<Sender<Vec<DbEntry>>>,
    shutdown: Arc<AtomicBool>,
    dirs_count: Arc<AtomicU64>,
    files_count: Arc<AtomicU64>,
    bytes_count: Arc<AtomicU64>,
    errors_count: Arc<AtomicU64>,
    active_workers: Arc<AtomicUsize>,
    pending_work: Arc<AtomicU64>,
    max_depth: Option<usize>,
    dirs_only: bool,
    batch_size: usize,
    pipeline_depth: usize,
    big_dir_split_after: u64,
    metrics: Arc<crate::scanlog::ScanMetrics>,
) {
    debug!("Worker {} (pipelined depth={}) started", id, pipeline_depth);

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
                None => match nfs.resolve_path_to_fh(&work.path) {
                    Ok(fh) => fh,
                    Err(e) => {
                        errors_count.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            "Worker {} pipelined LOOKUP failed: {} -> {}",
                            id, work.path, e
                        );
                        pending_work.fetch_sub(1, Ordering::SeqCst);
                        continue;
                    }
                },
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
                            id, tag, work.path, work.depth
                        );
                    } else {
                        debug!(
                            "Worker {} pipelined submit (resume): tag={:#x} {} cookie={:#x}",
                            id, tag, work.path, start_cookie
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
                    metrics.enter_dir(id, tag, work.path.clone());
                    slots.push(slot);
                    states.push(DirState {
                        work,
                        file_handle: fh,
                        cookie: start_cookie,
                        cookieverf: start_cookieverf,
                        submitted_at: Instant::now(),
                        tag,
                        entries_seen: 0,
                    });
                }
                Err(e) => {
                    errors_count.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "Worker {} pipelined submit failed: {} -> {}",
                        id, work.path, e
                    );
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
                    id, e, slots.len()
                );
                let n = slots.len() as u64;
                errors_count.fetch_add(n, Ordering::Relaxed);
                pending_work.fetch_sub(n, Ordering::SeqCst);
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
                // moved into the for loop below. We use the raw page count
                // (including "." / "..") so threshold accounting is
                // monotonic and matches what the server sent.
                let entries_in_page = result.entries.len() as u64;
                metrics.record_entries(id, state.tag, entries_in_page);

                for nfs_entry in result.entries {
                    if nfs_entry.name == "." || nfs_entry.name == ".." {
                        continue;
                    }

                    let full_path = if state.work.path == "/" {
                        format!("/{}", nfs_entry.name)
                    } else {
                        format!("{}/{}", state.work.path, nfs_entry.name)
                    };

                    let is_dir = nfs_entry.entry_type == EntryType::Directory;

                    if dirs_only && !is_dir {
                        continue;
                    }

                    let extension = if nfs_entry.entry_type == EntryType::File {
                        nfs_entry
                            .name
                            .rsplit('.')
                            .next()
                            .filter(|ext| ext.len() < 10 && !ext.contains('/'))
                            .map(|s| s.to_lowercase())
                    } else {
                        None
                    };

                    let db_entry = DbEntry {
                        parent_path: Some(state.work.path.clone()),
                        name: nfs_entry.name.clone(),
                        path: full_path.clone(),
                        entry_type: nfs_entry.entry_type,
                        size: nfs_entry.size(),
                        mtime: nfs_entry.mtime(),
                        atime: nfs_entry.atime(),
                        ctime: nfs_entry.ctime(),
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
                        depth: state.work.depth + 1,
                        extension,
                        blocks: nfs_entry.blocks(),
                        // Pipelined mode does not (yet) compute these.
                        checksum: None,
                        file_type: None,
                    };

                    if is_dir {
                        subdir_count += 1;
                        pending_work.fetch_add(1, Ordering::SeqCst);
                        local.push(DirWork::fresh(
                            full_path.clone(),
                            state.work.depth + 1,
                            nfs_entry.file_handle.clone(),
                        ));
                    } else {
                        chunk_file_count += 1;
                        chunk_byte_count += nfs_entry.size();
                    }

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

                state.entries_seen =
                    state.entries_seen.saturating_add(entries_in_page);

                if should_split_now(
                    state.entries_seen,
                    big_dir_split_after,
                    result.eof,
                ) {
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
                        id, state.work.path, state.entries_seen, result.next_cookie
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
                        id, state.work.path, subdir_count
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
                            errors_count.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                "Worker {} pipelined re-submit failed: {} -> {}",
                                id, state.work.path, e
                            );
                            pending_work.fetch_sub(1, Ordering::SeqCst);
                            metrics.exit_dir(id, state.tag);
                        }
                    }
                }
            } else {
                // RPC- or NFS3-level error. Drop the slot, count it,
                // resolve the work item. (No retry — matches the
                // legacy worker.)
                errors_count.fetch_add(1, Ordering::Relaxed);
                let s = result.status;
                if s == 0 {
                    // Should not happen — completed slot with status 0
                    // and not SUCCESS — but guard anyway.
                    debug!(
                        "Worker {} pipelined unexpected zero status: {}",
                        id, state.work.path
                    );
                } else {
                    debug!(
                        "Worker {} pipelined READDIRPLUS error status={} path={}",
                        id, s, state.work.path
                    );
                }
                pending_work.fetch_sub(1, Ordering::SeqCst);
                metrics.exit_dir(id, state.tag);
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
            (0u64,         1_000_000u64, false, false),
            (999_999,      1_000_000,    false, false),
            (1_000_000,    1_000_000,    false, true),
            (5_000_000,    1_000_000,    false, true),
            (5_000_000,    1_000_000,    true,  false), // EOF wins
            (5_000_000,    0,            false, false), // disabled
            (0,            0,            false, false), // disabled, empty
            (0,            0,            true,  false), // disabled + EOF
            (1,            1,            false, true),  // exactly at threshold
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
            assert!(*v, "page {i} was never read — split dispatch dropped a page");
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
        let dw = DirWork::continuation(
            "/a/b".into(),
            3,
            vec![1, 2, 3, 4],
            42,
            [9; 8],
        );
        assert!(dw.resume.is_some());
        let r = dw.resume.unwrap();
        assert_eq!(r.cookie, 42);
        assert_eq!(r.cookieverf, [9i8; 8]);
        assert_eq!(dw.file_handle.as_deref(), Some(&[1, 2, 3, 4][..]));
    }

    #[test]
    fn test_dirwork_fresh_has_no_resume() {
        let dw = DirWork::fresh("/a".into(), 0, None);
        assert!(dw.resume.is_none());
        assert!(dw.file_handle.is_none());
    }

}
