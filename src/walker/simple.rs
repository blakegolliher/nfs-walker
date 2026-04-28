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
//! └── Writer Thread: recv entries → batch insert to SQLite/RocksDB
//! ```

use crate::config::WalkConfig;
#[cfg(feature = "rocksdb")]
use crate::config::OutputFormat;
#[cfg(feature = "rocksdb")]
use crate::rocksdb::schema::path_to_shard;
use crate::content::{checksum::compute_gxhash, filetype::detect_file_type as detect_mime_type};
use crate::db::schema::{create_database, create_indexes, keys, optimize_for_reads, set_walk_info};
use crate::error::{Result, WalkerError};
use crate::nfs::{resolve_dns, NfsConnection, NfsConnectionBuilder};
use crate::nfs::types::{BigDirEntry, DbEntry, EntryType};
#[cfg(feature = "rocksdb")]
use crate::rocksdb::{
    finalize_rocks_db, meta_keys, RocksHandle, RocksWriter, RocksWriterConfig, WalkStatsSnapshot,
};
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
    /// Cached file handle from parent's READDIRPLUS response
    /// When set, we can skip LOOKUP RPCs and use this handle directly
    file_handle: Option<Vec<u8>>,
}

/// Per-worker fan-out helper. Each entry is routed to its owning
/// path-shard channel by `path_to_shard(entry.path, shards)`. Workers
/// hold N partial batches in parallel; shards == 1 collapses to a
/// single channel and matches the legacy behavior bit-for-bit.
#[cfg(feature = "rocksdb")]
struct ShardedSender {
    senders: Vec<Sender<Vec<DbEntry>>>,
    batches: Vec<Vec<DbEntry>>,
    batch_size: usize,
    shards: usize,
}

#[cfg(feature = "rocksdb")]
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
    /// Number of big directories found (only set in big-dir-hunt mode)
    pub big_dirs_found: u64,
    /// Whether this was a big-dir-hunt run
    pub big_dir_hunt_mode: bool,
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
    big_dirs_count: Arc<AtomicU64>,
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
            big_dirs_count: Arc::new(AtomicU64::new(0)),
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
        // Dispatch based on output format and mode
        #[cfg(feature = "rocksdb")]
        {
            if self.config.big_dir_hunt {
                return self.run_big_dir_hunt();
            }
            match self.config.output_format {
                OutputFormat::Sqlite => self.run_sqlite(),
                OutputFormat::RocksDb => self.run_rocksdb(),
            }
        }

        #[cfg(not(feature = "rocksdb"))]
        {
            if self.config.big_dir_hunt {
                return self.run_big_dir_hunt_sqlite();
            }
            self.run_sqlite()
        }
    }

    /// Run walker with SQLite output
    fn run_sqlite(&self) -> Result<WalkStats> {
        let start = Instant::now();

        // Open SQLite database
        info!("Opening SQLite database: {}", self.config.output_path.display());
        let db = self.open_sqlite_database()?;

        // Channel for entries to write (workers -> writer).
        // SQLite path stays single-shard: there is one writer thread.
        let (entry_tx, entry_rx) = bounded::<Vec<DbEntry>>(1024);

        // Spawn dedicated writer thread
        let writer_handle = self.spawn_sqlite_writer(db, entry_rx);

        // Run workers (single-channel fan-in)
        self.run_workers(vec![entry_tx])?;

        // Wait for writer to finish
        let db = writer_handle.join().expect("Writer thread panicked");

        // Finalize database
        info!("Finalizing SQLite database...");
        self.finalize_sqlite_database(&db, start.elapsed(), !self.shutdown.load(Ordering::Relaxed))?;

        let stats = WalkStats {
            dirs: self.dirs_count.load(Ordering::Relaxed),
            files: self.files_count.load(Ordering::Relaxed),
            bytes: self.bytes_count.load(Ordering::Relaxed),
            errors: self.errors_count.load(Ordering::Relaxed),
            duration: start.elapsed(),
            completed: !self.shutdown.load(Ordering::Relaxed),
            big_dirs_found: 0,
            big_dir_hunt_mode: false,
        };

        Ok(stats)
    }

    /// Run walker in big-dir-hunt mode with SQLite output
    #[cfg(not(feature = "rocksdb"))]
    fn run_big_dir_hunt_sqlite(&self) -> Result<WalkStats> {
        let start = Instant::now();

        info!(
            "Starting big-dir-hunt mode (SQLite) with threshold {} files",
            self.config.big_dir_threshold
        );

        // Open SQLite database
        info!("Opening SQLite database: {}", self.config.output_path.display());
        let db = self.open_sqlite_database()?;

        // Create big_dirs table
        db.execute(
            "CREATE TABLE IF NOT EXISTS big_dirs (
                path TEXT PRIMARY KEY,
                file_count INTEGER NOT NULL,
                discovered_at TEXT DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        ).map_err(|e| WalkerError::Database(crate::error::DbError::Sqlite(e)))?;

        // Channel for big directory entries (workers -> writer)
        let (big_dir_tx, big_dir_rx) = bounded::<BigDirEntry>(1024);

        // Spawn dedicated writer thread for big dirs
        let writer_handle = self.spawn_big_dir_writer_sqlite(db, big_dir_rx);

        // Run workers in big-dir-hunt mode
        self.run_big_dir_workers(big_dir_tx)?;

        // Wait for writer to finish
        let db = writer_handle.join().expect("Big-dir writer thread panicked");

        // Finalize database
        info!("Finalizing SQLite database...");
        let stats_snapshot = (
            self.dirs_count.load(Ordering::Relaxed),
            self.files_count.load(Ordering::Relaxed),
            self.bytes_count.load(Ordering::Relaxed),
            self.errors_count.load(Ordering::Relaxed),
        );

        // Record metadata
        let _ = db.execute(
            "INSERT OR REPLACE INTO walk_info (key, value) VALUES ('big_dir_hunt', 'true')",
            [],
        );
        let _ = db.execute(
            "INSERT OR REPLACE INTO walk_info (key, value) VALUES ('big_dir_threshold', ?1)",
            params![self.config.big_dir_threshold.to_string()],
        );
        let _ = db.execute(
            "INSERT OR REPLACE INTO walk_info (key, value) VALUES ('dirs_scanned', ?1)",
            params![stats_snapshot.0.to_string()],
        );
        let _ = db.execute(
            "INSERT OR REPLACE INTO walk_info (key, value) VALUES ('big_dirs_found', ?1)",
            params![self.big_dirs_count.load(Ordering::Relaxed).to_string()],
        );
        let _ = db.execute(
            "INSERT OR REPLACE INTO walk_info (key, value) VALUES ('duration_secs', ?1)",
            params![start.elapsed().as_secs().to_string()],
        );

        let stats = WalkStats {
            dirs: stats_snapshot.0,
            files: stats_snapshot.1,
            bytes: stats_snapshot.2,
            errors: stats_snapshot.3,
            duration: start.elapsed(),
            completed: !self.shutdown.load(Ordering::Relaxed),
            big_dirs_found: self.big_dirs_count.load(Ordering::Relaxed),
            big_dir_hunt_mode: true,
        };

        Ok(stats)
    }

    /// Spawn SQLite writer thread for big-dir-hunt mode
    #[cfg(not(feature = "rocksdb"))]
    fn spawn_big_dir_writer_sqlite(
        &self,
        db: Connection,
        big_dir_rx: Receiver<BigDirEntry>,
    ) -> JoinHandle<Connection> {
        thread::Builder::new()
            .name("big-dir-sqlite-writer".into())
            .spawn(move || {
                let mut stmt = db
                    .prepare_cached("INSERT OR REPLACE INTO big_dirs (path, file_count) VALUES (?1, ?2)")
                    .expect("Failed to prepare statement");

                for entry in big_dir_rx {
                    if let Err(e) = stmt.execute(params![entry.path, entry.file_count as i64]) {
                        error!("Failed to insert big dir: {}", e);
                    }
                }

                drop(stmt);
                db
            })
            .expect("Failed to spawn big-dir-sqlite-writer thread")
    }

    /// Run walker with RocksDB output. Fans out to `writer_shards`
    /// independent writer threads when `--writer-shards N > 1`; legacy
    /// single-writer behavior for shards == 1.
    #[cfg(feature = "rocksdb")]
    fn run_rocksdb(&self) -> Result<WalkStats> {
        let start = Instant::now();

        info!("Opening RocksDB: {}", self.config.output_path.display());
        let rocks_path = self.config.output_path.clone();
        let shards = self.config.writer_shards.max(1);

        // Decide whether to fan out to a streaming Parquet writer.
        // Streaming Parquet is single-threaded and not yet sharded, so
        // it's only allowed when shards == 1 (validated at config parse).
        #[cfg(feature = "parquet")]
        let parquet_spawn = self.maybe_spawn_parquet_writer(&rocks_path)?;
        #[cfg(not(feature = "parquet"))]
        let parquet_tx: Option<Sender<Vec<DbEntry>>> = None;

        #[cfg(feature = "parquet")]
        let parquet_tx_root = parquet_spawn.tx.clone();

        let scan_id_for_rocks: Option<(String, i64)>;
        #[cfg(feature = "parquet")]
        {
            scan_id_for_rocks = parquet_spawn
                .scan_id
                .as_ref()
                .map(|id| (id.clone(), chrono::Utc::now().timestamp_micros()));
        }
        #[cfg(not(feature = "parquet"))]
        {
            scan_id_for_rocks = None;
        }

        let rocks_handle: Arc<RocksHandle> = if shards <= 1 {
            // Single-shard path: keep the legacy single rocks-writer
            // thread so streaming-parquet keeps working unchanged.
            let (entry_tx, entry_rx) = bounded::<Vec<DbEntry>>(1024);
            #[cfg(feature = "parquet")]
            let parquet_tx = parquet_tx_root.clone();
            #[cfg(not(feature = "parquet"))]
            let parquet_tx: Option<Sender<Vec<DbEntry>>> = None;

            let writer_handle = self.spawn_rocksdb_writer(
                rocks_path.clone(),
                entry_rx,
                parquet_tx,
                scan_id_for_rocks,
            )?;

            self.run_workers(vec![entry_tx])?;

            let h = writer_handle
                .join()
                .expect("RocksDB writer thread panicked")
                .map_err(WalkerError::Rocks)?;
            Arc::new(h)
        } else {
            // Multi-shard path: spawn N writers, each owning one path
            // shard CF. Final summary is the merge of each shard's
            // accumulator.
            let (rocks_handle, writer_handles, txs) =
                self.spawn_sharded_rocksdb_writers(rocks_path.clone(), shards)?;

            // Run workers (route per-entry into shard channels).
            self.run_workers(txs)?;

            // Join all shard writers; merge their accumulators into a
            // single summary and flush it to the summary CF.
            use crate::rocksdb::summary::SummaryAccumulator;
            let mut merged = SummaryAccumulator::new();
            for (shard_idx, h) in writer_handles.into_iter().enumerate() {
                let shard_summary = h
                    .join()
                    .expect("RocksDB shard writer thread panicked")
                    .map_err(WalkerError::Rocks)?;
                debug!(
                    "shard {} contributed: files={} dirs={}",
                    shard_idx, shard_summary.total.total_files, shard_summary.total.total_dirs
                );
                merged.merge_from(&shard_summary);
            }
            merged.touch_now();

            let mut wopts = rocksdb::WriteOptions::default();
            wopts.disable_wal(true);
            flush_summary_to_cf(&rocks_handle, &merged, &wopts).map_err(WalkerError::Rocks)?;
            rocks_handle.db.flush().map_err(|e| {
                WalkerError::Rocks(crate::error::RocksError::Rocks(e))
            })?;

            rocks_handle
        };

        // Wait for the streaming Parquet writer if it was spawned.
        #[cfg(feature = "parquet")]
        if let Some(join) = parquet_spawn.join {
            match join.join().expect("streaming parquet writer thread panicked") {
                Ok(stats) => info!(
                    "Streaming Parquet finished: {} rows in {} parts ({} bytes)",
                    stats.rows_written, stats.parts_written, stats.bytes_written
                ),
                Err(e) => warn!("Streaming Parquet writer error: {}", e),
            }
        }

        // Finalize database
        info!("Finalizing RocksDB...");
        let stats_snapshot = WalkStatsSnapshot {
            dirs: self.dirs_count.load(Ordering::Relaxed),
            files: self.files_count.load(Ordering::Relaxed),
            bytes: self.bytes_count.load(Ordering::Relaxed),
            errors: self.errors_count.load(Ordering::Relaxed),
        };
        finalize_rocks_db(
            &rocks_handle,
            start.elapsed(),
            !self.shutdown.load(Ordering::Relaxed),
            &stats_snapshot,
        ).map_err(WalkerError::Rocks)?;

        let stats = WalkStats {
            dirs: stats_snapshot.dirs,
            files: stats_snapshot.files,
            bytes: stats_snapshot.bytes,
            errors: stats_snapshot.errors,
            duration: start.elapsed(),
            completed: !self.shutdown.load(Ordering::Relaxed),
            big_dirs_found: 0,
            big_dir_hunt_mode: false,
        };

        Ok(stats)
    }

    /// If `--stream-parquet` is enabled, prepare the streaming writer:
    /// generate the scan_id, refuse to overwrite an existing scan dir,
    /// open the writer, and spawn its thread.
    #[cfg(all(feature = "rocksdb", feature = "parquet"))]
    fn maybe_spawn_parquet_writer(
        &self,
        rocks_path: &std::path::Path,
    ) -> Result<StreamingParquetSpawn> {
        if !self.config.stream_parquet {
            return Ok(StreamingParquetSpawn::default());
        }

        use crate::parquet::{StreamingParquetConfig, StreamingParquetWriter};

        let scan_id = uuid::Uuid::new_v4().to_string();
        let scan_timestamp_us = chrono::Utc::now().timestamp_micros();

        let scan_dir = streaming_parquet_dir(rocks_path, &scan_id);
        if scan_dir.exists() {
            return Err(WalkerError::Parquet(crate::error::ParquetError::Other(format!(
                "Streaming Parquet target {} already exists. Refusing to overwrite -- \
                 delete it or run without --stream-parquet.",
                scan_dir.display()
            ))));
        }

        let cfg = StreamingParquetConfig::defaults_for(scan_dir.clone(), scan_id.clone(), scan_timestamp_us);
        let writer = StreamingParquetWriter::open(cfg)?;

        info!(
            "Streaming Parquet enabled: scan_id={} dir={}",
            scan_id,
            scan_dir.display()
        );

        // Channel sized to match the rocks/worker channel (1024 batches).
        let (tx, rx) = bounded::<Vec<DbEntry>>(1024);
        let join = spawn_parquet_writer(writer, rx);
        Ok(StreamingParquetSpawn {
            tx: Some(tx),
            join: Some(join),
            scan_id: Some(scan_id),
        })
    }

    /// Run walker in big-dir-hunt mode (RocksDB only)
    #[cfg(feature = "rocksdb")]
    fn run_big_dir_hunt(&self) -> Result<WalkStats> {
        let start = Instant::now();

        info!(
            "Starting big-dir-hunt mode with threshold {} files",
            self.config.big_dir_threshold
        );

        // Open RocksDB
        info!("Opening RocksDB: {}", self.config.output_path.display());
        let rocks_path = self.config.output_path.clone();

        // Channel for big directory entries (workers -> writer)
        let (big_dir_tx, big_dir_rx) = bounded::<BigDirEntry>(1024);

        // Spawn dedicated big-dir writer thread
        let writer_handle = self.spawn_big_dir_writer(rocks_path.clone(), big_dir_rx)?;

        // Run workers in big-dir-hunt mode
        self.run_big_dir_workers(big_dir_tx)?;

        // Wait for writer to finish
        let rocks_handle = writer_handle
            .join()
            .expect("Big-dir writer thread panicked")
            .map_err(WalkerError::Rocks)?;

        // Finalize database
        info!("Finalizing RocksDB...");
        let stats_snapshot = WalkStatsSnapshot {
            dirs: self.dirs_count.load(Ordering::Relaxed),
            files: self.files_count.load(Ordering::Relaxed),
            bytes: self.bytes_count.load(Ordering::Relaxed),
            errors: self.errors_count.load(Ordering::Relaxed),
        };
        finalize_rocks_db(
            &rocks_handle,
            start.elapsed(),
            !self.shutdown.load(Ordering::Relaxed),
            &stats_snapshot,
        )
        .map_err(WalkerError::Rocks)?;

        // Also record big-dir-hunt metadata
        rocks_handle
            .set_metadata("big_dir_hunt", "true")
            .map_err(|e| WalkerError::Rocks(crate::error::RocksError::Rocks(e)))?;
        rocks_handle
            .set_metadata("big_dir_threshold", &self.config.big_dir_threshold.to_string())
            .map_err(|e| WalkerError::Rocks(crate::error::RocksError::Rocks(e)))?;

        let stats = WalkStats {
            dirs: stats_snapshot.dirs,
            files: stats_snapshot.files,
            bytes: stats_snapshot.bytes,
            errors: stats_snapshot.errors,
            duration: start.elapsed(),
            completed: !self.shutdown.load(Ordering::Relaxed),
            big_dirs_found: self.big_dirs_count.load(Ordering::Relaxed),
            big_dir_hunt_mode: true,
        };

        Ok(stats)
    }

    /// Run workers for big-dir-hunt mode
    fn run_big_dir_workers(&self, big_dir_tx: Sender<BigDirEntry>) -> Result<()> {
        // Work-stealing deque for directories
        let injector: Arc<Injector<DirWork>> = Arc::new(Injector::new());

        // Track active workers and pending work
        let active_workers = Arc::new(AtomicUsize::new(0));
        let pending_work = Arc::new(AtomicU64::new(1)); // Start with 1 for root

        // Push root directory (no cached file handle - will do path lookup)
        let start_path = self.config.nfs_url.walk_start_path();
        injector.push(DirWork {
            path: start_path.clone(),
            depth: 0,
            file_handle: None,
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
            info!(
                "DNS resolved {} to {} IPs: {:?}",
                self.config.nfs_url.server,
                server_ips.len(),
                server_ips
            );
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

            // Create NfsUrl for this worker, potentially with resolved IP
            let mut nfs_url = self.config.nfs_url.clone();
            if let Some(resolved_ip) = ip {
                nfs_url.server = resolved_ip;
            }
            info!("Worker {} will connect to {}:{}", id, nfs_url.server, nfs_url.export);

            let injector = Arc::clone(&injector);
            let stealers = Arc::clone(&stealers);
            let big_dir_tx = big_dir_tx.clone();
            let shutdown = Arc::clone(&self.shutdown);
            let dirs_count = Arc::clone(&self.dirs_count);
            let files_count = Arc::clone(&self.files_count);
            let bytes_count = Arc::clone(&self.bytes_count);
            let errors_count = Arc::clone(&self.errors_count);
            let big_dirs_count = Arc::clone(&self.big_dirs_count);
            let active_workers = Arc::clone(&active_workers);
            let pending_work = Arc::clone(&pending_work);
            let max_depth = self.config.max_depth;
            let threshold = self.config.big_dir_threshold;
            let worker_count = self.config.worker_count;
            let timeout_secs = self.config.timeout_secs;

            let handle = thread::Builder::new()
                .name(format!("big-dir-{}", id))
                .spawn(move || {
                    big_dir_worker_loop(
                        id,
                        nfs_url,
                        local,
                        injector,
                        stealers,
                        big_dir_tx,
                        shutdown,
                        dirs_count,
                        files_count,
                        bytes_count,
                        errors_count,
                        big_dirs_count,
                        active_workers,
                        pending_work,
                        max_depth,
                        threshold,
                        worker_count,
                        timeout_secs,
                    );
                })
                .expect("Failed to spawn big-dir worker thread");

            handles.push(handle);
        }

        // Drop our sender so writer knows when to stop
        drop(big_dir_tx);

        // Wait for workers
        for handle in handles {
            let _ = handle.join();
        }

        Ok(())
    }

    /// Spawn big-dir writer thread
    #[cfg(feature = "rocksdb")]
    fn spawn_big_dir_writer(
        &self,
        path: std::path::PathBuf,
        big_dir_rx: Receiver<BigDirEntry>,
    ) -> Result<JoinHandle<std::result::Result<RocksHandle, crate::error::RocksError>>> {
        // Remove existing directory if present
        if path.exists() {
            std::fs::remove_dir_all(&path).map_err(WalkerError::Io)?;
        }

        // Create RocksDB with metadata
        let config = RocksWriterConfig::default();
        let writer = RocksWriter::open(&path, config).map_err(WalkerError::Rocks)?;

        // Set initial metadata
        writer
            .set_metadata(meta_keys::SOURCE_URL, &self.config.nfs_url.to_display_string())
            .map_err(WalkerError::Rocks)?;
        writer
            .set_metadata(meta_keys::START_TIME, &chrono::Utc::now().to_rfc3339())
            .map_err(WalkerError::Rocks)?;
        writer
            .set_metadata(meta_keys::STATUS, "running")
            .map_err(WalkerError::Rocks)?;
        writer
            .set_metadata(meta_keys::WORKER_COUNT, &self.config.worker_count.to_string())
            .map_err(WalkerError::Rocks)?;

        let handle = writer.into_handle();

        // Spawn writer thread
        let handle = thread::Builder::new()
            .name("big-dir-writer".to_string())
            .spawn(move || big_dir_writer_loop(handle, big_dir_rx))
            .expect("Failed to spawn big-dir writer thread");

        Ok(handle)
    }

    /// Run worker threads (shared between SQLite and RocksDB modes).
    ///
    /// `entry_txs` carries one sender per writer shard. Length 1 selects
    /// the legacy single-writer fan-in; length N (with the RocksDB
    /// multi-shard writer) makes each worker route per-entry via
    /// `gxhash(path) % N` into the matching writer's channel.
    fn run_workers(&self, entry_txs: Vec<Sender<Vec<DbEntry>>>) -> Result<()> {
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

        // Track active workers and pending work
        let active_workers = Arc::new(AtomicUsize::new(0));
        let pending_work = Arc::new(AtomicU64::new(1)); // Start with 1 for root

        // Push root directory (no cached file handle - will do path lookup)
        let start_path = self.config.nfs_url.walk_start_path();
        injector.push(DirWork {
            path: start_path.clone(),
            depth: 0,
            file_handle: None,
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
            let entry_txs = entry_txs.clone();
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
            let batch_size = self.config.batch_size;
            let compute_checksum = self.config.compute_checksum;
            let detect_file_type = self.config.detect_file_type;
            let max_checksum_size = self.config.max_checksum_size;
            let pipeline_depth = self.config.pipeline_depth;

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
                        );
                    }
                })
                .expect("Failed to spawn worker thread");

            handles.push(handle);
        }

        // Drop our senders so writers know when to stop.
        drop(entry_txs);

        // Wait for workers
        for handle in handles {
            let _ = handle.join();
        }

        Ok(())
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

    fn open_sqlite_database(&self) -> Result<Connection> {
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

    fn finalize_sqlite_database(&self, conn: &Connection, duration: Duration, completed: bool) -> Result<()> {
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

    /// Create connection for big-dir-hunt with small readdir buffer
    /// Small buffer = fewer entries per RPC = faster early termination
    /// Note: Currently unused - big-dir-hunt now uses RawRpcContext instead
    #[cfg(feature = "rocksdb")]
    #[allow(dead_code)]
    fn create_connection_for_big_dir_hunt(&self, ip: Option<&str>) -> Result<NfsConnection> {
        let timeout = Duration::from_secs(self.config.timeout_secs as u64);

        // Use small readdir buffer (4KB) to limit entries per RPC
        // This allows us to stop early after hitting the threshold
        // Without this, server might send thousands of entries before we can stop
        let readdir_buffer = 4096; // 4KB - minimum practical size

        let mut builder = NfsConnectionBuilder::new(self.config.nfs_url.clone())
            .timeout(timeout)
            .retries(self.config.retry_count)
            .readdir_buffer_size(readdir_buffer);

        if let Some(ip) = ip {
            builder = builder.with_ip(ip.to_string());
        }

        builder.connect().map_err(|e| WalkerError::Nfs(e))
    }

    fn spawn_sqlite_writer(&self, conn: Connection, entry_rx: Receiver<Vec<DbEntry>>) -> JoinHandle<Connection> {
        let batch_size = self.config.batch_size;
        thread::Builder::new()
            .name("sqlite-writer".to_string())
            .spawn(move || {
                sqlite_writer_loop(conn, entry_rx, batch_size)
            })
            .expect("Failed to spawn SQLite writer thread")
    }

    /// Spawn N RocksDB writer threads, one per path-CF shard. Returns:
    ///   - the shared `Arc<RocksHandle>` (kept alive by every writer
    ///     thread for the duration of the scan; the caller also keeps a
    ///     copy to write final metadata + finalize),
    ///   - the join handles, in shard order — each yields the shard's
    ///     `SummaryAccumulator` on success,
    ///   - the per-shard `Sender<Vec<DbEntry>>` to thread into workers.
    ///
    /// Streaming Parquet is intentionally **not** wired into this path:
    /// `--stream-parquet` requires `--writer-shards 1` (config validation
    /// rejects the combination at startup).
    #[cfg(feature = "rocksdb")]
    #[allow(clippy::type_complexity)]
    fn spawn_sharded_rocksdb_writers(
        &self,
        path: std::path::PathBuf,
        shards: usize,
    ) -> Result<(
        Arc<RocksHandle>,
        Vec<JoinHandle<std::result::Result<crate::rocksdb::summary::SummaryAccumulator, crate::error::RocksError>>>,
        Vec<Sender<Vec<DbEntry>>>,
    )> {
        debug_assert!(shards >= 2);

        if path.exists() {
            std::fs::remove_dir_all(&path).map_err(WalkerError::Io)?;
        }

        // Open with N path-shard CFs and write initial metadata.
        let writer = RocksWriter::open_with_shards(
            &path,
            shards,
            RocksWriterConfig::default(),
        )
        .map_err(WalkerError::Rocks)?;

        writer
            .set_metadata(meta_keys::SOURCE_URL, &self.config.nfs_url.to_display_string())
            .map_err(WalkerError::Rocks)?;
        writer
            .set_metadata(meta_keys::START_TIME, &chrono::Utc::now().to_rfc3339())
            .map_err(WalkerError::Rocks)?;
        writer
            .set_metadata(meta_keys::STATUS, "running")
            .map_err(WalkerError::Rocks)?;
        writer
            .set_metadata(meta_keys::WORKER_COUNT, &self.config.worker_count.to_string())
            .map_err(WalkerError::Rocks)?;

        let handle = Arc::new(writer.into_handle());
        let batch_size = self.config.batch_size;

        let flush_lock = Arc::new(std::sync::Mutex::new(()));
        let flush_counter = Arc::new(AtomicU64::new(0));

        let mut txs: Vec<Sender<Vec<DbEntry>>> = Vec::with_capacity(shards);
        let mut joins: Vec<
            JoinHandle<
                std::result::Result<
                    crate::rocksdb::summary::SummaryAccumulator,
                    crate::error::RocksError,
                >,
            >,
        > = Vec::with_capacity(shards);

        for shard_idx in 0..shards {
            let (tx, rx) = bounded::<Vec<DbEntry>>(1024);
            txs.push(tx);
            let h = Arc::clone(&handle);
            let fl = Arc::clone(&flush_lock);
            let fc = Arc::clone(&flush_counter);
            let join = thread::Builder::new()
                .name(format!("rocks-writer-{}", shard_idx))
                .spawn(move || {
                    rocksdb_writer_loop_shard(h, shard_idx, rx, None, batch_size, fl, fc)
                })
                .expect("Failed to spawn RocksDB shard writer thread");
            joins.push(join);
        }

        Ok((handle, joins, txs))
    }

    #[cfg(feature = "rocksdb")]
    fn spawn_rocksdb_writer(
        &self,
        path: std::path::PathBuf,
        entry_rx: Receiver<Vec<DbEntry>>,
        parquet_tx: Option<Sender<Vec<DbEntry>>>,
        scan_id_meta: Option<(String, i64)>,
    ) -> Result<RocksWriterSpawn> {
        // Create RocksDB with metadata
        let config = RocksWriterConfig::default();

        // Remove existing directory if present
        if path.exists() {
            std::fs::remove_dir_all(&path).map_err(WalkerError::Io)?;
        }

        // Create writer to initialize database
        let writer = RocksWriter::open(&path, config)
            .map_err(WalkerError::Rocks)?;

        // Set initial metadata
        writer.set_metadata(meta_keys::SOURCE_URL, &self.config.nfs_url.to_display_string())
            .map_err(WalkerError::Rocks)?;
        writer.set_metadata(meta_keys::START_TIME, &chrono::Utc::now().to_rfc3339())
            .map_err(WalkerError::Rocks)?;
        writer.set_metadata(meta_keys::STATUS, "running")
            .map_err(WalkerError::Rocks)?;
        writer.set_metadata(meta_keys::WORKER_COUNT, &self.config.worker_count.to_string())
            .map_err(WalkerError::Rocks)?;

        // When streaming is enabled, persist SCAN_ID so a later post-scan
        // export-parquet can detect the streamed dir and reuse the id.
        if let Some((scan_id, _ts_us)) = scan_id_meta {
            writer
                .set_metadata(meta_keys::SCAN_ID, &scan_id)
                .map_err(WalkerError::Rocks)?;
        }

        let handle = writer.into_handle();
        let batch_size = self.config.batch_size;

        // Spawn writer thread
        let join = thread::Builder::new()
            .name("rocks-writer".to_string())
            .spawn(move || {
                rocksdb_writer_loop(handle, entry_rx, parquet_tx, batch_size)
            })
            .expect("Failed to spawn RocksDB writer thread");

        Ok(join)
    }
}

/// Type alias for the rocksdb writer thread join handle.
#[cfg(feature = "rocksdb")]
type RocksWriterSpawn = JoinHandle<std::result::Result<RocksHandle, crate::error::RocksError>>;

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
        let mut process_entries = |chunk: Vec<crate::nfs::types::NfsDirEntry>| -> bool {
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
                    local.push(DirWork {
                        path: full_path.clone(),
                        depth: work.depth + 1,
                        file_handle: nfs_entry.file_handle.clone(),
                    });
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

        // Use cached file handle if available, otherwise resolve path
        let result = if let Some(ref fh) = work.file_handle {
            nfs.readdir_plus_by_fh(fh, batch_size, &mut process_entries)
        } else {
            nfs.readdir_plus_with_fh(&work.path, batch_size, &mut process_entries)
        };

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

            match nfs.submit_readdirplus_by_fh(&fh, 0, [0i8; 8], tag) {
                Ok(slot) => {
                    debug!(
                        "Worker {} pipelined submit: tag={:#x} {} (depth={})",
                        id, tag, work.path, work.depth
                    );
                    slots.push(slot);
                    states.push(DirState {
                        work,
                        file_handle: fh,
                        cookie: 0,
                        cookieverf: [0i8; 8],
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

            // Successful response (matches legacy: status SUCCESS path).
            if result.status == ffi_rpc_status_success() {
                let mut subdir_count = 0usize;
                let mut chunk_file_count = 0u64;
                let mut chunk_byte_count = 0u64;
                let mut channel_broken = false;

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
                        local.push(DirWork {
                            path: full_path.clone(),
                            depth: state.work.depth + 1,
                            file_handle: nfs_entry.file_handle.clone(),
                        });
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
                    slots.clear();
                    states.clear();
                    if active_flag {
                        active_workers.fetch_sub(1, Ordering::Relaxed);
                        active_flag = false;
                    }
                    break 'outer;
                }

                if result.eof {
                    debug!(
                        "Worker {} pipelined EOF: {} ({} subdirs in this page)",
                        id, state.work.path, subdir_count
                    );
                    dirs_count.fetch_add(1, Ordering::Relaxed);
                    pending_work.fetch_sub(1, Ordering::SeqCst);
                    // state + slot dropped here.
                } else {
                    // More pages for the same dir — advance cookie and
                    // re-submit on the same fh, same tag.
                    state.cookie = result.next_cookie;
                    state.cookieverf = result.next_cookieverf;
                    let tag = next_tag;
                    next_tag = next_tag.wrapping_add(1);
                    match nfs.submit_readdirplus_by_fh(
                        &state.file_handle,
                        state.cookie,
                        state.cookieverf,
                        tag,
                    ) {
                        Ok(new_slot) => {
                            slots.push(new_slot);
                            states.push(state);
                        }
                        Err(e) => {
                            errors_count.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                "Worker {} pipelined re-submit failed: {} -> {}",
                                id, state.work.path, e
                            );
                            pending_work.fetch_sub(1, Ordering::SeqCst);
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

/// SQLite writer thread - handles all database writes with optimized bulk loading
fn sqlite_writer_loop(mut conn: Connection, entry_rx: Receiver<Vec<DbEntry>>, batch_size: usize) -> Connection {
    debug!("SQLite writer thread started with batch_size={}", batch_size);

    // Optimize for bulk loading - these settings dramatically improve write performance
    conn.execute_batch(
        "PRAGMA synchronous = OFF;
         PRAGMA journal_mode = OFF;
         PRAGMA cache_size = -64000;
         PRAGMA temp_store = MEMORY;"
    ).expect("Failed to set bulk load pragmas");

    let mut total_written = 0u64;
    let mut pending: Vec<DbEntry> = Vec::with_capacity(batch_size * 2);

    // Receive batches and write to DB
    while let Ok(entries) = entry_rx.recv() {
        pending.extend(entries);

        // Write when we have enough
        if pending.len() >= batch_size {
            if let Err(e) = write_sqlite_batch(&mut conn, &pending) {
                error!("SQLite write failed: {}", e);
            } else {
                total_written += pending.len() as u64;
            }
            pending.clear();
        }
    }

    // Write remaining entries
    if !pending.is_empty() {
        if let Err(e) = write_sqlite_batch(&mut conn, &pending) {
            error!("Final SQLite write failed: {}", e);
        } else {
            total_written += pending.len() as u64;
        }
    }

    // Re-enable safety for final operations
    conn.execute_batch("PRAGMA synchronous = NORMAL;").ok();

    debug!("SQLite writer thread finished, wrote {} entries", total_written);
    conn
}

/// RocksDB writer thread - handles all database writes (single-shard).
///
/// `parquet_tx` is `Some` only when `--stream-parquet` is enabled. Each
/// successfully-written pending batch is forwarded via `try_send`; on a
/// full channel the batch is dropped and `parquet_drops` increments.
/// Drops surface in the writer-thread's final log line so they're
/// visible at end-of-scan -- ingest never blocks on the parquet writer.
#[cfg(feature = "rocksdb")]
fn rocksdb_writer_loop(
    handle: RocksHandle,
    entry_rx: Receiver<Vec<DbEntry>>,
    parquet_tx: Option<Sender<Vec<DbEntry>>>,
    batch_size: usize,
) -> std::result::Result<RocksHandle, crate::error::RocksError> {
    use crate::error::RocksError;
    use crate::rocksdb::summary::SummaryAccumulator;

    use rocksdb::WriteOptions;

    debug!("RocksDB writer thread started with batch_size={}", batch_size);

    let mut total_written = 0u64;
    let mut pending: Vec<DbEntry> = Vec::with_capacity(batch_size * 2);
    let mut entries_since_flush = 0u64;
    let mut writes_since_summary_flush = 0u32;
    let mut summary = SummaryAccumulator::new();
    let mut parquet_drops: u64 = 0;
    let mut parquet_drop_rows: u64 = 0;

    const FLUSH_INTERVAL: u64 = 1_000_000;
    const SUMMARY_FLUSH_EVERY_N_WRITES: u32 = 100;

    let mut write_opts = WriteOptions::default();
    write_opts.disable_wal(true);

    while let Ok(entries) = entry_rx.recv() {
        pending.extend(entries);

        if pending.len() >= batch_size {
            write_rocks_batch_shard(&handle, 0, &pending, &write_opts)?;
            summary.update(&pending);
            total_written += pending.len() as u64;
            entries_since_flush += pending.len() as u64;
            writes_since_summary_flush += 1;

            let written_batch = std::mem::replace(&mut pending, Vec::with_capacity(batch_size * 2));
            if let Some(ref tx) = parquet_tx {
                let row_count = written_batch.len();
                if let Err(crossbeam_channel::TrySendError::Full(_)) = tx.try_send(written_batch) {
                    parquet_drops += 1;
                    parquet_drop_rows += row_count as u64;
                }
            }

            if writes_since_summary_flush >= SUMMARY_FLUSH_EVERY_N_WRITES {
                summary.touch_now();
                flush_summary_to_cf(&handle, &summary, &write_opts)?;
                writes_since_summary_flush = 0;
            }

            if entries_since_flush >= FLUSH_INTERVAL {
                debug!("RocksDB periodic flush at {} entries", total_written);
                handle.db.flush().map_err(RocksError::Rocks)?;
                entries_since_flush = 0;
            }
        }
    }

    if !pending.is_empty() {
        write_rocks_batch_shard(&handle, 0, &pending, &write_opts)?;
        summary.update(&pending);
        total_written += pending.len() as u64;

        let final_batch = std::mem::take(&mut pending);
        if let Some(ref tx) = parquet_tx {
            let row_count = final_batch.len();
            if let Err(crossbeam_channel::TrySendError::Full(_)) = tx.try_send(final_batch) {
                parquet_drops += 1;
                parquet_drop_rows += row_count as u64;
            }
        }
    }

    drop(parquet_tx);

    summary.touch_now();
    flush_summary_to_cf(&handle, &summary, &write_opts)?;
    handle.db.flush().map_err(RocksError::Rocks)?;

    if parquet_drops > 0 {
        warn!(
            "Streaming Parquet dropped {} batches ({} rows) under backpressure -- \
             ingest was not stalled, but the parquet directory is missing those rows. \
             Re-running export-parquet against the finished RocksDB will produce a complete export.",
            parquet_drops, parquet_drop_rows
        );
    }
    debug!(
        "RocksDB writer thread finished, wrote {} entries (summary: {} files, {} dirs)",
        total_written, summary.total.total_files, summary.total.total_dirs
    );
    Ok(handle)
}

/// Per-shard writer thread loop for the multi-shard ingest path.
///
/// Each shard owns one path-CF and shares the inode CF with its peers
/// (per-CF memtables + `allow_concurrent_memtable_write` make this
/// safe). Periodic `db.flush()` is serialized via `flush_lock` so two
/// shards don't issue overlapping flushes; the accumulator is updated
/// per-shard and merged once at end-of-scan in the spawning thread.
#[cfg(feature = "rocksdb")]
#[allow(clippy::too_many_arguments)]
fn rocksdb_writer_loop_shard(
    handle: Arc<RocksHandle>,
    shard_idx: usize,
    entry_rx: Receiver<Vec<DbEntry>>,
    parquet_tx: Option<Sender<Vec<DbEntry>>>,
    batch_size: usize,
    flush_lock: Arc<std::sync::Mutex<()>>,
    flush_counter: Arc<AtomicU64>,
) -> std::result::Result<crate::rocksdb::summary::SummaryAccumulator, crate::error::RocksError> {
    use crate::error::RocksError;
    use crate::rocksdb::summary::SummaryAccumulator;
    use rocksdb::WriteOptions;

    debug!(
        "RocksDB writer thread (shard {}) started with batch_size={}",
        shard_idx, batch_size
    );

    let mut total_written = 0u64;
    let mut pending: Vec<DbEntry> = Vec::with_capacity(batch_size * 2);
    let mut summary = SummaryAccumulator::new();
    let mut parquet_drops: u64 = 0;
    let mut parquet_drop_rows: u64 = 0;

    // Across-shard flush threshold. Any single shard crossing this since
    // its last contribution triggers one global db.flush(); sharing the
    // counter (rather than per-shard) avoids amplifying flush count
    // proportional to N when the ingest rate is fixed.
    const GLOBAL_FLUSH_INTERVAL: u64 = 1_000_000;

    let mut write_opts = WriteOptions::default();
    write_opts.disable_wal(true);

    while let Ok(entries) = entry_rx.recv() {
        pending.extend(entries);

        if pending.len() >= batch_size {
            let batch_len = pending.len() as u64;
            write_rocks_batch_shard(&handle, shard_idx, &pending, &write_opts)?;
            summary.update(&pending);
            total_written += batch_len;

            let written_batch = std::mem::replace(&mut pending, Vec::with_capacity(batch_size * 2));
            if let Some(ref tx) = parquet_tx {
                let row_count = written_batch.len();
                if let Err(crossbeam_channel::TrySendError::Full(_)) = tx.try_send(written_batch) {
                    parquet_drops += 1;
                    parquet_drop_rows += row_count as u64;
                }
            }

            // Bump the shared cross-shard counter; check the threshold
            // outside the mutex so the lock is held only when an actual
            // flush is needed.
            flush_counter.fetch_add(batch_len, Ordering::Relaxed);
        }

        if flush_counter.load(Ordering::Relaxed) >= GLOBAL_FLUSH_INTERVAL {
            if let Ok(guard) = flush_lock.try_lock() {
                // Recheck under the lock to avoid duplicate flushes.
                if flush_counter.load(Ordering::Relaxed) >= GLOBAL_FLUSH_INTERVAL {
                    debug!(
                        "RocksDB global flush triggered by shard {}",
                        shard_idx
                    );
                    handle.db.flush().map_err(RocksError::Rocks)?;
                    flush_counter.store(0, Ordering::Relaxed);
                }
                drop(guard);
            }
        }
    }

    if !pending.is_empty() {
        write_rocks_batch_shard(&handle, shard_idx, &pending, &write_opts)?;
        summary.update(&pending);
        total_written += pending.len() as u64;

        let final_batch = std::mem::take(&mut pending);
        if let Some(ref tx) = parquet_tx {
            let row_count = final_batch.len();
            if let Err(crossbeam_channel::TrySendError::Full(_)) = tx.try_send(final_batch) {
                parquet_drops += 1;
                parquet_drop_rows += row_count as u64;
            }
        }
    }

    drop(parquet_tx);

    if parquet_drops > 0 {
        warn!(
            "Streaming Parquet dropped {} batches ({} rows) on shard {} under backpressure",
            parquet_drops, parquet_drop_rows, shard_idx
        );
    }
    debug!(
        "RocksDB writer thread (shard {}) finished, wrote {} entries (shard summary: {} files, {} dirs)",
        shard_idx, total_written, summary.total.total_files, summary.total.total_dirs
    );
    Ok(summary)
}


/// Serialize the in-memory accumulator into the five summary keys and
/// flush them via a single WAL-disabled WriteBatch.
#[cfg(feature = "rocksdb")]
fn flush_summary_to_cf(
    handle: &RocksHandle,
    summary: &crate::rocksdb::summary::SummaryAccumulator,
    write_opts: &rocksdb::WriteOptions,
) -> std::result::Result<(), crate::error::RocksError> {
    use crate::error::RocksError;
    use rocksdb::WriteBatch;

    let cf = match handle.cf_summary() {
        Some(cf) => cf,
        // Should not happen for DBs created by this binary. If a primary
        // somehow opens a legacy DB without the CF, just skip flushing.
        None => return Ok(()),
    };

    let kv = summary
        .serialize_kv()
        .map_err(|e| RocksError::Bincode(e.to_string()))?;

    let mut batch = WriteBatch::default();
    for (k, v) in kv {
        batch.put_cf(cf, k, &v);
    }
    handle
        .db
        .write_opt(batch, write_opts)
        .map_err(RocksError::Rocks)
}

/// Write a batch of entries owned by `shard_idx` to RocksDB.
///
/// All entries in the batch must hash to `shard_idx` under
/// `path_to_shard(...)` — the caller (worker `ShardedSender`) guarantees
/// this by routing per-entry. The path entry goes to that shard's CF
/// and the inode entry goes to the (shared) inode CF. Inode-CF
/// concurrent writes from N shards are safe because RocksDB has
/// `allow_concurrent_memtable_write = true` set globally.
#[cfg(feature = "rocksdb")]
fn write_rocks_batch_shard(
    handle: &RocksHandle,
    shard_idx: usize,
    entries: &[DbEntry],
    write_opts: &rocksdb::WriteOptions,
) -> std::result::Result<(), crate::error::RocksError> {
    use crate::error::RocksError;
    use crate::rocksdb::schema::{encode_inode_key, encode_path_key, RocksEntry};
    use rocksdb::WriteBatch;

    let mut batch = WriteBatch::default();
    let cf_path = handle.cf_entries_by_path_shard(shard_idx);
    let cf_inode = handle.cf_entries_by_inode();

    for entry in entries {
        let rocks_entry = RocksEntry::from_db_entry(entry);
        let value = rocks_entry
            .to_bytes()
            .map_err(|e| RocksError::Bincode(e.to_string()))?;

        let path_key = encode_path_key(&entry.path);
        let inode_key = encode_inode_key(entry.inode);

        batch.put_cf(&cf_path, &path_key, &value);
        batch.put_cf(&cf_inode, &inode_key, &value);
    }

    handle.db.write_opt(batch, write_opts).map_err(RocksError::Rocks)
}

/// Big directory worker thread - counts files per directory and reports large ones
fn big_dir_worker_loop(
    id: usize,
    nfs_url: crate::config::NfsUrl,
    local: DequeWorker<DirWork>,
    injector: Arc<Injector<DirWork>>,
    stealers: Arc<Vec<Stealer<DirWork>>>,
    big_dir_tx: Sender<BigDirEntry>,
    shutdown: Arc<AtomicBool>,
    dirs_count: Arc<AtomicU64>,
    _files_count: Arc<AtomicU64>,
    _bytes_count: Arc<AtomicU64>,
    errors_count: Arc<AtomicU64>,
    big_dirs_count: Arc<AtomicU64>,
    active_workers: Arc<AtomicUsize>,
    pending_work: Arc<AtomicU64>,
    max_depth: Option<usize>,
    threshold: u64,
    _worker_count: usize,
    timeout_secs: u32,
) {
    use crate::nfs::types::EntryType;

    debug!("Big-dir worker {} started (threshold={})", id, threshold);

    // Create NfsConnection for this worker using the full NfsUrl config
    let conn = match NfsConnectionBuilder::new(nfs_url)
        .timeout(Duration::from_secs(timeout_secs as u64))
        .retries(3)
        .connect()
    {
        Ok(c) => {
            debug!("Big-dir worker {} connected via NfsConnection", id);
            c
        }
        Err(e) => {
            error!("Big-dir worker {} failed to connect: {}", id, e);
            return;
        }
    };

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

        debug!("Big-dir worker {} READDIRPLUS: {}", id, work.path);

        // Use readdir_plus_chunked to scan directory with early exit on threshold
        let mut file_count: u64 = 0;
        let mut threshold_hit = false;
        let mut subdirs: Vec<String> = Vec::new();

        let scan_result = conn.readdir_plus_chunked(&work.path, 1000, |entries| {
            for entry in entries {
                // Skip . and ..
                if entry.name == "." || entry.name == ".." {
                    continue;
                }

                if entry.entry_type == EntryType::Directory {
                    subdirs.push(entry.name);
                } else if !threshold_hit {
                    file_count += 1;
                    if file_count >= threshold {
                        threshold_hit = true;
                        // Return false to stop early - we found a big directory
                        return false;
                    }
                }
            }
            // Continue reading
            true
        });

        match scan_result {
            Ok(_) => {
                dirs_count.fetch_add(1, Ordering::Relaxed);

                // Queue subdirectories for processing
                for subdir_name in &subdirs {
                    let full_path = if work.path == "/" {
                        format!("/{}", subdir_name)
                    } else {
                        format!("{}/{}", work.path, subdir_name)
                    };
                    pending_work.fetch_add(1, Ordering::SeqCst);
                    local.push(DirWork {
                        path: full_path,
                        depth: work.depth + 1,
                        file_handle: None, // big-dir-hunt mode doesn't have cached handles
                    });
                }

                // If this directory hit/exceeded threshold, record it
                if threshold_hit || file_count >= threshold {
                    big_dirs_count.fetch_add(1, Ordering::Relaxed);
                    let big_dir = BigDirEntry {
                        path: work.path.clone(),
                        file_count,
                    };
                    if big_dir_tx.send(big_dir).is_err() {
                        // Channel closed, shutdown
                        break;
                    }
                    info!(
                        "Found big directory: {} ({}+ files)",
                        work.path, file_count
                    );
                }

                debug!(
                    "Big-dir worker {} complete: {} -> {} subdirs, {} files (threshold_hit={})",
                    id, work.path, subdirs.len(), file_count, threshold_hit
                );
            }
            Err(e) => {
                errors_count.fetch_add(1, Ordering::Relaxed);
                let err_str = format!("{:?}", e);
                if err_str.contains("NotFound") || err_str.contains("not found")
                    || err_str.contains("PermissionDenied") || err_str.contains("Permission denied") {
                    debug!("Big-dir worker {} READDIRPLUS error: {} -> {:?}", id, work.path, e);
                } else {
                    warn!(
                        "Big-dir worker {} READDIRPLUS failed: {} -> {:?}",
                        id, work.path, e
                    );
                }
            }
        }

        // Mark this work item as done
        pending_work.fetch_sub(1, Ordering::SeqCst);
        active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    debug!("Big-dir worker {} finished", id);
}

/// Big directory writer thread - writes big directories to RocksDB
#[cfg(feature = "rocksdb")]
fn big_dir_writer_loop(
    handle: RocksHandle,
    big_dir_rx: Receiver<BigDirEntry>,
) -> std::result::Result<RocksHandle, crate::error::RocksError> {
    use crate::error::RocksError;

    debug!("Big-dir writer thread started");

    let mut total_written = 0u64;

    // Receive big directories and write to DB
    while let Ok(big_dir) = big_dir_rx.recv() {
        handle
            .put_big_dir(&big_dir.path, big_dir.file_count)
            .map_err(RocksError::Rocks)?;
        total_written += 1;
    }

    // Flush memtables
    handle.db.flush().map_err(RocksError::Rocks)?;

    debug!(
        "Big-dir writer thread finished, wrote {} big directories",
        total_written
    );
    Ok(handle)
}

/// Write a batch of entries to SQLite using prepared statement
fn write_sqlite_batch(conn: &mut Connection, entries: &[DbEntry]) -> Result<()> {
    let tx = conn.transaction()
        .map_err(|e| WalkerError::Database(e.into()))?;

    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO entries (parent_id, name, path, entry_type, size, mtime, atime, ctime, mode, uid, gid, nlink, inode, depth, extension, blocks, checksum, file_type)
             VALUES (NULL, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)"
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
                entry.extension,
                entry.blocks as i64,
                entry.checksum,
                entry.file_type,
            ]).map_err(|e| WalkerError::Database(e.into()))?;
        }
    }

    tx.commit().map_err(|e| WalkerError::Database(e.into()))?;
    Ok(())
}

/// Build the canonical streaming-Parquet directory for a RocksDB path.
///
/// Format: `<rocks_path>.parquet/scans/<scan_id>/`
/// Example: `/data/scan.rocks` + `abc123` → `/data/scan.rocks.parquet/scans/abc123/`
#[cfg(all(feature = "rocksdb", feature = "parquet"))]
fn streaming_parquet_dir(rocks_path: &std::path::Path, scan_id: &str) -> std::path::PathBuf {
    let mut sibling = rocks_path.as_os_str().to_owned();
    sibling.push(".parquet");
    std::path::PathBuf::from(sibling).join("scans").join(scan_id)
}

/// Bundle of items returned when the streaming Parquet writer is spawned.
#[cfg(all(feature = "rocksdb", feature = "parquet"))]
#[derive(Default)]
struct StreamingParquetSpawn {
    /// Sender threaded into the rocksdb writer loop. None = streaming off.
    tx: Option<Sender<Vec<DbEntry>>>,
    /// Join handle for the streaming writer thread.
    join: Option<JoinHandle<Result<crate::parquet::StreamingParquetStats>>>,
    /// scan_id generated at scan start (used to populate RocksDB metadata).
    scan_id: Option<String>,
}

/// Spawn the streaming Parquet writer thread. Consumes batches from
/// `parquet_rx`, drives the writer to rotation, and closes cleanly when
/// the channel disconnects (which happens when the rocksdb writer
/// thread drops its `Sender`).
#[cfg(all(feature = "rocksdb", feature = "parquet"))]
fn spawn_parquet_writer(
    mut writer: crate::parquet::StreamingParquetWriter,
    parquet_rx: Receiver<Vec<DbEntry>>,
) -> JoinHandle<Result<crate::parquet::StreamingParquetStats>> {
    thread::Builder::new()
        .name("parquet-writer".to_string())
        .spawn(move || -> Result<crate::parquet::StreamingParquetStats> {
            debug!("Streaming Parquet writer thread started");
            while let Ok(entries) = parquet_rx.recv() {
                if let Err(e) = writer.write_batch(&entries) {
                    warn!(
                        "Streaming Parquet write_batch failed: {} (skipping {} rows)",
                        e,
                        entries.len()
                    );
                    // On a write failure we keep running; the rocksdb
                    // writer is the source of truth and a later
                    // export-parquet against the finished DB can
                    // produce a clean export.
                }
            }
            debug!("Streaming Parquet writer thread closing");
            writer.close()
        })
        .expect("Failed to spawn streaming Parquet writer thread")
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

    /// End-to-end test for the PR #2 streaming pipeline.
    ///
    /// Drives `rocksdb_writer_loop` directly (skipping the NFS workers)
    /// with a synthetic batch stream, fans out to `spawn_parquet_writer`,
    /// then verifies both stores end up with the same row count and that
    /// the streamed Parquet files round-trip through the Arrow reader.
    #[cfg(all(feature = "rocksdb", feature = "parquet"))]
    #[test]
    fn streaming_writer_pipeline_round_trips_through_parquet() {
        use crate::nfs::types::EntryType;
        use crate::parquet::{StreamingParquetConfig, StreamingParquetWriter};
        use crate::rocksdb::RocksWriterConfig;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let rocks_path = dir.path().join("scan.rocks");
        let scan_dir = dir.path().join("scan.rocks.parquet/scans/test");

        // Open a fresh RocksDB and pull out the handle.
        let writer = RocksWriter::open(&rocks_path, RocksWriterConfig::default()).unwrap();
        let rocks_handle = writer.into_handle();

        // Open a streaming parquet writer.
        let pq_cfg = StreamingParquetConfig {
            scan_dir: scan_dir.clone(),
            row_group_size: 100,
            target_file_size: 1_000_000_000,
            compression_level: 3,
            scan_id: "test".to_string(),
            scan_timestamp_us: 1_700_000_000_000_000,
        };
        let pq_writer = StreamingParquetWriter::open(pq_cfg).unwrap();

        // Channels.
        let (entry_tx, entry_rx) = bounded::<Vec<DbEntry>>(100);
        let (pq_tx, pq_rx) = bounded::<Vec<DbEntry>>(100);

        // Spawn parquet writer thread.
        let pq_join = spawn_parquet_writer(pq_writer, pq_rx);

        // Drive the rocksdb writer in another thread so we can feed batches.
        let rocks_join = thread::Builder::new()
            .name("rocks-writer-test".to_string())
            .spawn(move || rocksdb_writer_loop(rocks_handle, entry_rx, Some(pq_tx), 100))
            .unwrap();

        // Synthetic batches: 5 batches of 250 entries each = 1250 rows total,
        // which exceeds batch_size=100 multiple times so the forward path
        // is exercised.
        for chunk in 0..5 {
            let batch: Vec<DbEntry> = (0..250)
                .map(|i| DbEntry {
                    parent_path: Some("/".to_string()),
                    name: format!("file_{}_{}.txt", chunk, i),
                    path: format!("/file_{}_{}.txt", chunk, i),
                    entry_type: EntryType::File,
                    size: 100 + i as u64,
                    mtime: None,
                    atime: None,
                    ctime: None,
                    mode: Some(0o644),
                    uid: Some(1000),
                    gid: Some(1000),
                    nlink: Some(1),
                    inode: (chunk * 1000 + i) as u64,
                    depth: 1,
                    extension: Some("txt".to_string()),
                    blocks: 1,
                    checksum: None,
                    file_type: None,
                })
                .collect();
            entry_tx.send(batch).unwrap();
        }
        drop(entry_tx);

        let _rocks_handle = rocks_join.join().unwrap().unwrap();
        let pq_stats = pq_join.join().unwrap().unwrap();

        // All 1250 rows landed in Parquet.
        assert_eq!(pq_stats.rows_written, 1250);
        assert!(pq_stats.parts_written >= 1);

        // Re-read the parquet directory and count rows.
        let mut total = 0usize;
        for entry in std::fs::read_dir(&scan_dir).unwrap() {
            let path = entry.unwrap().path();
            if path
                .extension()
                .map(|e| e == "parquet")
                .unwrap_or(false)
                && !path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with('.')
            {
                let file = File::open(&path).unwrap();
                let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                    .unwrap()
                    .build()
                    .unwrap();
                for batch in reader {
                    total += batch.unwrap().num_rows();
                }
            }
        }
        assert_eq!(total, 1250);

        // No leftover .tmp files.
        for entry in std::fs::read_dir(&scan_dir).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().to_string();
            assert!(
                !name.ends_with(".tmp"),
                "stray .tmp file: {}",
                name
            );
        }
    }
}
