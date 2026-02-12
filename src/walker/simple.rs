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

        // Channel for entries to write (workers -> writer)
        let (entry_tx, entry_rx) = bounded::<Vec<DbEntry>>(100);

        // Spawn dedicated writer thread
        let writer_handle = self.spawn_sqlite_writer(db, entry_rx);

        // Run workers
        self.run_workers(entry_tx)?;

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
        let (big_dir_tx, big_dir_rx) = bounded::<BigDirEntry>(100);

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

    /// Run walker with RocksDB output
    #[cfg(feature = "rocksdb")]
    fn run_rocksdb(&self) -> Result<WalkStats> {
        let start = Instant::now();

        // Open RocksDB
        info!("Opening RocksDB: {}", self.config.output_path.display());
        let rocks_path = self.config.output_path.clone();

        // Channel for entries to write (workers -> writer)
        let (entry_tx, entry_rx) = bounded::<Vec<DbEntry>>(100);

        // Spawn dedicated RocksDB writer thread
        let writer_handle = self.spawn_rocksdb_writer(rocks_path.clone(), entry_rx)?;

        // Run workers
        self.run_workers(entry_tx)?;

        // Wait for writer to finish
        let rocks_handle = writer_handle.join().expect("RocksDB writer thread panicked")
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
        let (big_dir_tx, big_dir_rx) = bounded::<BigDirEntry>(100);

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

    /// Run worker threads (shared between SQLite and RocksDB modes)
    fn run_workers(&self, entry_tx: Sender<Vec<DbEntry>>) -> Result<()> {
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
            let entry_tx = entry_tx.clone();
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

            let handle = thread::Builder::new()
                .name(format!("walker-{}", id))
                .spawn(move || {
                    worker_loop(
                        id,
                        nfs,
                        local,
                        injector,
                        stealers,
                        entry_tx,
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
                })
                .expect("Failed to spawn worker thread");

            handles.push(handle);
        }

        // Drop our sender so writer knows when to stop
        drop(entry_tx);

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

    #[cfg(feature = "rocksdb")]
    fn spawn_rocksdb_writer(
        &self,
        path: std::path::PathBuf,
        entry_rx: Receiver<Vec<DbEntry>>,
    ) -> Result<JoinHandle<std::result::Result<RocksHandle, crate::error::RocksError>>> {
        

        // Create RocksDB with metadata
        let config = RocksWriterConfig::default();

        // Remove existing directory if present
        if path.exists() {
            std::fs::remove_dir_all(&path).map_err(|e| WalkerError::Io(e))?;
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

        let handle = writer.into_handle();
        let batch_size = self.config.batch_size;

        // Spawn writer thread
        let handle = thread::Builder::new()
            .name("rocks-writer".to_string())
            .spawn(move || {
                rocksdb_writer_loop(handle, entry_rx, batch_size)
            })
            .expect("Failed to spawn RocksDB writer thread");

        Ok(handle)
    }
}

/// Worker thread - processes directories using READDIRPLUS
fn worker_loop(
    id: usize,
    nfs: NfsConnection,
    local: DequeWorker<DirWork>,
    injector: Arc<Injector<DirWork>>,
    stealers: Arc<Vec<Stealer<DirWork>>>,
    entry_tx: Sender<Vec<DbEntry>>,
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

    let mut batch: Vec<DbEntry> = Vec::with_capacity(batch_size);
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

                // Track index before pushing (for content analysis)
                let entry_idx = batch.len();
                batch.push(db_entry);

                if is_dir {
                    // Queue subdirectory for processing with cached file handle
                    subdir_count += 1;
                    pending_work.fetch_add(1, Ordering::SeqCst);
                    local.push(DirWork {
                        path: full_path,
                        depth: work.depth + 1,
                        file_handle: nfs_entry.file_handle.clone(),
                    });
                } else {
                    chunk_file_count += 1;
                    chunk_byte_count += nfs_entry.size();

                    // Track files for content analysis (will process after dir walk)
                    if needs_content {
                        let file_size = nfs_entry.size();
                        files_for_content.push((full_path.clone(), entry_idx, file_size));
                    }
                }

                // Send batch if full (skip if content analysis needed - process after walk)
                if batch.len() >= batch_size && !needs_content {
                    if entry_tx.send(std::mem::replace(&mut batch, Vec::with_capacity(batch_size))).is_err() {
                        channel_broken = true;
                        return false; // Stop reading if channel is broken
                    }
                    // Update progress counters incrementally for real-time display
                    // (especially important for large flat directories)
                    files_count.fetch_add(chunk_file_count, Ordering::Relaxed);
                    bytes_count.fetch_add(chunk_byte_count, Ordering::Relaxed);
                    chunk_file_count = 0;
                    chunk_byte_count = 0;
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
                        if idx >= batch.len() {
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
                                batch[idx].checksum = Some(compute_gxhash(&data));
                            }
                            if detect_file_type {
                                let header_len = std::cmp::min(data.len(), 8192);
                                batch[idx].file_type = detect_mime_type(&data[..header_len]);
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

                // Send batch now if content analysis was enabled (deferred sending)
                if needs_content && !batch.is_empty() {
                    if entry_tx.send(std::mem::replace(&mut batch, Vec::with_capacity(batch_size))).is_err() {
                        // Channel closed, we're shutting down
                        debug!("Worker {} channel closed during content analysis send", id);
                    }
                }
            }
            Err(e) => {
                errors_count.fetch_add(1, Ordering::Relaxed);
                // Not found errors are common on active filesystems (race condition)
                // Log them at debug level to reduce noise
                if e.to_string().contains("not found") || e.to_string().contains("No such file") {
                    debug!("Worker {} READDIRPLUS not found: {}", id, work.path);
                } else {
                    warn!("Worker {} READDIRPLUS failed: {} -> {}", id, work.path, e);
                }
            }
        }

        // Mark this work item as done
        pending_work.fetch_sub(1, Ordering::SeqCst);
        active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    // Send remaining batch
    if !batch.is_empty() {
        let _ = entry_tx.send(batch);
    }

    debug!("Worker {} finished", id);
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

/// RocksDB writer thread - handles all database writes
#[cfg(feature = "rocksdb")]
fn rocksdb_writer_loop(
    handle: RocksHandle,
    entry_rx: Receiver<Vec<DbEntry>>,
    batch_size: usize,
) -> std::result::Result<RocksHandle, crate::error::RocksError> {
    use crate::error::RocksError;

    use rocksdb::WriteOptions;

    debug!("RocksDB writer thread started with batch_size={}", batch_size);

    let mut total_written = 0u64;
    let mut pending: Vec<DbEntry> = Vec::with_capacity(batch_size * 2);
    let mut entries_since_flush = 0u64;

    // Flush every 1M entries to limit memory usage
    // This prevents unbounded memtable growth for large scans
    const FLUSH_INTERVAL: u64 = 1_000_000;

    // Write options with WAL disabled for speed
    let mut write_opts = WriteOptions::default();
    write_opts.disable_wal(true);

    // Receive batches and write to DB
    while let Ok(entries) = entry_rx.recv() {
        pending.extend(entries);

        // Write when we have enough
        if pending.len() >= batch_size {
            write_rocks_batch(&handle, &pending, &write_opts)?;
            total_written += pending.len() as u64;
            entries_since_flush += pending.len() as u64;
            pending.clear();

            // Periodic flush to limit memory usage
            if entries_since_flush >= FLUSH_INTERVAL {
                debug!("RocksDB periodic flush at {} entries", total_written);
                handle.db.flush().map_err(RocksError::Rocks)?;
                entries_since_flush = 0;
            }
        }
    }

    // Write remaining entries
    if !pending.is_empty() {
        write_rocks_batch(&handle, &pending, &write_opts)?;
        total_written += pending.len() as u64;
    }

    // Final flush
    handle.db.flush().map_err(RocksError::Rocks)?;

    debug!("RocksDB writer thread finished, wrote {} entries", total_written);
    Ok(handle)
}

/// Write a batch of entries to RocksDB
#[cfg(feature = "rocksdb")]
fn write_rocks_batch(
    handle: &RocksHandle,
    entries: &[DbEntry],
    write_opts: &rocksdb::WriteOptions,
) -> std::result::Result<(), crate::error::RocksError> {
    use crate::error::RocksError;
    use crate::rocksdb::schema::{encode_inode_key, encode_path_key, RocksEntry};
    use rocksdb::WriteBatch;

    let mut batch = WriteBatch::default();
    let cf_path = handle.cf_entries_by_path();
    let cf_inode = handle.cf_entries_by_inode();

    for entry in entries {
        let rocks_entry = RocksEntry::from_db_entry(entry);
        let value = rocks_entry.to_bytes()
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
                if err_str.contains("NotFound") || err_str.contains("not found") {
                    debug!("Big-dir worker {} READDIRPLUS not found: {}", id, work.path);
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
}
