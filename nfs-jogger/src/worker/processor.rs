//! Bundle processor for worker nodes
//!
//! Processes bundles pulled from the Redis queue, fetching file
//! metadata via NFS and writing results to a database.

use crate::bundle::{Bundle, BundleState};
use crate::config::WorkerConfig;
use crate::error::{JoggerError, Result, WorkerError};
use crate::nfs::{DbEntry, NfsConnection, NfsConnectionBuilder};
use crate::queue::{RedisQueue, WorkQueue};

use rusqlite::{params, Connection, Transaction};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Progress information during worker operation
#[derive(Debug, Clone, Default)]
pub struct WorkerProgress {
    /// Bundles processed
    pub bundles_processed: u64,
    /// Bundles failed
    pub bundles_failed: u64,
    /// Files processed
    pub files_processed: u64,
    /// Directories processed
    pub dirs_processed: u64,
    /// Bytes processed
    pub bytes_processed: u64,
    /// Errors encountered
    pub errors: u64,
    /// Current bundle ID being processed
    pub current_bundle: Option<String>,
    /// Elapsed time
    pub elapsed: Duration,
}

/// Final statistics from worker
#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    /// Total bundles processed
    pub bundles_processed: u64,
    /// Total bundles failed
    pub bundles_failed: u64,
    /// Total files processed
    pub files_processed: u64,
    /// Total directories processed
    pub dirs_processed: u64,
    /// Total bytes processed
    pub bytes_processed: u64,
    /// Total errors
    pub errors: u64,
    /// Total duration
    pub duration: Duration,
}

/// Worker for processing bundles
pub struct BundleWorker {
    config: WorkerConfig,
    shutdown: Arc<AtomicBool>,
}

impl BundleWorker {
    /// Create a new bundle worker
    pub fn new(config: WorkerConfig) -> Self {
        Self {
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Run the worker
    pub async fn run<F>(&self, progress_callback: F) -> Result<WorkerStats>
    where
        F: Fn(WorkerProgress) + Send + Sync + 'static,
    {
        let start = Instant::now();

        // Connect to Redis queue
        let queue = Arc::new(
            RedisQueue::new(self.config.queue_config.clone())
                .await
                .map_err(JoggerError::Queue)?
        );

        // Initialize database
        let db_path = &self.config.output_path;
        self.init_database(db_path)?;

        // Create atomic counters
        let bundles_processed = Arc::new(AtomicU64::new(0));
        let bundles_failed = Arc::new(AtomicU64::new(0));
        let files_processed = Arc::new(AtomicU64::new(0));
        let dirs_processed = Arc::new(AtomicU64::new(0));
        let bytes_processed = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let current_bundle = Arc::new(RwLock::new(None::<String>));

        // Spawn progress reporter
        let progress_handle = if self.config.show_progress {
            let bundles_processed_clone = bundles_processed.clone();
            let bundles_failed_clone = bundles_failed.clone();
            let files_processed_clone = files_processed.clone();
            let dirs_processed_clone = dirs_processed.clone();
            let bytes_processed_clone = bytes_processed.clone();
            let errors_clone = errors.clone();
            let current_bundle_clone = current_bundle.clone();
            let shutdown_clone = self.shutdown.clone();

            let callback = Arc::new(progress_callback);

            Some(tokio::spawn(async move {
                let start = Instant::now();
                while !shutdown_clone.load(Ordering::Relaxed) {
                    let current = current_bundle_clone.read().await.clone();
                    let progress = WorkerProgress {
                        bundles_processed: bundles_processed_clone.load(Ordering::Relaxed),
                        bundles_failed: bundles_failed_clone.load(Ordering::Relaxed),
                        files_processed: files_processed_clone.load(Ordering::Relaxed),
                        dirs_processed: dirs_processed_clone.load(Ordering::Relaxed),
                        bytes_processed: bytes_processed_clone.load(Ordering::Relaxed),
                        errors: errors_clone.load(Ordering::Relaxed),
                        current_bundle: current,
                        elapsed: start.elapsed(),
                    };
                    callback(progress);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }))
        } else {
            None
        };

        // Spawn heartbeat task
        let queue_clone = queue.clone();
        let worker_id = self.config.worker_id.clone();
        let shutdown_clone = self.shutdown.clone();
        let heartbeat_interval = self.config.queue_config.heartbeat_interval;

        let heartbeat_handle = tokio::spawn(async move {
            while !shutdown_clone.load(Ordering::Relaxed) {
                if let Err(e) = queue_clone.heartbeat(&worker_id).await {
                    tracing::warn!("Failed to send heartbeat: {}", e);
                }
                tokio::time::sleep(heartbeat_interval).await;
            }
        });

        // NFS connections cache (per server)
        let mut nfs_connections: HashMap<String, NfsConnection> = HashMap::new();

        // Main processing loop
        let mut processed_count = 0u64;

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            // Check max bundles limit
            if let Some(max) = self.config.max_bundles {
                if processed_count >= max {
                    break;
                }
            }

            // Try to get a bundle
            let timeout = if self.config.continuous {
                Duration::from_secs(5)
            } else {
                Duration::from_secs(1)
            };

            match queue.pop(&self.config.worker_id, timeout).await {
                Ok(Some(bundle)) => {
                    let bundle_id = bundle.id().to_string();
                    *current_bundle.write().await = Some(bundle_id.clone());

                    tracing::info!("Processing bundle {} ({} paths)", bundle_id, bundle.len());

                    // Process the bundle
                    match self.process_bundle(
                        &bundle,
                        &mut nfs_connections,
                        db_path,
                    ).await {
                        Ok((files, dirs, bytes)) => {
                            files_processed.fetch_add(files, Ordering::Relaxed);
                            dirs_processed.fetch_add(dirs, Ordering::Relaxed);
                            bytes_processed.fetch_add(bytes, Ordering::Relaxed);
                            bundles_processed.fetch_add(1, Ordering::Relaxed);

                            // Acknowledge the bundle
                            if let Err(e) = queue.ack(&bundle_id).await {
                                tracing::error!("Failed to ack bundle {}: {}", bundle_id, e);
                            }
                        }
                        Err(e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            bundles_failed.fetch_add(1, Ordering::Relaxed);
                            tracing::error!("Failed to process bundle {}: {}", bundle_id, e);

                            // Nack the bundle
                            if let Err(e2) = queue.nack(&bundle_id, &e.to_string()).await {
                                tracing::error!("Failed to nack bundle {}: {}", bundle_id, e2);
                            }
                        }
                    }

                    *current_bundle.write().await = None;
                    processed_count += 1;
                }
                Ok(None) => {
                    // No bundle available
                    if !self.config.continuous {
                        // Check if queue is empty
                        match queue.is_empty().await {
                            Ok(true) => {
                                tracing::info!("Queue is empty, exiting");
                                break;
                            }
                            Ok(false) => {
                                // Queue not empty, keep waiting
                            }
                            Err(e) => {
                                tracing::warn!("Failed to check queue status: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to pop bundle: {}", e);
                    errors.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        // Shutdown
        self.shutdown.store(true, Ordering::SeqCst);

        // Wait for background tasks
        heartbeat_handle.abort();
        if let Some(handle) = progress_handle {
            handle.abort();
        }

        Ok(WorkerStats {
            bundles_processed: bundles_processed.load(Ordering::Relaxed),
            bundles_failed: bundles_failed.load(Ordering::Relaxed),
            files_processed: files_processed.load(Ordering::Relaxed),
            dirs_processed: dirs_processed.load(Ordering::Relaxed),
            bytes_processed: bytes_processed.load(Ordering::Relaxed),
            errors: errors.load(Ordering::Relaxed),
            duration: start.elapsed(),
        })
    }

    /// Initialize the output database
    fn init_database(&self, path: &Path) -> Result<()> {
        let conn = Connection::open(path).map_err(|e| {
            JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        // Set pragmas for performance
        conn.execute_batch(
            "PRAGMA synchronous = OFF;
             PRAGMA journal_mode = WAL;
             PRAGMA cache_size = -128000;
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = 1073741824;"
        ).map_err(|e| {
            JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        // Create tables
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS entries (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                path TEXT NOT NULL UNIQUE,
                entry_type INTEGER NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                mtime INTEGER,
                atime INTEGER,
                ctime INTEGER,
                mode INTEGER,
                uid INTEGER,
                gid INTEGER,
                nlink INTEGER,
                inode INTEGER,
                depth INTEGER NOT NULL,
                extension TEXT,
                blocks INTEGER DEFAULT 0,
                bundle_id TEXT
            );

            CREATE TABLE IF NOT EXISTS bundles (
                id TEXT PRIMARY KEY,
                nfs_server TEXT NOT NULL,
                nfs_export TEXT NOT NULL,
                path_prefix TEXT NOT NULL,
                paths_count INTEGER NOT NULL,
                processed_at TEXT NOT NULL,
                duration_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS worker_info (
                key TEXT PRIMARY KEY,
                value TEXT
            );"
        ).map_err(|e| {
            JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        // Record worker info
        conn.execute(
            "INSERT OR REPLACE INTO worker_info (key, value) VALUES ('worker_id', ?1)",
            params![self.config.worker_id],
        ).map_err(|e| {
            JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        Ok(())
    }

    /// Process a single bundle
    async fn process_bundle(
        &self,
        bundle: &Bundle,
        nfs_connections: &mut HashMap<String, NfsConnection>,
        db_path: &Path,
    ) -> Result<(u64, u64, u64)> {
        let start = Instant::now();

        // Get or create NFS connection for this server
        let server_key = format!("{}:{}", bundle.metadata.nfs_server, bundle.metadata.nfs_export);

        if !nfs_connections.contains_key(&server_key) {
            let conn = NfsConnectionBuilder::new(crate::config::NfsUrl {
                server: bundle.metadata.nfs_server.clone(),
                port: None,
                export: bundle.metadata.nfs_export.clone(),
                subpath: String::new(),
            })
            .timeout(self.config.timeout)
            .retries(self.config.retry_count)
            .connect()
            .map_err(|e| JoggerError::Nfs(e))?;

            nfs_connections.insert(server_key.clone(), conn);
        }

        let nfs = nfs_connections.get_mut(&server_key).unwrap();

        // Open database
        let mut conn = Connection::open(db_path).map_err(|e| {
            JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        // Process paths and collect entries
        let mut entries = Vec::new();
        let mut files_count = 0u64;
        let mut dirs_count = 0u64;
        let mut bytes_count = 0u64;

        for path_entry in &bundle.paths {
            // Get file attributes via NFS
            match nfs.stat(&path_entry.path) {
                Ok(stat) => {
                    let entry = DbEntry::from_stat(
                        &path_entry.path,
                        path_entry.path.rsplit('/').next().unwrap_or(&path_entry.path),
                        &stat,
                        path_entry.depth,
                    );

                    if path_entry.is_directory {
                        dirs_count += 1;
                    } else {
                        files_count += 1;
                        bytes_count += stat.size;
                    }

                    entries.push(entry);
                }
                Err(e) => {
                    if self.config.verbose {
                        tracing::warn!("Failed to stat {}: {}", path_entry.path, e);
                    }
                    // Skip this entry but continue processing
                }
            }
        }

        // Write entries to database in a transaction
        let tx = conn.transaction().map_err(|e| {
            JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO entries
                 (name, path, entry_type, size, mtime, atime, ctime, mode, uid, gid,
                  nlink, inode, depth, extension, blocks, bundle_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)"
            ).map_err(|e| {
                JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            for entry in &entries {
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
                    bundle.id(),
                ]).map_err(|e| {
                    JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                })?;
            }
        }

        // Record bundle processing
        let duration_ms = start.elapsed().as_millis() as i64;
        tx.execute(
            "INSERT OR REPLACE INTO bundles
             (id, nfs_server, nfs_export, path_prefix, paths_count, processed_at, duration_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'), ?6)",
            params![
                bundle.id(),
                bundle.metadata.nfs_server,
                bundle.metadata.nfs_export,
                bundle.metadata.path_prefix,
                bundle.len() as i64,
                duration_ms,
            ],
        ).map_err(|e| {
            JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        tx.commit().map_err(|e| {
            JoggerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        Ok((files_count, dirs_count, bytes_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_progress() {
        let progress = WorkerProgress {
            bundles_processed: 10,
            bundles_failed: 1,
            files_processed: 5000,
            dirs_processed: 100,
            bytes_processed: 1024 * 1024 * 100,
            errors: 5,
            current_bundle: Some("bundle-123".to_string()),
            elapsed: Duration::from_secs(60),
        };

        assert_eq!(progress.bundles_processed, 10);
        assert_eq!(progress.bundles_failed, 1);
    }
}
