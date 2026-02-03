//! Filesystem scanner for the discovery phase
//!
//! Scans NFS filesystems and creates work bundles for distribution.

use crate::bundle::{Bundle, BundleBuilder, PathEntry};
use crate::config::DiscoveryConfig;
use crate::error::{DiscoveryError, JoggerError, Result};
use crate::nfs::{NfsConnection, NfsConnectionBuilder};
use crate::queue::{RedisQueue, WorkQueue};

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Injector, Stealer, Worker};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Progress information during discovery
#[derive(Debug, Clone, Default)]
pub struct DiscoveryProgress {
    /// Directories scanned
    pub dirs_scanned: u64,
    /// Files found
    pub files_found: u64,
    /// Total bytes found
    pub bytes_found: u64,
    /// Bundles created
    pub bundles_created: u64,
    /// Bundles submitted to queue
    pub bundles_submitted: u64,
    /// Current queue depth (directories pending)
    pub queue_depth: u64,
    /// Active workers
    pub active_workers: u64,
    /// Errors encountered
    pub errors: u64,
    /// Elapsed time
    pub elapsed: Duration,
}

/// Final statistics from discovery
#[derive(Debug, Clone, Default)]
pub struct DiscoveryStats {
    /// Total directories scanned
    pub dirs_scanned: u64,
    /// Total files found
    pub files_found: u64,
    /// Total bytes found
    pub bytes_found: u64,
    /// Total bundles created
    pub bundles_created: u64,
    /// Total errors
    pub errors: u64,
    /// Total duration
    pub duration: Duration,
    /// Average files per bundle
    pub avg_files_per_bundle: f64,
    /// Average bytes per bundle
    pub avg_bytes_per_bundle: f64,
}

/// Directory work item
#[derive(Debug, Clone)]
struct DirWork {
    path: String,
    depth: u32,
    file_handle: Option<Vec<u8>>,
}

/// Filesystem scanner for discovery phase
pub struct DiscoveryScanner {
    config: DiscoveryConfig,
    shutdown: Arc<AtomicBool>,
}

impl DiscoveryScanner {
    /// Create a new discovery scanner
    pub fn new(config: DiscoveryConfig) -> Self {
        Self {
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Run the discovery phase
    pub async fn run<F>(&self, progress_callback: F) -> Result<DiscoveryStats>
    where
        F: Fn(DiscoveryProgress) + Send + Sync + 'static,
    {
        let start = Instant::now();

        // Connect to Redis queue
        let queue = Arc::new(
            RedisQueue::new(self.config.queue_config.clone())
                .await
                .map_err(|e| JoggerError::Queue(e))?
        );

        // Create atomic counters
        let dirs_scanned = Arc::new(AtomicU64::new(0));
        let files_found = Arc::new(AtomicU64::new(0));
        let bytes_found = Arc::new(AtomicU64::new(0));
        let bundles_created = Arc::new(AtomicU64::new(0));
        let bundles_submitted = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let active_workers = Arc::new(AtomicU64::new(0));
        let pending_work = Arc::new(AtomicU64::new(1)); // Start with root

        // Create work queue
        let injector = Arc::new(Injector::new());

        // Push root directory
        let start_path = self.config.nfs_url.walk_start_path();
        injector.push(DirWork {
            path: start_path.clone(),
            depth: 0,
            file_handle: None,
        });

        // Create bundle channel
        let (bundle_tx, bundle_rx): (Sender<Bundle>, Receiver<Bundle>) = bounded(100);

        // Spawn bundle submitter thread
        let queue_clone = queue.clone();
        let bundles_submitted_clone = bundles_submitted.clone();
        let shutdown_clone = self.shutdown.clone();

        let submitter_handle = tokio::spawn(async move {
            while let Ok(bundle) = bundle_rx.recv() {
                if shutdown_clone.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) = queue_clone.push(bundle).await {
                    tracing::error!("Failed to submit bundle: {}", e);
                } else {
                    bundles_submitted_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        // Spawn progress reporter
        let progress_handle = if self.config.show_progress {
            let dirs_scanned_clone = dirs_scanned.clone();
            let files_found_clone = files_found.clone();
            let bytes_found_clone = bytes_found.clone();
            let bundles_created_clone = bundles_created.clone();
            let bundles_submitted_clone = bundles_submitted.clone();
            let errors_clone = errors.clone();
            let active_workers_clone = active_workers.clone();
            let pending_work_clone = pending_work.clone();
            let shutdown_clone = self.shutdown.clone();

            let callback = Arc::new(progress_callback);

            Some(thread::spawn(move || {
                let start = Instant::now();
                while !shutdown_clone.load(Ordering::Relaxed) {
                    let progress = DiscoveryProgress {
                        dirs_scanned: dirs_scanned_clone.load(Ordering::Relaxed),
                        files_found: files_found_clone.load(Ordering::Relaxed),
                        bytes_found: bytes_found_clone.load(Ordering::Relaxed),
                        bundles_created: bundles_created_clone.load(Ordering::Relaxed),
                        bundles_submitted: bundles_submitted_clone.load(Ordering::Relaxed),
                        queue_depth: pending_work_clone.load(Ordering::Relaxed),
                        active_workers: active_workers_clone.load(Ordering::Relaxed),
                        errors: errors_clone.load(Ordering::Relaxed),
                        elapsed: start.elapsed(),
                    };
                    callback(progress);
                    thread::sleep(Duration::from_millis(100));

                    // Exit if no more work
                    if pending_work_clone.load(Ordering::Relaxed) == 0
                        && active_workers_clone.load(Ordering::Relaxed) == 0
                    {
                        break;
                    }
                }
            }))
        } else {
            None
        };

        // Create worker stealers
        let mut workers: Vec<Worker<DirWork>> = Vec::new();
        let mut stealers: Vec<Stealer<DirWork>> = Vec::new();

        for _ in 0..self.config.worker_count {
            let w = Worker::new_fifo();
            stealers.push(w.stealer());
            workers.push(w);
        }

        let stealers = Arc::new(stealers);

        // Spawn discovery workers
        let mut handles = Vec::new();

        for (id, local) in workers.into_iter().enumerate() {
            let config = self.config.clone();
            let injector = injector.clone();
            let stealers = stealers.clone();
            let bundle_tx = bundle_tx.clone();
            let shutdown = self.shutdown.clone();

            let dirs_scanned = dirs_scanned.clone();
            let files_found = files_found.clone();
            let bytes_found = bytes_found.clone();
            let bundles_created = bundles_created.clone();
            let errors = errors.clone();
            let active_workers = active_workers.clone();
            let pending_work = pending_work.clone();

            let handle = thread::Builder::new()
                .name(format!("discovery-{}", id))
                .spawn(move || {
                    Self::discovery_worker(
                        id,
                        config,
                        local,
                        injector,
                        stealers,
                        bundle_tx,
                        shutdown,
                        dirs_scanned,
                        files_found,
                        bytes_found,
                        bundles_created,
                        errors,
                        active_workers,
                        pending_work,
                    )
                })
                .map_err(|e| JoggerError::Io(e))?;

            handles.push(handle);
        }

        // Drop our copy of the bundle sender
        drop(bundle_tx);

        // Wait for all workers to complete
        for handle in handles {
            let _ = handle.join();
        }

        // Signal shutdown and wait for submitter
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = submitter_handle.await;

        // Wait for progress reporter
        if let Some(handle) = progress_handle {
            let _ = handle.join();
        }

        let duration = start.elapsed();
        let total_bundles = bundles_created.load(Ordering::Relaxed);
        let total_files = files_found.load(Ordering::Relaxed);
        let total_bytes = bytes_found.load(Ordering::Relaxed);

        Ok(DiscoveryStats {
            dirs_scanned: dirs_scanned.load(Ordering::Relaxed),
            files_found: total_files,
            bytes_found: total_bytes,
            bundles_created: total_bundles,
            errors: errors.load(Ordering::Relaxed),
            duration,
            avg_files_per_bundle: if total_bundles > 0 {
                total_files as f64 / total_bundles as f64
            } else {
                0.0
            },
            avg_bytes_per_bundle: if total_bundles > 0 {
                total_bytes as f64 / total_bundles as f64
            } else {
                0.0
            },
        })
    }

    /// Discovery worker thread
    fn discovery_worker(
        id: usize,
        config: DiscoveryConfig,
        local: Worker<DirWork>,
        injector: Arc<Injector<DirWork>>,
        stealers: Arc<Vec<Stealer<DirWork>>>,
        bundle_tx: Sender<Bundle>,
        shutdown: Arc<AtomicBool>,
        dirs_scanned: Arc<AtomicU64>,
        files_found: Arc<AtomicU64>,
        bytes_found: Arc<AtomicU64>,
        bundles_created: Arc<AtomicU64>,
        errors: Arc<AtomicU64>,
        active_workers: Arc<AtomicU64>,
        pending_work: Arc<AtomicU64>,
    ) {
        // Connect to NFS
        let mut nfs = match NfsConnectionBuilder::new(crate::config::NfsUrl {
            server: config.nfs_url.server.clone(),
            port: config.nfs_url.port,
            export: config.nfs_url.export.clone(),
            subpath: String::new(),
        })
        .timeout(config.timeout)
        .retries(3)
        .connect()
        {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("Worker {} failed to connect: {}", id, e);
                return;
            }
        };

        // Create bundle builder
        let mut builder = BundleBuilder::new(
            &config.nfs_url.server,
            &config.nfs_url.export,
            "/",
        )
        .max_paths(config.max_paths)
        .max_bytes(config.max_bytes);

        let mut empty_iterations = 0;

        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            // Try to get work
            let work = local.pop().or_else(|| {
                // Try global queue
                loop {
                    match injector.steal() {
                        crossbeam_deque::Steal::Success(w) => return Some(w),
                        crossbeam_deque::Steal::Empty => break,
                        crossbeam_deque::Steal::Retry => continue,
                    }
                }

                // Try stealing from other workers
                for stealer in stealers.iter() {
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

            match work {
                Some(dir_work) => {
                    empty_iterations = 0;
                    active_workers.fetch_add(1, Ordering::Relaxed);

                    // Check depth limit
                    if let Some(max_depth) = config.max_depth {
                        if dir_work.depth as usize > max_depth {
                            pending_work.fetch_sub(1, Ordering::Relaxed);
                            active_workers.fetch_sub(1, Ordering::Relaxed);
                            continue;
                        }
                    }

                    // Check exclusions
                    if config.is_excluded(&dir_work.path) {
                        pending_work.fetch_sub(1, Ordering::Relaxed);
                        active_workers.fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }

                    // Read directory
                    let entries = if let Some(ref fh) = dir_work.file_handle {
                        nfs.readdir_plus_by_fh(&dir_work.path, fh)
                    } else {
                        nfs.readdir_plus(&dir_work.path)
                    };

                    match entries {
                        Ok(entries) => {
                            dirs_scanned.fetch_add(1, Ordering::Relaxed);

                            for entry in entries {
                                if entry.is_special() {
                                    continue;
                                }

                                let full_path = if dir_work.path == "/" {
                                    format!("/{}", entry.name)
                                } else {
                                    format!("{}/{}", dir_work.path, entry.name)
                                };

                                if config.is_excluded(&full_path) {
                                    continue;
                                }

                                if entry.entry_type.is_dir() {
                                    // Queue subdirectory
                                    pending_work.fetch_add(1, Ordering::Relaxed);
                                    local.push(DirWork {
                                        path: full_path.clone(),
                                        depth: dir_work.depth + 1,
                                        file_handle: entry.file_handle.clone(),
                                    });

                                    // Add directory to bundle if not dirs_only
                                    if !config.dirs_only {
                                        let path_entry = PathEntry::directory(
                                            full_path,
                                            dir_work.depth + 1,
                                        );

                                        if !builder.try_add(path_entry) {
                                            // Bundle full, send it
                                            if let Ok(bundle) = builder.take() {
                                                bundles_created.fetch_add(1, Ordering::Relaxed);
                                                let _ = bundle_tx.send(bundle);
                                            }
                                        }
                                    }
                                } else {
                                    // Add file to bundle
                                    let size = entry.size();
                                    files_found.fetch_add(1, Ordering::Relaxed);
                                    bytes_found.fetch_add(size, Ordering::Relaxed);

                                    if !config.dirs_only {
                                        let path_entry = PathEntry::file(
                                            full_path,
                                            size,
                                            dir_work.depth + 1,
                                        );

                                        if !builder.try_add(path_entry) {
                                            // Bundle full, send it
                                            if let Ok(bundle) = builder.take() {
                                                bundles_created.fetch_add(1, Ordering::Relaxed);
                                                let _ = bundle_tx.send(bundle);
                                            }
                                            // Add the entry to the new bundle
                                            builder.add(PathEntry::file(
                                                format!("{}/{}", dir_work.path, entry.name),
                                                size,
                                                dir_work.depth + 1,
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            if config.verbose {
                                tracing::warn!(
                                    "Worker {}: Failed to read {}: {}",
                                    id,
                                    dir_work.path,
                                    e
                                );
                            }
                        }
                    }

                    pending_work.fetch_sub(1, Ordering::Relaxed);
                    active_workers.fetch_sub(1, Ordering::Relaxed);
                }
                None => {
                    // No work available
                    empty_iterations += 1;

                    // Check if we're done
                    if pending_work.load(Ordering::SeqCst) == 0
                        && active_workers.load(Ordering::SeqCst) == 0
                    {
                        break;
                    }

                    // Backoff
                    if empty_iterations > 100 {
                        thread::sleep(Duration::from_millis(10));
                    } else if empty_iterations > 10 {
                        thread::sleep(Duration::from_millis(1));
                    } else {
                        thread::yield_now();
                    }
                }
            }
        }

        // Send any remaining bundle
        if !builder.is_empty() {
            if let Ok(bundle) = builder.build() {
                bundles_created.fetch_add(1, Ordering::Relaxed);
                let _ = bundle_tx.send(bundle);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_progress() {
        let progress = DiscoveryProgress {
            dirs_scanned: 100,
            files_found: 1000,
            bytes_found: 1024 * 1024 * 100,
            bundles_created: 5,
            bundles_submitted: 4,
            queue_depth: 10,
            active_workers: 4,
            errors: 2,
            elapsed: Duration::from_secs(10),
        };

        assert_eq!(progress.dirs_scanned, 100);
        assert_eq!(progress.files_found, 1000);
    }
}
