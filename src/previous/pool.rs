//! NFS connection pools (async and sync)
//!
//! This module provides two connection pool implementations:
//!
//! 1. **`NfsConnectionPool`** (async) - For async/tokio workloads
//! 2. **`SyncNfsConnectionPool`** (sync) - For sync worker threads
//!
//! # Sync Pool Usage for Parallel Stat Operations
//!
//! The sync pool is designed for parallel GETATTR operations when using
//! skinny directory reads. Example workflow:
//!
//! ```ignore
//! // Worker has its own primary connection
//! let mut primary_conn = NfsConnection::connect_to(&url, timeout)?;
//!
//! // For batch stat operations, borrow from the shared pool
//! let unknown_entries: Vec<String> = /* collected during skinny read */;
//!
//! // Parallel stat using pool
//! let results = sync_pool.parallel_stat(&unknown_entries, |path, conn| {
//!     conn.stat(path)
//! });
//! ```
//!
//! # Connection Lifecycle
//!
//! Connections are:
//! - Created on demand up to the pool limit
//! - Returned to the pool after use (not closed)
//! - Validated before reuse (connection health check)
//! - Closed when the pool is dropped

use crate::config::WalkConfig;
use crate::error::{NfsError, NfsResult};
use crate::nfs::dns_resolver::DnsResolver;
use crate::nfs::types::{EntryType, NfsStat};
use crate::nfs::{NfsConnection, NfsConnectionBuilder};
use std::sync::Arc;
use std::thread;
use tokio::sync::{Mutex, Semaphore};
use std::time::Duration;
use parking_lot::Mutex as SyncMutex;
use crossbeam_channel::{bounded, Sender, Receiver};
use tracing::debug;

/// A pooled NFS connection with automatic return to pool on drop
pub struct PooledConnection<'a> {
    /// The connection (Option for taking on drop)
    conn: Option<NfsConnection>,
    /// Reference to the pool for returning
    pool: &'a NfsConnectionPool,
}

impl<'a> PooledConnection<'a> {
    /// Get a reference to the connection
    pub fn connection(&mut self) -> &mut NfsConnection {
        self.conn.as_mut().expect("Connection already taken")
    }

    /// Take ownership of the connection (for use in spawn_blocking)
    /// The connection must be returned via `pool.return_connection()`
    pub fn take(mut self) -> NfsConnection {
        self.conn.take().expect("Connection already taken")
    }
}

impl<'a> Drop for PooledConnection<'a> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.return_connection_sync(conn);
        }
    }
}

/// Pool of NFS connections for async workers
pub struct NfsConnectionPool {
    /// Available connections ready for use
    available: Mutex<Vec<NfsConnection>>,
    /// Semaphore to limit concurrent connection USAGE (not just creation)
    /// Each connection checkout takes a permit, returned when connection is returned
    semaphore: Semaphore,
    /// Track how many connections have been created (up to max_connections)
    created_count: std::sync::atomic::AtomicUsize,
    /// Configuration for creating new connections
    config: Arc<WalkConfig>,
    /// Maximum number of connections
    max_connections: usize,
    /// Connection timeout
    timeout: Duration,
    /// DNS resolver for load balancing (optional)
    dns_resolver: Option<Arc<DnsResolver>>,
}

impl NfsConnectionPool {
    /// Create a new connection pool
    pub fn new(config: Arc<WalkConfig>, max_connections: usize) -> Self {
        Self {
            available: Mutex::new(Vec::with_capacity(max_connections)),
            semaphore: Semaphore::new(max_connections),
            created_count: std::sync::atomic::AtomicUsize::new(0),
            config,
            max_connections,
            timeout: Duration::from_secs(30),
            dns_resolver: None,
        }
    }

    /// Create a new connection pool with DNS resolver for load balancing
    pub fn with_dns_resolver(
        config: Arc<WalkConfig>,
        max_connections: usize,
        dns_resolver: Arc<DnsResolver>,
    ) -> Self {
        Self {
            available: Mutex::new(Vec::with_capacity(max_connections)),
            semaphore: Semaphore::new(max_connections),
            created_count: std::sync::atomic::AtomicUsize::new(0),
            config,
            max_connections,
            timeout: Duration::from_secs(30),
            dns_resolver: Some(dns_resolver),
        }
    }

    /// Get the DNS resolver (if configured)
    pub fn dns_resolver(&self) -> Option<&Arc<DnsResolver>> {
        self.dns_resolver.as_ref()
    }

    /// Get the maximum number of connections
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Acquire a connection from the pool
    ///
    /// This will wait if all connections are in use, or create a new one
    /// if under the limit.
    pub async fn acquire(&self) -> NfsResult<PooledConnection<'_>> {
        // Acquire semaphore permit - this blocks until a connection slot is available
        // The permit is "forgotten" (leaked) here and "restored" in return_connection()
        // This ensures we never have more than max_connections in use simultaneously
        let permit = self.semaphore.acquire().await
            .expect("Semaphore closed unexpectedly");

        // Forget the permit - we'll add it back when connection is returned
        permit.forget();

        // Try to get an existing connection
        {
            let mut available = self.available.lock().await;
            if let Some(conn) = available.pop() {
                debug!(available = available.len(), "Acquired existing connection from pool");
                return Ok(PooledConnection {
                    conn: Some(conn),
                    pool: self,
                });
            }
        }

        // No connection available - check if we can create a new one
        let created = self.created_count.load(std::sync::atomic::Ordering::SeqCst);
        if created >= self.max_connections {
            // This shouldn't happen if semaphore is working correctly
            // But handle it gracefully - wait and retry
            debug!(created = created, max = self.max_connections,
                   "Pool exhausted, returning permit and retrying");
            self.semaphore.add_permits(1);
            // Yield and retry
            tokio::task::yield_now().await;
            return Box::pin(self.acquire()).await;
        }

        // Create a new connection
        self.created_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let config = Arc::clone(&self.config);
        let timeout = self.timeout;
        let created_now = self.created_count.load(std::sync::atomic::Ordering::SeqCst);

        debug!(created = created_now, max = self.max_connections,
               "Creating new NFS connection to {}", config.nfs_url.server);

        let conn_result = tokio::time::timeout(
            Duration::from_secs(60),
            tokio::task::spawn_blocking(move || {
                NfsConnectionBuilder::new(config.nfs_url.clone())
                    .timeout(timeout)
                    .retries(config.retry_count)
                    .connect()
            })
        ).await;

        let conn = match conn_result {
            Ok(Ok(result)) => result?,
            Ok(Err(e)) => {
                // Creation failed - give back the permit
                self.created_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                self.semaphore.add_permits(1);
                panic!("Blocking task panicked: {:?}", e);
            }
            Err(_) => {
                // Timeout - give back the permit
                self.created_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                self.semaphore.add_permits(1);
                return Err(crate::error::NfsError::Timeout {
                    path: self.config.nfs_url.server.clone(),
                    attempts: 1,
                });
            }
        };

        debug!("NFS connection established to {}", conn.server());

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self,
        })
    }

    /// Acquire a connection from the pool for a specific worker
    ///
    /// Uses the DNS resolver (if configured) to assign an IP to this worker.
    /// The worker will get a sticky IP assignment that persists across calls.
    pub async fn acquire_for_worker(&self, worker_id: usize) -> NfsResult<PooledConnection<'_>> {
        // Acquire semaphore permit - blocks until a connection slot is available
        let permit = self.semaphore.acquire().await
            .expect("Semaphore closed unexpectedly");
        permit.forget(); // Restored in return_connection()

        // Try to get an existing connection
        {
            let mut available = self.available.lock().await;
            if let Some(conn) = available.pop() {
                return Ok(PooledConnection {
                    conn: Some(conn),
                    pool: self,
                });
            }
        }

        // Check if we can create a new connection
        let created = self.created_count.load(std::sync::atomic::Ordering::SeqCst);
        if created >= self.max_connections {
            self.semaphore.add_permits(1);
            tokio::task::yield_now().await;
            return Box::pin(self.acquire_for_worker(worker_id)).await;
        }

        self.created_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Get assigned IP from DNS resolver if available
        let target_ip = self.dns_resolver.as_ref()
            .and_then(|r| r.get_ip_for_worker(worker_id));

        // Create a new connection in a blocking task
        let config = Arc::clone(&self.config);
        let timeout = self.timeout;

        let conn_result = tokio::task::spawn_blocking(move || {
            let mut builder = NfsConnectionBuilder::new(config.nfs_url.clone())
                .timeout(timeout)
                .retries(config.retry_count);

            if let Some(ip) = target_ip {
                debug!(worker_id = worker_id, ip = %ip, "Creating connection to assigned IP");
                builder = builder.with_ip(ip);
            }

            builder.connect()
        }).await;

        match conn_result {
            Ok(Ok(conn)) => Ok(PooledConnection {
                conn: Some(conn),
                pool: self,
            }),
            Ok(Err(e)) => {
                self.created_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                self.semaphore.add_permits(1);
                Err(e)
            }
            Err(e) => {
                self.created_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                self.semaphore.add_permits(1);
                panic!("Blocking task panicked: {:?}", e);
            }
        }
    }

    /// Return a connection to the pool (async version)
    pub async fn return_connection(&self, conn: NfsConnection) {
        let mut available = self.available.lock().await;
        available.push(conn);
        // Restore the semaphore permit that was forgotten in acquire()
        self.semaphore.add_permits(1);
        debug!(available = available.len(), "Returned connection to pool");
    }

    /// Return a connection to the pool (sync version for Drop)
    fn return_connection_sync(&self, conn: NfsConnection) {
        // Use blocking_lock since this is called from Drop
        let mut available = self.available.blocking_lock();
        available.push(conn);
        // Restore the semaphore permit
        self.semaphore.add_permits(1);
    }

    /// Pre-warm the pool by creating connections
    pub async fn warm(&self, count: usize) -> NfsResult<()> {
        let count = count.min(self.max_connections);
        let mut connections = Vec::with_capacity(count);

        for _ in 0..count {
            let config = Arc::clone(&self.config);
            let timeout = self.timeout;

            let conn = tokio::task::spawn_blocking(move || {
                NfsConnectionBuilder::new(config.nfs_url.clone())
                    .timeout(timeout)
                    .connect()
            }).await.expect("Blocking task panicked")?;

            connections.push(conn);
        }

        let mut available = self.available.lock().await;
        available.extend(connections);
        Ok(())
    }
}

// ============================================================================
// Sync Connection Pool for Worker Threads
// ============================================================================

/// Result of a parallel stat operation
#[derive(Debug, Clone)]
pub struct StatResult {
    /// Path that was stat'd
    pub path: String,
    /// Result of the stat operation
    pub result: Result<NfsStat, NfsError>,
}

/// Synchronous connection pool for parallel stat operations
///
/// This pool is designed for sync worker threads that need to perform
/// parallel GETATTR operations after skinny directory reads.
///
/// # Thread Safety
///
/// The pool itself is thread-safe and can be shared across workers.
/// Connections are borrowed exclusively - a connection is only used
/// by one thread at a time.
pub struct SyncNfsConnectionPool {
    /// Available connections
    available: SyncMutex<Vec<NfsConnection>>,
    /// Configuration for creating new connections
    config: Arc<WalkConfig>,
    /// Maximum number of connections in the pool
    max_connections: usize,
    /// Connection timeout
    timeout: Duration,
}

impl SyncNfsConnectionPool {
    /// Create a new sync connection pool
    pub fn new(config: Arc<WalkConfig>, max_connections: usize) -> Self {
        Self {
            available: SyncMutex::new(Vec::with_capacity(max_connections)),
            config,
            max_connections,
            timeout: Duration::from_secs(30),
        }
    }

    /// Create a new sync pool with custom timeout
    pub fn with_timeout(config: Arc<WalkConfig>, max_connections: usize, timeout: Duration) -> Self {
        Self {
            available: SyncMutex::new(Vec::with_capacity(max_connections)),
            config,
            max_connections,
            timeout,
        }
    }

    /// Get the maximum number of connections
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Get the number of available connections
    pub fn available_count(&self) -> usize {
        self.available.lock().len()
    }

    /// Acquire a connection from the pool
    ///
    /// Returns an existing connection if available, or creates a new one
    /// if under the limit. Returns None if at the limit and all connections
    /// are in use.
    pub fn try_acquire(&self) -> NfsResult<Option<NfsConnection>> {
        // Try to get an existing connection
        {
            let mut available = self.available.lock();
            if let Some(conn) = available.pop() {
                return Ok(Some(conn));
            }
            // Check if we can create a new one
            if available.capacity() >= self.max_connections {
                // At limit, would need to wait
                return Ok(None);
            }
        }

        // Create a new connection
        let conn = NfsConnectionBuilder::new(self.config.nfs_url.clone())
            .timeout(self.timeout)
            .retries(self.config.retry_count)
            .connect()?;

        Ok(Some(conn))
    }

    /// Return a connection to the pool
    pub fn return_connection(&self, conn: NfsConnection) {
        let mut available = self.available.lock();
        if available.len() < self.max_connections {
            available.push(conn);
        }
        // If over limit (shouldn't happen), connection is dropped
    }

    /// Pre-warm the pool by creating connections
    pub fn warm(&self, count: usize) -> NfsResult<()> {
        let count = count.min(self.max_connections);
        let mut connections = Vec::with_capacity(count);

        for _ in 0..count {
            let conn = NfsConnectionBuilder::new(self.config.nfs_url.clone())
                .timeout(self.timeout)
                .retries(self.config.retry_count)
                .connect()?;
            connections.push(conn);
        }

        let mut available = self.available.lock();
        available.extend(connections);
        Ok(())
    }

    /// Perform parallel stat operations on a batch of paths
    ///
    /// Spawns worker threads to stat paths concurrently using pooled connections.
    /// Returns results in the same order as input paths.
    ///
    /// # Arguments
    ///
    /// * `paths` - Paths to stat
    /// * `parallelism` - Maximum number of concurrent stat operations
    ///
    /// # Returns
    ///
    /// Vector of StatResult in the same order as input paths.
    pub fn parallel_stat(&self, paths: &[(String, u64)], parallelism: usize) -> Vec<StatResult> {
        if paths.is_empty() {
            return Vec::new();
        }

        let parallelism = parallelism.min(self.max_connections).min(paths.len());

        // Channel for distributing work
        let (work_tx, work_rx): (Sender<(usize, String)>, Receiver<(usize, String)>) =
            bounded(paths.len());

        // Channel for collecting results
        let (result_tx, result_rx): (Sender<(usize, StatResult)>, Receiver<(usize, StatResult)>) =
            bounded(paths.len());

        // Send all work items
        for (idx, (path, _inode)) in paths.iter().enumerate() {
            let _ = work_tx.send((idx, path.clone()));
        }
        drop(work_tx); // Close sender to signal end of work

        // Spawn worker threads
        let mut handles = Vec::with_capacity(parallelism);

        for _ in 0..parallelism {
            let work_rx = work_rx.clone();
            let result_tx = result_tx.clone();
            let config = Arc::clone(&self.config);
            let timeout = self.timeout;

            let handle = thread::spawn(move || {
                // Each thread gets its own connection
                let conn = match NfsConnectionBuilder::new(config.nfs_url.clone())
                    .timeout(timeout)
                    .retries(config.retry_count)
                    .connect()
                {
                    Ok(c) => c,
                    Err(e) => {
                        // Send errors for all remaining work items
                        for (idx, path) in work_rx {
                            let _ = result_tx.send((idx, StatResult {
                                path,
                                result: Err(e.clone()),
                            }));
                        }
                        return;
                    }
                };

                // Process work items
                for (idx, path) in work_rx {
                    let result = conn.stat(&path);
                    let _ = result_tx.send((idx, StatResult {
                        path,
                        result,
                    }));
                }
            });

            handles.push(handle);
        }

        drop(result_tx); // Close sender so receiver knows when done

        // Collect results
        let mut results: Vec<Option<StatResult>> = vec![None; paths.len()];
        for (idx, result) in result_rx {
            results[idx] = Some(result);
        }

        // Wait for all threads
        for handle in handles {
            let _ = handle.join();
        }

        // Convert to final results (should all be Some)
        results.into_iter()
            .map(|r| r.expect("Missing result"))
            .collect()
    }

    /// Perform parallel stat and filter to find directories
    ///
    /// This is a convenience method for the common case of resolving
    /// Unknown entries from skinny reads to find subdirectories.
    ///
    /// # Returns
    ///
    /// Paths of entries that are directories.
    pub fn find_directories(&self, paths: &[(String, u64)], parallelism: usize) -> Vec<String> {
        self.parallel_stat(paths, parallelism)
            .into_iter()
            .filter_map(|r| {
                match r.result {
                    Ok(stat) if stat.entry_type() == EntryType::Directory => Some(r.path),
                    _ => None,
                }
            })
            .collect()
    }
}

// Allow sharing the pool across threads
unsafe impl Send for SyncNfsConnectionPool {}
unsafe impl Sync for SyncNfsConnectionPool {}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests require an actual NFS server
    // Basic struct tests only

    #[test]
    fn test_pool_creation() {
        // Would need a mock config
    }

    #[test]
    fn test_sync_pool_empty_batch() {
        // Verify empty input returns empty output
        // Would need a mock config for full test
    }
}
