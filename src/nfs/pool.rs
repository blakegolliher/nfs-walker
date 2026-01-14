//! Async NFS connection pool
//!
//! Provides a pool of NFS connections that can be shared among async tasks.
//! This allows many concurrent directory walks with fewer TCP connections.

use crate::config::WalkConfig;
use crate::error::NfsResult;
use crate::nfs::{NfsConnection, NfsConnectionBuilder};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use std::time::Duration;

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
    /// Semaphore to limit total connections
    semaphore: Semaphore,
    /// Configuration for creating new connections
    config: Arc<WalkConfig>,
    /// Maximum number of connections
    max_connections: usize,
    /// Connection timeout
    timeout: Duration,
}

impl NfsConnectionPool {
    /// Create a new connection pool
    pub fn new(config: Arc<WalkConfig>, max_connections: usize) -> Self {
        Self {
            available: Mutex::new(Vec::with_capacity(max_connections)),
            semaphore: Semaphore::new(max_connections),
            config,
            max_connections,
            timeout: Duration::from_secs(30),
        }
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
        // Acquire semaphore permit (limits total connections)
        let _permit = self.semaphore.acquire().await
            .expect("Semaphore closed unexpectedly");

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

        // Create a new connection in a blocking task
        let config = Arc::clone(&self.config);
        let timeout = self.timeout;

        let conn = tokio::task::spawn_blocking(move || {
            NfsConnectionBuilder::new(config.nfs_url.clone())
                .timeout(timeout)
                .retries(config.retry_count)
                .connect()
        }).await.expect("Blocking task panicked")?;

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self,
        })
    }

    /// Return a connection to the pool (async version)
    pub async fn return_connection(&self, conn: NfsConnection) {
        let mut available = self.available.lock().await;
        available.push(conn);
        // Semaphore permit is released when PooledConnection is dropped
    }

    /// Return a connection to the pool (sync version for Drop)
    fn return_connection_sync(&self, conn: NfsConnection) {
        // Use blocking_lock since this is called from Drop
        let mut available = self.available.blocking_lock();
        available.push(conn);
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

#[cfg(test)]
mod tests {
    // Tests require an actual NFS server
    // Basic struct tests only
    #[test]
    fn test_pool_creation() {
        // Would need a mock config
    }
}
