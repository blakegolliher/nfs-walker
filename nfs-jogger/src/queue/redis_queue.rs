//! Redis-backed distributed queue implementation
//!
//! Uses Redis Streams for reliable message delivery with:
//! - At-least-once delivery semantics
//! - Consumer groups for parallel workers
//! - Automatic redelivery of abandoned messages

use crate::bundle::{Bundle, BundleState};
use crate::error::{QueueError, QueueResult};
use crate::queue::WorkQueue;

use redis::{
    aio::MultiplexedConnection, AsyncCommands, Client, RedisResult,
    streams::{StreamReadOptions, StreamReadReply},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Redis key for the main bundle queue (stream)
pub const BUNDLE_QUEUE_KEY: &str = "nfs-jogger:bundles:queue";

/// Redis key for bundles currently being processed
pub const PROCESSING_SET_KEY: &str = "nfs-jogger:bundles:processing";

/// Redis key for completed bundles
pub const COMPLETED_SET_KEY: &str = "nfs-jogger:bundles:completed";

/// Redis key for failed bundles
pub const FAILED_SET_KEY: &str = "nfs-jogger:bundles:failed";

/// Redis key prefix for worker heartbeats
pub const WORKER_HEARTBEAT_KEY: &str = "nfs-jogger:workers:heartbeat";

/// Redis key for global statistics
pub const STATS_KEY: &str = "nfs-jogger:stats";

/// Consumer group name
const CONSUMER_GROUP: &str = "jogger-workers";

/// Configuration for Redis queue
#[derive(Debug, Clone)]
pub struct RedisQueueConfig {
    /// Redis connection URL
    pub url: String,
    /// Key prefix for namespacing
    pub key_prefix: String,
    /// Worker heartbeat interval
    pub heartbeat_interval: Duration,
    /// Worker heartbeat timeout (consider worker dead after this)
    pub heartbeat_timeout: Duration,
    /// Maximum retry count before moving to failed
    pub max_retries: u32,
    /// Claim timeout for abandoned messages
    pub claim_timeout: Duration,
}

impl Default for RedisQueueConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: "nfs-jogger".to_string(),
            heartbeat_interval: Duration::from_secs(10),
            heartbeat_timeout: Duration::from_secs(60),
            max_retries: 3,
            claim_timeout: Duration::from_secs(120),
        }
    }
}

impl RedisQueueConfig {
    /// Create config with custom Redis URL
    pub fn with_url(url: &str) -> Self {
        Self {
            url: url.to_string(),
            ..Default::default()
        }
    }
}

/// Queue statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueStats {
    /// Number of bundles waiting in queue
    pub pending: u64,
    /// Number of bundles currently being processed
    pub processing: u64,
    /// Number of completed bundles
    pub completed: u64,
    /// Number of failed bundles
    pub failed: u64,
    /// Total bundles submitted
    pub total_submitted: u64,
    /// Total files processed
    pub total_files: u64,
    /// Total bytes processed
    pub total_bytes: u64,
    /// Number of active workers
    pub active_workers: u64,
}

/// Redis-backed distributed queue
pub struct RedisQueue {
    client: Client,
    connection: Arc<RwLock<MultiplexedConnection>>,
    config: RedisQueueConfig,
}

impl RedisQueue {
    /// Create a new Redis queue
    pub async fn new(config: RedisQueueConfig) -> QueueResult<Self> {
        let client = Client::open(config.url.as_str()).map_err(|e| {
            QueueError::ConnectionFailed {
                url: config.url.clone(),
                reason: e.to_string(),
            }
        })?;

        let connection = client.get_multiplexed_async_connection().await.map_err(|e| {
            QueueError::ConnectionFailed {
                url: config.url.clone(),
                reason: e.to_string(),
            }
        })?;

        let queue = Self {
            client,
            connection: Arc::new(RwLock::new(connection)),
            config,
        };

        // Initialize consumer group (ignore if already exists)
        queue.init_consumer_group().await?;

        Ok(queue)
    }

    /// Initialize the consumer group
    async fn init_consumer_group(&self) -> QueueResult<()> {
        let mut conn = self.connection.write().await;

        // Create the stream and consumer group
        // XGROUP CREATE creates the stream if it doesn't exist with MKSTREAM
        let result: RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(BUNDLE_QUEUE_KEY)
            .arg(CONSUMER_GROUP)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut *conn)
            .await;

        // Ignore "BUSYGROUP" error (group already exists)
        match result {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
            Err(e) => Err(QueueError::OperationFailed(e.to_string())),
        }
    }

    /// Get a fresh connection
    async fn get_conn(&self) -> QueueResult<MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await.map_err(|e| {
            QueueError::ConnectionFailed {
                url: self.config.url.clone(),
                reason: e.to_string(),
            }
        })
    }

    /// Store bundle data in a hash
    async fn store_bundle(&self, bundle: &Bundle) -> QueueResult<()> {
        let mut conn = self.connection.write().await;
        let key = format!("{}:bundle:{}", self.config.key_prefix, bundle.id());
        let data = bundle.to_json().map_err(|e| {
            QueueError::Serialization(e.to_string())
        })?;

        conn.set_ex(&key, &data, 86400 * 7).await?; // 7 day TTL
        Ok(())
    }

    /// Retrieve bundle data from hash
    async fn get_bundle(&self, bundle_id: &str) -> QueueResult<Option<Bundle>> {
        let mut conn = self.connection.write().await;
        let key = format!("{}:bundle:{}", self.config.key_prefix, bundle_id);

        let data: Option<String> = conn.get(&key).await?;

        match data {
            Some(json) => {
                let bundle = Bundle::from_json(&json).map_err(|e| {
                    QueueError::Serialization(e.to_string())
                })?;
                Ok(Some(bundle))
            }
            None => Ok(None),
        }
    }

    /// Update bundle in storage
    async fn update_bundle(&self, bundle: &Bundle) -> QueueResult<()> {
        self.store_bundle(bundle).await
    }

    /// Claim pending messages from dead workers
    async fn claim_abandoned_messages(&self, worker_id: &str) -> QueueResult<Option<Bundle>> {
        let mut conn = self.connection.write().await;

        // XAUTOCLAIM claims messages that have been idle for claim_timeout
        let claim_ms = self.config.claim_timeout.as_millis() as u64;

        let result: RedisResult<(String, Vec<(String, Vec<(String, String)>)>)> = redis::cmd("XAUTOCLAIM")
            .arg(BUNDLE_QUEUE_KEY)
            .arg(CONSUMER_GROUP)
            .arg(worker_id)
            .arg(claim_ms)
            .arg("0-0")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut *conn)
            .await;

        match result {
            Ok((_, messages)) if !messages.is_empty() => {
                // Got a claimed message
                if let Some((msg_id, fields)) = messages.into_iter().next() {
                    if let Some((_, bundle_id)) = fields.into_iter().find(|(k, _)| k == "bundle_id") {
                        drop(conn); // Release lock before getting bundle
                        if let Some(mut bundle) = self.get_bundle(&bundle_id).await? {
                            bundle.metadata.mark_abandoned();
                            bundle.metadata.mark_processing(worker_id);
                            self.update_bundle(&bundle).await?;
                            return Ok(Some(bundle));
                        }
                    }
                }
                Ok(None)
            }
            Ok(_) => Ok(None),
            Err(e) if e.to_string().contains("NOGROUP") => Ok(None),
            Err(e) => Err(QueueError::OperationFailed(e.to_string())),
        }
    }
}

#[async_trait::async_trait]
impl WorkQueue for RedisQueue {
    async fn push(&self, mut bundle: Bundle) -> QueueResult<()> {
        // Store the full bundle data
        self.store_bundle(&bundle).await?;

        // Add to the stream with just the bundle ID
        let mut conn = self.connection.write().await;

        redis::cmd("XADD")
            .arg(BUNDLE_QUEUE_KEY)
            .arg("*")
            .arg("bundle_id")
            .arg(bundle.id())
            .query_async::<String>(&mut *conn)
            .await?;

        // Update stats
        conn.hincr::<_, _, _, i64>(STATS_KEY, "total_submitted", 1).await?;
        conn.hincr::<_, _, _, i64>(STATS_KEY, "pending", 1).await?;

        // Update bundle state
        bundle.metadata.mark_queued();
        drop(conn);
        self.update_bundle(&bundle).await?;

        Ok(())
    }

    async fn pop(&self, worker_id: &str, timeout: Duration) -> QueueResult<Option<Bundle>> {
        // First try to claim any abandoned messages
        if let Some(bundle) = self.claim_abandoned_messages(worker_id).await? {
            return Ok(Some(bundle));
        }

        let mut conn = self.connection.write().await;

        // Read from consumer group
        let opts = StreamReadOptions::default()
            .group(CONSUMER_GROUP, worker_id)
            .count(1)
            .block(timeout.as_millis() as usize);

        let result: StreamReadReply = conn
            .xread_options(&[BUNDLE_QUEUE_KEY], &[">"], &opts)
            .await?;

        if result.keys.is_empty() {
            return Ok(None);
        }

        // Get the first message
        if let Some(stream) = result.keys.into_iter().next() {
            if let Some(msg) = stream.ids.into_iter().next() {
                // Extract bundle_id from message
                if let Some(bundle_id) = msg.map.get("bundle_id") {
                    let bundle_id: String = redis::FromRedisValue::from_redis_value(bundle_id)
                        .map_err(|e| QueueError::Serialization(e.to_string()))?;

                    drop(conn);

                    // Get the full bundle
                    if let Some(mut bundle) = self.get_bundle(&bundle_id).await? {
                        bundle.metadata.mark_processing(worker_id);
                        self.update_bundle(&bundle).await?;

                        // Update stats
                        let mut conn = self.connection.write().await;
                        conn.hincr::<_, _, _, i64>(STATS_KEY, "pending", -1).await?;
                        conn.hincr::<_, _, _, i64>(STATS_KEY, "processing", 1).await?;

                        return Ok(Some(bundle));
                    }
                }
            }
        }

        Ok(None)
    }

    async fn ack(&self, bundle_id: &str) -> QueueResult<()> {
        // Update bundle state
        if let Some(mut bundle) = self.get_bundle(bundle_id).await? {
            bundle.metadata.mark_completed();
            self.update_bundle(&bundle).await?;

            // Update stats
            let mut conn = self.connection.write().await;
            conn.hincr::<_, _, _, i64>(STATS_KEY, "processing", -1).await?;
            conn.hincr::<_, _, _, i64>(STATS_KEY, "completed", 1).await?;
            conn.hincr::<_, _, _, i64>(STATS_KEY, "total_files", bundle.stats.file_count as i64).await?;
            conn.hincr::<_, _, _, i64>(STATS_KEY, "total_bytes", bundle.stats.total_bytes as i64).await?;

            // Add to completed set
            conn.sadd::<_, _, ()>(COMPLETED_SET_KEY, bundle_id).await?;
        }

        Ok(())
    }

    async fn nack(&self, bundle_id: &str, error: &str) -> QueueResult<()> {
        // Update bundle state
        if let Some(mut bundle) = self.get_bundle(bundle_id).await? {
            bundle.metadata.mark_failed(error);
            self.update_bundle(&bundle).await?;

            // Update stats
            let mut conn = self.connection.write().await;
            conn.hincr::<_, _, _, i64>(STATS_KEY, "processing", -1).await?;
            conn.hincr::<_, _, _, i64>(STATS_KEY, "failed", 1).await?;

            // Add to failed set
            conn.sadd::<_, _, ()>(FAILED_SET_KEY, bundle_id).await?;
        }

        Ok(())
    }

    async fn stats(&self) -> QueueResult<QueueStats> {
        let mut conn = self.connection.write().await;

        // Get stats from hash
        let values: Vec<Option<i64>> = redis::cmd("HMGET")
            .arg(STATS_KEY)
            .arg("pending")
            .arg("processing")
            .arg("completed")
            .arg("failed")
            .arg("total_submitted")
            .arg("total_files")
            .arg("total_bytes")
            .query_async(&mut *conn)
            .await?;

        // Count active workers by scanning heartbeat keys
        let pattern = format!("{}:*", WORKER_HEARTBEAT_KEY);
        let keys: Vec<String> = conn.keys(&pattern).await?;
        let active_workers = keys.len() as u64;

        Ok(QueueStats {
            pending: values.get(0).copied().flatten().unwrap_or(0) as u64,
            processing: values.get(1).copied().flatten().unwrap_or(0) as u64,
            completed: values.get(2).copied().flatten().unwrap_or(0) as u64,
            failed: values.get(3).copied().flatten().unwrap_or(0) as u64,
            total_submitted: values.get(4).copied().flatten().unwrap_or(0) as u64,
            total_files: values.get(5).copied().flatten().unwrap_or(0) as u64,
            total_bytes: values.get(6).copied().flatten().unwrap_or(0) as u64,
            active_workers,
        })
    }

    async fn heartbeat(&self, worker_id: &str) -> QueueResult<()> {
        let mut conn = self.connection.write().await;
        let key = format!("{}:{}", WORKER_HEARTBEAT_KEY, worker_id);
        let timeout_secs = self.config.heartbeat_timeout.as_secs();

        conn.set_ex(&key, chrono::Utc::now().timestamp(), timeout_secs).await?;
        Ok(())
    }

    async fn pending_count(&self) -> QueueResult<u64> {
        let mut conn = self.connection.write().await;
        let count: i64 = conn.hget(STATS_KEY, "pending").await.unwrap_or(0);
        Ok(count as u64)
    }

    async fn is_empty(&self) -> QueueResult<bool> {
        Ok(self.pending_count().await? == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests would require a running Redis instance
    // These are unit tests for the configuration

    #[test]
    fn test_config_defaults() {
        let config = RedisQueueConfig::default();
        assert_eq!(config.url, "redis://127.0.0.1:6379");
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_config_with_url() {
        let config = RedisQueueConfig::with_url("redis://custom:6380");
        assert_eq!(config.url, "redis://custom:6380");
    }
}
