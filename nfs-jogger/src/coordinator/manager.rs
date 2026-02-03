//! Coordinator manager for system-wide monitoring and control

use crate::error::{JoggerError, Result};
use crate::queue::{QueueStats, RedisQueue, RedisQueueConfig, WorkQueue};

use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Information about a worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    /// Worker ID
    pub id: String,
    /// Last heartbeat time
    pub last_heartbeat: DateTime<Utc>,
    /// Whether worker is considered alive
    pub is_alive: bool,
    /// Time since last heartbeat (seconds)
    pub seconds_since_heartbeat: i64,
}

/// System-wide status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    /// Queue statistics
    pub queue: QueueStats,
    /// Active workers
    pub workers: Vec<WorkerInfo>,
    /// System health (healthy, degraded, critical)
    pub health: String,
    /// Timestamp of this status
    pub timestamp: DateTime<Utc>,
    /// Messages/warnings
    pub messages: Vec<String>,
}

/// Coordinator for managing the distributed system
pub struct Coordinator {
    queue: RedisQueue,
    config: RedisQueueConfig,
}

impl Coordinator {
    /// Create a new coordinator
    pub async fn new(config: RedisQueueConfig) -> Result<Self> {
        let queue = RedisQueue::new(config.clone())
            .await
            .map_err(JoggerError::Queue)?;

        Ok(Self { queue, config })
    }

    /// Get system status
    pub async fn status(&self) -> Result<SystemStatus> {
        let queue_stats = self.queue.stats().await.map_err(JoggerError::Queue)?;

        // Get worker information
        let workers = self.get_workers().await?;

        // Determine system health
        let health = self.determine_health(&queue_stats, &workers);

        // Generate messages
        let mut messages = Vec::new();

        if queue_stats.failed > 0 {
            messages.push(format!(
                "{} bundles have failed - consider retrying with 'nfs-jogger retry --all'",
                queue_stats.failed
            ));
        }

        if workers.is_empty() && queue_stats.pending > 0 {
            messages.push("No active workers but bundles are pending - start workers to process them".to_string());
        }

        let dead_workers: Vec<_> = workers.iter().filter(|w| !w.is_alive).collect();
        if !dead_workers.is_empty() {
            messages.push(format!(
                "{} workers have stopped responding (may be stalled or crashed)",
                dead_workers.len()
            ));
        }

        Ok(SystemStatus {
            queue: queue_stats,
            workers,
            health,
            timestamp: Utc::now(),
            messages,
        })
    }

    /// Get information about all workers
    async fn get_workers(&self) -> Result<Vec<WorkerInfo>> {
        let client = redis::Client::open(self.config.url.as_str())
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        let mut conn = client.get_multiplexed_async_connection()
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        // Scan for worker heartbeat keys
        let pattern = format!("{}:*", crate::queue::WORKER_HEARTBEAT_KEY);
        let keys: Vec<String> = conn.keys(&pattern)
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        let mut workers = Vec::new();
        let now = Utc::now();
        let timeout_secs = self.config.heartbeat_timeout.as_secs() as i64;

        for key in keys {
            // Extract worker ID from key
            let worker_id = key
                .strip_prefix(&format!("{}:", crate::queue::WORKER_HEARTBEAT_KEY))
                .unwrap_or(&key)
                .to_string();

            // Get last heartbeat timestamp
            let timestamp: Option<i64> = conn.get(&key)
                .await
                .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

            if let Some(ts) = timestamp {
                let last_heartbeat = DateTime::from_timestamp(ts, 0)
                    .unwrap_or_else(|| Utc::now());
                let seconds_since = (now - last_heartbeat).num_seconds();
                let is_alive = seconds_since < timeout_secs;

                workers.push(WorkerInfo {
                    id: worker_id,
                    last_heartbeat,
                    is_alive,
                    seconds_since_heartbeat: seconds_since,
                });
            }
        }

        // Sort by worker ID
        workers.sort_by(|a, b| a.id.cmp(&b.id));

        Ok(workers)
    }

    /// Determine system health
    fn determine_health(&self, stats: &QueueStats, workers: &[WorkerInfo]) -> String {
        let alive_workers = workers.iter().filter(|w| w.is_alive).count();
        let total_workers = workers.len();

        if stats.pending == 0 && stats.processing == 0 && stats.failed == 0 {
            return "idle".to_string();
        }

        if alive_workers == 0 && stats.pending > 0 {
            return "critical".to_string();
        }

        if stats.failed > stats.completed / 10 {
            return "degraded".to_string();
        }

        if alive_workers < total_workers / 2 {
            return "degraded".to_string();
        }

        "healthy".to_string()
    }

    /// Reset the queue (clear all bundles)
    pub async fn reset(&self, include_completed: bool) -> Result<()> {
        let client = redis::Client::open(self.config.url.as_str())
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        let mut conn = client.get_multiplexed_async_connection()
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        // Delete queue stream
        conn.del::<_, ()>(crate::queue::BUNDLE_QUEUE_KEY)
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        // Delete processing set
        conn.del::<_, ()>(crate::queue::PROCESSING_SET_KEY)
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        // Delete failed set
        conn.del::<_, ()>(crate::queue::FAILED_SET_KEY)
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        // Reset stats
        let stats_key = crate::queue::STATS_KEY;
        conn.hset::<_, _, _, ()>(stats_key, "pending", 0)
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
        conn.hset::<_, _, _, ()>(stats_key, "processing", 0)
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
        conn.hset::<_, _, _, ()>(stats_key, "failed", 0)
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        if include_completed {
            conn.del::<_, ()>(crate::queue::COMPLETED_SET_KEY)
                .await
                .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
            conn.hset::<_, _, _, ()>(stats_key, "completed", 0)
                .await
                .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
            conn.hset::<_, _, _, ()>(stats_key, "total_submitted", 0)
                .await
                .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
            conn.hset::<_, _, _, ()>(stats_key, "total_files", 0)
                .await
                .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
            conn.hset::<_, _, _, ()>(stats_key, "total_bytes", 0)
                .await
                .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
        }

        // Clean up bundle data
        let pattern = format!("{}:bundle:*", self.config.key_prefix);
        let keys: Vec<String> = conn.keys(&pattern)
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        for key in keys {
            conn.del::<_, ()>(&key)
                .await
                .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
        }

        Ok(())
    }

    /// Retry failed bundles
    pub async fn retry_failed(&self, bundle_id: Option<&str>) -> Result<u64> {
        let client = redis::Client::open(self.config.url.as_str())
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        let mut conn = client.get_multiplexed_async_connection()
            .await
            .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

        let mut retried = 0u64;

        if let Some(id) = bundle_id {
            // Retry specific bundle
            let is_failed: bool = conn.sismember(crate::queue::FAILED_SET_KEY, id)
                .await
                .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

            if is_failed {
                // Remove from failed set
                conn.srem::<_, _, ()>(crate::queue::FAILED_SET_KEY, id)
                    .await
                    .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

                // Re-add to queue
                redis::cmd("XADD")
                    .arg(crate::queue::BUNDLE_QUEUE_KEY)
                    .arg("*")
                    .arg("bundle_id")
                    .arg(id)
                    .query_async::<String>(&mut conn)
                    .await
                    .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

                // Update stats
                conn.hincr::<_, _, _, i64>(crate::queue::STATS_KEY, "failed", -1)
                    .await
                    .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
                conn.hincr::<_, _, _, i64>(crate::queue::STATS_KEY, "pending", 1)
                    .await
                    .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

                retried = 1;
            }
        } else {
            // Retry all failed bundles
            let failed_ids: Vec<String> = conn.smembers(crate::queue::FAILED_SET_KEY)
                .await
                .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

            for id in &failed_ids {
                // Remove from failed set
                conn.srem::<_, _, ()>(crate::queue::FAILED_SET_KEY, id)
                    .await
                    .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

                // Re-add to queue
                redis::cmd("XADD")
                    .arg(crate::queue::BUNDLE_QUEUE_KEY)
                    .arg("*")
                    .arg("bundle_id")
                    .arg(id)
                    .query_async::<String>(&mut conn)
                    .await
                    .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;

                retried += 1;
            }

            // Update stats
            if retried > 0 {
                conn.hincr::<_, _, _, i64>(crate::queue::STATS_KEY, "failed", -(retried as i64))
                    .await
                    .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
                conn.hincr::<_, _, _, i64>(crate::queue::STATS_KEY, "pending", retried as i64)
                    .await
                    .map_err(|e| JoggerError::Queue(crate::error::QueueError::Redis(e.to_string())))?;
            }
        }

        Ok(retried)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_info() {
        let info = WorkerInfo {
            id: "worker-1".to_string(),
            last_heartbeat: Utc::now(),
            is_alive: true,
            seconds_since_heartbeat: 5,
        };

        assert_eq!(info.id, "worker-1");
        assert!(info.is_alive);
    }
}
