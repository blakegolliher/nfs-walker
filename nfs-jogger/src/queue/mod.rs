//! Queue module for distributed work coordination
//!
//! Provides a distributed queue abstraction backed by Redis.
//! Supports reliable message delivery with acknowledgments.

mod redis_queue;

pub use redis_queue::{
    RedisQueue, RedisQueueConfig, QueueStats,
    BUNDLE_QUEUE_KEY, PROCESSING_SET_KEY, COMPLETED_SET_KEY,
    FAILED_SET_KEY, WORKER_HEARTBEAT_KEY, STATS_KEY,
};

use crate::bundle::Bundle;
use crate::error::QueueResult;
use std::time::Duration;

/// Trait for queue implementations
#[async_trait::async_trait]
pub trait WorkQueue: Send + Sync {
    /// Push a bundle to the queue
    async fn push(&self, bundle: Bundle) -> QueueResult<()>;

    /// Pop a bundle from the queue (blocking with timeout)
    async fn pop(&self, worker_id: &str, timeout: Duration) -> QueueResult<Option<Bundle>>;

    /// Acknowledge successful processing of a bundle
    async fn ack(&self, bundle_id: &str) -> QueueResult<()>;

    /// Report a bundle processing failure
    async fn nack(&self, bundle_id: &str, error: &str) -> QueueResult<()>;

    /// Get queue statistics
    async fn stats(&self) -> QueueResult<QueueStats>;

    /// Send worker heartbeat
    async fn heartbeat(&self, worker_id: &str) -> QueueResult<()>;

    /// Get number of pending bundles
    async fn pending_count(&self) -> QueueResult<u64>;

    /// Check if queue is empty
    async fn is_empty(&self) -> QueueResult<bool>;
}

// Re-export async_trait for consumers
pub use async_trait::async_trait;
