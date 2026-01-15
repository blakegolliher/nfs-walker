//! Work queue with backpressure support
//!
//! This module provides a bounded work queue for directory tasks.
//! When the queue is full, backpressure is applied by processing
//! subdirectories inline rather than blocking.
//!
//! Also provides an entry queue for distributing file entries from
//! large directories across all workers.

use crate::nfs::types::DbEntry;
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// A task to walk a directory
#[derive(Debug, Clone)]
pub struct DirTask {
    /// Full path to the directory
    pub path: String,

    /// Database ID of this directory entry (for parent_id references)
    pub entry_id: Option<i64>,

    /// Depth from root (0 = root)
    pub depth: u32,
}

impl DirTask {
    /// Create a new directory task
    pub fn new(path: String, entry_id: Option<i64>, depth: u32) -> Self {
        Self {
            path,
            entry_id,
            depth,
        }
    }

    /// Create the root task
    pub fn root(path: String) -> Self {
        Self {
            path,
            entry_id: None,
            depth: 0,
        }
    }
}

/// Statistics for the work queue
#[derive(Debug, Default)]
pub struct QueueStats {
    /// Total tasks enqueued
    pub enqueued: AtomicU64,

    /// Total tasks dequeued
    pub dequeued: AtomicU64,

    /// Tasks processed inline due to backpressure
    pub inline_processed: AtomicU64,

    /// Number of times backpressure was applied
    pub backpressure_events: AtomicU64,
}

impl QueueStats {
    /// Get queue throughput (dequeued tasks)
    pub fn throughput(&self) -> u64 {
        self.dequeued.load(Ordering::Relaxed)
    }

    /// Get number of inline-processed tasks
    pub fn inline_count(&self) -> u64 {
        self.inline_processed.load(Ordering::Relaxed)
    }

    /// Get backpressure event count
    pub fn backpressure_count(&self) -> u64 {
        self.backpressure_events.load(Ordering::Relaxed)
    }
}

/// Work queue with backpressure support
pub struct WorkQueue {
    /// Sender for adding tasks
    sender: Sender<DirTask>,

    /// Receiver for getting tasks
    receiver: Receiver<DirTask>,

    /// Queue capacity
    capacity: usize,

    /// Number of active workers
    active_workers: Arc<AtomicUsize>,

    /// Queue statistics
    stats: Arc<QueueStats>,
}

impl WorkQueue {
    /// Create a new work queue with the specified capacity
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);

        Self {
            sender,
            receiver,
            capacity,
            active_workers: Arc::new(AtomicUsize::new(0)),
            stats: Arc::new(QueueStats::default()),
        }
    }

    /// Get a sender for this queue (clone for each worker)
    pub fn sender(&self) -> WorkQueueSender {
        WorkQueueSender {
            sender: self.sender.clone(),
            stats: Arc::clone(&self.stats),
        }
    }

    /// Get a receiver for this queue (clone for each worker)
    pub fn receiver(&self) -> WorkQueueReceiver {
        WorkQueueReceiver {
            receiver: self.receiver.clone(),
            active_workers: Arc::clone(&self.active_workers),
            stats: Arc::clone(&self.stats),
        }
    }

    /// Get the active worker counter
    pub fn active_workers(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active_workers)
    }

    /// Get queue statistics
    pub fn stats(&self) -> Arc<QueueStats> {
        Arc::clone(&self.stats)
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    /// Get current queue length
    pub fn len(&self) -> usize {
        self.receiver.len()
    }

    /// Get queue capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Seed the queue with the root directory
    pub fn seed(&self, root_path: String) -> Result<(), TrySendError<DirTask>> {
        let task = DirTask::root(root_path);
        self.sender.try_send(task)
    }

    /// Check if all work is complete
    ///
    /// Work is complete when:
    /// 1. Queue is empty
    /// 2. No workers are actively processing
    pub fn is_complete(&self) -> bool {
        self.receiver.is_empty() && self.active_workers.load(Ordering::SeqCst) == 0
    }
}

/// Handle for sending tasks to the queue
#[derive(Clone)]
pub struct WorkQueueSender {
    sender: Sender<DirTask>,
    stats: Arc<QueueStats>,
}

impl WorkQueueSender {
    /// Try to send a task to the queue
    ///
    /// Returns `Ok(true)` if sent successfully
    /// Returns `Ok(false)` if queue is full (backpressure)
    /// Returns `Err` if queue is disconnected
    pub fn try_send(&self, task: DirTask) -> Result<bool, ()> {
        match self.sender.try_send(task) {
            Ok(()) => {
                self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(TrySendError::Full(_)) => {
                self.stats.backpressure_events.fetch_add(1, Ordering::Relaxed);
                Ok(false)
            }
            Err(TrySendError::Disconnected(_)) => Err(()),
        }
    }

    /// Send a task, blocking if necessary
    pub fn send(&self, task: DirTask) -> Result<(), ()> {
        self.sender.send(task).map_err(|_| ())?;
        self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Record that a task was processed inline (for stats)
    pub fn record_inline(&self) {
        self.stats.inline_processed.fetch_add(1, Ordering::Relaxed);
    }
}

/// Handle for receiving tasks from the queue
#[derive(Clone)]
pub struct WorkQueueReceiver {
    receiver: Receiver<DirTask>,
    active_workers: Arc<AtomicUsize>,
    stats: Arc<QueueStats>,
}

impl WorkQueueReceiver {
    /// Receive a task from the queue
    ///
    /// This will block until a task is available or the queue is disconnected.
    pub fn recv(&self) -> Option<DirTask> {
        match self.receiver.recv() {
            Ok(task) => {
                self.stats.dequeued.fetch_add(1, Ordering::Relaxed);
                Some(task)
            }
            Err(_) => None,
        }
    }

    /// Try to receive a task without blocking
    pub fn try_recv(&self) -> Option<DirTask> {
        match self.receiver.try_recv() {
            Ok(task) => {
                self.stats.dequeued.fetch_add(1, Ordering::Relaxed);
                Some(task)
            }
            Err(_) => None,
        }
    }

    /// Receive with timeout
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Option<DirTask> {
        match self.receiver.recv_timeout(timeout) {
            Ok(task) => {
                self.stats.dequeued.fetch_add(1, Ordering::Relaxed);
                Some(task)
            }
            Err(_) => None,
        }
    }

    /// Mark this worker as active
    pub fn begin_work(&self) {
        self.active_workers.fetch_add(1, Ordering::SeqCst);
    }

    /// Mark this worker as idle
    pub fn end_work(&self) {
        self.active_workers.fetch_sub(1, Ordering::SeqCst);
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    /// Get current queue length
    pub fn len(&self) -> usize {
        self.receiver.len()
    }
}

/// RAII guard for marking work as active
pub struct WorkGuard<'a> {
    receiver: &'a WorkQueueReceiver,
}

impl<'a> WorkGuard<'a> {
    /// Create a new work guard (marks worker as active)
    pub fn new(receiver: &'a WorkQueueReceiver) -> Self {
        receiver.begin_work();
        Self { receiver }
    }
}

impl<'a> Drop for WorkGuard<'a> {
    fn drop(&mut self) {
        self.receiver.end_work();
    }
}

/// Threshold for distributing entries across workers
/// Directories with more entries than this will use the shared entry queue
pub const LARGE_DIR_THRESHOLD: usize = 10_000;

/// Queue for distributing file entries from large directories
///
/// When a directory has many entries, they are pushed to this queue
/// so that all workers can help send them to the database writer.
pub struct EntryQueue {
    /// Sender for adding entries
    sender: Sender<DbEntry>,

    /// Receiver for getting entries
    receiver: Receiver<DbEntry>,

    /// Statistics
    stats: Arc<EntryQueueStats>,
}

/// Statistics for the entry queue
#[derive(Debug, Default)]
pub struct EntryQueueStats {
    /// Total entries pushed
    pub entries_pushed: AtomicU64,

    /// Total entries processed
    pub entries_processed: AtomicU64,
}

impl EntryQueue {
    /// Create a new entry queue with the specified capacity
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);

        Self {
            sender,
            receiver,
            stats: Arc::new(EntryQueueStats::default()),
        }
    }

    /// Get a sender handle
    pub fn sender(&self) -> EntryQueueSender {
        EntryQueueSender {
            sender: self.sender.clone(),
            stats: Arc::clone(&self.stats),
        }
    }

    /// Get a receiver handle
    pub fn receiver(&self) -> EntryQueueReceiver {
        EntryQueueReceiver {
            receiver: self.receiver.clone(),
            stats: Arc::clone(&self.stats),
        }
    }

    /// Get statistics
    pub fn stats(&self) -> Arc<EntryQueueStats> {
        Arc::clone(&self.stats)
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    /// Get current queue length
    pub fn len(&self) -> usize {
        self.receiver.len()
    }
}

/// Handle for sending entries to the queue
#[derive(Clone)]
pub struct EntryQueueSender {
    sender: Sender<DbEntry>,
    stats: Arc<EntryQueueStats>,
}

impl EntryQueueSender {
    /// Try to send an entry without blocking
    pub fn try_send(&self, entry: DbEntry) -> Result<bool, ()> {
        match self.sender.try_send(entry) {
            Ok(()) => {
                self.stats.entries_pushed.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(TrySendError::Full(_)) => Ok(false),
            Err(TrySendError::Disconnected(_)) => Err(()),
        }
    }

    /// Send an entry, blocking if necessary
    pub fn send(&self, entry: DbEntry) -> Result<(), ()> {
        self.sender.send(entry).map_err(|_| ())?;
        self.stats.entries_pushed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Push multiple entries (blocks until all are sent)
    pub fn send_batch(&self, entries: Vec<DbEntry>) -> Result<usize, ()> {
        let mut count = 0;
        for entry in entries {
            self.sender.send(entry).map_err(|_| ())?;
            count += 1;
        }
        self.stats.entries_pushed.fetch_add(count, Ordering::Relaxed);
        Ok(count as usize)
    }
}

/// Handle for receiving entries from the queue
#[derive(Clone)]
pub struct EntryQueueReceiver {
    receiver: Receiver<DbEntry>,
    stats: Arc<EntryQueueStats>,
}

impl EntryQueueReceiver {
    /// Try to receive an entry without blocking
    pub fn try_recv(&self) -> Option<DbEntry> {
        match self.receiver.try_recv() {
            Ok(entry) => {
                self.stats.entries_processed.fetch_add(1, Ordering::Relaxed);
                Some(entry)
            }
            Err(_) => None,
        }
    }

    /// Receive with timeout
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Option<DbEntry> {
        match self.receiver.recv_timeout(timeout) {
            Ok(entry) => {
                self.stats.entries_processed.fetch_add(1, Ordering::Relaxed);
                Some(entry)
            }
            Err(_) => None,
        }
    }

    /// Drain up to `max` entries from the queue
    pub fn drain(&self, max: usize) -> Vec<DbEntry> {
        let mut entries = Vec::with_capacity(max.min(100));
        for _ in 0..max {
            match self.receiver.try_recv() {
                Ok(entry) => entries.push(entry),
                Err(_) => break,
            }
        }
        self.stats.entries_processed.fetch_add(entries.len() as u64, Ordering::Relaxed);
        entries
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    /// Get current queue length
    pub fn len(&self) -> usize {
        self.receiver.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_basic() {
        let queue = WorkQueue::new(10);

        queue.seed("/test".into()).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 1);

        let receiver = queue.receiver();
        let task = receiver.recv().unwrap();
        assert_eq!(task.path, "/test");
        assert_eq!(task.depth, 0);
    }

    #[test]
    fn test_queue_backpressure() {
        let queue = WorkQueue::new(2);
        let sender = queue.sender();

        // Fill the queue
        assert!(sender.try_send(DirTask::new("/a".into(), None, 0)).unwrap());
        assert!(sender.try_send(DirTask::new("/b".into(), None, 0)).unwrap());

        // Queue is full - should return false (backpressure)
        assert!(!sender.try_send(DirTask::new("/c".into(), None, 0)).unwrap());

        // Check stats
        assert_eq!(queue.stats().backpressure_count(), 1);
    }

    #[test]
    fn test_queue_completion() {
        let queue = WorkQueue::new(10);
        let receiver = queue.receiver();

        // Empty queue with no active workers = complete
        assert!(queue.is_complete());

        // Add work
        queue.seed("/test".into()).unwrap();
        assert!(!queue.is_complete());

        // Take work
        let _guard = WorkGuard::new(&receiver);
        let _task = receiver.try_recv().unwrap();

        // Queue empty but worker active
        assert!(!queue.is_complete());

        drop(_guard);

        // Now complete
        assert!(queue.is_complete());
    }

    #[test]
    fn test_queue_stats() {
        let queue = WorkQueue::new(10);
        let sender = queue.sender();
        let receiver = queue.receiver();

        sender.send(DirTask::new("/a".into(), None, 0)).unwrap();
        sender.send(DirTask::new("/b".into(), None, 0)).unwrap();

        receiver.recv().unwrap();
        receiver.recv().unwrap();

        let stats = queue.stats();
        assert_eq!(stats.enqueued.load(Ordering::Relaxed), 2);
        assert_eq!(stats.dequeued.load(Ordering::Relaxed), 2);
    }
}
