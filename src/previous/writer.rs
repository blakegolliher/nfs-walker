//! Batched SQLite writer for high-throughput inserts
//!
//! This module provides a writer that batches inserts for optimal performance.
//! It runs in a dedicated thread and receives entries via a channel.
//!
//! # Performance Characteristics
//!
//! - Batched inserts with transactions (10K entries per batch default)
//! - Prepared statements for minimal parsing overhead
//! - WAL mode for concurrent reads during write
//! - Single writer thread avoids SQLite contention

use crate::db::schema::{self, keys};
use crate::error::{DbError, DbResult};
use crate::nfs::types::{DbEntry, DirStats};
use crossbeam_channel::{Receiver, Sender, bounded, TryRecvError};
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Message types sent to the writer thread
#[derive(Debug)]
pub enum WriterMessage {
    /// Insert a new entry
    Entry(DbEntry),

    /// Update directory statistics (using path to look up entry_id)
    DirStats { path: String, stats: DirStats },

    /// Flush pending writes
    Flush,

    /// Shutdown the writer
    Shutdown,
}

/// Statistics about write operations
#[derive(Debug, Default)]
pub struct WriterStats {
    /// Total entries written
    pub entries_written: AtomicU64,

    /// Total directories with stats recorded
    pub dir_stats_written: AtomicU64,

    /// Total batches committed
    pub batches_committed: AtomicU64,

    /// Total bytes processed (sum of file sizes)
    pub bytes_processed: AtomicU64,
}

impl WriterStats {
    /// Get entries per second rate
    pub fn entries_written(&self) -> u64 {
        self.entries_written.load(Ordering::Relaxed)
    }

    /// Get bytes processed
    pub fn bytes_processed(&self) -> u64 {
        self.bytes_processed.load(Ordering::Relaxed)
    }
}

/// Handle for sending messages to the writer
#[derive(Clone)]
pub struct WriterHandle {
    sender: Sender<WriterMessage>,
    stats: Arc<WriterStats>,
    shutdown: Arc<AtomicBool>,
}

impl WriterHandle {
    /// Send an entry to be written
    pub fn send_entry(&self, entry: DbEntry) -> DbResult<()> {
        self.sender
            .send(WriterMessage::Entry(entry))
            .map_err(|_| DbError::ChannelClosed)
    }

    /// Send directory stats (path will be resolved to entry_id by writer)
    pub fn send_dir_stats(&self, path: String, stats: DirStats) -> DbResult<()> {
        self.sender
            .send(WriterMessage::DirStats { path, stats })
            .map_err(|_| DbError::ChannelClosed)
    }

    /// Request a flush of pending writes
    pub fn flush(&self) -> DbResult<()> {
        self.sender
            .send(WriterMessage::Flush)
            .map_err(|_| DbError::ChannelClosed)
    }

    /// Request shutdown (waits for pending writes)
    pub fn shutdown(&self) -> DbResult<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        // Use send_timeout to avoid blocking forever if channel is full
        self.sender
            .send_timeout(WriterMessage::Shutdown, Duration::from_secs(5))
            .map_err(|_| DbError::ChannelClosed)
    }

    /// Get writer statistics
    pub fn stats(&self) -> &WriterStats {
        &self.stats
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }
}

/// Batched database writer that runs in its own thread
pub struct BatchedWriter {
    /// Thread handle
    handle: Option<JoinHandle<DbResult<()>>>,

    /// Writer handle for sending messages
    writer_handle: WriterHandle,

    /// Path to database (for reopening after write)
    db_path: std::path::PathBuf,
}

impl BatchedWriter {
    /// Create a new batched writer
    ///
    /// This spawns a dedicated writer thread that processes entries
    /// sent via the returned handle.
    pub fn new(db_path: &Path, batch_size: usize, channel_size: usize) -> DbResult<Self> {
        let (sender, receiver) = bounded(channel_size);
        let stats = Arc::new(WriterStats::default());
        let shutdown = Arc::new(AtomicBool::new(false));

        let writer_handle = WriterHandle {
            sender,
            stats: Arc::clone(&stats),
            shutdown: Arc::clone(&shutdown),
        };

        // Open database and create schema
        let conn = Connection::open(db_path)?;
        schema::create_database(&conn)?;

        // Store version info
        schema::set_walk_info(&conn, keys::SCHEMA_VERSION, &schema::SCHEMA_VERSION.to_string())?;
        schema::set_walk_info(&conn, keys::WALKER_VERSION, env!("CARGO_PKG_VERSION"))?;
        schema::set_walk_info(&conn, keys::STATUS, "running")?;

        let stats_clone = Arc::clone(&stats);
        let shutdown_clone = Arc::clone(&shutdown);
        let db_path_clone = db_path.to_path_buf();

        // Spawn writer thread
        let handle = thread::Builder::new()
            .name("db-writer".into())
            .spawn(move || {
                writer_thread(conn, receiver, stats_clone, batch_size, shutdown_clone)
            })
            .map_err(|e| DbError::CreateFailed {
                path: db_path.to_path_buf(),
                reason: format!("Failed to spawn writer thread: {}", e),
            })?;

        Ok(Self {
            handle: Some(handle),
            writer_handle,
            db_path: db_path_clone,
        })
    }

    /// Get a handle for sending messages to the writer
    pub fn handle(&self) -> WriterHandle {
        self.writer_handle.clone()
    }

    /// Wait for the writer to finish and finalize the database
    pub fn finish(mut self) -> DbResult<()> {
        // Request shutdown
        tracing::info!("Writer: sending shutdown signal");
        let _ = self.writer_handle.shutdown();

        // Wait for thread to finish with timeout
        let thread_finished = if let Some(handle) = self.handle.take() {
            // Use a polling approach with timeout
            let start = std::time::Instant::now();
            let timeout = Duration::from_secs(60);
            tracing::info!("Writer: waiting for thread to finish (60s timeout)");

            loop {
                if handle.is_finished() {
                    tracing::info!(elapsed_ms = start.elapsed().as_millis(), "Writer: thread finished");
                    match handle.join() {
                        Ok(result) => {
                            if let Err(e) = result {
                                tracing::warn!("Writer thread error: {}", e);
                            }
                        }
                        Err(_) => {
                            tracing::warn!("Writer thread panicked");
                        }
                    }
                    break true;
                }

                if start.elapsed() > timeout {
                    // Thread didn't finish in time - can't safely finalize
                    tracing::warn!("Writer thread did not finish in time, skipping finalization");
                    break false;
                }

                std::thread::sleep(Duration::from_millis(50));
            }
        } else {
            true
        };

        // Only finalize if the writer thread finished (released the db lock)
        if !thread_finished {
            tracing::warn!("Database may not be fully finalized - indexes not created");
            return Ok(());
        }

        // Reopen database for finalization
        let conn = Connection::open(&self.db_path)?;

        // Create indexes
        schema::create_indexes(&conn)?;

        // Update status
        schema::set_walk_info(&conn, keys::STATUS, "completed")?;

        // Optimize for reads
        schema::optimize_for_reads(&conn)?;

        Ok(())
    }

    /// Get the database path
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }
}

/// Internal writer thread function
fn writer_thread(
    conn: Connection,
    receiver: Receiver<WriterMessage>,
    stats: Arc<WriterStats>,
    batch_size: usize,
    shutdown: Arc<AtomicBool>,
) -> DbResult<()> {
    let mut entry_buffer: Vec<DbEntry> = Vec::with_capacity(batch_size);
    // DirStats now uses path (String) instead of entry_id
    let mut dir_stats_buffer: Vec<(String, DirStats)> = Vec::with_capacity(batch_size / 10);
    // Map from path -> database ID for parent lookups and dir_stats
    let mut path_to_id: HashMap<String, i64> = HashMap::new();

    loop {
        // Check shutdown flag on EVERY iteration - this is critical for clean exit
        // when async tasks are stuck in spawn_blocking and haven't dropped their handles
        if shutdown.load(Ordering::Relaxed) {
            // Shutdown requested - do final flush and exit immediately
            tracing::info!(
                buffered_entries = entry_buffer.len(),
                buffered_dir_stats = dir_stats_buffer.len(),
                "Writer thread: shutdown flag detected, flushing and exiting"
            );
            if !entry_buffer.is_empty() {
                let _ = flush_entries(&conn, &mut entry_buffer, &mut path_to_id, &stats);
            }
            if !dir_stats_buffer.is_empty() {
                let _ = flush_dir_stats(&conn, &mut dir_stats_buffer, &path_to_id, &stats);
            }
            tracing::info!("Writer thread: exiting");
            break;
        }

        // Try to receive without blocking first (drain queue)
        match receiver.try_recv() {
            Ok(msg) => {
                match msg {
                    WriterMessage::Entry(entry) => {
                        // Track bytes for files
                        if entry.entry_type.is_file() {
                            stats.bytes_processed.fetch_add(entry.size, Ordering::Relaxed);
                        }
                        entry_buffer.push(entry);

                        if entry_buffer.len() >= batch_size {
                            flush_entries(&conn, &mut entry_buffer, &mut path_to_id, &stats)?;
                        }
                    }
                    WriterMessage::DirStats { path, stats: ds } => {
                        dir_stats_buffer.push((path, ds));

                        if dir_stats_buffer.len() >= batch_size / 10 {
                            flush_dir_stats(&conn, &mut dir_stats_buffer, &path_to_id, &stats)?;
                        }
                    }
                    WriterMessage::Flush => {
                        if !entry_buffer.is_empty() {
                            flush_entries(&conn, &mut entry_buffer, &mut path_to_id, &stats)?;
                        }
                        if !dir_stats_buffer.is_empty() {
                            flush_dir_stats(&conn, &mut dir_stats_buffer, &path_to_id, &stats)?;
                        }
                    }
                    WriterMessage::Shutdown => {
                        // Final flush
                        if !entry_buffer.is_empty() {
                            flush_entries(&conn, &mut entry_buffer, &mut path_to_id, &stats)?;
                        }
                        if !dir_stats_buffer.is_empty() {
                            flush_dir_stats(&conn, &mut dir_stats_buffer, &path_to_id, &stats)?;
                        }
                        break;
                    }
                }
            }
            Err(TryRecvError::Empty) => {
                // Flush any pending entries before waiting
                if !entry_buffer.is_empty() && entry_buffer.len() >= batch_size / 4 {
                    flush_entries(&conn, &mut entry_buffer, &mut path_to_id, &stats)?;
                }

                // Block waiting for next message (short timeout to check shutdown flag)
                match receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(msg) => {
                        match msg {
                            WriterMessage::Entry(entry) => {
                                if entry.entry_type.is_file() {
                                    stats.bytes_processed.fetch_add(entry.size, Ordering::Relaxed);
                                }
                                entry_buffer.push(entry);
                            }
                            WriterMessage::DirStats { path, stats: ds } => {
                                dir_stats_buffer.push((path, ds));
                            }
                            WriterMessage::Flush => {
                                if !entry_buffer.is_empty() {
                                    flush_entries(&conn, &mut entry_buffer, &mut path_to_id, &stats)?;
                                }
                                if !dir_stats_buffer.is_empty() {
                                    flush_dir_stats(&conn, &mut dir_stats_buffer, &path_to_id, &stats)?;
                                }
                            }
                            WriterMessage::Shutdown => {
                                if !entry_buffer.is_empty() {
                                    flush_entries(&conn, &mut entry_buffer, &mut path_to_id, &stats)?;
                                }
                                if !dir_stats_buffer.is_empty() {
                                    flush_dir_stats(&conn, &mut dir_stats_buffer, &path_to_id, &stats)?;
                                }
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        // Timeout - continue loop to check shutdown flag
                    }
                }
            }
            Err(TryRecvError::Disconnected) => {
                // Channel closed - flush and exit
                if !entry_buffer.is_empty() {
                    flush_entries(&conn, &mut entry_buffer, &mut path_to_id, &stats)?;
                }
                if !dir_stats_buffer.is_empty() {
                    flush_dir_stats(&conn, &mut dir_stats_buffer, &path_to_id, &stats)?;
                }
                break;
            }
        }
    }

    Ok(())
}

/// Number of rows per multi-row INSERT (SQLite limit is 999 variables, 14 cols = 71 max, use 50)
const MULTI_INSERT_ROWS: usize = 50;

/// Flush entry buffer to database using multi-row INSERTs for speed
fn flush_entries(
    conn: &Connection,
    buffer: &mut Vec<DbEntry>,
    path_to_id: &mut HashMap<String, i64>,
    stats: &WriterStats,
) -> DbResult<i64> {
    if buffer.is_empty() {
        return Ok(0);
    }

    let tx = conn.unchecked_transaction()?;
    let mut last_id = 0i64;
    let entries_count = buffer.len();

    // Process in chunks for multi-row inserts
    for chunk in buffer.chunks(MULTI_INSERT_ROWS) {
        let chunk_size = chunk.len();

        // Build multi-row INSERT statement
        let placeholders: Vec<String> = (0..chunk_size)
            .map(|i| {
                let base = i * 14;
                format!(
                    "(?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{})",
                    base + 1, base + 2, base + 3, base + 4, base + 5,
                    base + 6, base + 7, base + 8, base + 9, base + 10,
                    base + 11, base + 12, base + 13, base + 14
                )
            })
            .collect();

        let sql = format!(
            "INSERT INTO entries (parent_id, name, path, entry_type, size, mtime, atime, ctime, mode, uid, gid, nlink, inode, depth) VALUES {}",
            placeholders.join(", ")
        );

        let mut stmt = tx.prepare_cached(&sql)?;

        // Build parameter array
        let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(chunk_size * 14);

        for entry in chunk {
            let parent_id = entry.parent_path.as_ref()
                .and_then(|p| path_to_id.get(p).copied());

            params.push(parent_id.map(|id| id.into()).unwrap_or(rusqlite::types::Value::Null));
            params.push(entry.name.clone().into());
            params.push(entry.path.clone().into());
            params.push((entry.entry_type.as_db_int() as i64).into());
            params.push((entry.size as i64).into());
            params.push(entry.mtime.map(|v| v.into()).unwrap_or(rusqlite::types::Value::Null));
            params.push(entry.atime.map(|v| v.into()).unwrap_or(rusqlite::types::Value::Null));
            params.push(entry.ctime.map(|v| v.into()).unwrap_or(rusqlite::types::Value::Null));
            params.push(entry.mode.map(|v| (v as i64).into()).unwrap_or(rusqlite::types::Value::Null));
            params.push(entry.uid.map(|v| (v as i64).into()).unwrap_or(rusqlite::types::Value::Null));
            params.push(entry.gid.map(|v| (v as i64).into()).unwrap_or(rusqlite::types::Value::Null));
            params.push(entry.nlink.map(|v| (v as i64).into()).unwrap_or(rusqlite::types::Value::Null));
            params.push((entry.inode as i64).into());
            params.push((entry.depth as i64).into());
        }

        // Execute the multi-row insert
        stmt.execute(rusqlite::params_from_iter(params))?;
        last_id = tx.last_insert_rowid();
    }

    // Track directory paths for parent_id lookups (after insert to get IDs)
    // Note: This is approximate for multi-row inserts but sufficient for HFC mode
    // where we typically have very few directories
    for entry in buffer.iter() {
        if entry.entry_type.is_dir() {
            // For directories, we need to look up the ID we just inserted
            // This is only done for directories which are rare in HFC mode
            if let Ok(id) = tx.query_row(
                "SELECT id FROM entries WHERE path = ?",
                [&entry.path],
                |row| row.get::<_, i64>(0)
            ) {
                path_to_id.insert(entry.path.clone(), id);
            }
        }
    }

    tx.commit()?;
    stats.entries_written.fetch_add(entries_count as u64, Ordering::Relaxed);
    stats.batches_committed.fetch_add(1, Ordering::Relaxed);

    buffer.clear();
    Ok(last_id)
}

/// Flush directory stats buffer to database
fn flush_dir_stats(
    conn: &Connection,
    buffer: &mut Vec<(String, DirStats)>,
    path_to_id: &HashMap<String, i64>,
    stats: &WriterStats,
) -> DbResult<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    let tx = conn.unchecked_transaction()?;

    {
        let mut stmt = tx.prepare_cached(
            "INSERT OR REPLACE INTO dir_stats (entry_id, direct_file_count, direct_dir_count, direct_symlink_count, direct_other_count, direct_bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        )?;

        // Prepare fallback lookup statement for paths not in the map
        let mut lookup_stmt = tx.prepare_cached(
            "SELECT id FROM entries WHERE path = ? AND entry_type = 1"
        )?;

        for (path, ds) in buffer.drain(..) {
            // Look up entry_id from path - try in-memory map first
            let entry_id = path_to_id.get(&path).copied().or_else(|| {
                // Fallback to database lookup if not in map
                lookup_stmt.query_row(params![&path], |row| row.get(0)).ok()
            });

            if let Some(entry_id) = entry_id {
                stmt.execute(params![
                    entry_id,
                    ds.file_count as i64,
                    ds.dir_count as i64,
                    ds.symlink_count as i64,
                    ds.other_count as i64,
                    ds.total_bytes as i64,
                ])?;

                stats.dir_stats_written.fetch_add(1, Ordering::Relaxed);
            }
            // If path not found anywhere, skip this dir_stats entry
            // This shouldn't happen normally - directory should always exist
        }
    }

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::EntryType;
    use tempfile::tempdir;

    #[test]
    fn test_writer_basic() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let writer = BatchedWriter::new(&db_path, 100, 1000).unwrap();
        let handle = writer.handle();

        // Insert some entries
        for i in 0..10 {
            let entry = DbEntry {
                parent_path: if i == 0 { None } else { Some("/test".to_string()) },
                name: format!("file{}.txt", i),
                path: format!("/test/file{}.txt", i),
                entry_type: EntryType::File,
                size: 1024 * i as u64,
                mtime: Some(1234567890),
                atime: None,
                ctime: None,
                mode: Some(0o644),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(1),
                inode: i as u64,
                depth: 1,
            };
            handle.send_entry(entry).unwrap();
        }

        // Finish and verify
        writer.finish().unwrap();

        // Reopen and check
        let conn = Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 10);
    }

    #[test]
    fn test_writer_stats() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let writer = BatchedWriter::new(&db_path, 100, 1000).unwrap();
        let handle = writer.handle();

        // Insert entries
        for i in 0..5 {
            let entry = DbEntry {
                parent_path: Some("/".to_string()),
                name: format!("file{}", i),
                path: format!("/file{}", i),
                entry_type: EntryType::File,
                size: 100,
                mtime: None,
                atime: None,
                ctime: None,
                mode: None,
                uid: None,
                gid: None,
                nlink: None,
                inode: i as u64,
                depth: 0,
            };
            handle.send_entry(entry).unwrap();
        }

        handle.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));

        assert!(handle.stats().entries_written() >= 5);
        assert_eq!(handle.stats().bytes_processed(), 500);

        writer.finish().unwrap();
    }
}
