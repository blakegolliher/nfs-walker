//! Async pipelined stat using libnfs async API
//!
//! This module provides high-throughput stat operations by pipelining
//! multiple nfs_stat64_async calls on a single connection.
//!
//! Instead of: stat() -> wait -> stat() -> wait (sequential, RTT-bound)
//! We do: stat_async() x N -> poll -> process completions -> fire more
//!
//! This allows 100+ in-flight requests per connection, dramatically
//! reducing the impact of network latency.

use super::ffi;
use super::types::{NfsStat, EntryType};
use crate::error::{NfsError, NfsResult};
use std::collections::VecDeque;
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, trace, warn};

/// Maximum concurrent in-flight stat requests per connection
const MAX_IN_FLIGHT: usize = 128;

/// Result of a single stat operation
#[derive(Debug)]
pub struct StatResult {
    pub name: String,
    pub path: String,
    pub stat: Option<NfsStat>,
    pub error: Option<String>,
}

/// Stats from async stat batch
#[derive(Debug, Default)]
pub struct AsyncStatStats {
    pub processed: u64,
    pub files: u64,
    pub dirs: u64,
    pub bytes: u64,
    pub errors: u64,
}

/// Optional live progress counters for HFC mode
#[derive(Clone)]
pub struct LiveProgress {
    pub processed: Arc<AtomicU64>,
    pub files: Arc<AtomicU64>,
    pub dirs: Arc<AtomicU64>,
    pub bytes: Arc<AtomicU64>,
}

/// Context passed to each async callback
struct CallbackContext {
    /// Index in the results array
    index: usize,
    /// Pointer back to the engine's results
    results_ptr: *mut Vec<StatResult>,
    /// Counter for completed operations
    completed: Arc<AtomicU64>,
    /// Optional live progress counters
    live_progress: Option<LiveProgress>,
}

// Safety: We ensure the results_ptr is valid for the lifetime of the async operations
unsafe impl Send for CallbackContext {}

/// Async stat engine that pipelines stat operations
pub struct AsyncStatEngine {
    /// Raw NFS context pointer
    nfs: *mut ffi::nfs_context,
    /// Pending paths to stat
    pending: VecDeque<(String, String)>, // (name, full_path)
    /// Number of in-flight requests
    in_flight: usize,
    /// Completed count
    completed: Arc<AtomicU64>,
    /// Results storage
    results: Vec<StatResult>,
    /// Total items to process
    total: usize,
    /// Optional live progress counters
    live_progress: Option<LiveProgress>,
}

impl AsyncStatEngine {
    /// Create a new async stat engine from an existing NFS context
    ///
    /// # Safety
    /// The nfs pointer must be a valid, connected nfs_context
    pub unsafe fn new(nfs: *mut ffi::nfs_context) -> Self {
        Self {
            nfs,
            pending: VecDeque::new(),
            in_flight: 0,
            completed: Arc::new(AtomicU64::new(0)),
            results: Vec::new(),
            total: 0,
            live_progress: None,
        }
    }

    /// Set live progress counters for real-time updates
    pub fn set_live_progress(&mut self, progress: LiveProgress) {
        self.live_progress = Some(progress);
    }

    /// Add paths to stat
    /// names: list of filenames
    /// parent_path: parent directory path
    pub fn add_paths(&mut self, names: Vec<String>, parent_path: &str) {
        for name in names {
            let full_path = if parent_path == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", parent_path, name)
            };
            self.pending.push_back((name, full_path));
        }
        self.total = self.pending.len();

        // CRITICAL: Pre-allocate results to exact size to prevent reallocation.
        // The callback contexts store raw pointers to self.results, so if the Vec
        // reallocates during fire_requests(), those pointers become dangling.
        // By reserving exact capacity AND pre-filling with placeholder entries,
        // we guarantee no reallocation occurs.
        self.results = Vec::with_capacity(self.total);
        for (name, path) in self.pending.iter() {
            self.results.push(StatResult {
                name: name.clone(),
                path: path.clone(),
                stat: None,
                error: None,
            });
        }
    }

    /// Run all stats and return results
    pub fn run(&mut self) -> NfsResult<(Vec<StatResult>, AsyncStatStats)> {
        debug!(
            total = self.total,
            max_in_flight = MAX_IN_FLIGHT,
            "Starting async stat pipeline"
        );

        // Fire initial batch of requests
        self.fire_requests()?;

        // Poll loop
        while self.in_flight > 0 || !self.pending.is_empty() {
            self.poll_and_service()?;

            // Fire more requests if we have capacity
            if self.in_flight < MAX_IN_FLIGHT && !self.pending.is_empty() {
                self.fire_requests()?;
            }

            // Progress logging
            let completed = self.completed.load(Ordering::Relaxed);
            if completed > 0 && completed % 10000 == 0 {
                debug!(
                    completed = completed,
                    in_flight = self.in_flight,
                    pending = self.pending.len(),
                    "Async stat progress"
                );
            }
        }

        // Calculate stats
        let mut stats = AsyncStatStats::default();
        for result in &self.results {
            stats.processed += 1;
            if let Some(ref stat) = result.stat {
                if stat.entry_type() == EntryType::Directory {
                    stats.dirs += 1;
                } else {
                    stats.files += 1;
                    stats.bytes += stat.size;
                }
            } else {
                stats.errors += 1;
            }
        }

        debug!(
            processed = stats.processed,
            files = stats.files,
            dirs = stats.dirs,
            errors = stats.errors,
            "Async stat pipeline complete"
        );

        Ok((std::mem::take(&mut self.results), stats))
    }

    /// Fire as many requests as we can (up to MAX_IN_FLIGHT)
    fn fire_requests(&mut self) -> NfsResult<()> {
        while self.in_flight < MAX_IN_FLIGHT {
            // Calculate index BEFORE popping - this is the index into pre-allocated results
            let result_index = self.total - self.pending.len();

            let (_name, path) = match self.pending.pop_front() {
                Some(item) => item,
                None => break,
            };

            let c_path = CString::new(path.as_bytes()).map_err(|_| NfsError::StatFailed {
                path: path.clone(),
                reason: "Path contains null bytes".to_string(),
            })?;

            // Create callback context with index into pre-allocated results
            // SAFETY: results was pre-allocated to exact size in add_paths() and will
            // not reallocate, so this pointer remains valid for the lifetime of run().
            let ctx = Box::new(CallbackContext {
                index: result_index,
                results_ptr: &mut self.results as *mut Vec<StatResult>,
                completed: Arc::clone(&self.completed),
                live_progress: self.live_progress.clone(),
            });

            // Fire async stat
            let ret = unsafe {
                ffi::nfs_stat64_async(
                    self.nfs,
                    c_path.as_ptr(),
                    Some(stat_callback),
                    Box::into_raw(ctx) as *mut std::ffi::c_void,
                )
            };

            if ret != 0 {
                warn!(path = %path, "Failed to start async stat");
                // Mark as error in pre-allocated results slot
                if let Some(result) = self.results.get_mut(result_index) {
                    result.error = Some("Failed to start async stat".to_string());
                }
                self.completed.fetch_add(1, Ordering::Relaxed);
            } else {
                self.in_flight += 1;
                trace!(path = %path, in_flight = self.in_flight, "Fired async stat");
            }
        }

        Ok(())
    }

    /// Poll for events and service the connection
    fn poll_and_service(&mut self) -> NfsResult<()> {
        let fd = unsafe { ffi::nfs_get_fd(self.nfs) };
        let events = unsafe { ffi::nfs_which_events(self.nfs) };

        // Use libc poll
        let mut pfd = libc::pollfd {
            fd,
            events: events as i16,
            revents: 0,
        };

        let ret = unsafe { libc::poll(&mut pfd, 1, 100) }; // 100ms timeout

        if ret < 0 {
            return Err(NfsError::Protocol {
                code: ret,
                message: "poll() failed".to_string(),
            });
        }

        if ret > 0 {
            let old_completed = self.completed.load(Ordering::Relaxed);

            let service_ret = unsafe { ffi::nfs_service(self.nfs, pfd.revents as i32) };

            if service_ret < 0 {
                return Err(NfsError::Protocol {
                    code: service_ret,
                    message: "nfs_service() failed".to_string(),
                });
            }

            // Update in_flight count based on completions
            let new_completed = self.completed.load(Ordering::Relaxed);
            let just_completed = (new_completed - old_completed) as usize;
            self.in_flight = self.in_flight.saturating_sub(just_completed);
        }

        Ok(())
    }
}

/// C callback for nfs_stat64_async
///
/// # Safety
/// This is called from C code. private_data must be a valid CallbackContext pointer.
unsafe extern "C" fn stat_callback(
    status: i32,
    _nfs: *mut ffi::nfs_context,
    data: *mut std::ffi::c_void,
    private_data: *mut std::ffi::c_void,
) {
    // Reconstruct the context (takes ownership back)
    let ctx = Box::from_raw(private_data as *mut CallbackContext);

    // Get the results vector
    let results = &mut *ctx.results_ptr;

    if status < 0 {
        // Error case - data is error string
        let error_msg = if !data.is_null() {
            let c_str = std::ffi::CStr::from_ptr(data as *const i8);
            c_str.to_string_lossy().to_string()
        } else {
            "Unknown error".to_string()
        };

        if let Some(result) = results.get_mut(ctx.index) {
            result.error = Some(error_msg);
        }
    } else {
        // Success - data is nfs_stat_64*
        let stat_ptr = data as *const ffi::nfs_stat_64;
        if !stat_ptr.is_null() {
            let stat = &*stat_ptr;
            let nfs_stat = NfsStat {
                size: stat.nfs_size,
                inode: stat.nfs_ino,
                nlink: stat.nfs_nlink,
                uid: stat.nfs_uid as u32,
                gid: stat.nfs_gid as u32,
                mode: stat.nfs_mode as u32,
                atime: Some(stat.nfs_atime as i64),
                mtime: Some(stat.nfs_mtime as i64),
                ctime: Some(stat.nfs_ctime as i64),
                blksize: stat.nfs_blksize,
                blocks: stat.nfs_blocks,
            };

            // Update live progress if available
            if let Some(ref progress) = ctx.live_progress {
                progress.processed.fetch_add(1, Ordering::Relaxed);
                if nfs_stat.entry_type() == EntryType::Directory {
                    progress.dirs.fetch_add(1, Ordering::Relaxed);
                } else {
                    progress.files.fetch_add(1, Ordering::Relaxed);
                    progress.bytes.fetch_add(nfs_stat.size, Ordering::Relaxed);
                }
            }

            if let Some(result) = results.get_mut(ctx.index) {
                result.stat = Some(nfs_stat);
            }
        }
    }

    // Signal completion
    ctx.completed.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_async_stat_stats_default() {
        let stats = AsyncStatStats::default();
        assert_eq!(stats.processed, 0);
        assert_eq!(stats.files, 0);
    }
}
