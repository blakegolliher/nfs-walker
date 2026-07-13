//! NFS connection wrapper using libnfs
//!
//! This module provides a safe Rust wrapper around the libnfs C library.
//! Each `NfsConnection` represents a single NFS mount and is NOT thread-safe.
//!
//! Key safety considerations:
//! - One connection per worker thread (libnfs contexts are not thread-safe)
//! - RAII for automatic cleanup (unmount + destroy on drop)
//! - All unsafe FFI calls are encapsulated with proper error handling

use crate::config::NfsUrl;
use crate::error::{NfsError, NfsResult};
use crate::nfs::types::{EntryType, NfsDirEntry, NfsStat};
use std::cell::Cell;
use std::ffi::{CStr, CString};
use std::ptr;
use std::time::Duration;

// Use pre-generated bindings from src/nfs/bindings.rs
pub use super::bindings as ffi;

/// Wrapper around libnfs context providing safe NFS operations
///
/// This struct owns an NFS connection and automatically cleans up
/// on drop. It is Send but NOT Sync - each worker thread needs its own.
pub struct NfsConnection {
    /// libnfs context pointer (never null after construction)
    context: *mut ffi::nfs_context,

    /// Server we're connected to
    server: String,

    /// Export path we're mounted on
    export: String,

    /// Whether we're currently mounted. `Cell` because an RPC timeout
    /// poisons the connection through `&self` (see `poison()`); the
    /// type is `!Sync` so this is single-thread interior mutability.
    mounted: Cell<bool>,

    /// RPC timeout in milliseconds (used for wait_for_rpc_completion)
    rpc_timeout_ms: i32,
}

// NfsConnection can be sent between threads but not shared
// Each worker thread should have its own connection
unsafe impl Send for NfsConnection {}
// NOT implementing Sync - libnfs is not thread-safe

/// READDIRPLUS transfer-size hints, shared by the synchronous and
/// pipelined submit paths so the two can't drift apart.
///
/// Deliberately small: large buffers make the server assemble huge
/// replies and time out on giant directories. ~16 KB ≈ 60–100 entries
/// per RPC, each of which completes quickly; the pipelined worker
/// recovers the round-trip cost by keeping several pages in flight.
const READDIRPLUS_DIRCOUNT: u32 = 8192;
const READDIRPLUS_MAXCOUNT: u32 = 16384;

/// Translate a negated NFS3 status code to a human-readable string.
///
/// libnfs callbacks store the NFS3 status as `-(res.status as i32)`,
/// so NFS3ERR_PERM (1) becomes -1, NFS3ERR_ACCES (13) becomes -13, etc.
fn nfs3_status_to_string(status: i32) -> String {
    // Callbacks store NFS3 errors negated; non-negative values are
    // RPC-layer statuses and must not be mistaken for NFS3 codes.
    if status >= 0 {
        return format!("unknown NFS3 status ({})", status);
    }
    match status.unsigned_abs() {
        ffi::nfsstat3_NFS3ERR_PERM => "NFS3ERR_PERM (operation not permitted)".into(),
        ffi::nfsstat3_NFS3ERR_NOENT => "NFS3ERR_NOENT (no such file or directory)".into(),
        ffi::nfsstat3_NFS3ERR_IO => "NFS3ERR_IO (I/O error)".into(),
        ffi::nfsstat3_NFS3ERR_NXIO => "NFS3ERR_NXIO (no such device or address)".into(),
        ffi::nfsstat3_NFS3ERR_ACCES => "NFS3ERR_ACCES (permission denied)".into(),
        ffi::nfsstat3_NFS3ERR_EXIST => "NFS3ERR_EXIST (file exists)".into(),
        ffi::nfsstat3_NFS3ERR_XDEV => "NFS3ERR_XDEV (cross-device link)".into(),
        ffi::nfsstat3_NFS3ERR_NODEV => "NFS3ERR_NODEV (no such device)".into(),
        ffi::nfsstat3_NFS3ERR_NOTDIR => "NFS3ERR_NOTDIR (not a directory)".into(),
        ffi::nfsstat3_NFS3ERR_ISDIR => "NFS3ERR_ISDIR (is a directory)".into(),
        ffi::nfsstat3_NFS3ERR_INVAL => "NFS3ERR_INVAL (invalid argument)".into(),
        ffi::nfsstat3_NFS3ERR_FBIG => "NFS3ERR_FBIG (file too large)".into(),
        ffi::nfsstat3_NFS3ERR_NOSPC => "NFS3ERR_NOSPC (no space left on device)".into(),
        ffi::nfsstat3_NFS3ERR_ROFS => "NFS3ERR_ROFS (read-only file system)".into(),
        ffi::nfsstat3_NFS3ERR_MLINK => "NFS3ERR_MLINK (too many links)".into(),
        ffi::nfsstat3_NFS3ERR_NAMETOOLONG => "NFS3ERR_NAMETOOLONG (name too long)".into(),
        ffi::nfsstat3_NFS3ERR_NOTEMPTY => "NFS3ERR_NOTEMPTY (directory not empty)".into(),
        ffi::nfsstat3_NFS3ERR_DQUOT => "NFS3ERR_DQUOT (disk quota exceeded)".into(),
        ffi::nfsstat3_NFS3ERR_STALE => "NFS3ERR_STALE (stale file handle)".into(),
        ffi::nfsstat3_NFS3ERR_REMOTE => "NFS3ERR_REMOTE (too many levels of remote in path)".into(),
        ffi::nfsstat3_NFS3ERR_BADHANDLE => "NFS3ERR_BADHANDLE (illegal NFS file handle)".into(),
        ffi::nfsstat3_NFS3ERR_NOT_SYNC => "NFS3ERR_NOT_SYNC (update synchronization mismatch)".into(),
        ffi::nfsstat3_NFS3ERR_BAD_COOKIE => "NFS3ERR_BAD_COOKIE (stale cookie)".into(),
        ffi::nfsstat3_NFS3ERR_NOTSUPP => "NFS3ERR_NOTSUPP (operation not supported)".into(),
        ffi::nfsstat3_NFS3ERR_TOOSMALL => "NFS3ERR_TOOSMALL (buffer or request too small)".into(),
        ffi::nfsstat3_NFS3ERR_SERVERFAULT => "NFS3ERR_SERVERFAULT (server fault)".into(),
        ffi::nfsstat3_NFS3ERR_BADTYPE => "NFS3ERR_BADTYPE (bad type)".into(),
        ffi::nfsstat3_NFS3ERR_JUKEBOX => "NFS3ERR_JUKEBOX (jukebox/try again later)".into(),
        _ => format!("unknown NFS3 status ({})", status),
    }
}

/// Convert a negated NFS3 status code to a typed NfsError with a path context.
fn nfs3_status_to_nfs_error(status: i32, path: &str) -> NfsError {
    if status >= 0 {
        return NfsError::ReadDirFailed {
            path: path.into(),
            reason: nfs3_status_to_string(status),
        };
    }
    match status.unsigned_abs() {
        ffi::nfsstat3_NFS3ERR_PERM | ffi::nfsstat3_NFS3ERR_ACCES => {
            NfsError::PermissionDenied { path: path.into() }
        }
        ffi::nfsstat3_NFS3ERR_NOENT => NfsError::NotFound { path: path.into() },
        ffi::nfsstat3_NFS3ERR_STALE => NfsError::StaleHandle { path: path.into() },
        _ => NfsError::ReadDirFailed {
            path: path.into(),
            reason: nfs3_status_to_string(status),
        },
    }
}

impl NfsConnection {
    /// Create a new NFS connection from a parsed URL
    ///
    /// This initializes the libnfs context but does not connect.
    /// Call `connect()` to establish the connection.
    pub fn new(url: &NfsUrl) -> NfsResult<Self> {
        let context = unsafe { ffi::nfs_init_context() };

        if context.is_null() {
            return Err(NfsError::InitFailed(
                "nfs_init_context() returned null".into(),
            ));
        }

        // Force NFSv3 - we don't support NFSv4
        // NFS_V3 = 3, NFS_V4 = 4 (from libnfs-raw-nfs.h)
        unsafe {
            ffi::nfs_set_version(context, 3);
        }

        // Set uid/gid to current user for proper NFS auth
        unsafe {
            let uid = libc::getuid() as i32;
            let gid = libc::getgid() as i32;
            ffi::nfs_set_uid(context, uid);
            ffi::nfs_set_gid(context, gid);
        }

        Ok(Self {
            context,
            server: url.server.clone(),
            export: url.export.clone(),
            mounted: Cell::new(false),
            rpc_timeout_ms: 30000, // Default 30 seconds, updated by connect()
        })
    }

    /// Connect and mount the NFS export
    pub fn connect(&mut self, timeout: Duration) -> NfsResult<()> {
        if self.mounted.get() {
            return Ok(());
        }

        // Set timeout (in milliseconds)
        let timeout_ms = timeout.as_millis() as i32;
        self.rpc_timeout_ms = timeout_ms;
        unsafe {
            ffi::nfs_set_timeout(self.context, timeout_ms);
        }

        // Note: Disabled buffer size override - caused hangs with some servers
        // libnfs default is 8KB, negotiated with server's dtpref during mount
        // TODO: Investigate why larger buffers cause issues

        // Convert strings to C strings
        let server_cstr = CString::new(self.server.as_str()).map_err(|_| {
            NfsError::ConnectionFailed {
                server: self.server.clone(),
                reason: "Server name contains null bytes".into(),
            }
        })?;

        let export_cstr = CString::new(self.export.as_str()).map_err(|_| {
            NfsError::MountFailed {
                server: self.server.clone(),
                export: self.export.clone(),
                reason: "Export path contains null bytes".into(),
            }
        })?;

        // Mount the export
        let result = unsafe {
            ffi::nfs_mount(self.context, server_cstr.as_ptr(), export_cstr.as_ptr())
        };

        if result != 0 {
            let error_msg = self.get_error();
            return Err(NfsError::MountFailed {
                server: self.server.clone(),
                export: self.export.clone(),
                reason: error_msg,
            });
        }

        self.mounted.set(true);
        Ok(())
    }

    /// Mark the connection unusable after an RPC left the context in an
    /// unknown state (e.g. a timed-out PDU still queued inside libnfs
    /// holding a pointer to a dead stack frame). Every subsequent
    /// operation fails fast with "Not mounted", so `rpc_service` is
    /// never driven again on this context and the stale callback can
    /// never fire. `Drop` also skips the unmount RPC for a poisoned
    /// connection — `nfs_destroy_context` cancels in-flight PDUs
    /// without invoking their callbacks.
    fn poison(&self) {
        self.mounted.set(false);
    }

    /// Read a directory using direct RPC, returning entries with file handles
    ///
    /// Unlike readdir_plus_chunked which uses the libnfs high-level API (which
    /// doesn't expose file handles), this function uses direct RPC calls to
    /// extract file handles from the READDIRPLUS response.
    ///
    /// The callback receives NfsDirEntry with file_handle populated for directories,
    /// enabling cached access to subdirectories without LOOKUP RPCs.
    pub fn readdir_plus_with_fh<F>(
        &self,
        path: &str,
        chunk_size: usize,
        callback: F,
    ) -> NfsResult<usize>
    where
        F: FnMut(Vec<NfsDirEntry>) -> bool,
    {
        if !self.mounted.get() {
            return Err(NfsError::ReadDirFailed {
                path: path.into(),
                reason: "Not mounted".into(),
            });
        }

        // Get RPC context from libnfs
        let rpc = unsafe { ffi::nfs_get_rpc_context(self.context) };
        if rpc.is_null() {
            return Err(NfsError::ReadDirFailed {
                path: path.into(),
                reason: "Failed to get RPC context".into(),
            });
        }

        // Drain any pending events to ensure clean state
        drain_pending_events(rpc);

        // Get root file handle
        let root_fh_ptr = unsafe { ffi::nfs_get_rootfh(self.context) };
        if root_fh_ptr.is_null() {
            return Err(NfsError::ReadDirFailed {
                path: path.into(),
                reason: "Failed to get root file handle".into(),
            });
        }

        // Walk path to get directory file handle (this does the LOOKUP RPCs)
        // Note: nfs_fh (libnfs type) has `len` and `val` fields, not `data.data_len`
        let root_fh = unsafe {
            let fh = &*root_fh_ptr;
            let mut data = [0u8; 128];
            let len = (fh.len as usize).min(128);
            std::ptr::copy_nonoverlapping(fh.val as *const u8, data.as_mut_ptr(), len);
            (len, data)
        };

        // Resolve path to file handle
        let dir_fh = self.lookup_path_internal(rpc, &root_fh.1[..root_fh.0], path)?;

        // Now use the file handle version
        self.readdir_plus_by_fh(&dir_fh, chunk_size, callback)
    }

    /// Internal helper to walk a path and get the file handle
    fn lookup_path_internal(
        &self,
        rpc: *mut ffi::rpc_context,
        root_fh: &[u8],
        path: &str,
    ) -> NfsResult<Vec<u8>> {
        let path = path.trim_start_matches('/');
        if path.is_empty() {
            return Ok(root_fh.to_vec());
        }

        let mut current_fh = root_fh.to_vec();

        for component in path.split('/') {
            if component.is_empty() {
                continue;
            }

            let name_cstr = CString::new(component).map_err(|_| NfsError::ReadDirFailed {
                path: path.into(),
                reason: format!("Invalid path component: {}", component),
            })?;

            let mut cb_data = LookupCallbackData {
                completed: Cell::new(false),
                status: 0,
                fh_len: 0,
                fh_data: [0; 128],
            };
            let cb_ptr: *mut LookupCallbackData = &mut cb_data;

            // Build LOOKUP args
            let mut args: ffi::LOOKUP3args = unsafe { std::mem::zeroed() };
            args.what.dir.data.data_len = current_fh.len() as u32;
            args.what.dir.data.data_val = current_fh.as_ptr() as *mut i8;
            args.what.name = name_cstr.as_ptr() as *mut i8;

            let pdu = unsafe {
                ffi::rpc_nfs3_lookup_task(
                    rpc,
                    Some(lookup_callback),
                    &mut args,
                    cb_ptr as *mut std::ffi::c_void,
                )
            };

            if pdu.is_null() {
                return Err(NfsError::ReadDirFailed {
                    path: path.into(),
                    reason: format!("Failed to queue LOOKUP for '{}'", component),
                });
            }

            let completed = unsafe { std::ptr::addr_of!((*cb_ptr).completed) };
            if let Err(e) = wait_for_rpc_completion(rpc, completed, self.rpc_timeout_ms) {
                // The PDU may still be queued inside libnfs with a
                // pointer to `cb_data` on this soon-dead stack frame.
                // Poison the connection so no code path ever services
                // this context again (which would fire the stale
                // callback into freed memory).
                self.poison();
                return Err(NfsError::ReadDirFailed {
                    path: path.into(),
                    reason: format!(
                        "LOOKUP '{}' failed: {} (connection poisoned)",
                        component, e
                    ),
                });
            }

            if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
                return Err(nfs3_status_to_nfs_error(cb_data.status, path));
            }

            if cb_data.fh_len == 0 {
                return Err(NfsError::ReadDirFailed {
                    path: path.into(),
                    reason: format!("LOOKUP '{}' returned empty handle", component),
                });
            }

            current_fh = cb_data.fh_data[..cb_data.fh_len].to_vec();
        }

        Ok(current_fh)
    }

    /// Read a directory using direct RPC with a cached file handle
    ///
    /// This bypasses the libnfs high-level API and uses the file handle directly,
    /// eliminating all LOOKUP RPCs that would otherwise be needed to resolve the path.
    /// This is critical for narrow-deep directory trees where path resolution
    /// causes O(n²) LOOKUP RPCs.
    ///
    /// The callback receives chunks of NfsDirEntry which include file handles
    /// for subdirectories, enabling recursive cached access.
    pub fn readdir_plus_by_fh<F>(
        &self,
        file_handle: &[u8],
        chunk_size: usize,
        mut callback: F,
    ) -> NfsResult<usize>
    where
        F: FnMut(Vec<NfsDirEntry>) -> bool, // Return false to stop early
    {
        if !self.mounted.get() {
            return Err(NfsError::ReadDirFailed {
                path: "(by file handle)".into(),
                reason: "Not mounted".into(),
            });
        }

        // Get RPC context from libnfs
        let rpc = unsafe { ffi::nfs_get_rpc_context(self.context) };
        if rpc.is_null() {
            return Err(NfsError::ReadDirFailed {
                path: "(by file handle)".into(),
                reason: "Failed to get RPC context".into(),
            });
        }

        // Drain any pending events to ensure clean state
        drain_pending_events(rpc);

        let mut total_entries = 0;
        let mut cookie: u64 = 0;
        let mut cookieverf: [i8; 8] = [0; 8];

        loop {
            let mut cb_data = ReaddirplusFullData {
                completed: Cell::new(false),
                status: 0,
                eof: false,
                cookie: 0,
                cookieverf: [0i8; 8],
                entries: Vec::with_capacity(chunk_size),
            };
            let cb_ptr: *mut ReaddirplusFullData = &mut cb_data;

            // Build READDIRPLUS args with the cached file handle
            let mut args: ffi::READDIRPLUS3args = unsafe { std::mem::zeroed() };
            args.dir.data.data_len = file_handle.len() as u32;
            args.dir.data.data_val = file_handle.as_ptr() as *mut i8;
            args.cookie = cookie;
            args.cookieverf = cookieverf;
            args.dircount = READDIRPLUS_DIRCOUNT;
            args.maxcount = READDIRPLUS_MAXCOUNT;

            let pdu = unsafe {
                ffi::rpc_nfs3_readdirplus_task(
                    rpc,
                    Some(readdirplus_full_callback),
                    &mut args,
                    cb_ptr as *mut std::ffi::c_void,
                )
            };

            if pdu.is_null() {
                return Err(NfsError::ReadDirFailed {
                    path: "(by file handle)".into(),
                    reason: "Failed to queue READDIRPLUS RPC".into(),
                });
            }

            // Wait for completion
            let completed = unsafe { std::ptr::addr_of!((*cb_ptr).completed) };
            if let Err(e) = wait_for_rpc_completion(rpc, completed, self.rpc_timeout_ms) {
                // Same rationale as the LOOKUP path: the timed-out PDU
                // may still reference this stack frame. Poison so the
                // context is never serviced again.
                self.poison();
                return Err(NfsError::ReadDirFailed {
                    path: "(by file handle)".into(),
                    reason: format!("READDIRPLUS failed: {} (connection poisoned)", e),
                });
            }

            if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
                return Err(nfs3_status_to_nfs_error(cb_data.status, "(by file handle)"));
            }

            total_entries += cb_data.entries.len();

            // Send entries to callback in chunks
            let mut chunk = Vec::with_capacity(chunk_size);
            for entry in cb_data.entries {
                chunk.push(entry);
                if chunk.len() >= chunk_size
                    && !callback(std::mem::replace(&mut chunk, Vec::with_capacity(chunk_size)))
                {
                    return Ok(total_entries);
                }
            }
            // Send remaining entries
            if !chunk.is_empty() && !callback(chunk) {
                return Ok(total_entries);
            }

            // Check if we're done
            if cb_data.eof {
                break;
            }

            // Prepare for next call
            cookie = cb_data.cookie;
            cookieverf = cb_data.cookieverf;
        }

        Ok(total_entries)
    }

    // ============================================================
    // Pipelined READDIRPLUS primitives (see docs/PIPELINED_READDIRPLUS_DESIGN.md)
    //
    // The methods below let a worker keep N READDIRPLUS RPCs in flight
    // on a single libnfs context, demuxing replies as they land. They
    // do NOT replace `readdir_plus_by_fh` — both code paths coexist.
    // ============================================================

    /// Submit a READDIRPLUS RPC by file handle without blocking.
    ///
    /// On success the returned [`InflightReaddir`] owns a heap-pinned
    /// `Box<ReaddirplusFullData>` whose address has been handed to libnfs
    /// as the PDU's `private_data`. The caller MUST keep the
    /// `InflightReaddir` alive until either (a) `is_completed()` returns
    /// true and `take_result()` has been called, or (b) the
    /// `NfsConnection` is dropped (which destroys the rpc_context and
    /// cancels all in-flight PDUs without firing their callbacks).
    ///
    /// `tag` is opaque to this layer; the caller uses it to map a
    /// completion back to a `DirState` slot.
    pub fn submit_readdirplus_by_fh(
        &self,
        file_handle: &[u8],
        cookie: u64,
        cookieverf: [i8; 8],
        tag: u64,
    ) -> NfsResult<InflightReaddir> {
        if !self.mounted.get() {
            return Err(NfsError::ReadDirFailed {
                path: "(pipelined)".into(),
                reason: "Not mounted".into(),
            });
        }

        let rpc = unsafe { ffi::nfs_get_rpc_context(self.context) };
        if rpc.is_null() {
            return Err(NfsError::ReadDirFailed {
                path: "(pipelined)".into(),
                reason: "Failed to get RPC context".into(),
            });
        }

        // Heap-pinned cb_data: libnfs writes into it from the rpc_service
        // callback fired on whatever thread calls rpc_service. We must
        // not move it. The Box is consumed into the returned
        // InflightReaddir; the raw pointer below stays valid until that
        // InflightReaddir is dropped.
        let mut cb_data: Box<ReaddirplusFullData> = Box::new(ReaddirplusFullData {
            completed: Cell::new(false),
            status: 0,
            eof: false,
            cookie: 0,
            cookieverf: [0i8; 8],
            // A ~16 KB READDIRPLUS page holds ~60–100 entries; one
            // up-front reservation avoids the realloc ladder per page.
            entries: Vec::with_capacity(128),
        });

        // Build args. The args struct can stay stack-allocated because
        // libnfs XDR-encodes it into the PDU buffer at submit time and
        // does not retain a pointer.
        let mut args: ffi::READDIRPLUS3args = unsafe { std::mem::zeroed() };
        args.dir.data.data_len = file_handle.len() as u32;
        args.dir.data.data_val = file_handle.as_ptr() as *mut i8;
        args.cookie = cookie;
        args.cookieverf = cookieverf;
        args.dircount = READDIRPLUS_DIRCOUNT;
        args.maxcount = READDIRPLUS_MAXCOUNT;

        let private =
            (&mut *cb_data) as *mut ReaddirplusFullData as *mut std::ffi::c_void;

        let pdu = unsafe {
            ffi::rpc_nfs3_readdirplus_task(
                rpc,
                Some(readdirplus_full_callback),
                &mut args,
                private,
            )
        };

        if pdu.is_null() {
            return Err(NfsError::ReadDirFailed {
                path: "(pipelined)".into(),
                reason: "Failed to queue READDIRPLUS RPC".into(),
            });
        }

        Ok(InflightReaddir {
            cb_data,
            tag,
            _not_send: std::marker::PhantomData,
        })
    }

    /// Drive the rpc_context until at least `min_completions` of the
    /// supplied slots have flipped to completed, or until `timeout_ms`
    /// elapses with no further progress.
    ///
    /// Returns the total number of slots currently completed (which
    /// may exceed `min_completions` if multiple replies land in one
    /// `rpc_service` call). On a clean timeout returns the count
    /// without erroring; only fd-level errors return Err.
    ///
    /// Edge cases:
    /// - empty `slots` slice → returns 0 immediately.
    /// - all slots already completed → returns count without polling.
    /// - `rpc_which_events` returns 0 → service with revents=0, sleep
    ///   10 ms, retry (mirrors legacy `wait_for_rpc_completion`).
    pub fn pump(
        &self,
        slots: &[InflightReaddir],
        min_completions: usize,
        timeout_ms: i32,
    ) -> NfsResult<usize> {
        // A poisoned context must never be serviced again: a timed-out
        // PDU may hold a private_data pointer into a dead stack frame,
        // and rpc_service would fire its callback.
        if !self.mounted.get() {
            return Err(NfsError::ReadDirFailed {
                path: "(pipelined)".into(),
                reason: "connection poisoned (not mounted)".into(),
            });
        }
        if slots.is_empty() {
            return Ok(0);
        }

        let count_completed =
            |s: &[InflightReaddir]| -> usize { s.iter().filter(|i| i.is_completed()).count() };

        let already = count_completed(slots);
        if already >= min_completions {
            return Ok(already);
        }

        let rpc = unsafe { ffi::nfs_get_rpc_context(self.context) };
        if rpc.is_null() {
            return Err(NfsError::ReadDirFailed {
                path: "(pipelined)".into(),
                reason: "Failed to get RPC context".into(),
            });
        }

        use std::os::unix::io::RawFd;
        let fd: RawFd = unsafe { ffi::rpc_get_fd(rpc) };
        if fd < 0 {
            return Err(NfsError::ReadDirFailed {
                path: "(pipelined)".into(),
                reason: "Invalid RPC fd".into(),
            });
        }

        let start = std::time::Instant::now();
        let total_budget =
            std::time::Duration::from_millis(timeout_ms.max(0) as u64);

        loop {
            let done = count_completed(slots);
            if done >= min_completions {
                return Ok(done);
            }
            if start.elapsed() >= total_budget {
                return Ok(done);
            }

            let events = unsafe { ffi::rpc_which_events(rpc) };

            if events == 0 {
                let svc = unsafe { ffi::rpc_service(rpc, 0) };
                if svc < 0 {
                    return Err(NfsError::ReadDirFailed {
                        path: "(pipelined)".into(),
                        reason: "rpc_service failed (no events)".into(),
                    });
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
                continue;
            }

            let mut pfd = libc::pollfd {
                fd,
                events: events as i16,
                revents: 0,
            };

            // Cap a single poll wait so we re-check completions promptly.
            let remaining_ms =
                total_budget.saturating_sub(start.elapsed()).as_millis() as i32;
            let poll_wait = remaining_ms.clamp(1, 100);
            let ret = unsafe { libc::poll(&mut pfd, 1, poll_wait) };

            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(NfsError::ReadDirFailed {
                    path: "(pipelined)".into(),
                    reason: format!("poll failed: {}", err),
                });
            }

            if ret > 0 {
                let revents = if pfd.revents != 0 { pfd.revents as i32 } else { events };
                let svc = unsafe { ffi::rpc_service(rpc, revents) };
                if svc < 0 {
                    return Err(NfsError::ReadDirFailed {
                        path: "(pipelined)".into(),
                        reason: format!("rpc_service failed: {}", svc),
                    });
                }
            }
        }
    }

    /// Resolve an NFS path to a file handle by walking LOOKUP RPCs from
    /// the root. Synchronous; safe to call while pipelined slots are in
    /// flight on the same context (the LOOKUP completion and any READDIRPLUS
    /// completions share the rpc_service loop without interfering — each
    /// has its own private_data).
    ///
    /// Lifted from the head of `readdir_plus_with_fh` so the pipelined
    /// worker can fall back to a sync lookup for fh-less work items
    /// (the root dir, plus any externally-injected dir).
    pub fn resolve_path_to_fh(&self, path: &str) -> NfsResult<Vec<u8>> {
        if !self.mounted.get() {
            return Err(NfsError::ReadDirFailed {
                path: path.into(),
                reason: "Not mounted".into(),
            });
        }

        let rpc = unsafe { ffi::nfs_get_rpc_context(self.context) };
        if rpc.is_null() {
            return Err(NfsError::ReadDirFailed {
                path: path.into(),
                reason: "Failed to get RPC context".into(),
            });
        }

        let root_fh_ptr = unsafe { ffi::nfs_get_rootfh(self.context) };
        if root_fh_ptr.is_null() {
            return Err(NfsError::ReadDirFailed {
                path: path.into(),
                reason: "Failed to get root file handle".into(),
            });
        }

        let root_fh = unsafe {
            let fh = &*root_fh_ptr;
            let mut data = [0u8; 128];
            let len = (fh.len as usize).min(128);
            std::ptr::copy_nonoverlapping(fh.val as *const u8, data.as_mut_ptr(), len);
            (len, data)
        };

        // NOTE: deliberately no drain_pending_events here. In pipelined
        // mode this fn may be called between submit/pump cycles with
        // other READDIRPLUS slots in flight; draining would discard
        // their pending completions.
        self.lookup_path_internal(rpc, &root_fh.1[..root_fh.0], path)
    }

    /// Get the current error message from libnfs
    fn get_error(&self) -> String {
        let err_ptr = unsafe { ffi::nfs_get_error(self.context) };
        if err_ptr.is_null() {
            return "Unknown error".into();
        }

        let c_str = unsafe { CStr::from_ptr(err_ptr) };
        c_str.to_string_lossy().into_owned()
    }

    /// Get the server name
    pub fn server(&self) -> &str {
        &self.server
    }

    /// Get the export path
    pub fn export(&self) -> &str {
        &self.export
    }

    /// Check if we're connected (false after `poison()`)
    pub fn is_connected(&self) -> bool {
        self.mounted.get()
    }
}

impl Drop for NfsConnection {
    fn drop(&mut self) {
        if !self.context.is_null() {
            // Skip the unmount RPC for poisoned connections: servicing
            // the context could fire a stale callback into a dead stack
            // frame (see `poison()`). nfs_destroy_context alone cancels
            // in-flight PDUs without invoking callbacks.
            if self.mounted.get() {
                unsafe {
                    ffi::nfs_umount(self.context);
                }
                self.mounted.set(false);
            }

            unsafe {
                ffi::nfs_destroy_context(self.context);
            }
            self.context = ptr::null_mut();
        }
    }
}

/// Context passed to RPC callbacks for LOOKUP operations.
///
/// `completed` is a `Cell` because the poll loop reads it while the
/// libnfs callback (same thread, inside `rpc_service`) writes it — the
/// Cell makes that access pattern well-defined without pretending the
/// flag is immutable.
struct LookupCallbackData {
    completed: Cell<bool>,
    status: i32,
    fh_len: usize,
    fh_data: [u8; 128], // NFS3 max file handle is 64 bytes, but use 128 for safety
}

/// Callback for LOOKUP RPC
unsafe extern "C" fn lookup_callback(
    _rpc: *mut ffi::rpc_context,
    status: ::std::os::raw::c_int,
    data: *mut ::std::os::raw::c_void,
    private_data: *mut ::std::os::raw::c_void,
) {
    let cb_data = &mut *(private_data as *mut LookupCallbackData);
    cb_data.completed.set(true);
    cb_data.status = status;

    if status == ffi::RPC_STATUS_SUCCESS as i32 {
        let res = &*(data as *const ffi::LOOKUP3res);
        if res.status == 0 {
            // NFS3_OK
            let fh = &res.LOOKUP3res_u.resok.object;
            let len = fh.data.data_len as usize;
            if len <= cb_data.fh_data.len() {
                cb_data.fh_len = len;
                std::ptr::copy_nonoverlapping(
                    fh.data.data_val as *const u8,
                    cb_data.fh_data.as_mut_ptr(),
                    len,
                );
            }
        } else {
            cb_data.status = -(res.status as i32);
        }
    }
}

struct ReaddirplusFullData {
    /// See `LookupCallbackData::completed` for why this is a `Cell`.
    completed: Cell<bool>,
    status: i32,
    eof: bool,
    cookie: u64,
    cookieverf: [i8; 8],
    entries: Vec<NfsDirEntry>,
}

/// Result drained from a completed `InflightReaddir` slot.
///
/// `next_cookie`/`next_cookieverf` are valid only when `eof` is false;
/// the caller passes them back into `submit_readdirplus_by_fh` to fetch
/// the next page of the same directory. When `eof` is true the
/// directory is fully read.
#[derive(Default)]
pub struct ReaddirplusResult {
    pub entries: Vec<NfsDirEntry>,
    pub eof: bool,
    pub next_cookie: u64,
    pub next_cookieverf: [i8; 8],
    pub status: i32,
}

/// Heap-pinned per-PDU state for a READDIRPLUS RPC submitted via
/// [`NfsConnection::submit_readdirplus_by_fh`].
///
/// Memory-safety contract (see `docs/PIPELINED_READDIRPLUS_DESIGN.md` §5):
///
/// 1. `cb_data` is a `Box`, so its address is heap-pinned across moves
///    of the `InflightReaddir` itself. Never `mem::swap` / `mem::replace`
///    the inner box.
/// 2. `InflightReaddir` is `!Send`. The libnfs callback writes into
///    `cb_data` from whichever thread calls `rpc_service`; we always
///    drive that from the worker thread that owns the `NfsConnection`.
/// 3. The `Vec<InflightReaddir>` owned by a worker MUST be dropped
///    before its `NfsConnection`. Stack-frame drop order in
///    `worker_loop_pipelined` satisfies this automatically; do not
///    stash slots in a struct that outlives the connection.
///
/// Compile-time assertion that the `!Send` invariant holds — if the
/// PhantomData marker is ever removed, this doc-test will start
/// passing the compile step and `cargo test` will fail it:
///
/// ```compile_fail
/// use nfs_walker::nfs::connection::InflightReaddir;
/// fn requires_send<T: Send>() {}
/// requires_send::<InflightReaddir>();
/// ```
pub struct InflightReaddir {
    cb_data: Box<ReaddirplusFullData>,
    /// Worker-supplied tag (opaque here). Pipelined worker uses this
    /// to map a completion back to its `DirState` slot.
    pub tag: u64,
    // !Send marker. cb_data is mutated from the libnfs callback running
    // on whichever thread calls rpc_service. We bind that thread to the
    // worker that owns the NfsConnection.
    _not_send: std::marker::PhantomData<*const ()>,
}

impl InflightReaddir {
    /// Has the libnfs callback fired for this slot?
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.cb_data.completed.get()
    }

    /// Raw RPC status from the callback. Meaningful once
    /// `is_completed()` is true.
    #[inline]
    pub fn status(&self) -> i32 {
        self.cb_data.status
    }

    /// Drain the result. Caller invokes after `is_completed()` returns
    /// true. Idempotent: calling again returns an empty
    /// `ReaddirplusResult` with the original status preserved.
    pub fn take_result(&mut self) -> ReaddirplusResult {
        ReaddirplusResult {
            entries: std::mem::take(&mut self.cb_data.entries),
            eof: self.cb_data.eof,
            next_cookie: self.cb_data.cookie,
            next_cookieverf: self.cb_data.cookieverf,
            status: self.cb_data.status,
        }
    }
}

/// Callback for READDIRPLUS RPC (full entry collection with file handles)
///
/// This callback extracts complete NfsDirEntry structs including file handles
/// from the raw READDIRPLUS response, enabling cache-based directory access.
unsafe extern "C" fn readdirplus_full_callback(
    _rpc: *mut ffi::rpc_context,
    status: ::std::os::raw::c_int,
    data: *mut ::std::os::raw::c_void,
    private_data: *mut ::std::os::raw::c_void,
) {
    let cb_data = &mut *(private_data as *mut ReaddirplusFullData);
    cb_data.completed.set(true);
    cb_data.status = status;

    if status == ffi::RPC_STATUS_SUCCESS as i32 {
        let res = &*(data as *const ffi::READDIRPLUS3res);
        if res.status == 0 {
            // NFS3_OK
            let resok = &res.READDIRPLUS3res_u.resok;
            cb_data.eof = resok.reply.eof != 0;

            // Copy cookieverf for next call
            cb_data.cookieverf.copy_from_slice(&resok.cookieverf);

            // Collect entries with full attributes and file handles.
            //
            // libnfs XDR-decodes the entry list into 4-byte-aligned
            // arena memory, but entryplus3 contains u64 fields (fileid,
            // cookie) that make Rust demand 8-byte alignment — `&*ptr`
            // here is UB and panics under debug's misaligned-deref
            // check. Copy each node out with read_unaligned; interior
            // pointers (name, handle data, nextentry) stay valid, they
            // just point back into the arena.
            let mut entry_ptr = resok.reply.entries;
            while !entry_ptr.is_null() {
                let entry = std::ptr::read_unaligned(entry_ptr);
                cb_data.cookie = entry.cookie;

                // Get entry name
                let name = if entry.name.is_null() {
                    String::new()
                } else {
                    CStr::from_ptr(entry.name).to_string_lossy().into_owned()
                };

                // Skip . and ..
                if name != "." && name != ".." {
                    // Extract file type and attributes
                    let (entry_type, stat) = if entry.name_attributes.attributes_follow != 0 {
                        let attrs = &entry.name_attributes.post_op_attr_u.attributes;
                        let et = match attrs.type_ {
                            1 => EntryType::File,      // NF3REG
                            2 => EntryType::Directory, // NF3DIR
                            5 => EntryType::Symlink,   // NF3LNK
                            3 => EntryType::BlockDevice, // NF3BLK
                            4 => EntryType::CharDevice,  // NF3CHR
                            6 => EntryType::Socket,    // NF3SOCK
                            7 => EntryType::Fifo,      // NF3FIFO
                            _ => EntryType::Unknown,
                        };
                        let s = NfsStat {
                            size: attrs.size,
                            inode: attrs.fileid,
                            nlink: attrs.nlink as u64,
                            uid: attrs.uid,
                            gid: attrs.gid,
                            mode: attrs.mode,
                            mtime_sec: Some(attrs.mtime.seconds as i64),
                            mtime_nsec: Some(attrs.mtime.nseconds as i32),
                            atime_sec: Some(attrs.atime.seconds as i64),
                            atime_nsec: Some(attrs.atime.nseconds as i32),
                            ctime_sec: Some(attrs.ctime.seconds as i64),
                            ctime_nsec: Some(attrs.ctime.nseconds as i32),
                            blocks: attrs.used.div_ceil(512), // Convert used bytes to 512-byte blocks
                        };
                        (et, Some(s))
                    } else {
                        (EntryType::Unknown, None)
                    };

                    // Extract file handle (for directories, to enable cached access)
                    let file_handle = if entry.name_handle.handle_follows != 0 {
                        let fh = &entry.name_handle.post_op_fh3_u.handle;
                        let len = fh.data.data_len as usize;
                        if len > 0 && len <= 128 {
                            let mut fh_data = vec![0u8; len];
                            std::ptr::copy_nonoverlapping(
                                fh.data.data_val as *const u8,
                                fh_data.as_mut_ptr(),
                                len,
                            );
                            Some(fh_data)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    cb_data.entries.push(NfsDirEntry {
                        name,
                        entry_type,
                        stat,
                        inode: entry.fileid,
                        file_handle,
                    });
                }

                entry_ptr = entry.nextentry;
            }
        } else {
            cb_data.status = -(res.status as i32);
        }
    }
}

/// Wait for an RPC operation to complete by polling
///
/// `completed` points at the callback-data's flag; the RPC callback sets
/// it from inside `rpc_service` on this same thread. This function keeps
/// polling until either the callback fires or the timeout elapses.
///
/// IMPORTANT: on `Err`, the PDU may still be queued inside libnfs with a
/// pointer to the caller's callback data. Callers must poison the
/// connection (see `NfsConnection::poison`) before letting that data go
/// out of scope.
///
/// # Safety
/// The `completed` pointer must remain valid for the duration of this call.
fn wait_for_rpc_completion(
    rpc: *mut ffi::rpc_context,
    completed: *const Cell<bool>,
    timeout_ms: i32,
) -> Result<(), String> {
    use std::os::unix::io::RawFd;

    let fd: RawFd = unsafe { ffi::rpc_get_fd(rpc) };
    if fd < 0 {
        return Err("Invalid RPC fd".to_string());
    }

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms as u64);
    let mut iteration = 0u32;

    while !unsafe { (*completed).get() } {
        if start.elapsed() > timeout {
            tracing::debug!(
                "RPC timeout after {} iterations, fd={}, elapsed={:?}",
                iteration, fd, start.elapsed()
            );
            return Err("RPC timeout".to_string());
        }

        let events = unsafe { ffi::rpc_which_events(rpc) };

        // Log first few iterations for debugging
        if iteration < 5 {
            tracing::debug!(
                "RPC wait iter={}, events={:#x} (POLLIN={}, POLLOUT={})",
                iteration,
                events,
                events & libc::POLLIN as i32,
                events & libc::POLLOUT as i32
            );
        }

        // If no events needed and not completed, something is wrong
        if events == 0 {
            // Try servicing with 0 events to process any internal state
            let service_ret = unsafe { ffi::rpc_service(rpc, 0) };
            if service_ret < 0 {
                return Err("rpc_service failed (no events)".to_string());
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
            iteration += 1;
            continue;
        }

        let mut pfd = libc::pollfd {
            fd,
            events: events as i16,
            revents: 0,
        };

        let poll_timeout = 100; // 100ms poll intervals
        let ret = unsafe { libc::poll(&mut pfd, 1, poll_timeout) };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue; // EINTR - retry
            }
            return Err(format!("poll failed: {}", err));
        }

        if ret > 0 {
            if iteration < 5 {
                tracing::debug!(
                    "RPC poll returned: ret={}, revents={:#x}",
                    ret, pfd.revents
                );
            }
            // Process whatever events we got
            let revents = if pfd.revents != 0 { pfd.revents as i32 } else { events };
            let service_ret = unsafe { ffi::rpc_service(rpc, revents) };
            if service_ret < 0 {
                tracing::debug!("rpc_service returned {}", service_ret);
                return Err(format!("rpc_service failed: {}", service_ret));
            }
        }

        iteration += 1;
    }

    tracing::debug!("RPC completed after {} iterations", iteration);
    Ok(())
}

/// Process any pending events on the RPC context to ensure it's in a clean state
fn drain_pending_events(rpc: *mut ffi::rpc_context) {
    use std::os::unix::io::RawFd;

    let fd: RawFd = unsafe { ffi::rpc_get_fd(rpc) };
    if fd < 0 {
        return;
    }

    // Do a few rounds of non-blocking poll/service to drain any pending state
    for _ in 0..3 {
        let events = unsafe { ffi::rpc_which_events(rpc) };
        if events == 0 {
            break;
        }

        let mut pfd = libc::pollfd {
            fd,
            events: events as i16,
            revents: 0,
        };

        // Non-blocking poll
        let ret = unsafe { libc::poll(&mut pfd, 1, 0) };
        if ret > 0 && pfd.revents != 0 {
            unsafe { ffi::rpc_service(rpc, pfd.revents as i32) };
        } else {
            break;
        }
    }
}


/// Builder for NFS connections with retry support
pub struct NfsConnectionBuilder {
    url: NfsUrl,
    timeout: Duration,
    retries: u32,
    /// Override server with specific IP (for DNS round-robin)
    override_ip: Option<String>,
}

impl NfsConnectionBuilder {
    /// Create a new builder
    pub fn new(url: NfsUrl) -> Self {
        Self {
            url,
            timeout: Duration::from_secs(30),
            retries: 3,
            override_ip: None,
        }
    }

    /// Override the server hostname with a specific IP address
    /// Used for DNS round-robin load balancing
    pub fn with_ip(mut self, ip: String) -> Self {
        self.override_ip = Some(ip);
        self
    }

    /// Set connection timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set retry count
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Build and connect with retries
    pub fn connect(self) -> NfsResult<NfsConnection> {
        let mut last_error = None;

        // Use override IP if provided, otherwise use URL server
        let url = if let Some(ip) = self.override_ip {
            NfsUrl {
                server: ip,
                port: self.url.port,
                export: self.url.export.clone(),
                subpath: self.url.subpath.clone(),
            }
        } else {
            self.url.clone()
        };

        for attempt in 0..=self.retries {
            if attempt > 0 {
                // Exponential backoff: 100ms, 200ms, 400ms, ...
                let delay = Duration::from_millis(100 * (1 << (attempt - 1)));
                std::thread::sleep(delay);
            }

            match NfsConnection::new(&url) {
                Ok(mut conn) => match conn.connect(self.timeout) {
                    Ok(()) => return Ok(conn),
                    Err(e) => {
                        last_error = Some(e);
                    }
                },
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| NfsError::ConnectionFailed {
            server: url.server,
            reason: "Connection failed after all retries".into(),
        }))
    }
}

/// Resolve a hostname to all its IP addresses
///
/// Returns a list of IP addresses for DNS round-robin load balancing.
/// Makes repeated queries via the `host` command to bypass system
/// resolver caching and catch rotating DNS servers, stopping early once
/// two consecutive queries add nothing new. Note: when the local
/// resolver caches upstream answers, rotation is invisible no matter
/// how many attempts run — `--server-ips` is the reliable escape hatch.
/// If resolution fails entirely, returns the hostname as the only entry.
pub fn resolve_dns(hostname: &str) -> Vec<String> {
    resolve_dns_with_attempts(hostname, 6)
}

/// Resolve DNS with a bounded number of attempts to catch rotating IPs.
pub fn resolve_dns_with_attempts(hostname: &str, attempts: usize) -> Vec<String> {
    use std::collections::HashSet;
    use std::process::Command;

    let mut all_ips = HashSet::new();
    let mut stale_rounds = 0;

    // Each `host` invocation does a fresh DNS query (unlike getaddrinfo,
    // which may serve the nscd/systemd-resolved cache).
    for _ in 0..attempts {
        let before = all_ips.len();
        if let Ok(output) = Command::new("host")
            .arg(hostname)
            .output()
        {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    // Parse lines like: "hostname has address 1.2.3.4"
                    if line.contains("has address") {
                        if let Some(ip) = line.split_whitespace().last() {
                            all_ips.insert(ip.to_string());
                        }
                    }
                }
            }
        }
        if all_ips.len() == before {
            stale_rounds += 1;
            if stale_rounds >= 2 && !all_ips.is_empty() {
                break;
            }
        } else {
            stale_rounds = 0;
        }
    }

    if all_ips.is_empty() {
        // Fallback to system resolver if `host` command fails
        use std::net::ToSocketAddrs;
        let addr_str = format!("{}:0", hostname);
        if let Ok(addrs) = addr_str.to_socket_addrs() {
            for addr in addrs {
                let ip = match addr {
                    std::net::SocketAddr::V4(v4) => v4.ip().to_string(),
                    std::net::SocketAddr::V6(v6) => v6.ip().to_string(),
                };
                all_ips.insert(ip);
            }
        }
    }

    if all_ips.is_empty() {
        // Resolution failed, return hostname as-is (libnfs will resolve it)
        vec![hostname.to_string()]
    } else {
        // Sort for deterministic ordering
        let mut ips: Vec<String> = all_ips.into_iter().collect();
        ips.sort();
        ips
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Most tests require an actual NFS server
    // These are unit tests for the non-FFI parts

    #[test]
    fn test_nfs_url_to_connection() {
        // This test would fail without libnfs installed
        // It's here to document the expected API
        let url = NfsUrl {
            server: "localhost".into(),
            port: None,
            export: "/test".into(),
            subpath: String::new(),
        };

        // Just verify the builder compiles
        let _builder = NfsConnectionBuilder::new(url)
            .timeout(Duration::from_secs(10))
            .retries(2);
    }

    // ========================================================
    // Pipelined READDIRPLUS unit tests
    //
    // The non-NFS-server tests run on every `cargo test` invocation.
    // The NFS-server tests are gated behind the `NFS_TEST_URL` env var
    // (e.g. `NFS_TEST_URL=nfs://localhost/export cargo test --
    // pipelined --ignored`) and are `#[ignore]` so they don't block
    // CI when no loopback NFS is available.
    // ========================================================

    /// Helper for static-only tests: construct an InflightReaddir whose
    /// cb_data has been pre-filled to a "completed" state. We never
    /// hand the pointer to libnfs, so this is a safe simulation of the
    /// post-callback state.
    fn fake_completed_inflight(
        eof: bool,
        status: i32,
        cookie: u64,
        n_entries: usize,
    ) -> InflightReaddir {
        let entries = (0..n_entries)
            .map(|i| NfsDirEntry {
                name: format!("entry-{i}"),
                entry_type: EntryType::File,
                stat: None,
                inode: i as u64,
                file_handle: None,
            })
            .collect();
        InflightReaddir {
            cb_data: Box::new(ReaddirplusFullData {
                completed: Cell::new(true),
                status,
                eof,
                cookie,
                cookieverf: [0i8; 8],
                entries,
            }),
            tag: 0xDEAD_BEEF,
            _not_send: std::marker::PhantomData,
        }
    }

    #[test]
    fn pipelined_take_result_drains_and_is_idempotent() {
        let mut slot = fake_completed_inflight(false, 0, 12345, 7);
        assert!(slot.is_completed());
        assert_eq!(slot.status(), 0);

        let r1 = slot.take_result();
        assert_eq!(r1.entries.len(), 7);
        assert!(!r1.eof);
        assert_eq!(r1.next_cookie, 12345);
        assert_eq!(r1.status, 0);

        // Second call: cb_data was drained, but completed/eof/cookie
        // are still readable. take_result returns an empty entries vec
        // and the same cookie/status.
        let r2 = slot.take_result();
        assert_eq!(r2.entries.len(), 0);
        assert!(!r2.eof);
        assert_eq!(r2.next_cookie, 12345);
        assert_eq!(r2.status, 0);
    }


    #[test]
    fn pipelined_readdirplus_result_default_is_sane() {
        let r = ReaddirplusResult::default();
        assert!(r.entries.is_empty());
        assert!(!r.eof);
        assert_eq!(r.next_cookie, 0);
        assert_eq!(r.next_cookieverf, [0i8; 8]);
        assert_eq!(r.status, 0);
    }

    // ----- NFS-server-backed tests (env-var gated) -----

    fn test_nfs_url() -> Option<NfsUrl> {
        let raw = std::env::var("NFS_TEST_URL").ok()?;
        NfsUrl::parse(&raw).ok()
    }

    /// Connect (mount) to whatever loopback / staging server the env
    /// var points at. Returns None if the env var is unset, letting
    /// the caller skip cleanly.
    fn connect_test_nfs() -> Option<NfsConnection> {
        let url = test_nfs_url()?;
        NfsConnectionBuilder::new(url)
            .timeout(Duration::from_secs(10))
            .retries(1)
            .connect()
            .ok()
    }

    #[test]
    #[ignore = "requires NFS_TEST_URL=nfs://host/export"]
    fn pipelined_submit_two_completes_both() {
        let nfs = match connect_test_nfs() {
            Some(n) => n,
            None => {
                eprintln!("skip: NFS_TEST_URL not set or unreachable");
                return;
            }
        };

        let root_fh = nfs
            .resolve_path_to_fh("/")
            .expect("resolve / failed");

        let s1 = nfs
            .submit_readdirplus_by_fh(&root_fh, 0, [0i8; 8], 1)
            .expect("submit 1");
        let s2 = nfs
            .submit_readdirplus_by_fh(&root_fh, 0, [0i8; 8], 2)
            .expect("submit 2");

        let slots = vec![s1, s2];
        let n = nfs.pump(&slots, 2, 30_000).expect("pump");
        assert!(n >= 2, "expected at least 2 completions, got {n}");
        assert!(slots[0].is_completed());
        assert!(slots[1].is_completed());
    }

    #[test]
    #[ignore = "requires NFS_TEST_URL=nfs://host/export"]
    fn pipelined_drop_with_inflight_does_not_segfault() {
        let nfs = match connect_test_nfs() {
            Some(n) => n,
            None => {
                eprintln!("skip: NFS_TEST_URL not set or unreachable");
                return;
            }
        };

        let root_fh = nfs.resolve_path_to_fh("/").expect("resolve");

        // Submit several RPCs and drop the slots vec (and then the
        // connection) without pumping. The Box<ReaddirplusFullData>
        // outlives the libnfs-cancellation triggered by
        // nfs_destroy_context — that's the lifetime contract we want
        // to validate.
        {
            let mut slots = Vec::new();
            for i in 0..4 {
                if let Ok(s) = nfs.submit_readdirplus_by_fh(&root_fh, 0, [0i8; 8], i) {
                    slots.push(s);
                }
            }
            // slots dropped here BEFORE nfs (stack-frame order)
        }
        // nfs dropped here. If the design is correct, no segfault and
        // no leak. (Run under valgrind/asan for a stronger check.)
    }

    #[test]
    #[ignore = "requires NFS_TEST_URL=nfs://host/export pointing at a dir with >5000 entries"]
    fn pipelined_cookie_chain_advances_correctly() {
        let nfs = match connect_test_nfs() {
            Some(n) => n,
            None => {
                eprintln!("skip: NFS_TEST_URL not set or unreachable");
                return;
            }
        };

        let dir = std::env::var("NFS_TEST_BIG_DIR").unwrap_or_else(|_| "/".into());
        let dir_fh = nfs
            .resolve_path_to_fh(&dir)
            .expect("resolve big dir");

        let mut cookie = 0u64;
        let mut cookieverf = [0i8; 8];
        let mut total = 0usize;
        let mut iters = 0u32;
        loop {
            let mut slot = nfs
                .submit_readdirplus_by_fh(&dir_fh, cookie, cookieverf, iters as u64)
                .expect("submit page");
            let n = nfs
                .pump(std::slice::from_ref(&slot), 1, 30_000)
                .expect("pump page");
            assert!(n >= 1, "page did not complete");
            let result = slot.take_result();
            assert_eq!(
                result.status,
                ffi::RPC_STATUS_SUCCESS as i32,
                "READDIRPLUS page failed: status={}",
                result.status
            );
            total += result.entries.len();
            if result.eof {
                break;
            }
            cookie = result.next_cookie;
            cookieverf = result.next_cookieverf;
            iters += 1;
            if iters > 10_000 {
                panic!("cookie chain did not terminate after 10k pages");
            }
        }
        eprintln!("walked {total} entries in {iters} pages");
    }
}
