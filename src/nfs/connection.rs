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

    /// Whether we're currently mounted
    mounted: bool,

    /// RPC timeout in milliseconds (used for wait_for_rpc_completion)
    rpc_timeout_ms: i32,
}

// NfsConnection can be sent between threads but not shared
// Each worker thread should have its own connection
unsafe impl Send for NfsConnection {}
// NOT implementing Sync - libnfs is not thread-safe

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
            mounted: false,
            rpc_timeout_ms: 30000, // Default 30 seconds, updated by connect()
        })
    }

    /// Set READDIR buffer size before connecting
    ///
    /// Must be called before `connect()`. Larger buffers reduce RPC round-trips
    /// for directory listings. Default is 8KB, max is 4MB.
    ///
    /// Note: Some servers may not handle large buffers well. Test with your server.
    pub fn set_readdir_buffer_size(&self, size: u32) {
        unsafe {
            ffi::nfs_set_readdir_max_buffer_size(self.context, size, size);
        }
    }

    /// Connect and mount the NFS export
    pub fn connect(&mut self, timeout: Duration) -> NfsResult<()> {
        if self.mounted {
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

        self.mounted = true;
        Ok(())
    }

    /// Create a connected NFS connection in one step
    pub fn connect_to(url: &NfsUrl, timeout: Duration) -> NfsResult<Self> {
        let mut conn = Self::new(url)?;
        conn.connect(timeout)?;
        Ok(conn)
    }

    /// Read directory entries using READDIRPLUS for efficiency
    ///
    /// This returns all entries in the directory with their attributes,
    /// avoiding separate stat calls for each entry.
    pub fn readdir_plus(&self, path: &str) -> NfsResult<Vec<NfsDirEntry>> {
        if !self.mounted {
            return Err(NfsError::ReadDirFailed {
                path: path.into(),
                reason: "Not mounted".into(),
            });
        }

        let path_cstr = CString::new(path).map_err(|_| NfsError::ReadDirFailed {
            path: path.into(),
            reason: "Path contains null bytes".into(),
        })?;

        // Open directory
        let mut dir_handle: *mut ffi::nfsdir = ptr::null_mut();
        let result = unsafe {
            ffi::nfs_opendir(self.context, path_cstr.as_ptr(), &mut dir_handle)
        };

        if result != 0 {
            return Err(self.translate_error(path, result));
        }

        // Read all entries
        let mut entries = Vec::new();

        loop {
            let dirent = unsafe { ffi::nfs_readdir(self.context, dir_handle) };

            if dirent.is_null() {
                break;
            }

            // Safety: dirent is valid until next readdir call or closedir
            let entry = unsafe { self.convert_dirent(dirent) };
            entries.push(entry);
        }

        // Close directory
        unsafe {
            ffi::nfs_closedir(self.context, dir_handle);
        }

        Ok(entries)
    }

    /// Read directory entries in chunks, calling a callback for each chunk
    ///
    /// This is more memory-efficient for large directories and allows
    /// distributing work across workers as entries are read.
    ///
    /// Returns the total number of entries processed.
    pub fn readdir_plus_chunked<F>(
        &self,
        path: &str,
        chunk_size: usize,
        mut callback: F,
    ) -> NfsResult<usize>
    where
        F: FnMut(Vec<NfsDirEntry>) -> bool, // Return false to stop early
    {
        if !self.mounted {
            return Err(NfsError::ReadDirFailed {
                path: path.into(),
                reason: "Not mounted".into(),
            });
        }

        let path_cstr = CString::new(path).map_err(|_| NfsError::ReadDirFailed {
            path: path.into(),
            reason: "Path contains null bytes".into(),
        })?;

        // Open directory
        let mut dir_handle: *mut ffi::nfsdir = ptr::null_mut();
        let open_start = std::time::Instant::now();
        let result = unsafe {
            ffi::nfs_opendir(self.context, path_cstr.as_ptr(), &mut dir_handle)
        };
        let open_elapsed = open_start.elapsed();

        if result != 0 {
            return Err(self.translate_error(path, result));
        }

        // Log if opendir was slow (> 100ms) - debug level so it only shows with -v
        if open_elapsed.as_millis() > 100 {
            tracing::debug!("nfs_opendir({}) took {:?}", path, open_elapsed);
        }

        let mut total_entries = 0;
        let mut chunk = Vec::with_capacity(chunk_size);
        let mut first_read = true;
        let read_start = std::time::Instant::now();

        loop {
            let dirent = unsafe { ffi::nfs_readdir(self.context, dir_handle) };

            // Log if first readdir was slow - debug level so it only shows with -v
            if first_read {
                let first_read_elapsed = read_start.elapsed();
                if first_read_elapsed.as_millis() > 100 {
                    tracing::debug!("first nfs_readdir({}) took {:?}", path, first_read_elapsed);
                }
                first_read = false;
            }

            if dirent.is_null() {
                // End of directory - send final chunk if any
                if !chunk.is_empty() {
                    total_entries += chunk.len();
                    callback(chunk);
                }
                break;
            }

            // Safety: dirent is valid until next readdir call or closedir
            let entry = unsafe { self.convert_dirent(dirent) };
            chunk.push(entry);

            // When chunk is full, send it to callback
            if chunk.len() >= chunk_size {
                total_entries += chunk.len();
                let continue_reading = callback(chunk);
                chunk = Vec::with_capacity(chunk_size);

                if !continue_reading {
                    break;
                }
            }
        }

        // Close directory
        unsafe {
            ffi::nfs_closedir(self.context, dir_handle);
        }

        Ok(total_entries)
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
        if !self.mounted {
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
                completed: false,
                status: 0,
                fh_len: 0,
                fh_data: [0; 128],
            };

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
                    &mut cb_data as *mut _ as *mut std::ffi::c_void,
                )
            };

            if pdu.is_null() {
                return Err(NfsError::ReadDirFailed {
                    path: path.into(),
                    reason: format!("Failed to queue LOOKUP for '{}'", component),
                });
            }

            if let Err(e) = wait_for_rpc_completion(rpc, &cb_data.completed, self.rpc_timeout_ms) {
                return Err(NfsError::ReadDirFailed {
                    path: path.into(),
                    reason: format!("LOOKUP '{}' failed: {}", component, e),
                });
            }

            if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
                return Err(NfsError::ReadDirFailed {
                    path: path.into(),
                    reason: format!("LOOKUP '{}' error: status={}", component, cb_data.status),
                });
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
        if !self.mounted {
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

        // Use reasonably large buffer sizes for efficiency
        let dircount: u32 = 65536;  // 64KB
        let maxcount: u32 = 131072; // 128KB

        loop {
            let mut cb_data = ReaddirplusFullData {
                completed: false,
                status: 0,
                eof: false,
                cookie: 0,
                cookieverf: [0i8; 8],
                entries: Vec::with_capacity(chunk_size),
            };

            // Build READDIRPLUS args with the cached file handle
            let mut args: ffi::READDIRPLUS3args = unsafe { std::mem::zeroed() };
            args.dir.data.data_len = file_handle.len() as u32;
            args.dir.data.data_val = file_handle.as_ptr() as *mut i8;
            args.cookie = cookie;
            args.cookieverf = cookieverf;
            args.dircount = dircount;
            args.maxcount = maxcount;

            let pdu = unsafe {
                ffi::rpc_nfs3_readdirplus_task(
                    rpc,
                    Some(readdirplus_full_callback),
                    &mut args,
                    &mut cb_data as *mut _ as *mut std::ffi::c_void,
                )
            };

            if pdu.is_null() {
                return Err(NfsError::ReadDirFailed {
                    path: "(by file handle)".into(),
                    reason: "Failed to queue READDIRPLUS RPC".into(),
                });
            }

            // Wait for completion
            if let Err(e) = wait_for_rpc_completion(rpc, &cb_data.completed, self.rpc_timeout_ms) {
                return Err(NfsError::ReadDirFailed {
                    path: "(by file handle)".into(),
                    reason: format!("READDIRPLUS failed: {}", e),
                });
            }

            if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
                return Err(NfsError::ReadDirFailed {
                    path: "(by file handle)".into(),
                    reason: format!("READDIRPLUS error: status={}", cb_data.status),
                });
            }

            total_entries += cb_data.entries.len();

            // Send entries to callback in chunks
            let mut chunk = Vec::with_capacity(chunk_size);
            for entry in cb_data.entries {
                chunk.push(entry);
                if chunk.len() >= chunk_size {
                    if !callback(std::mem::replace(&mut chunk, Vec::with_capacity(chunk_size))) {
                        return Ok(total_entries);
                    }
                }
            }
            // Send remaining entries
            if !chunk.is_empty() {
                if !callback(chunk) {
                    return Ok(total_entries);
                }
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

    /// Open a directory for manual iteration with seek support
    ///
    /// This allows for cookie-based parallel reading:
    /// 1. Open directory
    /// 2. Read entries, periodically calling telldir() to get position
    /// 3. Later, seekdir() to jump to a saved position
    pub fn opendir(&self, path: &str) -> NfsResult<NfsDirHandle> {
        if !self.mounted {
            return Err(NfsError::ReadDirFailed {
                path: path.into(),
                reason: "Not mounted".into(),
            });
        }

        let path_cstr = CString::new(path).map_err(|_| NfsError::ReadDirFailed {
            path: path.into(),
            reason: "Path contains null bytes".into(),
        })?;

        let mut dir_handle: *mut ffi::nfsdir = ptr::null_mut();
        let result = unsafe {
            ffi::nfs_opendir(self.context, path_cstr.as_ptr(), &mut dir_handle)
        };

        if result != 0 {
            return Err(self.translate_error(path, result));
        }

        Ok(NfsDirHandle {
            context: self.context,
            handle: dir_handle,
            path: path.to_string(),
        })
    }

    /// Stat a single path
    pub fn stat(&self, path: &str) -> NfsResult<NfsStat> {
        if !self.mounted {
            return Err(NfsError::StatFailed {
                path: path.into(),
                reason: "Not mounted".into(),
            });
        }

        let path_cstr = CString::new(path).map_err(|_| NfsError::StatFailed {
            path: path.into(),
            reason: "Path contains null bytes".into(),
        })?;

        let mut stat: ffi::nfs_stat_64 = unsafe { std::mem::zeroed() };

        let result = unsafe {
            ffi::nfs_stat64(self.context, path_cstr.as_ptr(), &mut stat)
        };

        if result != 0 {
            return Err(self.translate_error(path, result));
        }

        Ok(self.convert_stat(&stat))
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

    /// Translate an NFS error code to our error type
    fn translate_error(&self, path: &str, code: i32) -> NfsError {
        let error_msg = self.get_error();

        // Common NFS error codes (from nfsc/libnfs-raw-nfs.h)
        match code {
            -13 => NfsError::PermissionDenied { path: path.into() }, // EACCES
            -2 => NfsError::NotFound { path: path.into() },         // ENOENT
            -20 => NfsError::NotFound { path: path.into() },        // ENOTDIR
            -70 => NfsError::StaleHandle { path: path.into() },     // ESTALE
            _ => NfsError::Protocol {
                code,
                message: error_msg,
            },
        }
    }

    /// Convert a libnfs dirent to our NfsDirEntry
    ///
    /// Safety: dirent must be valid
    unsafe fn convert_dirent(&self, dirent: *mut ffi::nfsdirent) -> NfsDirEntry {
        let d = &*dirent;

        // Get the name
        let name = if d.name.is_null() {
            String::new()
        } else {
            CStr::from_ptr(d.name).to_string_lossy().into_owned()
        };

        // Determine entry type
        let entry_type = EntryType::from_mode(d.mode);

        // Build stat if we have full attributes
        let stat = Some(NfsStat {
            size: d.size,
            inode: d.inode,
            nlink: d.nlink as u64,
            uid: d.uid,
            gid: d.gid,
            mode: d.mode,
            atime: Some(d.atime.tv_sec as i64),
            mtime: Some(d.mtime.tv_sec as i64),
            ctime: Some(d.ctime.tv_sec as i64),
            blksize: d.blksize,
            blocks: d.blocks,
        });

        NfsDirEntry {
            name,
            entry_type,
            stat,
            inode: d.inode,
            file_handle: None, // libnfs high-level API doesn't expose file handles
        }
    }

    /// Convert a libnfs stat structure to our NfsStat
    fn convert_stat(&self, stat: &ffi::nfs_stat_64) -> NfsStat {
        NfsStat {
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
        }
    }

    /// Get the server name
    pub fn server(&self) -> &str {
        &self.server
    }

    /// Get the export path
    pub fn export(&self) -> &str {
        &self.export
    }

    /// Check if we're connected
    pub fn is_connected(&self) -> bool {
        self.mounted
    }

    /// Get raw NFS context pointer for async operations
    ///
    /// # Safety
    /// The returned pointer is only valid while this NfsConnection exists.
    /// The caller must not use this pointer after the connection is dropped.
    /// The caller must not call any functions that would invalidate the context.
    #[allow(dead_code)]
    pub unsafe fn raw_context(&self) -> *mut ffi::nfs_context {
        self.context
    }
}

impl Drop for NfsConnection {
    fn drop(&mut self) {
        if !self.context.is_null() {
            if self.mounted {
                unsafe {
                    ffi::nfs_umount(self.context);
                }
                self.mounted = false;
            }

            unsafe {
                ffi::nfs_destroy_context(self.context);
            }
            self.context = ptr::null_mut();
        }
    }
}

/// Handle to an open NFS directory for sequential or parallel reading
///
/// Provides access to telldir/seekdir for cookie-based positioning.
/// The directory is automatically closed when the handle is dropped.
pub struct NfsDirHandle {
    context: *mut ffi::nfs_context,
    handle: *mut ffi::nfsdir,
    path: String,
}

// NfsDirHandle must be used with the same NfsConnection that created it
// It's Send because NfsConnection is Send
unsafe impl Send for NfsDirHandle {}

impl NfsDirHandle {
    /// Get the current position (cookie) in the directory
    ///
    /// This can be saved and later used with seekdir() to resume reading
    /// from this position, potentially from a different connection.
    pub fn telldir(&self) -> i64 {
        unsafe { ffi::nfs_telldir(self.context, self.handle) as i64 }
    }

    /// Seek to a previously saved position (cookie)
    ///
    /// After seeking, the next readdir() call will return entries
    /// starting from that position.
    pub fn seekdir(&self, cookie: i64) {
        unsafe { ffi::nfs_seekdir(self.context, self.handle, cookie as std::ffi::c_long) }
    }

    /// Rewind to the beginning of the directory
    pub fn rewinddir(&self) {
        unsafe { ffi::nfs_rewinddir(self.context, self.handle) }
    }

    /// Read the next directory entry
    ///
    /// Returns None when there are no more entries.
    pub fn readdir(&self) -> Option<NfsDirEntry> {
        let dirent = unsafe { ffi::nfs_readdir(self.context, self.handle) };

        if dirent.is_null() {
            return None;
        }

        // Safety: dirent is valid until next readdir call or closedir
        Some(unsafe { Self::convert_dirent(dirent) })
    }

    /// Read up to `count` entries, returning them with the final cookie position
    ///
    /// Returns (entries, final_cookie) where final_cookie is the position
    /// after the last entry read.
    pub fn read_batch(&self, count: usize) -> (Vec<NfsDirEntry>, i64) {
        let mut entries = Vec::with_capacity(count);

        for _ in 0..count {
            match self.readdir() {
                Some(entry) => entries.push(entry),
                None => break,
            }
        }

        let cookie = self.telldir();
        (entries, cookie)
    }

    /// Get the directory path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Convert a libnfs dirent to our NfsDirEntry (same as NfsConnection::convert_dirent)
    unsafe fn convert_dirent(dirent: *mut ffi::nfsdirent) -> NfsDirEntry {
        let d = &*dirent;

        let name = if d.name.is_null() {
            String::new()
        } else {
            CStr::from_ptr(d.name).to_string_lossy().into_owned()
        };

        let entry_type = EntryType::from_mode(d.mode);

        let stat = Some(NfsStat {
            size: d.size,
            inode: d.inode,
            nlink: d.nlink as u64,
            uid: d.uid,
            gid: d.gid,
            mode: d.mode,
            atime: Some(d.atime.tv_sec as i64),
            mtime: Some(d.mtime.tv_sec as i64),
            ctime: Some(d.ctime.tv_sec as i64),
            blksize: d.blksize,
            blocks: d.blocks,
        });

        NfsDirEntry {
            name,
            entry_type,
            stat,
            inode: d.inode,
            file_handle: None, // libnfs high-level API doesn't expose file handles
        }
    }
}

impl Drop for NfsDirHandle {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                ffi::nfs_closedir(self.context, self.handle);
            }
            self.handle = ptr::null_mut();
        }
    }
}

/// Result of a big-dir check operation
#[derive(Debug, Clone)]
pub enum BigDirCheckResult {
    /// Directory has at least `threshold` entries
    IsBig { count: u64 },
    /// Directory has fewer than `threshold` entries
    NotBig { total_count: u64 },
    /// Error occurred during check
    Error(String),
}

/// Result of scanning a directory for big-dir-hunt mode
#[derive(Debug, Clone)]
pub struct BigDirScanResult {
    /// Number of non-directory entries (files, symlinks, etc.)
    pub file_count: u64,
    /// Whether we hit the threshold (count may be higher)
    pub threshold_hit: bool,
    /// Names of subdirectories found
    pub subdirs: Vec<String>,
}

/// Entry info collected during READDIRPLUS
struct EntryInfo {
    name: String,
    is_dir: bool,
}

/// Context for READDIRPLUS with entry collection
struct ReaddirplusScanData {
    completed: bool,
    status: i32,
    eof: bool,
    cookie: u64,
    cookieverf: [i8; 8],
    entries: Vec<EntryInfo>,
}

/// Context passed to RPC callbacks for LOOKUP operations
struct LookupCallbackData {
    completed: bool,
    status: i32,
    fh_len: usize,
    fh_data: [u8; 128], // NFS3 max file handle is 64 bytes, but use 128 for safety
}

/// Context passed to RPC callbacks for READDIRPLUS operations
struct ReaddirplusCallbackData {
    completed: bool,
    status: i32,
    entry_count: u64,
    eof: bool,
    cookie: u64,
    cookieverf: [i8; 8], // NFS cookieverf is char[8] which is i8 in Rust FFI
}

/// Callback for LOOKUP RPC
unsafe extern "C" fn lookup_callback(
    _rpc: *mut ffi::rpc_context,
    status: ::std::os::raw::c_int,
    data: *mut ::std::os::raw::c_void,
    private_data: *mut ::std::os::raw::c_void,
) {
    let cb_data = &mut *(private_data as *mut LookupCallbackData);
    cb_data.completed = true;
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

/// Callback for READDIRPLUS RPC (counting only)
unsafe extern "C" fn readdirplus_callback(
    _rpc: *mut ffi::rpc_context,
    status: ::std::os::raw::c_int,
    data: *mut ::std::os::raw::c_void,
    private_data: *mut ::std::os::raw::c_void,
) {
    let cb_data = &mut *(private_data as *mut ReaddirplusCallbackData);
    cb_data.completed = true;
    cb_data.status = status;

    if status == ffi::RPC_STATUS_SUCCESS as i32 {
        let res = &*(data as *const ffi::READDIRPLUS3res);
        if res.status == 0 {
            // NFS3_OK
            let resok = &res.READDIRPLUS3res_u.resok;
            cb_data.eof = resok.reply.eof != 0;

            // Copy cookieverf for next call
            cb_data.cookieverf.copy_from_slice(&resok.cookieverf);

            // Count entries and get last cookie
            let mut entry_ptr = resok.reply.entries;
            while !entry_ptr.is_null() {
                let entry = &*entry_ptr;
                cb_data.entry_count += 1;
                cb_data.cookie = entry.cookie;
                entry_ptr = entry.nextentry;
            }
        } else {
            cb_data.status = -(res.status as i32);
        }
    }
}

/// Callback for READDIRPLUS RPC (scanning with entry collection)
unsafe extern "C" fn readdirplus_scan_callback(
    _rpc: *mut ffi::rpc_context,
    status: ::std::os::raw::c_int,
    data: *mut ::std::os::raw::c_void,
    private_data: *mut ::std::os::raw::c_void,
) {
    let cb_data = &mut *(private_data as *mut ReaddirplusScanData);
    cb_data.completed = true;
    cb_data.status = status;

    if status == ffi::RPC_STATUS_SUCCESS as i32 {
        let res = &*(data as *const ffi::READDIRPLUS3res);
        if res.status == 0 {
            // NFS3_OK
            let resok = &res.READDIRPLUS3res_u.resok;
            cb_data.eof = resok.reply.eof != 0;

            // Copy cookieverf for next call
            cb_data.cookieverf.copy_from_slice(&resok.cookieverf);

            // Collect entries
            let mut entry_ptr = resok.reply.entries;
            while !entry_ptr.is_null() {
                let entry = &*entry_ptr;
                cb_data.cookie = entry.cookie;

                // Get entry name
                let name = if entry.name.is_null() {
                    String::new()
                } else {
                    CStr::from_ptr(entry.name).to_string_lossy().into_owned()
                };

                // Skip . and ..
                if name != "." && name != ".." {
                    // Check if it's a directory from attributes
                    let is_dir = if entry.name_attributes.attributes_follow != 0 {
                        // fattr3.type: NF3DIR = 2
                        entry.name_attributes.post_op_attr_u.attributes.type_ == 2
                    } else {
                        false // Conservative: if no attrs, assume not a dir
                    };

                    cb_data.entries.push(EntryInfo { name, is_dir });
                }

                entry_ptr = entry.nextentry;
            }
        } else {
            cb_data.status = -(res.status as i32);
        }
    }
}

/// Context for READDIRPLUS with full entry collection including file handles
struct ReaddirplusFullData {
    completed: bool,
    status: i32,
    eof: bool,
    cookie: u64,
    cookieverf: [i8; 8],
    entries: Vec<NfsDirEntry>,
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
    cb_data.completed = true;
    cb_data.status = status;

    if status == ffi::RPC_STATUS_SUCCESS as i32 {
        let res = &*(data as *const ffi::READDIRPLUS3res);
        if res.status == 0 {
            // NFS3_OK
            let resok = &res.READDIRPLUS3res_u.resok;
            cb_data.eof = resok.reply.eof != 0;

            // Copy cookieverf for next call
            cb_data.cookieverf.copy_from_slice(&resok.cookieverf);

            // Collect entries with full attributes and file handles
            let mut entry_ptr = resok.reply.entries;
            while !entry_ptr.is_null() {
                let entry = &*entry_ptr;
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
                            atime: Some(attrs.atime.seconds as i64),
                            mtime: Some(attrs.mtime.seconds as i64),
                            ctime: Some(attrs.ctime.seconds as i64),
                            blksize: 4096, // NFS3 doesn't provide blksize
                            blocks: (attrs.used + 511) / 512, // Convert used bytes to 512-byte blocks
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
/// The `completed` pointer points to a bool that is set by the RPC callback.
/// This function keeps polling until either the callback is called or timeout.
///
/// # Safety
/// The `completed` pointer must remain valid for the duration of this call.
fn wait_for_rpc_completion(
    rpc: *mut ffi::rpc_context,
    completed: *const bool,
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

    while !unsafe { *completed } {
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

impl NfsConnection {
    /// Check if a directory is "big" (has at least `threshold` non-directory entries)
    ///
    /// This uses raw NFS RPC calls to efficiently count directory entries without
    /// buffering the entire directory. It makes READDIRPLUS calls with small
    /// maxcount to minimize network traffic and stops as soon as threshold is reached.
    ///
    /// Returns:
    /// - `BigDirCheckResult::IsBig` if count >= threshold (count is approximate lower bound)
    /// - `BigDirCheckResult::NotBig` if directory has fewer than threshold entries
    /// - `BigDirCheckResult::Error` if an error occurred
    pub fn check_big_dir(&self, path: &str, threshold: u64) -> BigDirCheckResult {
        if !self.mounted {
            return BigDirCheckResult::Error("Not mounted".to_string());
        }

        // Get RPC context
        let rpc = unsafe { ffi::nfs_get_rpc_context(self.context) };
        if rpc.is_null() {
            return BigDirCheckResult::Error("Failed to get RPC context".to_string());
        }

        // Get root file handle
        let root_fh = unsafe { ffi::nfs_get_rootfh(self.context) };
        if root_fh.is_null() {
            return BigDirCheckResult::Error("Failed to get root file handle".to_string());
        }

        // Walk path to get directory file handle
        let dir_fh = match self.lookup_path(rpc, root_fh, path) {
            Ok(fh) => fh,
            Err(e) => return BigDirCheckResult::Error(e),
        };

        // Now do READDIRPLUS calls to count entries
        let mut total_count: u64 = 0;
        let mut cookie: u64 = 0;
        let mut cookieverf: [i8; 8] = [0; 8];

        // Use small maxcount to get ~100-1000 entries per call
        // Each entry is roughly 100-200 bytes (name + attributes)
        // 32KB should give us ~200-300 entries per call
        let dircount: u32 = 32768;
        let maxcount: u32 = 65536;

        loop {
            let mut cb_data = ReaddirplusCallbackData {
                completed: false,
                status: 0,
                entry_count: 0,
                eof: false,
                cookie: 0,
                cookieverf: [0i8; 8],
            };

            // Build READDIRPLUS args
            let mut args: ffi::READDIRPLUS3args = unsafe { std::mem::zeroed() };
            args.dir.data.data_len = dir_fh.len as u32;
            args.dir.data.data_val = dir_fh.data.as_ptr() as *mut i8;
            args.cookie = cookie;
            args.cookieverf = cookieverf;
            args.dircount = dircount;
            args.maxcount = maxcount;

            let pdu = unsafe {
                ffi::rpc_nfs3_readdirplus_task(
                    rpc,
                    Some(readdirplus_callback),
                    &mut args,
                    &mut cb_data as *mut _ as *mut std::ffi::c_void,
                )
            };

            if pdu.is_null() {
                return BigDirCheckResult::Error("Failed to queue READDIRPLUS".to_string());
            }

            // Wait for completion - pass pointer to completed flag
            if let Err(e) = wait_for_rpc_completion(rpc, &cb_data.completed, self.rpc_timeout_ms) {
                return BigDirCheckResult::Error(format!("READDIRPLUS failed: {}", e));
            }

            // cb_data.completed is guaranteed true here since wait_for_rpc_completion returned Ok

            if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
                return BigDirCheckResult::Error(format!(
                    "READDIRPLUS error: status={}",
                    cb_data.status
                ));
            }

            total_count += cb_data.entry_count;

            // Check if we've hit the threshold
            if total_count >= threshold {
                return BigDirCheckResult::IsBig { count: total_count };
            }

            // Check if we've read all entries
            if cb_data.eof {
                return BigDirCheckResult::NotBig {
                    total_count,
                };
            }

            // Prepare for next call
            cookie = cb_data.cookie;
            cookieverf = cb_data.cookieverf;
        }
    }

    /// Scan a directory for big-dir-hunt mode
    ///
    /// This uses raw NFS RPC calls to efficiently count directory entries and
    /// discover subdirectories without buffering the entire directory.
    /// It stops counting non-directory entries once threshold is reached,
    /// but continues to read all entries to discover subdirectories.
    ///
    /// Returns a BigDirScanResult with:
    /// - file_count: number of non-directory entries (capped at threshold if threshold_hit)
    /// - threshold_hit: true if we stopped counting because threshold was reached
    /// - subdirs: names of all subdirectories found
    pub fn scan_big_dir(&self, path: &str, threshold: u64) -> Result<BigDirScanResult, String> {
        if !self.mounted {
            return Err("Not mounted".to_string());
        }

        // Get RPC context
        let rpc = unsafe { ffi::nfs_get_rpc_context(self.context) };
        if rpc.is_null() {
            return Err("Failed to get RPC context".to_string());
        }

        // Drain any pending events to ensure clean state
        drain_pending_events(rpc);

        // Get root file handle
        let root_fh = unsafe { ffi::nfs_get_rootfh(self.context) };
        if root_fh.is_null() {
            return Err("Failed to get root file handle".to_string());
        }

        // Walk path to get directory file handle
        let dir_fh = self.lookup_path(rpc, root_fh, path)?;

        // Now do READDIRPLUS calls to scan entries
        let mut file_count: u64 = 0;
        let mut threshold_hit = false;
        let mut subdirs: Vec<String> = Vec::new();
        let mut cookie: u64 = 0;
        let mut cookieverf: [i8; 8] = [0; 8];

        // Use reasonable maxcount for scanning
        // We want to minimize calls while not getting too much data
        let dircount: u32 = 65536;  // 64KB
        let maxcount: u32 = 131072; // 128KB

        loop {
            let mut cb_data = ReaddirplusScanData {
                completed: false,
                status: 0,
                eof: false,
                cookie: 0,
                cookieverf: [0i8; 8],
                entries: Vec::with_capacity(1000),
            };

            // Build READDIRPLUS args
            let mut args: ffi::READDIRPLUS3args = unsafe { std::mem::zeroed() };
            args.dir.data.data_len = dir_fh.len as u32;
            args.dir.data.data_val = dir_fh.data.as_ptr() as *mut i8;
            args.cookie = cookie;
            args.cookieverf = cookieverf;
            args.dircount = dircount;
            args.maxcount = maxcount;

            let pdu = unsafe {
                ffi::rpc_nfs3_readdirplus_task(
                    rpc,
                    Some(readdirplus_scan_callback),
                    &mut args,
                    &mut cb_data as *mut _ as *mut std::ffi::c_void,
                )
            };

            if pdu.is_null() {
                return Err("Failed to queue READDIRPLUS".to_string());
            }

            // Wait for completion - pass pointer to completed flag
            wait_for_rpc_completion(rpc, &cb_data.completed, self.rpc_timeout_ms)?;

            // cb_data.completed is guaranteed true here

            if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
                return Err(format!("READDIRPLUS error: status={}", cb_data.status));
            }

            // Process collected entries
            for entry in cb_data.entries {
                if entry.is_dir {
                    subdirs.push(entry.name);
                } else if !threshold_hit {
                    file_count += 1;
                    if file_count >= threshold {
                        threshold_hit = true;
                        // Continue to collect subdirs
                    }
                }
            }

            // Check if we've read all entries
            if cb_data.eof {
                break;
            }

            // Prepare for next call
            cookie = cb_data.cookie;
            cookieverf = cb_data.cookieverf;
        }

        Ok(BigDirScanResult {
            file_count,
            threshold_hit,
            subdirs,
        })
    }

    /// Walk a path starting from a file handle, returning the final file handle
    fn lookup_path(
        &self,
        rpc: *mut ffi::rpc_context,
        start_fh: *const ffi::nfs_fh,
        path: &str,
    ) -> Result<FileHandle, String> {
        let start_fh = unsafe { &*start_fh };

        // Start with the given file handle
        let mut current_fh = FileHandle {
            len: start_fh.len as usize,
            data: [0u8; 128],
        };
        if current_fh.len > 128 {
            return Err("File handle too large".to_string());
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                start_fh.val as *const u8,
                current_fh.data.as_mut_ptr(),
                current_fh.len,
            );
        }

        // If path is "/" or empty, return root handle
        let path = path.trim_start_matches('/');
        if path.is_empty() {
            return Ok(current_fh);
        }

        // Walk each component
        for component in path.split('/') {
            if component.is_empty() {
                continue;
            }

            current_fh = self.lookup_single(rpc, &current_fh, component)?;
        }

        Ok(current_fh)
    }

    /// Do a single LOOKUP operation
    fn lookup_single(
        &self,
        rpc: *mut ffi::rpc_context,
        dir_fh: &FileHandle,
        name: &str,
    ) -> Result<FileHandle, String> {
        let name_cstr = CString::new(name).map_err(|_| "Invalid name".to_string())?;

        let mut cb_data = LookupCallbackData {
            completed: false,
            status: 0,
            fh_len: 0,
            fh_data: [0; 128],
        };

        // Build LOOKUP args
        let mut args: ffi::LOOKUP3args = unsafe { std::mem::zeroed() };
        args.what.dir.data.data_len = dir_fh.len as u32;
        args.what.dir.data.data_val = dir_fh.data.as_ptr() as *mut i8;
        args.what.name = name_cstr.as_ptr() as *mut i8;

        let pdu = unsafe {
            ffi::rpc_nfs3_lookup_task(
                rpc,
                Some(lookup_callback),
                &mut args,
                &mut cb_data as *mut _ as *mut std::ffi::c_void,
            )
        };

        if pdu.is_null() {
            return Err(format!("Failed to queue LOOKUP for '{}'", name));
        }

        // Wait for completion - pass pointer to completed flag
        wait_for_rpc_completion(rpc, &cb_data.completed, self.rpc_timeout_ms)?;

        // cb_data.completed is guaranteed true here

        if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
            return Err(format!("LOOKUP '{}' failed: status={}", name, cb_data.status));
        }

        if cb_data.fh_len == 0 {
            return Err(format!("LOOKUP '{}' returned empty handle", name));
        }

        Ok(FileHandle {
            len: cb_data.fh_len,
            data: cb_data.fh_data,
        })
    }
}

/// Internal file handle representation
struct FileHandle {
    len: usize,
    data: [u8; 128],
}

/// Builder for NFS connections with retry support
pub struct NfsConnectionBuilder {
    url: NfsUrl,
    timeout: Duration,
    retries: u32,
    /// Override server with specific IP (for DNS round-robin)
    override_ip: Option<String>,
    /// READDIR buffer size in bytes (default: 8KB, max: 4MB)
    /// Larger buffers reduce RPC round-trips for directory listings.
    readdir_buffer_size: Option<u32>,
}

impl NfsConnectionBuilder {
    /// Create a new builder
    pub fn new(url: NfsUrl) -> Self {
        Self {
            url,
            timeout: Duration::from_secs(30),
            retries: 3,
            override_ip: None,
            readdir_buffer_size: None,
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

    /// Set READDIR buffer size in bytes
    ///
    /// Larger buffers reduce RPC round-trips for directory listings.
    /// Default is 8KB, max is 4MB. Values are rounded to 4KB boundaries.
    /// For large directories, 1MB is a good choice.
    pub fn readdir_buffer_size(mut self, size: u32) -> Self {
        self.readdir_buffer_size = Some(size);
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

            // Create connection, apply settings, then connect
            match NfsConnection::new(&url) {
                Ok(mut conn) => {
                    // Apply readdir buffer size if configured
                    if let Some(size) = self.readdir_buffer_size {
                        conn.set_readdir_buffer_size(size);
                    }

                    match conn.connect(self.timeout) {
                        Ok(()) => return Ok(conn),
                        Err(e) => {
                            last_error = Some(e);
                        }
                    }
                }
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
/// Makes multiple resolution attempts using `host` command to bypass
/// system resolver caching and catch rotating DNS servers.
/// If resolution fails, returns the original hostname as the only entry.
pub fn resolve_dns(hostname: &str) -> Vec<String> {
    resolve_dns_with_attempts(hostname, 20)
}

/// Resolve DNS with a specified number of attempts to catch all rotating IPs
pub fn resolve_dns_with_attempts(hostname: &str, attempts: usize) -> Vec<String> {
    use std::collections::HashSet;
    use std::process::Command;

    let mut all_ips = HashSet::new();

    // Use `host` command to bypass system resolver caching
    // Each call to `host` does a fresh DNS query
    for _ in 0..attempts {
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

/// A raw RPC context for big-dir scanning that maintains its own connection.
///
/// This creates a completely separate RPC context that doesn't share state
/// with any `NfsConnection`. This avoids issues where mixing sync and async
/// operations on the same context causes callbacks to never fire.
///
/// Usage:
/// 1. Create with `RawRpcContext::connect()`
/// 2. Call `scan_big_dir()` to scan directories
/// 3. Drop when done (automatically cleans up)
pub struct RawRpcContext {
    /// The RPC context pointer
    rpc: *mut ffi::rpc_context,
    /// Root file handle from MOUNT
    root_fh: FileHandle,
    /// Server name (for logging/debugging)
    #[allow(dead_code)]
    server: String,
    /// Export path (for logging/debugging)
    #[allow(dead_code)]
    export: String,
    /// RPC timeout in milliseconds
    rpc_timeout_ms: i32,
}

// RawRpcContext owns its RPC context and can be sent between threads
unsafe impl Send for RawRpcContext {}

/// Callback data for simple RPC operations
struct SimpleCallbackData {
    completed: bool,
    status: i32,
    error_msg: Option<String>,
}

/// Callback data for MOUNT operation
struct MountCallbackData {
    completed: bool,
    status: i32,
    fh_len: usize,
    fh_data: [u8; 128],
    error_msg: Option<String>,
}

/// Generic RPC callback for connection
unsafe extern "C" fn connect_callback(
    _rpc: *mut ffi::rpc_context,
    status: ::std::os::raw::c_int,
    data: *mut ::std::os::raw::c_void,
    private_data: *mut ::std::os::raw::c_void,
) {
    let cb_data = &mut *(private_data as *mut SimpleCallbackData);
    cb_data.completed = true;
    cb_data.status = status;

    if status == ffi::RPC_STATUS_ERROR as i32 && !data.is_null() {
        let err_str = CStr::from_ptr(data as *const i8);
        cb_data.error_msg = Some(err_str.to_string_lossy().into_owned());
    }
}

/// Callback for MOUNT3/MNT operation
unsafe extern "C" fn mount_callback(
    _rpc: *mut ffi::rpc_context,
    status: ::std::os::raw::c_int,
    data: *mut ::std::os::raw::c_void,
    private_data: *mut ::std::os::raw::c_void,
) {
    let cb_data = &mut *(private_data as *mut MountCallbackData);
    cb_data.completed = true;
    cb_data.status = status;

    if status == ffi::RPC_STATUS_SUCCESS as i32 {
        let res = &*(data as *const ffi::mountres3);
        if res.fhs_status == ffi::mountstat3_MNT3_OK {
            // Extract file handle
            let fh = &res.mountres3_u.mountinfo.fhandle;
            let len = fh.fhandle3_len as usize;
            if len <= cb_data.fh_data.len() {
                cb_data.fh_len = len;
                std::ptr::copy_nonoverlapping(
                    fh.fhandle3_val as *const u8,
                    cb_data.fh_data.as_mut_ptr(),
                    len,
                );
            } else {
                cb_data.error_msg = Some(format!("File handle too large: {}", len));
            }
        } else {
            cb_data.error_msg = Some(format!("MOUNT failed: status={}", res.fhs_status as i32));
        }
    } else if status == ffi::RPC_STATUS_ERROR as i32 && !data.is_null() {
        let err_str = CStr::from_ptr(data as *const i8);
        cb_data.error_msg = Some(err_str.to_string_lossy().into_owned());
    }
}

impl RawRpcContext {
    /// NFS program number
    const NFS_PROGRAM: i32 = 100003;
    /// NFS version 3
    const NFS_V3: i32 = 3;
    /// MOUNT program number
    const MOUNT_PROGRAM: i32 = 100005;
    /// MOUNT version 3
    const MOUNT_V3: i32 = 3;

    /// Connect to an NFS server and mount the export using raw RPC.
    ///
    /// This creates a fresh RPC context, connects to the MOUNT service,
    /// gets the root file handle, then reconnects to the NFS service.
    pub fn connect(server: &str, export: &str, timeout_ms: i32) -> Result<Self, String> {
        // Create a fresh RPC context
        let rpc = unsafe { ffi::rpc_init_context() };
        if rpc.is_null() {
            return Err("Failed to create RPC context".to_string());
        }

        // Set uid/gid for auth
        unsafe {
            let uid = libc::getuid() as i32;
            let gid = libc::getgid() as i32;
            ffi::rpc_set_uid(rpc, uid);
            ffi::rpc_set_gid(rpc, gid);
            ffi::rpc_set_timeout(rpc, timeout_ms);
        }

        // Step 1: Connect to MOUNT service and get root file handle
        let root_fh = Self::do_mount(rpc, server, export, timeout_ms)?;

        // Step 2: Disconnect from MOUNT and reconnect to NFS
        // Note: rpc_disconnect doesn't exist in the way we'd expect - we just connect to a new program
        // Actually we need to create a new context for NFS since we're done with MOUNT
        unsafe { ffi::rpc_destroy_context(rpc); }

        // Create new context for NFS
        let rpc = unsafe { ffi::rpc_init_context() };
        if rpc.is_null() {
            return Err("Failed to create NFS RPC context".to_string());
        }

        // Set uid/gid and timeout again
        unsafe {
            let uid = libc::getuid() as i32;
            let gid = libc::getgid() as i32;
            ffi::rpc_set_uid(rpc, uid);
            ffi::rpc_set_gid(rpc, gid);
            ffi::rpc_set_timeout(rpc, timeout_ms);
        }

        // Connect to NFS service
        Self::connect_to_nfs(rpc, server, timeout_ms)?;

        Ok(Self {
            rpc,
            root_fh,
            server: server.to_string(),
            export: export.to_string(),
            rpc_timeout_ms: timeout_ms,
        })
    }

    /// Connect to MOUNT service and get the root file handle
    fn do_mount(rpc: *mut ffi::rpc_context, server: &str, export: &str, timeout_ms: i32) -> Result<FileHandle, String> {
        let server_cstr = CString::new(server).map_err(|_| "Invalid server name")?;
        let export_cstr = CString::new(export).map_err(|_| "Invalid export path")?;

        // Connect to MOUNT service using portmapper
        let mut cb_data = SimpleCallbackData {
            completed: false,
            status: 0,
            error_msg: None,
        };

        let ret = unsafe {
            ffi::rpc_connect_program_async(
                rpc,
                server_cstr.as_ptr(),
                Self::MOUNT_PROGRAM,
                Self::MOUNT_V3,
                Some(connect_callback),
                &mut cb_data as *mut _ as *mut std::ffi::c_void,
            )
        };

        if ret != 0 {
            return Err(format!("Failed to initiate MOUNT connection: ret={}", ret));
        }

        // Wait for connection
        Self::wait_for_completion(rpc, &cb_data.completed, timeout_ms)?;

        if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
            let msg = cb_data.error_msg.unwrap_or_else(|| "Unknown error".to_string());
            return Err(format!("MOUNT connection failed: {}", msg));
        }

        // Now do the actual MOUNT call
        let mut mount_data = MountCallbackData {
            completed: false,
            status: 0,
            fh_len: 0,
            fh_data: [0; 128],
            error_msg: None,
        };

        let pdu = unsafe {
            ffi::rpc_mount3_mnt_task(
                rpc,
                Some(mount_callback),
                export_cstr.as_ptr() as *mut i8,
                &mut mount_data as *mut _ as *mut std::ffi::c_void,
            )
        };

        if pdu.is_null() {
            return Err("Failed to queue MOUNT RPC".to_string());
        }

        // Wait for MOUNT to complete
        Self::wait_for_completion(rpc, &mount_data.completed, timeout_ms)?;

        if mount_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
            let msg = mount_data.error_msg.unwrap_or_else(|| "Unknown error".to_string());
            return Err(format!("MOUNT failed: {}", msg));
        }

        if mount_data.fh_len == 0 {
            let msg = mount_data.error_msg.unwrap_or_else(|| "Empty file handle".to_string());
            return Err(format!("MOUNT returned invalid handle: {}", msg));
        }

        Ok(FileHandle {
            len: mount_data.fh_len,
            data: mount_data.fh_data,
        })
    }

    /// Connect to the NFS service
    fn connect_to_nfs(rpc: *mut ffi::rpc_context, server: &str, timeout_ms: i32) -> Result<(), String> {
        let server_cstr = CString::new(server).map_err(|_| "Invalid server name")?;

        let mut cb_data = SimpleCallbackData {
            completed: false,
            status: 0,
            error_msg: None,
        };

        let ret = unsafe {
            ffi::rpc_connect_program_async(
                rpc,
                server_cstr.as_ptr(),
                Self::NFS_PROGRAM,
                Self::NFS_V3,
                Some(connect_callback),
                &mut cb_data as *mut _ as *mut std::ffi::c_void,
            )
        };

        if ret != 0 {
            return Err(format!("Failed to initiate NFS connection: ret={}", ret));
        }

        Self::wait_for_completion(rpc, &cb_data.completed, timeout_ms)?;

        if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
            let msg = cb_data.error_msg.unwrap_or_else(|| "Unknown error".to_string());
            return Err(format!("NFS connection failed: {}", msg));
        }

        Ok(())
    }

    /// Wait for an RPC operation to complete
    fn wait_for_completion(rpc: *mut ffi::rpc_context, completed: *const bool, timeout_ms: i32) -> Result<(), String> {
        use std::os::unix::io::RawFd;

        let fd: RawFd = unsafe { ffi::rpc_get_fd(rpc) };
        if fd < 0 {
            return Err(format!("Invalid RPC fd: {}", fd));
        }

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms as u64);
        let mut iteration = 0u32;

        while !unsafe { *completed } {
            if start.elapsed() > timeout {
                return Err(format!("RPC timeout after {} iterations", iteration));
            }

            let events = unsafe { ffi::rpc_which_events(rpc) };

            if iteration < 3 {
                tracing::debug!(
                    "RawRPC wait: iter={}, fd={}, events={:#x} (POLLIN={}, POLLOUT={})",
                    iteration, fd, events,
                    events & libc::POLLIN as i32,
                    events & libc::POLLOUT as i32
                );
            }

            if events == 0 {
                // Service with 0 events to process internal state
                let ret = unsafe { ffi::rpc_service(rpc, 0) };
                if ret < 0 {
                    let err_msg = unsafe {
                        let err_ptr = ffi::rpc_get_error(rpc);
                        if err_ptr.is_null() {
                            "unknown".to_string()
                        } else {
                            CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                        }
                    };
                    return Err(format!("rpc_service failed (no events): ret={}, err={}", ret, err_msg));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
                iteration += 1;
                continue;
            }

            let mut pfd = libc::pollfd {
                fd,
                events: events as i16,
                revents: 0,
            };

            let poll_timeout = std::cmp::min(100, (timeout - start.elapsed()).as_millis() as i32);
            let ret = unsafe { libc::poll(&mut pfd, 1, poll_timeout) };

            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(format!("poll failed: {}", err));
            }

            // Process events (or timeout)
            let revents = if pfd.revents != 0 { pfd.revents as i32 } else { 0 };

            if iteration < 3 {
                tracing::debug!(
                    "RawRPC service: iter={}, poll_ret={}, revents={:#x}",
                    iteration, ret, revents
                );
            }

            let service_ret = unsafe { ffi::rpc_service(rpc, revents) };
            if service_ret < 0 {
                let err_msg = unsafe {
                    let err_ptr = ffi::rpc_get_error(rpc);
                    if err_ptr.is_null() {
                        "unknown".to_string()
                    } else {
                        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                    }
                };
                return Err(format!("rpc_service failed: ret={}, revents={:#x}, err={}", service_ret, revents, err_msg));
            }

            iteration += 1;
        }

        tracing::debug!("RawRPC completed after {} iterations", iteration);
        Ok(())
    }

    /// Scan a directory for big-dir-hunt mode using raw NFS RPCs.
    ///
    /// This is much faster than nfs_opendir for huge directories because it:
    /// 1. Uses raw READDIRPLUS RPCs directly
    /// 2. Processes entries as they arrive (no buffering)
    /// 3. Stops immediately once threshold is reached (early exit)
    ///
    /// If `early_exit` is true, stops reading as soon as threshold is hit.
    /// This is much faster for huge directories but may miss some subdirectories.
    pub fn scan_big_dir(&self, path: &str, threshold: u64) -> Result<BigDirScanResult, String> {
        self.scan_big_dir_impl(path, threshold, true) // Default to early exit
    }

    /// Internal implementation with configurable early exit
    fn scan_big_dir_impl(&self, path: &str, threshold: u64, early_exit: bool) -> Result<BigDirScanResult, String> {
        // Walk path to get directory file handle
        let dir_fh = self.lookup_path(path)?;

        // Now do READDIRPLUS calls to scan entries
        let mut file_count: u64 = 0;
        let mut threshold_hit = false;
        let mut subdirs: Vec<String> = Vec::new();
        let mut cookie: u64 = 0;
        let mut cookieverf: [i8; 8] = [0; 8];

        // Use small maxcount to avoid TCP fragmentation issues in raw RPC mode
        // libnfs raw RPC doesn't handle fragmented responses well
        // 8KB maxcount should give us ~50-100 entries per call
        let dircount: u32 = 4096;   // 4KB
        let maxcount: u32 = 8192;   // 8KB

        loop {
            let mut cb_data = ReaddirplusScanData {
                completed: false,
                status: 0,
                eof: false,
                cookie: 0,
                cookieverf: [0i8; 8],
                entries: Vec::with_capacity(1000),
            };

            // Build READDIRPLUS args
            let mut args: ffi::READDIRPLUS3args = unsafe { std::mem::zeroed() };
            args.dir.data.data_len = dir_fh.len as u32;
            args.dir.data.data_val = dir_fh.data.as_ptr() as *mut i8;
            args.cookie = cookie;
            args.cookieverf = cookieverf;
            args.dircount = dircount;
            args.maxcount = maxcount;

            let pdu = unsafe {
                ffi::rpc_nfs3_readdirplus_task(
                    self.rpc,
                    Some(readdirplus_scan_callback),
                    &mut args,
                    &mut cb_data as *mut _ as *mut std::ffi::c_void,
                )
            };

            if pdu.is_null() {
                return Err("Failed to queue READDIRPLUS".to_string());
            }

            // Wait for completion
            Self::wait_for_completion(self.rpc, &cb_data.completed, self.rpc_timeout_ms)?;

            if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
                return Err(format!("READDIRPLUS error: status={}", cb_data.status));
            }

            // Process collected entries
            for entry in cb_data.entries {
                if entry.is_dir {
                    subdirs.push(entry.name);
                } else {
                    file_count += 1;
                    if file_count >= threshold {
                        threshold_hit = true;
                        if early_exit {
                            // Stop immediately - we found a big directory
                            return Ok(BigDirScanResult {
                                file_count,
                                threshold_hit: true,
                                subdirs,
                            });
                        }
                    }
                }
            }

            // Check if we've read all entries
            if cb_data.eof {
                break;
            }

            // Prepare for next call
            cookie = cb_data.cookie;
            cookieverf = cb_data.cookieverf;
        }

        Ok(BigDirScanResult {
            file_count,
            threshold_hit,
            subdirs,
        })
    }

    /// Walk a path starting from root, returning the final file handle
    fn lookup_path(&self, path: &str) -> Result<FileHandle, String> {
        // Start with root handle
        let mut current_fh = self.root_fh.clone();

        // If path is "/" or empty, return root handle
        let path = path.trim_start_matches('/');
        if path.is_empty() {
            return Ok(current_fh);
        }

        // Walk each component
        for component in path.split('/') {
            if component.is_empty() {
                continue;
            }
            current_fh = self.lookup_single(&current_fh, component)?;
        }

        Ok(current_fh)
    }

    /// Do a single LOOKUP operation
    fn lookup_single(&self, dir_fh: &FileHandle, name: &str) -> Result<FileHandle, String> {
        let name_cstr = CString::new(name).map_err(|_| "Invalid name")?;

        let mut cb_data = LookupCallbackData {
            completed: false,
            status: 0,
            fh_len: 0,
            fh_data: [0; 128],
        };

        // Build LOOKUP args
        let mut args: ffi::LOOKUP3args = unsafe { std::mem::zeroed() };
        args.what.dir.data.data_len = dir_fh.len as u32;
        args.what.dir.data.data_val = dir_fh.data.as_ptr() as *mut i8;
        args.what.name = name_cstr.as_ptr() as *mut i8;

        let pdu = unsafe {
            ffi::rpc_nfs3_lookup_task(
                self.rpc,
                Some(lookup_callback),
                &mut args,
                &mut cb_data as *mut _ as *mut std::ffi::c_void,
            )
        };

        if pdu.is_null() {
            return Err(format!("Failed to queue LOOKUP for '{}'", name));
        }

        Self::wait_for_completion(self.rpc, &cb_data.completed, self.rpc_timeout_ms)?;

        if cb_data.status != ffi::RPC_STATUS_SUCCESS as i32 {
            return Err(format!("LOOKUP '{}' failed: status={}", name, cb_data.status));
        }

        if cb_data.fh_len == 0 {
            return Err(format!("LOOKUP '{}' returned empty handle", name));
        }

        Ok(FileHandle {
            len: cb_data.fh_len,
            data: cb_data.fh_data,
        })
    }
}

impl Drop for RawRpcContext {
    fn drop(&mut self) {
        if !self.rpc.is_null() {
            unsafe {
                ffi::rpc_destroy_context(self.rpc);
            }
            self.rpc = ptr::null_mut();
        }
    }
}

// Implement Clone for FileHandle so we can copy it
impl Clone for FileHandle {
    fn clone(&self) -> Self {
        FileHandle {
            len: self.len,
            data: self.data,
        }
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
}
