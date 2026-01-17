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

// Include generated bindings
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(dead_code)]
pub(crate) mod ffi {
    include!(concat!(env!("OUT_DIR"), "/nfs_bindings.rs"));
}

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

        Ok(Self {
            context,
            server: url.server.clone(),
            export: url.export.clone(),
            mounted: false,
        })
    }

    /// Connect and mount the NFS export
    pub fn connect(&mut self, timeout: Duration) -> NfsResult<()> {
        if self.mounted {
            return Ok(());
        }

        // Set timeout (in milliseconds)
        let timeout_ms = timeout.as_millis() as i32;
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
        let result = unsafe {
            ffi::nfs_opendir(self.context, path_cstr.as_ptr(), &mut dir_handle)
        };

        if result != 0 {
            return Err(self.translate_error(path, result));
        }

        let mut total_entries = 0;
        let mut chunk = Vec::with_capacity(chunk_size);

        loop {
            let dirent = unsafe { ffi::nfs_readdir(self.context, dir_handle) };

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

    /// Open a directory starting from a specific NFS cookie position
    ///
    /// This enables parallel directory reading by having multiple workers
    /// start reading from different cookie positions within the same directory.
    ///
    /// # Arguments
    /// * `path` - Directory path to open
    /// * `cookie` - Starting cookie (0 = beginning, or value from a previous entry's cookie field)
    /// * `max_entries` - Maximum entries to fetch from server (0 = no limit, read until EOF)
    ///
    /// # Note
    /// The cookieverf is set to zero. Most NFS servers accept this, but some
    /// may reject non-zero cookies without a valid verifier.
    pub fn opendir_at_cookie(&self, path: &str, cookie: u64, max_entries: u32) -> NfsResult<NfsDirHandle> {
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
            ffi::nfs_opendir_at_cookie(self.context, path_cstr.as_ptr(), cookie, max_entries, &mut dir_handle)
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

    /// Open a directory for reading names only (READDIR)
    ///
    /// This uses READDIR instead of READDIRPLUS, which is dramatically faster
    /// for large directories because the server doesn't need to stat() each file.
    /// libnfs handles cookie management internally - just iterate with readdir().
    ///
    /// Returned entries will have:
    /// - name: populated
    /// - inode: populated
    /// - entry_type: Unknown (no attributes fetched)
    /// - stat: minimal (only inode populated)
    pub fn opendir_names_only(&self, path: &str) -> NfsResult<NfsDirHandle> {
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
            ffi::nfs_opendir_names_only(
                self.context,
                path_cstr.as_ptr(),
                &mut dir_handle,
            )
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

    /// Open a directory for reading names only, starting at a specific cookie position
    ///
    /// This uses READDIR instead of READDIRPLUS, which is dramatically faster
    /// for large directories because the server doesn't need to stat() each file.
    ///
    /// Returned entries will have:
    /// - name: populated
    /// - inode: populated
    /// - cookie: populated (use for next batch)
    /// - entry_type: Unknown (no attributes fetched)
    /// - stat: minimal (only inode populated)
    ///
    /// Use `NfsDirHandle::get_cookieverf()` to get the verifier for the next batch.
    ///
    /// # Arguments
    /// * `path` - Directory path to read
    /// * `cookie` - Starting position (0 for beginning, or last entry's cookie)
    /// * `cookieverf` - Cookie verifier from previous batch (0 for first call)
    /// * `max_entries` - Maximum entries to fetch (batch size)
    pub fn opendir_names_only_at_cookie(
        &self,
        path: &str,
        cookie: u64,
        cookieverf: u64,
        max_entries: u32,
    ) -> NfsResult<NfsDirHandle> {
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
            ffi::nfs_opendir_names_only_at_cookie(
                self.context,
                path_cstr.as_ptr(),
                cookie,
                cookieverf,
                max_entries,
                &mut dir_handle,
            )
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
            cookie: d.cookie,
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
    pub(crate) unsafe fn raw_context(&self) -> *mut ffi::nfs_context {
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

    /// Get the cookie verifier from the last READDIR response
    ///
    /// This is required for streaming with `opendir_names_only_at_cookie()`.
    /// Pass this verifier along with the last entry's cookie to resume reading.
    pub fn get_cookieverf(&self) -> u64 {
        unsafe { ffi::nfs_readdir_get_cookieverf(self.handle) }
    }

    /// Check if end-of-directory was reached in the last READDIR response
    ///
    /// This is the authoritative way to detect end of directory when streaming
    /// with `opendir_names_only_at_cookie()`. Do NOT rely on empty batches or
    /// repeated entries to detect EOF - always check this flag.
    pub fn is_eof(&self) -> bool {
        unsafe { ffi::nfs_readdir_is_eof(self.handle) != 0 }
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
            cookie: d.cookie,
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

            match NfsConnection::connect_to(&url, self.timeout) {
                Ok(conn) => return Ok(conn),
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
