# Handoff: File Handle Caching for Narrow-Deep Performance

## Context

nfs-walker is a high-performance NFS filesystem scanner using libnfs directly. It's 59x faster than `du` on wide trees but **30x slower** than `dust` on narrow-deep trees (701 directories, 1 subdir each).

## Root Cause Discovered

**libnfs does O(n²) LOOKUP RPCs for narrow-deep trees.**

When opening directory `/narrow-deep/l000/l001/.../l699`, libnfs does a LOOKUP RPC for EVERY path component:
- LOOKUP "narrow-deep"
- LOOKUP "l000"
- LOOKUP "l001"
- ... (699 more LOOKUPs)
- Then finally READDIRPLUS

For 701 directories at increasing depths:
- Total LOOKUPs = 1+2+3+...+700 ≈ **245,000 RPCs**
- Total READDIRPLUSes = 701

We verified this with tcpdump:
```
LOOKUPs: 177,083
READDIRPLUSes: 838
```

The kernel NFS client caches file handles in the dentry cache, avoiding repeated LOOKUPs. libnfs doesn't cache.

## The Solution

**Cache file handles from READDIRPLUS responses and reuse them.**

READDIRPLUS returns file handles for each entry in `entryplus3.name_handle`. Instead of passing paths to `nfs_opendir()`, use the cached file handle directly with `rpc_nfs3_readdirplus_task()`.

Expected improvement:
- Before: ~180,000 LOOKUPs + 700 READDIRPLUSes
- After: 0 LOOKUPs + 700 READDIRPLUSes
- **~250x fewer RPCs**

## Implementation Plan

### 1. Add FileHandle to DirWork struct

File: `src/walker/simple.rs` around line 41

```rust
struct DirWork {
    path: String,
    depth: u32,
    file_handle: Option<FileHandle>,  // NEW: cached from READDIRPLUS
}
```

You'll need to define or import `FileHandle`. Check `src/nfs/connection.rs` - there's already a `FileHandle` struct used in the big-dir code (around line 625).

### 2. Extract file handles in worker_loop

File: `src/walker/simple.rs` in the `worker_loop` function

Currently around line 1007-1016, subdirectories are collected:
```rust
if is_dir {
    subdirs.push(DirWork {
        path: full_path,
        depth: work.depth + 1,
    });
}
```

Need to extract file handle from `nfs_entry`. The `NfsDirEntry` struct needs to include the file handle from the READDIRPLUS response.

### 3. Modify NfsDirEntry to include file handle

File: `src/nfs/types.rs` - find `NfsDirEntry` struct and add:
```rust
pub struct NfsDirEntry {
    pub name: String,
    pub entry_type: EntryType,
    pub inode: u64,
    // ... existing fields ...
    pub file_handle: Option<Vec<u8>>,  // NEW: from READDIRPLUS name_handle
}
```

### 4. Extract file handle in convert_dirent

File: `src/nfs/connection.rs` - find `convert_dirent` function

The `nfsdirent` FFI struct should have access to the file handle. Check the libnfs `nfsdirent` structure - it may already include it, or you may need to use the raw `entryplus3` struct directly.

Reference: The `readdirplus_scan_callback` function (line ~726) shows how to access `entry.name_handle` from raw READDIRPLUS results.

### 5. Add READDIRPLUS-by-handle function

File: `src/nfs/connection.rs`

Add a new function similar to `readdir_plus_chunked` but takes a file handle instead of path:

```rust
pub fn readdir_plus_by_fh<F>(
    &self,
    fh: &[u8],
    chunk_size: usize,
    callback: F,
) -> NfsResult<usize>
where
    F: FnMut(Vec<NfsDirEntry>) -> bool,
{
    // Use rpc_nfs3_readdirplus_task directly with the file handle
    // See existing big-dir code for reference (around line 974)
}
```

### 6. Use cached handle in worker_loop

Modify the directory reading to check for cached handle:

```rust
let result = if let Some(fh) = &work.file_handle {
    nfs.readdir_plus_by_fh(fh, batch_size, |chunk| { ... })
} else {
    nfs.readdir_plus_chunked(&work.path, batch_size, |chunk| { ... })
};
```

## Key Files to Examine

1. `src/walker/simple.rs` - Main walker, `DirWork` struct, `worker_loop`
2. `src/nfs/connection.rs` - NFS operations, `convert_dirent`, big-dir RPC code
3. `src/nfs/types.rs` - `NfsDirEntry` struct
4. `src/nfs/bindings.rs` - FFI definitions, `entryplus3`, `post_op_fh3`

## Reference: Existing File Handle Code

The big-dir detection code already uses direct RPC with file handles. Key functions:
- `readdirplus_scan_callback` (line ~726) - extracts from READDIRPLUS response
- `lookup_single` (line ~1904) - does single LOOKUP by handle
- `rpc_nfs3_readdirplus_task` usage (line ~974) - direct RPC call

## Testing

After implementation:

```bash
# Build
cargo build --release

# Test narrow-deep (should improve dramatically)
sudo rm -rf /tmp/test.rocks
time sudo ./target/release/nfs-walker nfs://172.200.203.1/syncengine-demo/bench/narrow-deep -o /tmp/test.rocks -q

# Verify with tcpdump - should see ~700 RPCs, not 180,000
sudo tcpdump -i ens33 -c 5000 host 172.200.203.1 and port 2049 2>/dev/null | grep -c lookup
# Should be near 0 or very low (just initial path resolution)

# CRITICAL: Verify no regression on mix workload
sudo rm -rf /tmp/test.rocks
time sudo ./target/release/nfs-walker nfs://172.200.203.1/syncengine-demo/bench/mix -o /tmp/test.rocks -q
# Should still be ~4s
```

## Expected Results

| Metric | Before | After |
|--------|--------|-------|
| narrow-deep time | ~70s | ~2-5s |
| narrow-deep RPCs | ~180,000 | ~1,400 |
| mix time | ~4s | ~4s (no regression) |

## Report Back

After implementation, provide:
1. Summary of changes made
2. Any issues encountered
3. Test results for narrow-deep and mix workloads
4. tcpdump RPC count verification
