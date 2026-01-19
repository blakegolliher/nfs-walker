# Handoff: Streaming READDIR Implementation

## Session Summary

This session attempted to implement streaming READDIR to overlap directory listing with GETATTR processing for better performance on large directories. The goal was to dispatch batches of names to workers as they arrive, rather than waiting for the entire directory listing to complete.

## What Was Accomplished

### 1. EOF Bug Fix in libnfs (Partial)

**File:** `vendor/libnfs/lib/nfs_v3.c:3540-3565`

Added EOF tracking across the RPC chain within a single batch:
```c
/* Track EOF across the chain of RPCs for this batch. */
if (res->READDIR3res_u.resok.reply.eof) {
    nfsdir->eof = 1;
}
```

And removed the line that overwrote `nfsdir->eof` in the max_entries block.

**Status:** This fix compiles but doesn't solve the actual problem (see below).

### 2. Streaming READDIR Implementation in Rust

**File:** `src/walker/async_walker.rs:627-760`

Rewrote `process_directory()` to use streaming batches:
- Uses `opendir_names_only_at_cookie()` in a loop
- Dispatches 10K-entry batches to worker queue as they arrive
- First batch saved for self, subsequent batches pushed to injector
- Checks `is_eof()` to detect end of directory

**Status:** Code is complete but blocked by the libnfs EOF bug.

### 3. RPC Buffer Size Configuration

**File:** `src/nfs/connection.rs:745-760, 795-810`

Added `readdir_buffer_size()` option to `NfsConnectionBuilder`.

**Status:** Disabled - larger buffer sizes (32KB, 1MB) caused hangs with the VAST server. Using 8KB default.

## Current State

### Working
- Non-streaming READDIR via `opendir_names_only()` - returns exactly 1M entries correctly
- GETATTR pipelining - 512 concurrent requests per connection
- Full walker with `-w N` workers

### Broken
- Streaming READDIR via `opendir_names_only_at_cookie()` - never returns EOF, reads infinitely

## The Bug

**See:** `LIBNFS_STREAMING_READDIR_EOF_BUG.md` (create this from the bug report above)

### Symptoms
```
Batch 99: 10092 names, total=999035, eof=false
Batch 100: 10092 names, total=1009127, eof=false  <- Should be EOF here
Batch 101: 10092 names, total=1019219, eof=false
...continues to 1.7M+ entries from 1M file directory
```

### Key Observations
1. `nfs_readdir_is_eof()` always returns `false`
2. Server keeps returning ~10,092 entries per batch indefinitely
3. Cookies are unique 64-bit hash values (VAST-specific)
4. Cookie verifier stays constant at `1000001`
5. Non-streaming function works correctly

### Likely Cause
The server never returns `eof=1` in any RPC response because:
- We stop at `max_entries` before reaching natural EOF
- The final RPC in each batch has more data available, so it says `eof=0`
- When we resume with the next cookie, the server has more entries

The mystery: Why does the server return entries past the actual 1M file count? Either cookies are cycling or there's a server/client bug.

## Files to Focus On

### libnfs (C)
1. `vendor/libnfs/lib/nfs_v3.c:3473-3620` - `nfs3_opendir_names_only_at_cookie_cb()`
2. `vendor/libnfs/lib/nfs_v3.c:3676-3713` - `nfs3_opendir_names_only_at_cookie_async()`
3. `vendor/libnfs/lib/libnfs-sync.c:1417-1442` - sync wrapper
4. `vendor/libnfs/lib/libnfs.c:2057-2061` - `nfs_readdir_is_eof()`

### Rust (caller)
1. `src/walker/async_walker.rs:627-760` - `process_directory()` streaming loop
2. `src/nfs/connection.rs:421-460` - `opendir_names_only_at_cookie()` wrapper

## Debugging Suggestions

1. Add printf debugging in the C callback to see raw RPC responses:
   ```c
   fprintf(stderr, "RPC: entries=%d eof=%d cookie=%llu\n",
           count, res->READDIR3res_u.resok.reply.eof, last_cookie);
   ```

2. Test with a small directory (100 files) to see if EOF ever works

3. Compare code paths between working `nfs3_opendir_names_only_cb()` and broken `nfs3_opendir_names_only_at_cookie_cb()`

4. Check if VAST has specific behavior around cookie-based resumption

## Build Commands

```bash
# Build libnfs
cd vendor/libnfs/build && make -j$(nproc)

# Build nfs-walker
cargo build --release

# Set capability for privileged ports
sudo setcap 'cap_net_bind_service=+ep' target/release/nfs-walker

# Test streaming (currently broken)
./target/release/nfs-walker nfs://server/export/flat_1m --hfc -w 1 -p -v 2>&1 | grep "READDIR batch" | head -110

# Test non-streaming (works)
# Revert process_directory() to use opendir_names_only() instead
```

## Once Bug is Fixed

After the libnfs EOF bug is fixed:

1. Test streaming READDIR returns correct entry count
2. Verify EOF detection works
3. Test with multiple workers (`-w 4` or more)
4. Benchmark streaming vs non-streaming performance
5. Consider tuning `READDIR_STREAM_BATCH` size (currently 10,000)

## Performance Baseline

Non-streaming (current working mode):
- READDIR: 1M names in 12.65s (79,075 names/sec)
- GETATTR: ~22,000 stats/sec per worker
- Total 1M files: ~55 seconds with 1 worker

Expected with streaming:
- Overlap READDIR with GETATTR
- Start GETATTR after first 10K names instead of waiting for all 1M
- Estimated 10-15 second improvement on large directories

## Git Status

Modified files:
- `src/config.rs`
- `src/lib.rs`
- `src/main.rs`
- `src/nfs/connection.rs`
- `src/walker/async_walker.rs`
- `vendor/libnfs/lib/nfs_v3.c`

Untracked:
- `LIBNFS_STREAMING_BUG.md`
- This file
