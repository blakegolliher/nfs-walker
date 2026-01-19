# Bug Report: `nfs_opendir_names_only_at_cookie()` EOF Detection Failure

## Summary

The `nfs_opendir_names_only_at_cookie()` function in our patched libnfs never correctly returns EOF when streaming through a directory in batches. This causes infinite loops reading past the actual directory size.

## Observed Behavior

Testing against a VAST NFS server with a 1,000,000 file directory (`flat_1m`):

```
Batch 99: 10092 names, total=999035, cookie=7720626206197940324, eof=false
Batch 100: 10092 names, total=1009127, cookie=3611954886188466277, eof=false
Batch 101: 10092 names, total=1019219, cookie=5024687904652263526, eof=false
...
Batch 170: 10092 names, total=1715567, cookie=466113486166950059, eof=false
```

- Directory has exactly 1,000,000 files
- We read 1.7M+ entries before manual abort
- `eof=false` on EVERY batch (libnfs's `nfs_readdir_is_eof()` always returns 0)
- Each batch returns ~10,092 entries (slightly over the 10,000 `max_entries` requested)
- Cookies are all unique, large 64-bit values (VAST uses hash-based cookies)
- Cookie verifier stays constant at `1000001`

## Expected Behavior

Around batch 100, one of these should happen:
1. `is_eof()` returns `true`, OR
2. A batch returns fewer than `max_entries` entries, OR
3. A batch returns 0 entries

## Working Comparison

The non-streaming `nfs_opendir_names_only()` function works correctly:
```
READDIR: /flat_1m -> 1000000 names in 12.65s (79075 names/sec)
```

It reads exactly 1M entries and stops.

## Root Cause Analysis

### Hypothesis 1: EOF flag not propagated from final RPC

In `nfs3_opendir_names_only_at_cookie_cb()`, when we hit `max_entries`, we might be setting `nfsdir->eof` from an intermediate RPC that said `eof=0`, not the final one.

**Current code path:**
1. Request 10,000 entries via `max_entries` parameter
2. Chain ~3 RPCs (each returns ~3,000-4,000 entries from 8KB buffer)
3. After 3rd RPC, we have ~10,092 entries
4. Check: `count >= max_entries` → TRUE
5. Set `nfsdir->eof = res->eof` (from 3rd RPC, which is `0` because server had more)
6. Return

**Problem:** The 3rd RPC says `eof=0` because the server would have returned more entries. We stop early due to `max_entries`, but record `eof=0`.

### Hypothesis 2: Server-side cookie cycling

VAST uses hash-based cookies (not sequential). When we pass a cookie that's "past" all entries:
- The server might return entries starting from some default position
- Or the hash space might wrap around

Evidence: Cookies are random-looking 64-bit values, not sequential positions.

### Hypothesis 3: Incorrect cookie/verifier handling

We might be mishandling the cookie or verifier between batches, causing the server to restart from a wrong position.

## Relevant Code Locations

1. **Callback function:** `vendor/libnfs/lib/nfs_v3.c:3473-3620`
   - `nfs3_opendir_names_only_at_cookie_cb()` - handles RPC responses

2. **Continue function:** `vendor/libnfs/lib/nfs_v3.c:3622-3674`
   - `nfs3_opendir_names_only_at_cookie_continue_internal()` - initiates RPCs

3. **Entry point:** `vendor/libnfs/lib/nfs_v3.c:3676-3713`
   - `nfs3_opendir_names_only_at_cookie_async()` - async entry point

4. **Sync wrapper:** `vendor/libnfs/lib/libnfs-sync.c:1417-1442`
   - `nfs_opendir_names_only_at_cookie()` - sync wrapper

5. **EOF check:** `vendor/libnfs/lib/libnfs.c:2057-2061`
   - `nfs_readdir_is_eof()` - just returns `nfsdir->eof`

6. **Working comparison:** `vendor/libnfs/lib/nfs_v3.c:3280-3400`
   - `nfs3_opendir_names_only_cb()` - the working non-streaming version

## Suggested Debugging Steps

1. **Add RPC-level logging** in `nfs3_opendir_names_only_at_cookie_cb()`:
   ```c
   fprintf(stderr, "RPC response: entry_count=%d, eof=%d, last_cookie=%llu\n",
           entry_count_this_rpc,
           res->READDIR3res_u.resok.reply.eof,
           (unsigned long long)last_cookie);
   ```

2. **Check if server ever returns eof=1** in the raw RPC response

3. **Compare with working function** - trace what `nfs3_opendir_names_only_cb()` does differently

4. **Test with smaller directory** - does EOF work for a 100-file directory?

5. **Test cookie boundaries** - what happens when we request entries starting from the last valid cookie?

## Potential Fixes

### Fix A: Track server EOF across RPC chain (already attempted)

```c
// After parsing entries, BEFORE checking max_entries:
if (res->READDIR3res_u.resok.reply.eof) {
    nfsdir->eof = 1;  // Remember we've seen EOF
}
```

**Status:** Implemented at line 3546 but doesn't fix the issue - suggests the server never returns `eof=1` in any RPC while we have `max_entries` set.

### Fix B: Detect EOF by underflow

```c
// If we got fewer entries than the RPC buffer could hold, we're at EOF
if (entries_this_rpc < expected_entries_per_rpc) {
    nfsdir->eof = 1;
}
```

**Problem:** We're getting ~10,092 entries per batch (more than requested), so underflow never triggers.

### Fix C: Compare entry count to RPC request

When `max_entries` is set and we return exactly that many (or slightly over), check if the FINAL RPC in the chain had `eof=1`:

```c
// In the max_entries return path:
// Only say "not EOF" if the last RPC explicitly had more data
nfsdir->eof = (last_rpc_entry_count < readdir_dircount_entries) ||
              res->READDIR3res_u.resok.reply.eof;
```

### Fix D: Request one extra entry as EOF probe

When returning a batch, peek ahead by requesting 1 more entry:
```c
// After collecting max_entries, do one more RPC with count=1
// If it returns 0 entries or eof=1, set nfsdir->eof = 1
```

**Downside:** Extra RPC overhead per batch.

### Fix E: Don't rely on eof flag - use empty batch detection

In the Rust caller, detect EOF when a batch returns 0 entries:
```rust
if batch_size == 0 {
    break;  // EOF
}
```

**Problem:** With the current bug, we never get 0 entries - we keep getting 10K+.

### Fix F: Debug server behavior

The real question: why does the VAST server keep returning 10K+ entries per batch even after we've read 1.7M entries from a 1M file directory?

Possibilities:
1. Cookie space is wrapping/cycling
2. Server bug
3. We're misusing the cookie/verifier API

Need RPC-level traces to determine which.

## Test Commands

```bash
# Build libnfs with changes
cd vendor/libnfs/build && make -j$(nproc)

# Build nfs-walker
cargo build --release

# Set capability
sudo setcap 'cap_net_bind_service=+ep' target/release/nfs-walker

# Test streaming (currently broken)
./target/release/nfs-walker nfs://server/export/flat_1m --hfc -w 1 -p -v 2>&1 | grep "READDIR batch" | head -110

# Test non-streaming (works)
# Temporarily revert to opendir_names_only() in async_walker.rs
```

## Key Differences: Working vs Broken

### Working: `nfs3_opendir_names_only_cb()` (line ~3280)
- No `max_entries` parameter
- Chains RPCs until server returns `eof=1`
- Returns ALL entries in directory at once

### Broken: `nfs3_opendir_names_only_at_cookie_cb()` (line ~3473)
- Has `max_entries` parameter
- Stops when entry count >= max_entries
- Supposed to allow resuming from a cookie
- Never sees `eof=1` because we stop early

The fundamental issue: when using `max_entries`, we stop before the server naturally reaches EOF. The server's `eof` flag only means "no more entries after this RPC", not "no more entries in directory".

## Possible Architecture Change

Instead of relying on the server's `eof` flag, track total entries externally:
```rust
// In Rust caller
let mut seen_cookies = HashSet::new();
loop {
    let batch = nfs.opendir_names_only_at_cookie(...);
    for entry in batch {
        if !seen_cookies.insert(entry.cookie) {
            // Duplicate cookie - we've cycled, this is EOF
            return;
        }
    }
}
```

This would detect cookie cycling and stop appropriately.
