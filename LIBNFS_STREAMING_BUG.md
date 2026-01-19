# libnfs Streaming READDIR EOF Bug

## Summary

The `nfs_opendir_names_only_at_cookie()` function in our patched libnfs has a bug where it never correctly reports EOF when the `max_entries` parameter is used. This causes infinite loops when trying to stream directory listings in batches.

## Symptoms

When reading a directory with 1,000,000 files using batched streaming:
- Each batch correctly returns ~50,000 entries
- Cookies advance correctly between batches (different values each time)
- But `nfs_readdir_is_eof()` always returns `false`
- Result: The caller reads 2M+ entries from a 1M file directory before giving up

## Debug Output Showing the Bug

```
Worker 0 READDIR batch: path=/flat_1m input_cookie=0 input_verf=0 -> 50039 names, last_cookie=5983589415623065602 new_verf=1000001 eof=false
Worker 0 READDIR batch: path=/flat_1m input_cookie=5983589415623065602 input_verf=1000001 -> 50112 names, last_cookie=2198899229781393411 new_verf=1000001 eof=false
Worker 0 READDIR batch: path=/flat_1m input_cookie=2198899229781393411 input_verf=1000001 -> 50112 names, last_cookie=2312499757550927876 new_verf=1000001 eof=false
... (continues past 40 batches = 2M+ entries, eof never becomes true)
```

Note: The directory has exactly 1,000,000 files. After 20 batches (~1M entries), EOF should be true.

## Root Cause Analysis

The bug is in `vendor/libnfs/lib/nfs_v3.c` in the `nfs3_opendir_names_only_at_cookie_cb()` callback function.

### Current Code (lines ~3540-3562):

```c
/* Check if we've reached the max_entries limit */
if (data->max_entries > 0 &&
    nfsdir_count_entries(nfsdir) >= data->max_entries) {
    /* We've reached the limit, return what we have */
    if (res->READDIR3res_u.resok.dir_attributes.attributes_follow) {
        fattr3_to_nfs_attr(&nfsdir->attr,
            &res->READDIR3res_u.resok.dir_attributes.post_op_attr_u.attributes);
    }

    /* Store cookieverf for next batch */
    memcpy(&nfsdir->cookieverf, res->READDIR3res_u.resok.cookieverf,
           sizeof(cookieverf3));

    /* Store EOF flag from protocol response */
    nfsdir->eof = res->READDIR3res_u.resok.reply.eof;  // <-- THE BUG

    nfsdir->current = nfsdir->entries;
    data->cb(0, nfs, nfsdir, data->private_data);
    data->continue_data = NULL;
    free_nfs_cb_data(data);
    return;
}
```

### The Problem

When `max_entries` is used (e.g., 50,000), the function internally makes multiple READDIR RPCs to collect entries:

1. Each READDIR RPC returns entries that fit in `readdir_dircount` (default 8192 bytes, ~4000-8000 entries)
2. The callback chains RPCs until `entry_count >= max_entries`
3. When we hit max_entries, we set `nfsdir->eof` from the **current RPC's** EOF flag
4. But the current RPC said `eof=0` because there were more entries to read
5. Even if we've read past the actual end of the directory (in subsequent calls), each batch's "current RPC" never sees EOF because we stop at max_entries first

### Why Cookies Work But EOF Doesn't

- Cookies are stored per-entry and advance correctly
- Each call starts from the correct cookie position
- But EOF is set from the RPC that was in-flight when we hit max_entries
- That RPC almost never has `eof=1` because we're limiting before natural EOF

## Expected Behavior

For a 1,000,000 file directory with max_entries=50,000:
- Batches 1-19: Return 50,000 entries each, `eof=false` (correct)
- Batch 20: Return remaining ~50,000 entries, `eof=true` (currently broken - returns `eof=false`)

## Files to Modify

1. `vendor/libnfs/lib/nfs_v3.c` - The `nfs3_opendir_names_only_at_cookie_cb()` function
2. Possibly `vendor/libnfs/include/libnfs-private.h` - If we need to track additional state

## Suggested Fix Approaches

### Option A: Track Actual EOF Across RPCs

Store whether we've seen EOF from the server in the callback chain, not just the current RPC:

```c
// In the callback, when server returns eof=1, remember it
if (res->READDIR3res_u.resok.reply.eof) {
    nfsdir->server_said_eof = 1;  // New field to track this
}

// When returning due to max_entries limit:
nfsdir->eof = nfsdir->server_said_eof;
```

### Option B: Check Entry Count vs Expected

If we request more entries than we receive and server didn't return more, we're at EOF:

```c
// When hitting max_entries limit:
// If the last RPC returned fewer entries than requested, we're likely at EOF
// OR if server's eof flag is set
nfsdir->eof = res->READDIR3res_u.resok.reply.eof ||
              (entries_from_last_rpc < expected_from_rpc);
```

### Option C: Request One More Entry

When returning a batch, peek ahead by requesting 1 more entry to see if we're at EOF. This has overhead but is reliable.

## How to Test

1. Build libnfs: `cd vendor/libnfs/build && make -j$(nproc)`
2. Build nfs-walker: `cargo build --release`
3. Set capability: `sudo setcap 'cap_net_bind_service=+ep' target/release/nfs-walker`
4. Test with streaming code (need to re-enable in `src/walker/async_walker.rs`)

Test command:
```bash
./target/release/nfs-walker nfs://server/export/flat_1m --hfc -w 1 -p -v 2>&1 | head -30
```

Expected: ~20 READDIR batches, last one shows `eof=true`
Current: 40+ READDIR batches, `eof=false` forever

## Relevant Code Locations

- `vendor/libnfs/lib/nfs_v3.c:3480-3610` - The callback function with the bug
- `vendor/libnfs/lib/nfs_v3.c:3667-3700` - The async entry point
- `vendor/libnfs/lib/libnfs.c:2057-2061` - The `nfs_readdir_is_eof()` function (just returns `nfsdir->eof`)
- `vendor/libnfs/include/libnfs-private.h:820-830` - The `nfsdir` struct definition
- `src/walker/async_walker.rs:670-730` - The Rust code that calls the streaming API (currently disabled)

## Context

This is part of an NFS filesystem walker that needs to efficiently scan directories with millions of files. The streaming approach would allow overlapping READDIR and GETATTR operations for better performance, but it requires correct EOF detection to know when to stop reading.
