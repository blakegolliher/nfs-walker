# Parallel GETATTR Implementation Design

## Overview

This document describes the design for a parallel GETATTR mode to improve performance on huge flat directories (millions of files in a single directory).

## Problem Statement

The current implementation uses NFSv3 READDIRPLUS which returns directory entries WITH attributes in a single RPC call. This works well for typical directory trees but becomes a bottleneck for huge flat directories because:

1. The server must stat() each file before returning the READDIRPLUS response
2. One worker thread is blocked processing the entire directory
3. No parallelism is possible within a single directory

An existing "skinny mode" implementation attempted to solve this using READDIR (names only) + GETATTRs, but it has bugs and only returns ~10K of 10M files.

## Goal

Implement a correct parallel GETATTR mode that:
1. Uses READDIR to quickly enumerate directory entries (no server-side stats)
2. Dispatches GETATTR calls across multiple worker connections in parallel
3. Correctly handles all entries without missing any
4. Achieves significant speedup for directories with >100K files

## Current Architecture

```
┌────────────────────────────────────────────────────────────┐
│  Work Queue (directories)                                  │
│  [/dir1, /dir2, /dir3, ...]                               │
└────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   ┌─────────┐         ┌─────────┐         ┌─────────┐
   │Worker 1 │         │Worker 2 │         │Worker N │
   │READDIRP │         │READDIRP │         │READDIRP │
   │ /dir1   │         │ /dir2   │         │ /dir3   │
   └─────────┘         └─────────┘         └─────────┘
```

**Problem**: If `/dir1` has 10M files, Worker 1 is blocked for the entire READDIRPLUS operation. No parallelism within the directory.

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  When a "huge directory" is detected (>100K entries):           │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  READDIR Producer Thread                                  │  │
│  │  - Uses opendir_names_only_at_cookie() in batches        │  │
│  │  - Streams (dir_path, entry_name, inode) to StatQueue    │  │
│  │  - Tracks cookie/verifier correctly                      │  │
│  │  - Detects EOF using protocol EOF flag                   │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              ▼                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  StatQueue (bounded MPMC channel)                         │  │
│  │  - Holds batches of entries needing GETATTR              │  │
│  │  - Provides backpressure to prevent memory explosion     │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                  │
│        ┌─────────────────────┼─────────────────────┐           │
│        ▼                     ▼                     ▼           │
│  ┌───────────┐         ┌───────────┐         ┌───────────┐    │
│  │GETATTR    │         │GETATTR    │         │GETATTR    │    │
│  │Worker 1   │         │Worker 2   │         │Worker N   │    │
│  │(own conn) │         │(own conn) │         │(own conn) │    │
│  └───────────┘         └───────────┘         └───────────┘    │
│        │                     │                     │           │
│        └─────────────────────┼─────────────────────┘           │
│                              ▼                                  │
│                     ┌───────────────┐                          │
│                     │  DB Writer    │                          │
│                     └───────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Details

### 1. Threshold Detection

```rust
// In process_directory(), after initial probe:
const PARALLEL_GETATTR_THRESHOLD: usize = 100_000;

if probe_indicates_large_flat_dir && estimated_entries > PARALLEL_GETATTR_THRESHOLD {
    return process_with_parallel_getattr(task, config, nfs, ...);
}
```

### 2. READDIR Streaming

The current implementation has these bugs:
- `MAX_BATCHES = 100` artificially limits to 1M entries
- Uses inode-based wrap detection (fragile, can have false positives)
- Checks `batch_count == 0` for EOF (wrong - should use protocol EOF flag)

**Correct approach:**

```rust
fn readdir_producer(
    nfs: &mut NfsConnection,
    path: &str,
    stat_queue_tx: Sender<Vec<(String, String, u64)>>,  // (dir_path, name, inode)
) -> Result<u64, NfsError> {
    let mut cookie: u64 = 0;
    let mut cookieverf: u64 = 0;
    let mut total_entries: u64 = 0;

    loop {
        // Open directory at current position
        let dir_handle = nfs.opendir_names_only_at_cookie(
            path,
            cookie,
            cookieverf,
            10_000,  // batch size
        )?;

        let mut batch: Vec<(String, String, u64)> = Vec::with_capacity(10_000);
        let mut last_cookie = cookie;
        let mut saw_entries = false;

        // Read all entries from this batch
        while let Some(entry) = dir_handle.readdir() {
            if entry.is_special() {
                continue;
            }
            saw_entries = true;
            last_cookie = entry.cookie;  // CRITICAL: save cookie from EACH entry

            batch.push((path.to_string(), entry.name.clone(), entry.inode));
        }

        // Send batch to stat workers
        if !batch.is_empty() {
            total_entries += batch.len() as u64;
            stat_queue_tx.send(batch)?;
        }

        // Get verifier for next batch
        let new_verifier = dir_handle.get_cookieverf();

        // CRITICAL: Check EOF flag from the protocol, not empty batch
        // The libnfs patch needs to expose this - currently it's internal
        if dir_handle.is_eof() {
            break;
        }

        // Update for next iteration
        cookie = last_cookie;
        cookieverf = new_verifier;

        // Safety: if we got no entries but not EOF, something is wrong
        if !saw_entries {
            warn!("Empty batch but no EOF - possible protocol issue");
            break;
        }
    }

    Ok(total_entries)
}
```

### 3. libnfs Patch Enhancement

The current libnfs patch doesn't expose the EOF flag from READDIR responses. Need to add:

```c
// In struct nfsdir (include/libnfs-private.h):
int eof;  // Set to 1 when READDIR response indicates EOF

// In nfs3_opendir_names_only_at_cookie_cb() (lib/nfs_v3.c):
nfsdir->eof = res->READDIR3res_u.resok.reply.eof;

// New function (lib/libnfs.c):
int nfs_readdir_is_eof(struct nfsdir *nfsdir) {
    return nfsdir->eof;
}

// Declaration (include/nfsc/libnfs.h):
EXTERN int nfs_readdir_is_eof(struct nfsdir *nfsdir);
```

Then expose in Rust (`src/nfs/connection.rs`):
```rust
impl NfsDirHandle {
    pub fn is_eof(&self) -> bool {
        unsafe { ffi::nfs_readdir_is_eof(self.handle) != 0 }
    }
}
```

### 4. Parallel GETATTR Workers

```rust
fn getattr_worker(
    worker_id: usize,
    nfs: NfsConnection,  // Each worker owns its connection
    stat_queue_rx: Receiver<Vec<(String, String, u64)>>,
    result_tx: Sender<DbEntry>,
    subdir_tx: Sender<DirTask>,
) {
    while let Ok(batch) = stat_queue_rx.recv() {
        for (dir_path, name, inode) in batch {
            let full_path = format!("{}/{}", dir_path, name);

            match nfs.stat(&full_path) {
                Ok(stat) => {
                    let entry_type = EntryType::from_mode(stat.mode);

                    // If it's a directory, queue it for traversal
                    if entry_type == EntryType::Directory {
                        subdir_tx.send(DirTask::new(full_path.clone(), ...));
                    }

                    // Send to DB writer
                    result_tx.send(DbEntry {
                        path: full_path,
                        name,
                        entry_type,
                        size: stat.size,
                        mtime: stat.mtime,
                        // ... other fields
                    });
                }
                Err(e) => {
                    warn!(path = %full_path, error = %e, "GETATTR failed");
                }
            }
        }
    }
}
```

### 5. Coordination

```rust
fn process_with_parallel_getattr(
    task: &DirTask,
    config: &WalkConfig,
    nfs_url: &NfsUrl,
    writer: &WriterHandle,
    queue_tx: &WorkQueueSender,
) -> WalkOutcome {
    // Create channels
    let (stat_tx, stat_rx) = crossbeam::channel::bounded(100);  // 100 batches max in flight
    let (result_tx, result_rx) = crossbeam::channel::bounded(10_000);
    let (subdir_tx, subdir_rx) = crossbeam::channel::bounded(1_000);

    // Spawn GETATTR workers (use most of the worker pool)
    let num_stat_workers = config.num_workers.saturating_sub(1).max(1);
    let stat_workers: Vec<_> = (0..num_stat_workers)
        .map(|i| {
            let rx = stat_rx.clone();
            let result_tx = result_tx.clone();
            let subdir_tx = subdir_tx.clone();
            let url = nfs_url.clone();

            thread::spawn(move || {
                let nfs = NfsConnectionBuilder::new(url).connect().unwrap();
                getattr_worker(i, nfs, rx, result_tx, subdir_tx);
            })
        })
        .collect();

    drop(stat_rx);  // Workers have their clones
    drop(result_tx);
    drop(subdir_tx);

    // Spawn result collector
    let writer_handle = writer.clone();
    let collector = thread::spawn(move || {
        let mut count = 0u64;
        while let Ok(entry) = result_rx.recv() {
            writer_handle.send_entry(entry).ok();
            count += 1;
        }
        count
    });

    // Spawn subdir forwarder
    let queue_tx_clone = queue_tx.clone();
    let subdir_forwarder = thread::spawn(move || {
        while let Ok(task) = subdir_rx.recv() {
            queue_tx_clone.try_send(task).ok();
        }
    });

    // Run READDIR producer on current thread's connection
    let total = readdir_producer(&mut nfs, &task.path, stat_tx)?;

    // Wait for all workers to finish
    for w in stat_workers {
        w.join().ok();
    }

    let entries = collector.join().unwrap_or(0);
    subdir_forwarder.join().ok();

    WalkOutcome::Success {
        path: task.path.clone(),
        entries: entries as usize,
        subdirs: 0,  // counted by subdir_forwarder
    }
}
```

## Files to Modify

1. **`vendor/libnfs/lib/nfs_v3.c`** - Add EOF flag storage in callback
2. **`vendor/libnfs/lib/libnfs.c`** - Add `nfs_readdir_is_eof()` function
3. **`vendor/libnfs/include/libnfs-private.h`** - Add `eof` field to nfsdir struct
4. **`vendor/libnfs/include/nfsc/libnfs.h`** - Declare `nfs_readdir_is_eof()`
5. **`src/nfs/connection.rs`** - Add `is_eof()` method to `NfsDirHandle`
6. **`src/walker/worker.rs`** - Implement `process_with_parallel_getattr()`
7. **`src/walker/mod.rs`** - Add new module for parallel getattr coordination
8. **`src/config.rs`** - Add config option for parallel_getattr_threshold

## Testing Strategy

### Unit Tests
- Create mock directory with known entries, verify all are returned
- Test cookie/verifier handling across batch boundaries
- Test EOF detection

### Integration Tests (against real NFS)
```bash
# Create test directory with 100K files
for i in $(seq 1 100000); do touch /nfs/test/file_$i; done

# Run nfs-walker with parallel getattr mode
nfs-walker nfs://server/test --parallel-getattr -o scan.db

# Verify count matches exactly
sqlite3 scan.db "SELECT COUNT(*) FROM entries"
```

### Performance Benchmarks
- Compare READDIRPLUS vs parallel GETATTR on 1M file directory
- Measure throughput (files/sec)
- Measure memory usage
- Test with varying worker counts

## Success Criteria

1. **Correctness**: All files enumerated (zero missed)
2. **Performance**: >2x throughput improvement on directories with >100K files
3. **Memory**: Bounded memory usage via backpressure
4. **Reliability**: No hangs, no crashes, proper error handling

## Known Pitfalls to Avoid

1. **Don't use inode for wrap detection** - inodes can repeat across batches in large dirs
2. **Don't use empty batch for EOF** - use protocol's EOF flag
3. **Don't limit batches artificially** - remove MAX_BATCHES constant
4. **Don't forget cookieverf** - some servers require it for cookie validation
5. **Don't assume GETATTR always succeeds** - files can be deleted between READDIR and GETATTR

## Analysis of Current Bug

The existing skinny mode in `src/walker/worker.rs` fails due to:

### Bug 1: Artificial Batch Limit
Location: `src/walker/worker.rs:541-543`
```rust
// Safety limit - 100 batches of 10k = 1M entries max
const MAX_BATCHES: u32 = 100;
```
This caps enumeration at 1M entries regardless of actual directory size.

### Bug 2: Fragile Wrap Detection
Location: `src/walker/worker.rs:569-581`
```rust
// Check for wrap-around by seeing if we encounter an inode from first batch
if !is_first_batch && first_batch_inodes.contains(&entry.inode) {
    wrap_detected = true;
    break;
}
```
Inodes can legitimately appear multiple times in different parts of a large directory, causing false-positive wrap detection.

### Bug 3: Wrong EOF Detection
Location: `src/walker/worker.rs:650-658`
```rust
// EOF when we get an empty batch
if batch_count == 0 {
    break;
}
```
An empty batch doesn't necessarily mean EOF. The protocol has an explicit EOF flag that should be checked instead.

## Why Parallel GETATTR Helps

The performance gain comes from parallelizing work on both client and server:

**Client-side:**
- 64 connections making concurrent GETATTR requests
- vs 1 connection waiting for READDIRPLUS batches

**Server-side:**
- Can handle 64 concurrent GETATTR requests in parallel
- vs sequential stat() calls within READDIRPLUS processing

The READDIR (names only) phase is fast because the server just reads the directory B-tree without touching each file's inode. The GETATTR phase can then be parallelized across many workers.
