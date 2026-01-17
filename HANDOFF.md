# NFS-Walker Cleanup Handoff Document

## Overview

This document captures the current state of the nfs-walker project after implementing HFC (High File Count) mode with work-stealing and async pipelining. A cleanup pass is needed to remove dead code, update documentation, and ensure consistency.

## What Was Built

### HFC Mode (`--hfc` flag)
A fast scanning mode optimized for directories with millions of files:

1. **Phase 1 - Name Collection**: Uses `opendir_names_only()` to collect all filenames without stat overhead (single NFS connection)

2. **Phase 2 - Parallel Stat with Work-Stealing**:
   - Names split into 10k batches, placed in crossbeam channel
   - Workers pull batches dynamically (fast workers get more work)
   - Each worker maintains persistent NFS connection
   - Each connection pipelines 128 concurrent stat requests via libnfs async API
   - DNS round-robin distributes workers across server IPs

3. **Performance achieved**: ~56k files/sec with 8 workers on 10M file directory

### Key Files Created/Modified

#### New Files:
- `src/nfs/async_stat.rs` - Async pipelined stat engine using libnfs async API
- `src/walker/fast_walker.rs` - HFC mode implementation with work-stealing

#### Modified Files:
- `src/main.rs` - Added `run_hfc()` entry point, HFC progress display
- `src/config.rs` - Added `--hfc` flag
- `src/nfs/connection.rs` - Added `with_ip()`, `resolve_dns()` for DNS round-robin
- `src/nfs/mod.rs` - Export new types
- `src/walker/mod.rs` - Export HFC types
- `src/db/schema.rs` - SQLite optimizations (synchronous=OFF, larger cache, removed AUTOINCREMENT)
- `src/db/writer.rs` - Multi-row INSERT batching (50 rows per statement)
- `src/progress.rs` - Made ProgressReporter Clone
- `build.rs` - Added bindgen for async NFS functions

## Cleanup Tasks

### 1. Dead Code Removal

#### `src/walker/worker.rs`:
- `PARALLEL_READDIR_THRESHOLD` constant - unused (line 38)
- `stat_tx` parameter - unused (line 358)
- `continue_with_skinny_full()` function - unused (line 591)

#### `src/walker/parallel_stat.rs`:
- May be entirely unused now - verify and potentially remove

#### `src/walker/parallel_readdir.rs`:
- Contains old scout-based approach that was abandoned
- Check if any parts are still used, otherwise remove

### 2. Unused Imports

Run `cargo fix --lib -p nfs-walker` to auto-fix warnings, then manually review all files.

### 3. Documentation Updates

#### README.md - Needs complete update for:
- New `--hfc` flag and when to use it
- Performance numbers (56k files/sec for HFC, 56k files/sec for recursive)
- Usage examples for both modes
- Architecture overview
- When to use HFC vs standard mode

#### CLI Help:
- Verify `--hfc` help text is clear about use case
- Ensure `-v` verbose flag is documented

### 4. Files to Potentially Remove

These files may be obsolete after HFC implementation:
- `src/walker/parallel_stat.rs` - Old parallel stat approach
- `src/walker/parallel_readdir.rs` - Old scout-based approach
- `design-skinny.txt` - Old design doc (or archive it)

Verify none of these are imported/used before removing.

### 5. Code Consistency

- Ensure logging levels are consistent (info vs debug)
- Verify `-v` flag properly controls verbosity everywhere
- Check that HFC mode respects all relevant config options

## Architecture Summary (Current)

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI (main.rs)                        │
│  --hfc → run_hfc()    (otherwise) → run_sync()/run_async() │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────────┐
│     HFC Mode            │     │    Standard Recursive Mode  │
│   (FastWalker)          │     │  (WalkCoordinator/Async)    │
├─────────────────────────┤     ├─────────────────────────────┤
│ 1. collect_names()      │     │ - Work queue of directories │
│    - Single NFS conn    │     │ - Workers process dirs      │
│    - opendir_names_only │     │ - READDIRPLUS for entries   │
│                         │     │ - Recursive discovery       │
│ 2. parallel_stat_inner()│     │                             │
│    - Work-stealing queue│     │                             │
│    - 10k batches        │     │                             │
│    - N worker threads   │     │                             │
│    - 128 pipelined stats│     │                             │
│      per connection     │     │                             │
└─────────────────────────┘     └─────────────────────────────┘
              │                               │
              └───────────────┬───────────────┘
                              ▼
              ┌─────────────────────────────┐
              │     BatchedWriter           │
              │  - Crossbeam channel        │
              │  - Multi-row INSERTs (50)   │
              │  - 10k batch transactions   │
              │  - SQLite WAL mode          │
              │  - synchronous=OFF          │
              └─────────────────────────────┘
```

## Files to Review

| File | Action | Notes |
|------|--------|-------|
| `src/walker/worker.rs` | Remove dead code | Has 3 compiler warnings |
| `src/walker/parallel_stat.rs` | Check if used | Likely obsolete |
| `src/walker/parallel_readdir.rs` | Check if used | Old scout approach, likely obsolete |
| `src/walker/fast_walker.rs` | Review | Main HFC implementation - keep |
| `src/nfs/async_stat.rs` | Review | Async pipelining - keep |
| `src/config.rs` | Update docs | Verify all flags documented |
| `src/main.rs` | Review | Entry points |
| `README.md` | Rewrite | Update for HFC mode |
| `design-skinny.txt` | Archive/Remove | Old design notes |

## Testing Commands

```bash
# HFC mode (flat directory with millions of files)
sudo ./target/release/nfs-walker nfs://server/export/flat_dir --hfc -w 8 -p -o output.db

# Standard recursive mode (mixed directories/files)
sudo ./target/release/nfs-walker nfs://server/export -w 8 -p -o output.db

# With verbose logging
sudo ./target/release/nfs-walker nfs://server/export --hfc -w 8 -p -v -o output.db
```

## Performance Baselines

| Mode | Files | Time | Rate | Notes |
|------|-------|------|------|-------|
| HFC (8 workers) | 10M | 177s | 56,497/s | Optimal worker count |
| HFC (16 workers) | 10M | 223s | 44,839/s | Server saturated |
| Recursive | 2.1M + 7.2k dirs | 37.5s | 56,287/s | Mixed workload |

**Key finding**: 8 workers outperforms 16 workers. Server-side saturation is the limit, not client.

## Git Status (at handoff)

Modified files (need commit):
- build.rs
- src/config.rs
- src/main.rs
- src/nfs/connection.rs
- src/nfs/mod.rs
- src/nfs/types.rs
- src/progress.rs
- src/walker/coordinator.rs
- src/walker/mod.rs
- src/walker/queue.rs
- src/walker/worker.rs
- src/db/schema.rs
- src/db/writer.rs

New untracked files:
- src/nfs/async_stat.rs
- src/walker/fast_walker.rs
- HANDOFF.md

Potentially obsolete (verify before removing):
- src/walker/parallel_readdir.rs
- src/walker/parallel_stat.rs
- design-skinny.txt

## Cleanup Checklist

- [ ] Run `cargo fix --lib -p nfs-walker` to auto-fix warnings
- [ ] Remove `PARALLEL_READDIR_THRESHOLD` from worker.rs
- [ ] Fix unused `stat_tx` parameter in worker.rs
- [ ] Remove `continue_with_skinny_full()` from worker.rs
- [ ] Check if `parallel_stat.rs` is used anywhere - if not, remove
- [ ] Check if `parallel_readdir.rs` is used anywhere - if not, remove
- [ ] Update README.md with new usage and architecture
- [ ] Verify all CLI flags have proper help text
- [ ] Run full test suite: `cargo test`
- [ ] Test HFC mode manually on real data
- [ ] Test recursive mode manually on real data
- [ ] Archive or remove `design-skinny.txt`
- [ ] Commit all changes with descriptive message

## Notes for New Instance

1. The HFC mode is working and performant - don't change the core algorithm
2. Focus on cleanup, not new features
3. The bottleneck is NFS server throughput, not client-side code
4. 8 workers is typically optimal, more workers can actually be slower
5. SQLite writing is NOT a bottleneck - the optimizations helped but weren't necessary
