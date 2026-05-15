# NFS-Walker Architecture

High-performance NFS filesystem scanner that streams directly to
sharded Parquet output. Designed to saturate VAST-class clusters from a
single host: 690 K files/sec on the 810 M-file prod bench.

## Overview

nfs-walker wins over traditional tools (`find`, `du`, `ls -lR`) by:

1. **Direct NFS protocol access** — bypasses the kernel NFS client via libnfs.
2. **READDIRPLUS** — single RPC returns names AND attributes.
3. **Work-stealing parallelism** — every worker actively reads dirs; no coordinator bottleneck.
4. **Sharded Parquet writers** — N independent writer threads, each with its own Arrow builder + ZSTD encoder. The path keyspace is split via `gxhash % N`, so writers never contend.
5. **mimalloc global allocator** — sidesteps Zig's SmpAllocator NULL-return-at-high-thread-count that the static-musl build would otherwise hit.

## Performance

Reference run: 810.8 M-file tree on `se-var-n8` (128c / 376 GiB / Rocky
9.6), 230 workers, 32 writer shards, ZSTD-3, pipeline-depth 16. RC=0.

| Variant         |   Wall (s) | Rate (K ent/s) | Output bytes |
|-----------------|-----------:|---------------:|-------------:|
| **parquet-conc**|    1175.51 |        **690** |     49.1 GiB |
| parquet-snappy  |    1202.82 |            674 |     58.8 GiB |

See `tasks/parquet-experiment-review.md` for the full bench writeup.

## Architecture Diagram

```text
                              ┌─────────────────────────────────────┐
                              │            CLI (main.rs)            │
                              │  - Argument parsing (config.rs)     │
                              │  - Signal handling (Ctrl+C)         │
                              │  - Progress display                 │
                              └──────────────┬──────────────────────┘
                                             │
                                             ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                         SimpleWalker (walker/simple.rs)                        │
│                                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Work-Stealing Queue (crossbeam_deque)                │   │
│  │                         Injector + Worker Local Queues                  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│       │              │              │              │              │            │
│       ▼              ▼              ▼              ▼              ▼            │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐           │
│  │Worker 0 │   │Worker 1 │   │Worker 2 │   │Worker 3 │   │Worker N │           │
│  │READDIR+ │   │READDIR+ │   │READDIR+ │   │READDIR+ │   │READDIR+ │           │
│  └────┬────┘   └────┬────┘   └────┬────┘   └────┬────┘   └────┬────┘           │
│       │              │              │              │              │            │
│       └──────────────┴── ShardedSender (path_to_shard = gxhash % N) ───────────┤
│                                     │                                          │
│                ┌────────┬───────────┼───────────┬────────┐                     │
│                ▼        ▼           ▼           ▼        ▼                     │
│         shard-0 chan  shard-1   shard-2     shard-N-2  shard-N-1               │
│                │        │           │           │        │                     │
│                ▼        ▼           ▼           ▼        ▼                     │
│         parquet-w0    parquet-w1  parquet-w2  ...     parquet-w(N-1)           │
│                │        │           │           │        │                     │
└────────────────┼────────┼───────────┼───────────┼────────┼─────────────────────┘
                 ▼        ▼           ▼           ▼        ▼
       scans/<id>/part-r00-*.parquet  ...  part-r(N-1)-*.parquet
                                    │
                                    ▼
                         scans/<id>/metadata.json
```

## Components

### 1. NFS Connection (`src/nfs/`)

**connection.rs** — safe Rust wrapper around libnfs C library
- `NfsConnection` — manages NFS context lifecycle
- `NfsConnectionBuilder` — fluent API for connection setup
- READDIRPLUS support — returns names + attributes in single RPC
- Optional `--server-ips` override bypasses DNS round-robin when the
  resolver caches a single A record from a multi-VIP pool.

**dns_resolver.rs** — multi-IP load balancing
- Resolves hostname to all IPs
- Round-robin assignment to workers
- Failure-count threshold blacklist (3 consecutive flakes per VIP)

**types.rs** — data structures
- `EntryType` — File, Directory, Symlink, etc.
- `NfsDirEntry` — directory entry with attributes
- `DbEntry` — writer-side record fed into Arrow builders

### 2. Walker (`src/walker/`)

**simple.rs** — core parallel walker
- Work-stealing queue using `crossbeam_deque`
- Each worker owns its own NFS connection
- Workers pop directories, READDIRPLUS, push subdirs
- Per-entry routing to writer shards via `ShardedSender`
- Big-directory continuation: any one worker that crosses
  `--big-dir-split-after` hands its continuation cookie back to the
  deque so other workers can resume in parallel

**sharding.rs** — `path_to_shard(path, N) = gxhash(path) % N`
- Deterministic across processes (seed pinned to 0)
- Used by `ShardedSender` to fan walker output across writer shards

### 3. Parquet output (`src/parquet/`)

**direct_writer.rs** — per-shard streaming writer
- Owns one Arrow `RowBuilder` and one `ArrowWriter`
- Rotates to a new part file once
  `bytes_written >= --parquet-file-size-mb`
- Tail-flush + close on channel drop
- `InProgressPart` RAII guard removes half-written files on `Err`
  (unwind path; abort path leaves orphan part files but no
  metadata.json that lies about completeness)

**builder.rs** — shared row builder (`DbEntry` → Arrow columns)
- 24-column schema, scan_id dictionary-encoded
- borrows `parent_path` to avoid per-row clones

**schema.rs** — canonical Arrow schema definition

After all shards drain, the main thread emits
`scans/<scan_id>/metadata.json` with the union of part files plus a
running row count.

### 4. Configuration (`src/config.rs`)

CLI argument parsing with clap:
- NFS URL parsing (`nfs://server/export` or `server:/export`)
- Worker count, queue size, batch size
- Output path, writer shards (default 32, cap 32)
- Parquet tunables: compression, row-group size, file rotation
  threshold, per-shard channel depth
- Timeout / retry / `--server-ips` override

### 5. Error handling (`src/error.rs`)

Structured `thiserror`-derived enums:
- `WalkerError` — top level
- `NfsError` — protocol errors
- `ParquetError` — writer/encoder errors
- `ConfigError` — validation
- `ServerError` — analytics dashboard (feature-gated)

## Data flow

1. **Initialization**
   - Parse CLI args, validate config
   - Resolve DNS for the NFS server (gather all IPs) or use `--server-ips`
   - Create scan output directory `scans/<scan_id>/`
   - Spawn N parquet writer threads

2. **Walking**
   - Push root directory to the work-stealing queue
   - Workers steal directories, run READDIRPLUS (pipelined when
     `--pipeline-depth > 0`)
   - Each entry → `ShardedSender` → shard channel → writer
   - Subdirectories pushed back to the deque

3. **Writing**
   - Per-shard writer drains its channel, appends to Arrow builders
   - Row group flush at `parquet_row_group_size` rows
   - Part-file rotation at `parquet_file_size_bytes` bytes
   - On end-of-channel: tail flush + footer write

4. **Finalization**
   - All workers join (queue empty + no active workers)
   - All writer threads join, returning per-shard summaries
   - Main thread writes `metadata.json` listing the union of part files
   - On any shard error, `metadata.json` is not written so partial
     scans cannot be silently consumed

## Key design decisions

### Why READDIRPLUS?

Traditional `find` / `ls -lR` use:
```
READDIR  → get names
GETATTR  → stat each file (N separate RPCs!)
```
READDIRPLUS returns names AND attributes in one RPC, eliminating
nearly all of the round-trip cost.

### Why work-stealing?

- No central coordinator bottleneck
- Idle workers steal from busy ones, so one giant directory doesn't
  serialize the whole scan
- Better cache locality (workers process related dirs)
- Scales linearly with worker count

### Why sharded Parquet writers (not a single writer)?

A single writer becomes the bottleneck once the walker is fast enough
to feed it >300 K entries/sec. With N shards:
- N independent Arrow builders flush in parallel
- N independent ZSTD encoders use N cores
- The path-keyspace split is hash-deterministic so any reader can
  recompute shard ownership without consulting a sidecar map

### Why mimalloc as the global allocator?

The static-musl build (cargo-zigbuild) links to Zig's `SmpAllocator`
via Zig's libc shim, which returns `NULL` for ~30 KB allocations once
thread count crosses ~250 (we observed it reliably at 230 workers + 32
parquet writers). Rooting Rust's global allocator at mimalloc routes
every Rust-side allocation (Vec, hashbrown, parquet dict encoders)
through a thread-aware allocator. libnfs / libzstd / snappy still hit
the system malloc, but their allocation volume is far below the
failing threshold.

## Threading model

```text
Main Thread
├── Progress Reporter (optional)
├── Signal Handler
├── Progress logger snapshot thread
│
├── Worker 0 ──┐
├── Worker 1 ──┼── Work-stealing pool
├── Worker 2 ──┤   Each has its own NFS connection
└── Worker N ──┘
│
└── Parquet Writer 0..N-1 ── One Arrow builder per writer
```

## Memory usage

- **Bounded work queue** — prevents memory explosion on deep trees
- **Bounded shard channels** — caps in-flight memory at roughly
  `parquet_channel_depth × writer_shards × batch_size × ~200 B`
- **Per-shard Arrow builder** — sized to the row-group threshold
- Typical peak: ~2 GiB on default config (channel-depth 64, 32 shards)
