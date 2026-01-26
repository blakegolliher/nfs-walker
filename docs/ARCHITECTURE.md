# NFS-Walker Architecture

High-performance NFS filesystem scanner using direct protocol access.

## Overview

nfs-walker achieves **22x speedup** over traditional tools (`find`, `du`, `ls -lR`) by:

1. **Direct NFS Protocol Access** - Bypasses the kernel NFS client using libnfs
2. **READDIRPLUS Optimization** - Single RPC returns directory listing + file attributes
3. **Work-Stealing Parallelism** - All workers actively process directories
4. **Dedicated Writer Thread** - Zero-contention database writes

## Performance

| Mode | Files | Time | Throughput |
|------|-------|------|------------|
| Standard | 2.1M files + 7.2K dirs | 37s | **56K files/sec** |
| Large flat directory | 10M files | 177s | **56K files/sec** |

## Architecture Diagram

```
                              ┌─────────────────────────────────────┐
                              │            CLI (main.rs)            │
                              │  - Argument parsing (config.rs)     │
                              │  - Signal handling (Ctrl+C)         │
                              │  - Progress display                 │
                              └──────────────┬────────────────────-─┘
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
│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐          │
│  │Worker 0 │   │Worker 1 │   │Worker 2 │   │Worker 3 │   │Worker N │          │
│  │ (NFS)   │   │ (NFS)   │   │ (NFS)   │   │ (NFS)   │   │ (NFS)   │          │
│  └────┬────┘   └────┬────┘   └────┬────┘   └────┬────┘   └────┬────┘          │
│       │              │              │              │              │            │
│       └──────────────┴──────────────┴──────────────┴──────────────┘            │
│                                     │                                          │
│                                     ▼                                          │
│                        ┌────────────────────────┐                              │
│                        │   Crossbeam Channel    │                              │
│                        │   (bounded, 100 cap)   │                              │
│                        └───────────┬────────────┘                              │
│                                    │                                           │
│                                    ▼                                           │
│                        ┌────────────────────────┐                              │
│                        │    Writer Thread       │                              │
│                        │  - Batch inserts       │                              │
│                        │  - 10K transactions    │                              │
│                        │  - WAL mode disabled   │                              │
│                        └───────────┬────────────┘                              │
│                                    │                                           │
└────────────────────────────────────┼───────────────────────────────────────────┘
                                     │
                                     ▼
                          ┌────────────────────┐
                          │   SQLite Database  │
                          │   (output.db)      │
                          └────────────────────┘
```

## Components

### 1. NFS Connection (`src/nfs/`)

**connection.rs** - Safe Rust wrapper around libnfs C library
- `NfsConnection` - Manages NFS context lifecycle
- `NfsConnectionBuilder` - Fluent API for connection setup
- READDIRPLUS support - Returns names + attributes in single RPC

**dns_resolver.rs** - Multi-IP load balancing
- Resolves hostname to all IPs
- Round-robin assignment to workers
- Health tracking with automatic failover

**types.rs** - Data structures
- `EntryType` - File, Directory, Symlink, etc.
- `NfsDirEntry` - Directory entry with attributes
- `DbEntry` - Database-ready entry format

### 2. Walker (`src/walker/`)

**simple.rs** - Core parallel walker (main implementation)
- Work-stealing queue using `crossbeam_deque`
- Each worker has own NFS connection
- Workers pop directories, READDIRPLUS, push subdirs
- Entries batched and sent to writer via channel

**async_walker.rs** - Alternative async implementation
- Tokio-based for async/await patterns
- Connection pooling

### 3. Database (`src/db/`)

**schema.rs** - SQLite schema and operations
- `entries` table - All filesystem entries
- `dir_stats` table - Per-directory statistics
- `walk_info` table - Scan metadata
- Optimized indexes for common queries

### 4. Configuration (`src/config.rs`)

CLI argument parsing with clap:
- NFS URL parsing (`nfs://server/export` or `server:/export`)
- Worker count, queue size, batch size
- Output format and path
- Timeout and retry settings

### 5. Error Handling (`src/error.rs`)

Structured error types:
- `WalkerError` - Top-level errors
- `NfsError` - Protocol errors
- `DbError` - Database errors
- `ConfigError` - Configuration validation

## Data Flow

1. **Initialization**
   - Parse CLI args, validate config
   - Resolve DNS for server (get all IPs)
   - Open SQLite database
   - Spawn writer thread

2. **Walking**
   - Push root directory to work queue
   - Workers steal work from queue
   - READDIRPLUS returns entries + attributes
   - Subdirectories pushed back to queue
   - Entry batches sent to writer channel

3. **Writing**
   - Writer receives batches via channel
   - Accumulates to 10K entries
   - Bulk INSERT in single transaction
   - Pragmas optimized for bulk loading

4. **Finalization**
   - All workers complete (queue empty, no active)
   - Writer drains remaining entries
   - Create indexes
   - Update walk_info metadata
   - VACUUM and optimize

## Key Design Decisions

### Why READDIRPLUS?

Traditional `find`/`ls` use:
```
READDIR  → get names
GETATTR  → stat each file (N separate RPCs!)
```

READDIRPLUS returns names AND attributes in one RPC, eliminating N-1 round trips.

### Why Work-Stealing?

- No central coordinator bottleneck
- Fast workers automatically help slow workers
- Better cache locality (workers process related dirs)
- Scales linearly with worker count

### Why Dedicated Writer Thread?

- Workers never block on database
- No mutex contention between workers
- Writer can batch efficiently
- Backpressure via bounded channel

### Why Disable WAL Mode?

For bulk loading, sequential writes to a single file are faster than WAL's dual-write approach. WAL is better for concurrent reads/writes, which doesn't apply during scanning.

## Threading Model

```
Main Thread
├── Progress Reporter (optional)
├── Signal Handler
│
├── Worker 0 ──┐
├── Worker 1 ──┼── Work-stealing pool
├── Worker 2 ──┤   Each has own NFS connection
└── Worker N ──┘
│
└── Writer Thread ── Single consumer, bulk DB writes
```

## Memory Usage

- **Bounded work queue** - Prevents memory explosion on deep trees
- **Streaming writes** - Entries written as discovered, not held in memory
- **~200MB typical** - Regardless of filesystem size

## Future Enhancements

See `INCREMENTAL_SCAN_SPEC.md` for planned incremental scanning support.
