# nfs-walker

High-performance NFS filesystem scanner. Scans millions of files directly via NFS protocol, bypassing the kernel client.

## Features

- **Fast**: 48,000+ files/sec using READDIRPLUS and parallel workers
- **Direct NFS Protocol**: Bypasses kernel NFS client for maximum throughput
- **RocksDB Storage**: Write-optimized for large scans, with built-in analytics
- **Analytics Dashboard**: Web UI with 36 pre-built queries across 9 categories
- **SQLite Export**: Convert to SQLite for complex SQL queries
- **Content Analysis**: Optional checksum (gxhash) and file type detection (magic bytes)
- **Duplicate Detection**: Find duplicate files by content hash across the entire filesystem
- **Memory Efficient**: Periodic flushing keeps memory bounded

## Quick Start

```bash
# Scan an NFS export
nfs-walker nfs://server/export -o scan.rocks -w 16

# View statistics
nfs-walker stats scan.rocks

# Query by extension, largest files, etc.
nfs-walker stats scan.rocks --by-extension -n 20
nfs-walker stats scan.rocks --largest-files -n 10
nfs-walker stats scan.rocks --largest-dirs -n 10

# Scan with content analysis (checksum + file type detection)
nfs-walker nfs://server/export -o scan.rocks -c -t

# Find duplicate files and analyze file types
nfs-walker stats scan.rocks --duplicates
nfs-walker stats scan.rocks --by-file-type

# Convert to SQLite for complex queries
nfs-walker convert scan.rocks scan.db
sqlite3 scan.db "SELECT path, size FROM entries ORDER BY size DESC LIMIT 10"
```

## Installation

```bash
# Build portable binary with RocksDB (requires Docker or Podman)
make docker-rocky

# Binary output: ./build/nfs-walker-rocks
```

See [docs/BUILDING.md](docs/BUILDING.md) for detailed build instructions and alternative methods.

## Usage

### Scanning

```bash
# Basic scan
nfs-walker nfs://server/export -o scan.rocks

# With progress and 16 workers
nfs-walker nfs://192.168.1.100/data -w 16 -o scan.rocks

# Directories only (smaller output)
nfs-walker nfs://server/export --dirs-only -o dirs.rocks

# With exclusions
nfs-walker nfs://server/data --exclude ".snapshot" --exclude ".zfs" -o scan.rocks

# Limit depth
nfs-walker nfs://server/export -d 3 -o shallow.rocks
```

### Content Analysis

nfs-walker can optionally read file contents during scan to compute checksums and detect file types.

```bash
# Detect file types via magic bytes (reads first 8KB per file)
nfs-walker nfs://server/export -o scan.rocks -t

# Compute gxhash checksum for each file (reads full file content)
nfs-walker nfs://server/export -o scan.rocks -c

# Both checksum and file type detection
nfs-walker nfs://server/export -o scan.rocks -c -t

# Limit checksum to files under 100MB (default: 1GB)
nfs-walker nfs://server/export -o scan.rocks -c --max-checksum-size 104857600
```

**Schema additions:** Two nullable columns are added to each entry:
- `checksum` — 128-bit gxhash hex string (32 chars), set when `-c` is used
- `file_type` — MIME type string (e.g. `application/pdf`, `image/png`), set when `-t` is used

These fields are `NULL` when the corresponding flag is not enabled, or when the file exceeds `--max-checksum-size` (for checksum) or has unrecognizable magic bytes (for file type).

### Querying Results

**RocksDB** (fast, built-in queries):
```bash
nfs-walker stats scan.rocks                    # Overview
nfs-walker stats scan.rocks --by-extension     # Files by type
nfs-walker stats scan.rocks --largest-files    # Biggest files
nfs-walker stats scan.rocks --largest-dirs     # Fullest directories
nfs-walker stats scan.rocks --by-uid           # Usage by owner
nfs-walker stats scan.rocks --duplicates       # Duplicate files (requires -c scan)
nfs-walker stats scan.rocks --by-file-type     # MIME type distribution (requires -t scan)
nfs-walker stats scan.rocks --hardlink-groups  # Hard link groups
```

**SQLite** (full SQL power):
```bash
nfs-walker convert scan.rocks scan.db

# Duplicate files by checksum
sqlite3 scan.db "SELECT checksum, COUNT(*) as copies, SUM(size) as wasted
                 FROM entries WHERE checksum IS NOT NULL
                 GROUP BY checksum HAVING copies > 1
                 ORDER BY wasted DESC LIMIT 20"

# File type distribution
sqlite3 scan.db "SELECT file_type, COUNT(*), SUM(size)/1e9 as gb
                 FROM entries WHERE file_type IS NOT NULL
                 GROUP BY file_type ORDER BY gb DESC"
```

See [docs/QUERY_ROCKSDB.md](docs/QUERY_ROCKSDB.md) and [docs/QUERY_SQLITE.md](docs/QUERY_SQLITE.md) for query examples.

### Analytics Dashboard

The analytics dashboard provides a visual web UI for exploring scan results. It runs 36 pre-built SQL queries via DataFusion against Parquet exports.

**Step 1: Export scan to Parquet**

```bash
nfs-walker export-parquet scan.rocks parquet-output/
```

**Step 2: Build the dashboard** (one-time)

```bash
# Install Node.js dependencies and build the frontend
cd web && npm install && npm run build && cd ..

# Build the Rust server with dashboard support
cargo build --release --features server
```

**Step 3: Start the server**

```bash
nfs-walker serve --data-dir parquet-output/
# => Dashboard: http://localhost:8080
# => API:       http://localhost:8080/api/health
```

Options:
```
nfs-walker serve [OPTIONS] --data-dir <DIR>

  --data-dir <DIR>    Directory containing exported Parquet scans
  --port <PORT>       Server port [default: 8080]
  --bind <ADDR>       Bind address [default: 0.0.0.0]
```

**Dashboard pages:**

| Page | URL | What it shows |
|------|-----|---------------|
| Overview | `/` | Entry counts, size/age histograms, top directories |
| Capacity | `/capacity` | Allocation waste, depth breakdown, hard links, duplicate inodes |
| Files | `/files` | Size percentiles, growth trends, extensions, largest/zero-byte/temp files |
| Ownership | `/ownership` | Storage by UID/GID, ownership concentration, world-writable files |
| Directories | `/directories` | Depth/fanout distributions, widest/deepest/empty directories |
| Query Explorer | `/queries` | Browse and execute all 36 queries with custom parameters |

**API endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/scans` | List available scans |
| GET | `/api/queries` | List all 36 queries with metadata |
| POST | `/api/queries/:id/execute` | Execute a single query |
| POST | `/api/queries/batch` | Execute multiple queries in one request |

**Development mode** (hot-reload):

```bash
# Terminal 1: Rust API server
cargo run --features server -- serve --data-dir parquet-output/

# Terminal 2: Vite dev server with proxy
cd web && npm run dev
# => http://localhost:5173 (proxies /api/* to :8080)
```

### Command Reference

```
nfs-walker [OPTIONS] <NFS_URL>
nfs-walker stats <DB_PATH> [QUERY_OPTIONS]
nfs-walker convert <INPUT> <OUTPUT> [--progress]
nfs-walker export-parquet <INPUT> <OUTPUT_DIR>
nfs-walker serve --data-dir <DIR> [--port 8080] [--bind 0.0.0.0]

Scan Options:
  -o, --output <FILE>     Output database [default: walk.db]
  -w, --workers <NUM>     Worker threads [default: CPU count × 2]
  -d, --max-depth <NUM>   Maximum directory depth
  -q, --quiet             Suppress progress
  -v, --verbose           Show errors
  --dirs-only             Only record directories
  --exclude <PATTERN>     Exclude paths (repeatable)
  --sqlite                Force SQLite output (slower)
  -c, --checksum          Compute gxhash checksum per file (reads full content)
  -t, --file-type         Detect MIME type via magic bytes (reads first 8KB)
  --max-checksum-size N   Skip checksum for files larger than N bytes [default: 1GB]

Stats Options:
  --by-extension          Files by extension
  --largest-files         Biggest files
  --largest-dirs          Directories with most files
  --oldest-files          Oldest by mtime
  --most-links            Most hard links
  --by-uid                Usage by user ID
  --by-gid                Usage by group ID
  --duplicates            Duplicate files by checksum (requires -c scan)
  --by-file-type          MIME type distribution (requires -t scan)
  --hardlink-groups       Files sharing same inode
  --min-size <BYTES>      Minimum size for duplicate detection [default: 1024]
  -n, --top <N>           Limit results [default: 20]
```

## Performance

### Benchmark Results

Tested on a real NFS export: **4.1M files, 17,919 directories, 1.32 TiB** over NFS.

| Rank | Tool | Time | Files/sec | vs nfs-walker |
|------|------|------|-----------|---------------|
| 1 | **nfs-walker (RocksDB)** | **35.1s** | **119,883** | — |
| 2 | dust | 45.4s | ~91K | 1.3× slower |
| 3 | nfs-walker (SQLite) | 63.3s | 65,731 | 1.8× slower |
| 4 | rsync --dry-run | 3m 15s | ~21K | **5.6× slower** |
| 5 | fd-find | 3m 43s | ~18.6K | **6.3× slower** |
| 6 | find | 28m 20s | ~2.4K | **48× slower** |
| 7 | du | 28m 52s | ~2.4K | **49× slower** |

*All kernel-client tools (rsync, fd, find, du) use the standard NFS mount. nfs-walker bypasses the kernel and speaks NFS protocol directly.*

### Large-Scale Production

| Metric | Result |
|--------|--------|
| Files scanned | 43 million |
| Throughput | **48,401 files/sec** |
| Duration | 14.8 minutes |
| Peak Memory | ~5 GB |
| Database Size | 4.0 GiB |

### Content Analysis Performance

Tested on **770K files, 373 GiB** over NFS:

| Mode | Time | Files/sec | Notes |
|------|------|-----------|-------|
| Metadata only | **3.9s** | **196,509** | Default — READDIRPLUS only |
| File type detection (`-t`) | 3m 19s | 3,880 | Reads first 8KB per file |
| Checksum (`-c`) | 11m 16s | 1,140 | Reads full file content (gxhash) |

Content analysis is I/O-bound (reading file data over NFS), so throughput depends on network bandwidth and file sizes. Metadata-only scans remain unaffected.

### Why So Fast?

1. **Direct NFS protocol** - No kernel overhead, direct server communication
2. **READDIRPLUS** - Single RPC returns listing + attributes (no separate stat calls)
3. **Work-stealing parallelism** - All workers stay busy
4. **RocksDB** - Write-optimized storage, no transaction overhead

## Architecture

```
┌─────────────────────────────────────────────────┐
│                   CLI                           │
└──────────────────────┬──────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────┐
│              Work-Stealing Queue                │
│    ┌────────┐ ┌────────┐ ┌────────┐            │
│    │Worker 1│ │Worker 2│ │Worker N│ ← NFS conn │
│    └────┬───┘ └────┬───┘ └────┬───┘            │
│         └──────────┼──────────┘                 │
│                    ▼                            │
│           Bounded Channel                       │
│                    ▼                            │
│            Writer Thread                        │
└──────────────────────┬──────────────────────────┘
                       ▼
              ┌────────────────┐
              │ RocksDB/SQLite │
              └────────────────┘
```

## Documentation

- [Building](docs/BUILDING.md) - Build instructions and dependencies
- [RocksDB Queries](docs/QUERY_ROCKSDB.md) - Built-in query commands
- [SQLite Queries](docs/QUERY_SQLITE.md) - SQL examples and export
- [Analytics Dashboard](#analytics-dashboard) - Web UI setup and usage

## License

MIT
