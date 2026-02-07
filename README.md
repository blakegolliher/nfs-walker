# nfs-walker

High-performance NFS filesystem scanner. Scans millions of files directly via NFS protocol, bypassing the kernel client.

## Features

- **Fast**: 48,000+ files/sec using READDIRPLUS and parallel workers
- **Direct NFS Protocol**: Bypasses kernel NFS client for maximum throughput
- **RocksDB Storage**: Write-optimized for large scans, with built-in analytics
- **SQLite Export**: Convert to SQLite for complex SQL queries
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

### Querying Results

**RocksDB** (fast, built-in queries):
```bash
nfs-walker stats scan.rocks                    # Overview
nfs-walker stats scan.rocks --by-extension     # Files by type
nfs-walker stats scan.rocks --largest-files    # Biggest files
nfs-walker stats scan.rocks --largest-dirs     # Fullest directories
nfs-walker stats scan.rocks --by-uid           # Usage by owner
```

**SQLite** (full SQL power):
```bash
nfs-walker convert scan.rocks scan.db
sqlite3 scan.db "SELECT extension, COUNT(*), SUM(size)/1e9 as gb
                 FROM entries WHERE entry_type=0
                 GROUP BY extension ORDER BY gb DESC"
```

See [docs/QUERY_ROCKSDB.md](docs/QUERY_ROCKSDB.md) and [docs/QUERY_SQLITE.md](docs/QUERY_SQLITE.md) for query examples.

### Command Reference

```
nfs-walker [OPTIONS] <NFS_URL>
nfs-walker stats <DB_PATH> [QUERY_OPTIONS]
nfs-walker convert <INPUT> <OUTPUT> [--progress]

Scan Options:
  -o, --output <FILE>     Output database [default: walk.db]
  -w, --workers <NUM>     Worker threads [default: CPU count × 2]
  -d, --max-depth <NUM>   Maximum directory depth
  -q, --quiet             Suppress progress
  -v, --verbose           Show errors
  --dirs-only             Only record directories
  --exclude <PATTERN>     Exclude paths (repeatable)
  --sqlite                Force SQLite output (slower)

Stats Options:
  --by-extension          Files by extension
  --largest-files         Biggest files
  --largest-dirs          Directories with most files
  --oldest-files          Oldest by mtime
  --most-links            Most hard links
  --by-uid                Usage by user ID
  --by-gid                Usage by group ID
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
- [Product Ideas](docs/product_ideas.md) - Future direction and use cases

## License

MIT
