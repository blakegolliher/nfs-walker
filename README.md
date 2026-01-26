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
# Install dependencies (Ubuntu/Debian)
sudo apt install build-essential pkg-config libsqlite3-dev libnfs-dev libclang-dev

# Build with RocksDB support
make release-rocks
```

See [docs/BUILDING.md](docs/BUILDING.md) for detailed build instructions.

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

### Benchmark: 43 Million Files

| Metric | Result |
|--------|--------|
| Throughput | **48,401 files/sec** |
| Duration | 14.8 minutes |
| Peak Memory | ~5 GB |
| Database Size | 4.0 GiB |

### vs Traditional Tools

| Tool | Time | Relative |
|------|------|----------|
| `find` | 12m 13s | 1× |
| `ls -lR` | 9m 24s | 1.3× |
| `rsync --list-only` | 1m 54s | 6× |
| `tree` | 1m 26s | 8× |
| **nfs-walker** | **33s** | **22×** |

*Benchmark: 2.1M files over NFS*

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
