# nfs-walker

High-performance NFS filesystem scanner with RocksDB/SQLite output. Designed to scan hundreds of millions of files efficiently.

## Features

- **Direct NFS Protocol Access** - Uses libnfs for direct NFS protocol communication, bypassing the kernel NFS client
- **Parallel Scanning** - Work-stealing parallelism with configurable worker count
- **READDIRPLUS Optimization** - Single RPC returns directory listing + file attributes
- **Memory Efficient** - Periodic flushing and bounded queues prevent memory explosion
- **Dual Storage Backends**:
  - **RocksDB** (default) - Fast writes, ideal for large scans (100M+ files)
  - **SQLite** - Queryable database with SQL support
- **Built-in Analytics** - Query RocksDB directly without conversion
- **Single Binary** - Easy deployment to any Linux environment

## Quick Start

```bash
# Scan an NFS export (outputs to RocksDB by default)
nfs-walker nfs://server/export -o scan.rocks

# View statistics directly from RocksDB
nfs-walker stats scan.rocks

# Convert to SQLite for complex queries
nfs-walker convert scan.rocks scan.db

# Query with SQL
sqlite3 scan.db "SELECT path, size FROM entries ORDER BY size DESC LIMIT 10"
```

## Requirements

- Linux (Ubuntu 22.04+ recommended)
- Rust 1.82+ (for building)
- libnfs-dev

## Installation

### Install Dependencies

```bash
# Ubuntu/Debian
sudo apt install build-essential pkg-config libsqlite3-dev libnfs-dev libclang-dev

# Or use the Makefile
make install-deps
```

### Build

```bash
# Build with RocksDB support (recommended)
make release-rocks

# Standard build without RocksDB (SQLite only)
make release
```

The binary will be at `./build/nfs-walker`.

## Usage

### Scanning

```bash
# Basic scan to RocksDB (fast writes)
nfs-walker nfs://server/export -o scan.rocks

# Scan with progress display and 16 workers
nfs-walker nfs://192.168.1.100/data -w 16 -o scan.rocks

# Force SQLite output directly (slower but single step)
nfs-walker nfs://server/export -o scan.db --sqlite

# Directories only (smaller output)
nfs-walker server:/export --dirs-only -o dirs.rocks

# With exclusions
nfs-walker nfs://server/data --exclude ".snapshot" --exclude ".zfs" -o scan.rocks

# Limit depth
nfs-walker nfs://server/export -d 3 -o shallow.rocks

# Explicit export path (for multi-component exports like /volumes/uuid)
nfs-walker nfs://server/volumes/abc123/subdir --export /volumes/abc123 -o scan.rocks
```

### RocksDB Statistics (No Conversion Needed)

Query scan results directly from RocksDB without converting to SQLite:

```bash
# Overview statistics
nfs-walker stats scan.rocks

# Files by extension (top 15)
nfs-walker stats scan.rocks --by-extension -n 15

# Largest files
nfs-walker stats scan.rocks --largest-files -n 10

# Directories with most files
nfs-walker stats scan.rocks --largest-dirs -n 10

# Oldest files (by mtime)
nfs-walker stats scan.rocks --oldest-files -n 10

# Files with most hard links
nfs-walker stats scan.rocks --most-links -n 10

# Usage by user ID
nfs-walker stats scan.rocks --by-uid -n 10

# Usage by group ID
nfs-walker stats scan.rocks --by-gid -n 10

# Combine multiple queries
nfs-walker stats scan.rocks --by-extension --by-uid -n 10
```

**Stats Command Options:**

| Option | Description |
|--------|-------------|
| (none) | Overview: entries, files, dirs, symlinks, size, depth |
| `--by-extension` | Files grouped by extension with count/size |
| `--largest-files` | Biggest files by size |
| `--largest-dirs` | Directories with most direct children |
| `--oldest-files` | Files with oldest modification time |
| `--most-links` | Files with most hard links (nlink) |
| `--by-uid` | Usage breakdown by user ID |
| `--by-gid` | Usage breakdown by group ID |
| `-n N` | Limit results to top N (default: 20) |

### Converting RocksDB to SQLite

For complex SQL queries, convert RocksDB to SQLite:

```bash
# Basic conversion
nfs-walker convert scan.rocks scan.db

# With progress display
nfs-walker convert scan.rocks scan.db --progress
```

### Command Line Options

```
Arguments:
  <NFS_URL>  NFS path (nfs://server/export or server:/export/path)

Options:
  -o, --output <FILE>       Output database [default: walk.db]
  -w, --workers <NUM>       Number of worker threads [default: num_cpus * 2]
  -b, --batch-size <NUM>    Batch size for writes [default: 1000]
  -d, --max-depth <NUM>     Maximum directory depth
  -q, --quiet               Suppress progress output
  -v, --verbose             Verbose output (show errors)
  --sqlite                  Force SQLite output (slower, but single step)
  --dirs-only               Only record directories
  --no-atime                Skip atime attribute
  --exclude <PATTERN>       Exclude paths matching pattern (repeatable)
  --export <PATH>           Explicit NFS export path
  --timeout <SECS>          NFS connection timeout [default: 30]
  --retries <NUM>           Retry attempts for transient errors [default: 3]
  -h, --help                Print help
  -V, --version             Print version

Subcommands:
  stats                     Query RocksDB statistics directly
  convert                   Convert RocksDB to SQLite
```

## Performance

### Benchmark: 43 Million Files

| Metric | Result |
|--------|--------|
| Files Scanned | 42,993,009 |
| Directories | 39 |
| Duration | 888 seconds (14.8 minutes) |
| Throughput | **48,401 files/sec** |
| Database Size | 4.0 GiB |
| Peak Memory | ~5.4 GB |

### nfs-walker vs Traditional Tools

Scanning 2.1 million files over NFS:

| Tool | Time | Speedup |
|------|------|---------|
| `find` (count only) | 12m 13s | 1x |
| `find` (with stat) | 11m 43s | 1x |
| `du -s` | 11m 46s | 1x |
| `ls -lR` | 9m 24s | 1.3x |
| `rsync --list-only` | 1m 54s | 6.4x |
| `tree` | 1m 26s | 8.5x |
| **nfs-walker** | **33s** | **22x** |

### Why is nfs-walker faster?

1. **Direct NFS protocol** - Bypasses the kernel, communicates directly with the NFS server
2. **READDIRPLUS** - Single NFS operation returns directory listing + file attributes
3. **Work-stealing parallelism** - All workers actively process directories
4. **Dedicated writer thread** - Zero-contention database writes
5. **RocksDB** - Write-optimized storage for scan workloads

## SQLite Query Examples

After converting to SQLite (or using `--sqlite` directly):

```bash
sqlite3 scan.db
```

```sql
-- Total files, directories, and space
SELECT
    SUM(CASE WHEN entry_type = 0 THEN 1 ELSE 0 END) as files,
    SUM(CASE WHEN entry_type = 1 THEN 1 ELSE 0 END) as dirs,
    SUM(size) / 1024.0 / 1024 / 1024 as total_gb
FROM entries;

-- Top 20 largest files
SELECT path, size / 1024 / 1024 as mb
FROM entries
WHERE entry_type = 0
ORDER BY size DESC
LIMIT 20;

-- Files over 1GB
SELECT path, size / 1024.0 / 1024 / 1024 as gb
FROM entries
WHERE entry_type = 0 AND size > 1073741824
ORDER BY size DESC;

-- File count and size by extension
SELECT
    extension,
    COUNT(*) as count,
    SUM(size) / 1024.0 / 1024 / 1024 as gb
FROM entries
WHERE entry_type = 0
GROUP BY extension
ORDER BY gb DESC
LIMIT 20;

-- Space by top-level directory
SELECT
    '/' || SUBSTR(path, 2, INSTR(SUBSTR(path, 2), '/') - 1) as top_dir,
    COUNT(*) as files,
    SUM(size) / 1024.0 / 1024 / 1024 as gb
FROM entries
WHERE entry_type = 0 AND depth > 0
GROUP BY top_dir
ORDER BY gb DESC;

-- Files modified in last 7 days
SELECT path, size, datetime(mtime, 'unixepoch', 'localtime') as modified
FROM entries
WHERE entry_type = 0
  AND mtime > strftime('%s', 'now', '-7 days')
ORDER BY mtime DESC
LIMIT 100;

-- Oldest files by modification time
SELECT path, datetime(mtime, 'unixepoch', 'localtime') as modified,
       size / 1024 / 1024 as size_mb
FROM entries
WHERE entry_type = 0 AND mtime IS NOT NULL
ORDER BY mtime ASC
LIMIT 20;

-- Files by owner
SELECT uid, COUNT(*) as files, SUM(size) / 1024.0 / 1024 / 1024 as gb
FROM entries
WHERE entry_type = 0
GROUP BY uid
ORDER BY gb DESC
LIMIT 20;

-- Disk usage (allocated blocks vs logical size)
SELECT
    SUM(size) / 1024.0 / 1024 / 1024 as logical_gb,
    SUM(blocks) * 512.0 / 1024 / 1024 / 1024 as allocated_gb
FROM entries
WHERE entry_type = 0;
```

## Exporting Data

### SQLite to CSV

```bash
# Export all files to CSV
sqlite3 -header -csv scan.db "SELECT path, size, mtime FROM entries WHERE entry_type = 0" > files.csv

# Export large files list
sqlite3 scan.db "SELECT path FROM entries WHERE size > 1073741824" > large_files.txt

# Export with formatted size
sqlite3 -header -csv scan.db "
  SELECT path,
         ROUND(size / 1024.0 / 1024, 2) as size_mb,
         datetime(mtime, 'unixepoch') as modified
  FROM entries
  WHERE entry_type = 0
  ORDER BY size DESC
  LIMIT 1000
" > top_1000_files.csv

# Export directory sizes
sqlite3 -header -csv scan.db "
  SELECT parent_path,
         COUNT(*) as file_count,
         ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as size_gb
  FROM entries
  WHERE entry_type = 0
  GROUP BY parent_path
  ORDER BY SUM(size) DESC
" > directory_sizes.csv
```

### JSON Export

```bash
sqlite3 -json scan.db "SELECT path, size, mtime FROM entries LIMIT 100" > sample.json
```

## Database Schema

### SQLite Schema

```sql
CREATE TABLE entries (
    id INTEGER PRIMARY KEY,
    parent_path TEXT,            -- parent directory path
    name TEXT NOT NULL,          -- filename only
    path TEXT NOT NULL,          -- full path from mount root
    entry_type INTEGER NOT NULL, -- 0=file, 1=dir, 2=symlink
    size INTEGER DEFAULT 0,      -- file size in bytes
    mtime INTEGER,               -- modification time (unix epoch)
    atime INTEGER,               -- access time (unix epoch)
    ctime INTEGER,               -- change time (unix epoch)
    mode INTEGER,                -- permission bits
    uid INTEGER,                 -- owner user id
    gid INTEGER,                 -- owner group id
    nlink INTEGER,               -- hard link count
    inode INTEGER,               -- inode number
    depth INTEGER NOT NULL,      -- depth from root (0 = root)
    extension TEXT,              -- file extension (lowercase, no dot)
    blocks INTEGER DEFAULT 0     -- 512-byte blocks allocated
);

CREATE TABLE walk_info (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Indexes
CREATE INDEX idx_entries_path ON entries(path);
CREATE INDEX idx_entries_parent ON entries(parent_path);
CREATE INDEX idx_entries_type ON entries(entry_type);
CREATE INDEX idx_entries_size ON entries(size) WHERE entry_type = 0;
CREATE INDEX idx_entries_depth ON entries(depth);
CREATE INDEX idx_entries_ext ON entries(extension) WHERE entry_type = 0;
CREATE INDEX idx_entries_inode ON entries(inode);
```

### RocksDB Structure

RocksDB uses column families for efficient indexing:

- `entries_by_path` - Primary index, key = path bytes
- `entries_by_inode` - Secondary index, key = inode (big-endian u64)
- `metadata` - Walk info (source URL, timestamps, stats)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          CLI / Config                           │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                        SimpleWalker                             │
│                                                                 │
│  Work-Stealing Queue (crossbeam_deque)                          │
│       │         │         │         │                           │
│       ▼         ▼         ▼         ▼                           │
│  ┌────────┐┌────────┐┌────────┐┌────────┐                       │
│  │Worker 0││Worker 1││Worker 2││Worker N│  ← Each has NFS conn  │
│  └───┬────┘└───┬────┘└───┬────┘└───┬────┘                       │
│      │         │         │         │                            │
│      └─────────┴────┬────┴─────────┘                            │
│                     ▼                                           │
│            Crossbeam Channel (bounded)                          │
│                     │                                           │
│                     ▼                                           │
│              Writer Thread                                      │
│         (batched writes + periodic flush)                       │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  RocksDB / SQLite   │
                    └─────────────────────┘
```

## Development

```bash
# Run tests
make test

# Format code
make fmt

# Run clippy
make check

# Clean build
make clean

# List available binaries
make list
```

## License

MIT
