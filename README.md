# nfs-walker

High-performance NFS filesystem scanner with SQLite output. Designed to scan millions of files with minimal memory footprint.

## Features

- **Direct NFS Protocol Access** - Uses libnfs for direct NFS protocol communication, bypassing the kernel NFS client
- **Parallel Scanning** - Work-stealing parallelism with configurable worker count
- **READDIRPLUS Optimization** - Single RPC returns directory listing + file attributes
- **Memory Efficient** - Bounded work queue with backpressure prevents memory explosion
- **SQLite Output** - Queryable database with indexes for fast lookups
- **Single Binary** - Easy deployment to any Linux environment

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
# Standard build (dynamically linked)
make build

# Static build with musl (portable, no dependencies)
make static
```

The binary will be at `./build/nfs-walker`.

## Usage

### Basic Usage

```bash
# Scan an NFS export to SQLite
nfs-walker nfs://server/export -o scan.db

# With progress display
nfs-walker nfs://192.168.1.100/data -w 8 -p -o scan.db

# Legacy NFS path format
nfs-walker server:/export -o scan.db

# Directories only (smaller output)
nfs-walker server:/export --dirs-only -o dirs.db

# With exclusions
nfs-walker nfs://server/data --exclude ".snapshot" --exclude ".zfs" -o scan.db

# Limit depth
nfs-walker nfs://server/export -d 3 -o shallow.db
```

### Options

```
Arguments:
  <NFS_URL>  NFS path (nfs://server/export or server:/export/path)

Options:
  -o, --output <FILE>       Output SQLite database [default: walk.db]
  -w, --workers <NUM>       Number of worker threads [default: num_cpus * 2]
  -q, --queue-size <NUM>    Work queue size [default: 10000]
  -b, --batch-size <NUM>    Batch size for writes [default: 1000]
  -d, --max-depth <NUM>     Maximum directory depth
  -p, --progress            Show progress during walk
  -v, --verbose             Verbose output
  --dirs-only               Only record directories
  --no-atime                Skip atime attribute
  --exclude <PATTERN>       Exclude paths matching regex (repeatable)
  --timeout <SECS>          NFS connection timeout [default: 30]
  --retries <NUM>           Retry attempts for transient errors [default: 3]
  -h, --help                Print help
  -V, --version             Print version
```

## Performance

### Benchmark: nfs-walker vs Traditional Tools

Scanning 2.1 million files over NFS:

| Tool                  | Time     | Speedup |
|-----------------------|----------|---------|
| `find` (count only)   | 12m 13s  | 1x      |
| `find` (with stat)    | 11m 43s  | 1x      |
| `du -s`               | 11m 46s  | 1x      |
| `ls -lR`              | 9m 24s   | 1.3x    |
| `rsync --list-only`   | 1m 54s   | 6.4x    |
| `tree`                | 1m 26s   | 8.5x    |
| **nfs-walker**        | **33s**  | **22x** |

### Performance Metrics

| Dataset | Files | Time | Throughput |
|---------|-------|------|------------|
| Directory tree | 2.1M files + 7.2K dirs | 37s | **56K files/sec** |
| Large flat directory | 10M files | 177s | **56K files/sec** |

**Key finding**: 8 workers typically performs best. Server-side saturation is the limit, not client.

### Why is nfs-walker faster?

Traditional tools use the kernel NFS client, which serializes requests and has significant per-operation overhead. nfs-walker uses:

1. **Direct NFS protocol** - Bypasses the kernel, communicates directly with the NFS server
2. **READDIRPLUS** - Single NFS operation returns directory listing + file attributes (no separate stat calls)
3. **Work-stealing parallelism** - All workers actively process directories
4. **Dedicated writer thread** - Zero-contention database writes

## Query Examples

```bash
sqlite3 scan.db
```

```sql
-- Total files, directories, and space
SELECT
    SUM(CASE WHEN entry_type = 0 THEN 1 ELSE 0 END) as files,
    SUM(CASE WHEN entry_type = 1 THEN 1 ELSE 0 END) as dirs,
    SUM(size) / 1024 / 1024 / 1024 as total_gb
FROM entries;

-- Top 20 largest files
SELECT path, size / 1024 / 1024 as mb
FROM entries
WHERE entry_type = 0
ORDER BY size DESC
LIMIT 20;

-- Files over 1GB
SELECT path, size / 1024 / 1024 / 1024 as gb
FROM entries
WHERE entry_type = 0 AND size > 1073741824
ORDER BY size DESC;

-- File count and size by extension
SELECT
    LOWER(SUBSTR(name, INSTR(name, '.'))) as extension,
    COUNT(*) as count,
    SUM(size) / 1024 / 1024 / 1024 as gb
FROM entries
WHERE entry_type = 0 AND name LIKE '%.%'
GROUP BY extension
ORDER BY count DESC
LIMIT 20;

-- Space by top-level directory
SELECT
    '/' || SUBSTR(path, 2, INSTR(SUBSTR(path, 2), '/') - 1) as top_dir,
    COUNT(*) as files,
    SUM(size) / 1024 / 1024 / 1024 as gb
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

-- Oldest files by access time
SELECT path, datetime(atime, 'unixepoch', 'localtime') as last_accessed,
       size / 1024 / 1024 as size_mb
FROM entries
WHERE entry_type = 0 AND atime IS NOT NULL
ORDER BY atime ASC
LIMIT 10;
```

### Export Results

```bash
# SQLite to CSV
sqlite3 -header -csv scan.db "SELECT path, size FROM entries WHERE entry_type = 0" > files.csv

# Export large files list
sqlite3 scan.db "SELECT path FROM entries WHERE size > 1073741824" > large_files.txt
```

## Database Schema

```sql
-- Main entries table
CREATE TABLE entries (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER,           -- references parent directory's id
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
    depth INTEGER NOT NULL       -- depth from root (0 = root)
);

-- Walk metadata
CREATE TABLE walk_info (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Indexes for fast queries
CREATE INDEX idx_entries_path ON entries(path);
CREATE INDEX idx_entries_type ON entries(entry_type);
CREATE INDEX idx_entries_size ON entries(size) WHERE entry_type = 0;
CREATE INDEX idx_entries_depth ON entries(depth);
CREATE INDEX idx_entries_parent ON entries(parent_id);
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.

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
│            Crossbeam Channel                                    │
│                     │                                           │
│                     ▼                                           │
│              Writer Thread                                      │
│         (batch SQLite inserts)                                  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
                      ┌────────────────┐
                      │  SQLite DB     │
                      └────────────────┘
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
