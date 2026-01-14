# nfs-walker

High-performance NFS filesystem scanner with SQLite and Parquet output. Designed to scan billions of files with minimal memory footprint.

## Features

- **Direct NFS Protocol Access** - Uses libnfs for direct NFS protocol communication, bypassing the kernel NFS client
- **Parallel Scanning** - Connection pooling with configurable concurrency for maximum throughput
- **Memory Efficient** - Bounded work queue with backpressure prevents memory explosion
- **Multiple Output Formats**:
  - **SQLite** - Best for ad-hoc queries and interactive exploration
  - **Parquet** - Best for analytics, 8x smaller files, 6x faster writes
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
make build
```

The binary will be at `./build/nfs-walker`.

## Usage

```bash
# Basic scan with SQLite output
nfs-walker nfs://server/export -o scan.db

# High parallelism with progress display
nfs-walker nfs://192.168.1.100/data --connections 64 -p -o scan.db

# Parquet output (recommended for large scans)
nfs-walker nfs://server/export --format parquet -o scan.parquet

# Directories only (smaller output)
nfs-walker server:/export --dirs-only -o dirs.db

# With exclusions
nfs-walker nfs://server/data --exclude ".snapshot" --exclude ".zfs" -o scan.db
```

### Options

```
Arguments:
  <NFS_URL>  NFS path (nfs://server/export or server:/export/path)

Options:
  -o, --output <FILE>       Output file [default: walk.db]
  -w, --workers <NUM>       Number of concurrent tasks [default: num_cpus * 2]
  -q, --queue-size <NUM>    Work queue size [default: 10000]
  -b, --batch-size <NUM>    Batch size for writes [default: 10000]
  -d, --max-depth <NUM>     Maximum directory depth
  -p, --progress            Show progress during walk
  -v, --verbose             Verbose output
  --dirs-only               Only record directories
  --no-atime                Skip atime attribute
  --exclude <PATTERN>       Exclude paths matching regex (repeatable)
  --timeout <SECS>          NFS connection timeout [default: 30]
  --retries <NUM>           Retry attempts for transient errors [default: 3]
  --connections <NUM>       Number of NFS connections [default: 16]
  --format <FORMAT>         Output format: sqlite, parquet [default: sqlite]
  -h, --help                Print help
  -V, --version             Print version
```

## Output Formats

### SQLite

Best for interactive exploration and ad-hoc queries using standard SQL tools.

```bash
nfs-walker nfs://server/export -o scan.db
sqlite3 scan.db "SELECT path, size FROM entries WHERE size > 1000000000"
```

### Parquet

Best for analytics workloads. Produces 8x smaller files with 6x faster writes.
Query with DuckDB, Python (pandas/pyarrow/polars), or any Parquet-compatible tool.

```bash
nfs-walker nfs://server/export --format parquet -o scan.parquet
```

## Performance

Measured performance scanning 2.1 million files:

| Format  | Throughput       | Output Size |
|---------|------------------|-------------|
| SQLite  | 50K files/sec    | 646 MB      |
| Parquet | 313K files/sec   | 78 MB       |

- **Parquet is 6x faster** for writes
- **Parquet is 8x smaller** with ZSTD compression
- **Memory usage**: <200MB regardless of filesystem size

## Query Examples

### SQLite Queries

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

-- Directories with most files
SELECT e.path, ds.direct_file_count as files,
       ds.direct_bytes / 1024 / 1024 / 1024 as size_gb
FROM dir_stats ds
JOIN entries e ON ds.entry_id = e.id
ORDER BY ds.direct_file_count DESC
LIMIT 10;

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

-- Files by modification year
SELECT
    strftime('%Y', mtime, 'unixepoch') as year,
    COUNT(*) as files,
    SUM(size) / 1024 / 1024 / 1024 as gb
FROM entries
WHERE entry_type = 0 AND mtime IS NOT NULL
GROUP BY year
ORDER BY year DESC;
```

### Parquet Queries

Query Parquet files using Python:

```python
import pyarrow.parquet as pq
from collections import Counter

# Read parquet file
table = pq.read_table('scan.parquet')
print(f'Total entries: {table.num_rows:,}')

# Get data
paths = table.column('path').to_pylist()
sizes = table.column('size').to_pylist()
types = table.column('entry_type').to_pylist()

# Entry type distribution
counts = Counter(types)
type_names = {0: 'File', 1: 'Directory', 2: 'Symlink'}
for et, count in sorted(counts.items()):
    print(f'{type_names.get(et, et)}: {count:,}')

# Top 10 largest files
files = [(s, p) for s, p, t in zip(sizes, paths, types) if t == 0]
files.sort(reverse=True)
print('\nTop 10 largest files:')
for size, path in files[:10]:
    print(f'  {size/1024/1024/1024:.2f} GB - {path}')
```

Query with DuckDB (SQL on Parquet):

```bash
pip install duckdb
```

```sql
-- Run with: duckdb -c "SQL HERE"

-- Total files and size
SELECT
    COUNT(*) FILTER (WHERE entry_type = 0) as files,
    COUNT(*) FILTER (WHERE entry_type = 1) as dirs,
    SUM(size) / 1024 / 1024 / 1024 as total_gb
FROM 'scan.parquet';

-- Top 20 largest files
SELECT path, size/1024/1024/1024 as gb
FROM 'scan.parquet'
WHERE entry_type = 0
ORDER BY size DESC
LIMIT 20;

-- Files by extension
SELECT
    LOWER(REGEXP_EXTRACT(name, '\.([^.]+)$', 1)) as ext,
    COUNT(*) as count,
    SUM(size) / 1024 / 1024 / 1024 as gb
FROM 'scan.parquet'
WHERE entry_type = 0
GROUP BY ext
ORDER BY count DESC
LIMIT 20;

-- Space by top-level directory
SELECT
    SPLIT_PART(path, '/', 2) as top_dir,
    COUNT(*) as files,
    SUM(size) / 1024 / 1024 / 1024 as gb
FROM 'scan.parquet'
WHERE entry_type = 0 AND depth > 0
GROUP BY top_dir
ORDER BY gb DESC;

-- Files modified in last 7 days
SELECT path, size, TO_TIMESTAMP(mtime) as modified
FROM 'scan.parquet'
WHERE entry_type = 0
  AND mtime > EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days')
ORDER BY mtime DESC
LIMIT 100;
```

### Export Results

```bash
# SQLite to CSV
sqlite3 -header -csv scan.db "SELECT path, size FROM entries WHERE entry_type = 0" > files.csv

# Export large files list
sqlite3 scan.db "SELECT path FROM entries WHERE size > 1073741824" > large_files.txt

# Parquet to CSV with DuckDB
duckdb -c "COPY (SELECT * FROM 'scan.parquet') TO 'files.csv' (HEADER, DELIMITER ',')"
```

## Database Schema

### SQLite Schema

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

-- Directory statistics
CREATE TABLE dir_stats (
    entry_id INTEGER PRIMARY KEY,
    direct_file_count INTEGER,
    direct_dir_count INTEGER,
    direct_symlink_count INTEGER,
    direct_other_count INTEGER,
    direct_bytes INTEGER
);

-- Walk metadata
CREATE TABLE walk_info (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Indexes
CREATE INDEX idx_entries_path ON entries(path);
CREATE INDEX idx_entries_type ON entries(entry_type);
CREATE INDEX idx_entries_size ON entries(size) WHERE entry_type = 0;
CREATE INDEX idx_entries_depth ON entries(depth);
CREATE INDEX idx_entries_parent ON entries(parent_id);
```

### Parquet Schema

```
path: string (not null)
name: string (not null)
parent_path: string (nullable)
entry_type: int32 (not null)  -- 0=file, 1=dir, 2=symlink
size: uint64 (not null)
mtime: int64 (nullable)       -- unix epoch
atime: int64 (nullable)
ctime: int64 (nullable)
mode: uint32 (nullable)
uid: uint32 (nullable)
gid: uint32 (nullable)
nlink: uint64 (nullable)
inode: uint64 (not null)
depth: uint32 (not null)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        NFS Server                               │
└─────────────────────────────┬───────────────────────────────────┘
                              │ READDIRPLUS
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Connection Pool                              │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐         ┌─────────┐     │
│  │  Conn 1 │  │  Conn 2 │  │  Conn 3 │  ...    │  Conn N │     │
│  │ libnfs  │  │ libnfs  │  │ libnfs  │         │ libnfs  │     │
│  └────┬────┘  └────┬────┘  └────┬────┘         └────┬────┘     │
│       └────────────┼────────────┼────────────────────┘          │
│                    ▼            ▼                               │
│            ┌──────────────────────────┐                         │
│            │      Work Queue          │                         │
│            │   (bounded, backpressure)│                         │
│            └────────────┬─────────────┘                         │
│                         ▼                                       │
│            ┌──────────────────────────┐                         │
│            │    Batched Writer        │                         │
│            │  (SQLite or Parquet)     │                         │
│            └──────────────────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Output File     │
                    │ (.db or .parquet)│
                    └──────────────────┘
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
