# nfs-walker

High-performance NFS filesystem scanner. Scans millions of files directly via NFS protocol, bypassing the kernel client.

## Features

- **Fast**: 48,000+ files/sec using READDIRPLUS and parallel workers
- **Direct NFS Protocol**: Bypasses kernel NFS client for maximum throughput
- **RocksDB Storage**: Write-optimized for large scans, with a built-in scan-overview stat
- **Parquet Export**: Run `export-parquet`, then query in DuckDB / DataFusion / Polars / the bundled dashboard
- **SQLite Export**: Run `export-sql` for ad-hoc `sqlite3` queries against the same data
- **Content Analysis**: Optional checksum (gxhash) and file type detection (magic bytes)
- **Per-scan progress logfile**: Sidecar `<output>.log` records active workers, queue depth, write latency every 5s

## Quick Start

```bash
# Scan an NFS export to RocksDB
nfs-walker nfs://server/export -o scan.rocks -w 16

# View overview statistics (counts, total size, max depth)
nfs-walker stats scan.rocks

# Export to Parquet for analytics (DuckDB / DataFusion / dashboard)
nfs-walker export-parquet scan.rocks ./parquet-out -p --parallelism 64

# Scan with content analysis (checksum + file type detection)
nfs-walker nfs://server/export -o scan.rocks -c -t

# Export to SQLite for ad-hoc SQL
nfs-walker export-sql scan.rocks scan.db -p
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

**Overview from RocksDB** — fast counts, total size, max depth:
```bash
nfs-walker stats scan.rocks               # Overview (default mode)
nfs-walker stats scan.rocks --live        # Same, but works while a scan is still writing
```

The previous per-flag stat helpers (`--by-extension`, `--largest-files`, `--by-uid`,
`--duplicates`, etc.) were removed — convert to Parquet or SQLite and query there
instead. The Parquet path is the recommended one (DuckDB queries 4 B rows in seconds).

> **Querying during an active scan:** add `--live` to open the database in
> RocksDB secondary mode. The default read-only mode breaks under concurrent
> compactions ("No such file or directory: .../NNNNNN.sst"). See
> [docs/QUERY_ROCKSDB.md](docs/QUERY_ROCKSDB.md#live-querying-during-an-active-scan---live).

**Parquet** (recommended for analytics):
```bash
nfs-walker export-parquet scan.rocks ./parquet-out -p --parallelism 64

# DuckDB
duckdb -c "SELECT entry_type, COUNT(*), SUM(size)/1e9 AS gb \
           FROM read_parquet('parquet-out/scans/*/part-*.parquet') GROUP BY 1"
```

**SQLite** (single-file, full SQL):
```bash
nfs-walker export-sql scan.rocks scan.db -p

sqlite3 scan.db "SELECT checksum, COUNT(*) as copies, SUM(size) as wasted
                 FROM entries WHERE checksum IS NOT NULL
                 GROUP BY checksum HAVING copies > 1
                 ORDER BY wasted DESC LIMIT 20"

sqlite3 scan.db "SELECT file_type, COUNT(*), SUM(size)/1e9 as gb
                 FROM entries WHERE file_type IS NOT NULL
                 GROUP BY file_type ORDER BY gb DESC"
```

See [docs/QUERY_ROCKSDB.md](docs/QUERY_ROCKSDB.md) and [docs/QUERY_SQLITE.md](docs/QUERY_SQLITE.md) for query examples.

### Per-scan progress logfile

Every scan writes a sidecar logfile next to the output (`<output>.log` by default).
Snapshots fire every 5 s and capture: timestamp, elapsed, dirs/files/bytes, throughput,
active workers / total, write-batch latency (avg + p99), write-channel queue depth, and
on-disk RocksDB size. Tail it during a long scan to spot stalls:

```bash
nfs-walker nfs://server/export -o scan.rocks -w 32   # writes scan.rocks.log
tail -f scan.rocks.log
```

```
nfs-walker [OPTIONS]
  --log <PATH>           Override sidecar log path
  --no-log               Disable the progress logfile
  --log-fmt <text|json>  Snapshot format [default: text]
  --log-interval-secs N  Snapshot interval [default: 5]
```

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
nfs-walker stats <DB_PATH> [--live]
nfs-walker export-sql <INPUT> <OUTPUT> [--progress]
nfs-walker export-parquet <INPUT> <OUTPUT_DIR>
nfs-walker serve --data-dir <DIR> [--port 8080] [--bind 0.0.0.0]

Scan Options:
  -o, --output <PATH>     Output RocksDB directory [default: walk.rocks]
  -w, --workers <NUM>     Worker threads [default: CPU count × 2]
  -d, --max-depth <NUM>   Maximum directory depth
  -q, --quiet             Suppress progress
  -v, --verbose           Show errors
  --dirs-only             Only record directories
  --exclude <PATTERN>     Exclude paths (repeatable)
  -c, --checksum          Compute gxhash checksum per file (reads full content)
  -t, --file-type         Detect MIME type via magic bytes (reads first 8KB)
  --max-checksum-size N   Skip checksum for files larger than N bytes [default: 1GB]

Progress logfile:
  --log <PATH>            Override sidecar log path
  --no-log                Disable the progress logfile
  --log-fmt <text|json>   Snapshot format [default: text]
  --log-interval-secs N   Snapshot interval in seconds [default: 5]

Stats Options:
  --live                  Open RocksDB in secondary mode (works during a live scan)
```

## Performance

### Benchmark Results

Tested on a real NFS export: **4.1M files, 17,919 directories, 1.32 TiB** over NFS.

| Rank | Tool | Time | Files/sec | vs nfs-walker |
|------|------|------|-----------|---------------|
| 1 | **nfs-walker (RocksDB)** | **35.1s** | **119,883** | — |
| 2 | dust | 45.4s | ~91K | 1.3× slower |
| 3 | rsync --dry-run | 3m 15s | ~21K | **5.6× slower** |
| 4 | fd-find | 3m 43s | ~18.6K | **6.3× slower** |
| 5 | find | 28m 20s | ~2.4K | **48× slower** |
| 6 | du | 28m 52s | ~2.4K | **49× slower** |

*All kernel-client tools (rsync, fd, find, du) use the standard NFS mount. nfs-walker bypasses the kernel and speaks NFS protocol directly.*

### Large-Scale Production

| Metric | Result |
|--------|--------|
| Files scanned | 43 million |
| Throughput | **48,401 files/sec** |
| Duration | 14.8 minutes |
| Peak Memory | ~5 GB |
| Database Size | 4.0 GiB |

### Billion-Entry Workflow

Real production target: a multi-petabyte NFS export with ~4.4 billion entries (~80 M dirs, ~4.36 B files), driven from a 160-core / 1.4 TiB host with NVMe-backed RocksDB output, against a **34-cnode VAST cluster**. Three back-to-back ingest profiles were measured to characterize where the throughput ceiling actually lives.

**Profile A — single writer, pipeline-depth 8 (post writer-tweak baseline, commit `4297796`):**

```bash
./nfs-walker nfs://<server>/<export> \
    -w 1024 --pipeline-depth 8 \
    -o /mnt/nvme/scan-v2.rocks
```

| Metric | Result |
|--------|--------|
| Entries scanned | 4,361,995,918 (4.36 B) |
| Wall-clock | 3h 37m (13,063 s) |
| Throughput | **333,920 entries/sec** |
| RocksDB size | 775.93 GiB |
| Per-thread profile | one `rocks-writer` at 99.9 % CPU; 1024 walkers at < 1 % blocked on `entry_tx.send` |

**Profile B — eight writer shards, pipeline-depth 8:**

```bash
sudo ./nfs-walker nfs://<server>/<export> \
    -w 1024 --pipeline-depth 8 --writer-shards 8 \
    -o /mnt/nvme/scan-v3.rocks
```

| Metric | Result |
|--------|--------|
| Entries scanned | 4,374,686,367 (4.37 B) |
| Wall-clock | 3h 34m (12,876 s) — **−1.4 % vs Profile A** |
| Throughput | **339,748 entries/sec** |
| RocksDB size | 796.71 GiB (+2.7 %) |
| Per-thread profile | 8 `rocks-writer-N` threads at ~14 % CPU each (perfectly balanced); walkers still parked at 0–5 %; compaction picks up |

**Profile C — single writer, pipeline-depth 16:**

```bash
sudo ./nfs-walker nfs://<server>/<export> \
    -w 1024 --pipeline-depth 16 --writer-shards 1 \
    -o /mnt/nvme/scan-v4.rocks
```

| Metric | Result |
|--------|--------|
| Throughput (sustained) | **351,000 entries/sec** — **+5 % vs Profile A** |
| Per-thread profile | one `rocks-writer` at ~80 % CPU; walkers visibly active at 7–15 %; compaction at ~16 cores burst |

**The wall is the NFS server's response rate to a single client.** Three independent levers — adding 8× writer parallelism, doubling per-walker RPC concurrency, both at once — moved wall-clock by less than 5 %. Per-walker throughput barely budged between depth 8 (332 ent/s/walker) and depth 16 (343 ent/s/walker). That means walkers are not waiting for in-flight RPCs to drain; they're getting responses at a server-throttled cadence regardless of how many they queue up.

**Sizing factor:** the ~340–355 K entries/sec ceiling is a property of *this* server (a 34-cnode VAST cluster), not a property of nfs-walker. Larger clusters with more cnodes will deliver more requests/sec to a single client; smaller clusters less. Treat the numbers above as a **per-cluster-capacity datum**, not a tool benchmark — and re-run all three profiles when characterizing a new target.

**Recommended config for this class of target:**

```bash
./nfs-walker nfs://<server>/<export> \
    -w 1024 --pipeline-depth 8 \
    -o /mnt/nvme/scan.rocks
```

`--writer-shards` defaults to 1. Turn it on only when per-thread sampling (`ps -eLo pid,tid,pcpu,comm | sort -k3 -nr | head -40`) shows `rocks-writer` pinned at 99 % *and* walker threads at 0 % — that's the signature of a writer-bound scan, where Profile B's 4–6× lift can be claimed. On servers like the one above, that signature never appears.

**Parallel Parquet export** (read-only on the finished `.rocks`, can run alongside the next scan):

```bash
ulimit -n 1048576       # RocksDB read-only mode pins every SST file open
./nfs-walker export-parquet /mnt/nvme/scan.rocks /mnt/nvme/scan.parquet \
    --parallelism 160 -p
```

| Profile | Parquet wall-clock | Output size | Files |
|---------|-------------------|-------------|-------|
| A (1 writer)   | 7m 42s | **85.28 GiB** | 407 |
| B (8 writers)  | 8m 25s | **107.41 GiB** (+26 %) | 498 |

> ⚠️ **`--writer-shards > 1` costs ~25 % more Parquet bytes.** ZSTD compresses the path column heavily when adjacent rows in a row group share long common prefixes (`/videos/d8a/file-001.mp4`, `/videos/d8a/file-002.mp4`, …). With 8 shards, gxhash scatters siblings across 8 CFs — each shard's rows are internally sorted, but the path bytes within any one row group have far less mutual prefix overlap, so the compression dictionary becomes much less effective. Same compression level, worse compressibility. Another reason to leave `--writer-shards 1` as the default.

The Parquet directory is one logical scan (`metadata.json` lists every part), so DuckDB / DataFusion / Polars can `read_parquet('scan.parquet/scans/*/part-*.parquet')` and iterate the 4.4 B rows directly.

> Tuning notes for very large scans:
> - Scale RocksDB compaction parallelism with the host: the binary already sets `set_max_background_jobs((num_cpus / 2).clamp(4, 32))`.
> - Bump the producer→writer channel and the writer batch size on many-core boxes — defaults are 1024 / 5000 respectively.
> - `--pipeline-depth 8` is a sweet spot; `16` adds nothing on a server-bound target and adds memory pressure (16 in-flight READDIRPLUS responses per worker).
> - **The only client-side lever left after `--writer-shards 1 --pipeline-depth 8` is multi-host scan-out** — split the tree across N transfer hosts, each scanning a disjoint subtree. Per-client throttling at the NFS server is what the experiments above measure; running two clients should roughly double aggregate throughput on this class of cluster.

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
              │    RocksDB     │
              └────────────────┘
```

## Documentation

- [Building](docs/BUILDING.md) - Build instructions and dependencies
- [RocksDB Queries](docs/QUERY_ROCKSDB.md) - Built-in query commands
- [SQLite Queries](docs/QUERY_SQLITE.md) - SQL examples and export
- [Analytics Dashboard](#analytics-dashboard) - Web UI setup and usage

## License

MIT
