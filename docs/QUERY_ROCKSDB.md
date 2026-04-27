# Querying RocksDB Scans

nfs-walker can query RocksDB databases directly without converting to SQLite. This is faster for common queries.

## Basic Usage

```bash
# Overview statistics
nfs-walker stats scan.rocks

# With specific queries
nfs-walker stats scan.rocks --by-extension
nfs-walker stats scan.rocks --largest-files -n 20
```

---

## Available Queries

### Overview (default)

```bash
nfs-walker stats scan.rocks
```

Output:
```
Database Statistics
─────────────────────────────────────────────────
  Total entries:  42,993,048
  Files:          42,993,009
  Directories:    39
  Symlinks:       0
  Total size:     5.13 GiB
  Allocated:      5.51 GiB
  Max depth:      2
```

### Files by Extension

```bash
nfs-walker stats scan.rocks --by-extension -n 15
```

Output:
```
Files by Extension (top 15):
─────────────────────────────────────────────────
Extension       Count           Size      Allocated
---------       -----           ----      ---------
(none)      12,345,678      1.23 TiB      1.31 TiB
.log         8,234,567    892.45 GiB    945.12 GiB
.txt         5,123,456    234.56 GiB    256.78 GiB
.json        2,345,678    123.45 GiB    134.56 GiB
...
```

### Largest Files

```bash
nfs-walker stats scan.rocks --largest-files -n 10
```

Output:
```
Largest Files (top 10):
─────────────────────────────────────────────────
     12.34 GiB  /data/backups/full-backup-2024.tar.gz
      8.92 GiB  /data/logs/application.log
      5.67 GiB  /data/database/main.db
...
```

### Directories with Most Files

```bash
nfs-walker stats scan.rocks --largest-dirs -n 10
```

Output:
```
Directories with Most Files (top 10):
─────────────────────────────────────────────────
       Files  Path
       -----  ----
  12,345,678  /data/scratch
   5,234,567  /data/logs
   2,123,456  /home/user1
...
```

### Oldest Files

```bash
nfs-walker stats scan.rocks --oldest-files -n 10
```

Output:
```
Oldest Files (top 10):
─────────────────────────────────────────────────
2019-03-15 14:23      123.4 MiB  /data/archive/old-report.pdf
2019-05-22 09:15       45.6 MiB  /data/legacy/config.xml
2020-01-01 00:00      789.0 KiB  /data/backup/2020-snapshot.zip
...
```

### Files with Most Hard Links

```bash
nfs-walker stats scan.rocks --most-links -n 10
```

Output:
```
Files with Most Hard Links (top 10):
─────────────────────────────────────────────────
   Links          Size  Path
   -----          ----  ----
    1234      1.23 GiB  /data/shared/common-library.so
     567    456.78 MiB  /data/dedup/block-abc123
     234    123.45 MiB  /data/dedup/block-def456
...
```

### Usage by User ID

```bash
nfs-walker stats scan.rocks --by-uid -n 10
```

Output:
```
Usage by User ID (top 10):
─────────────────────────────────────────────────
     UID        Files        Dirs     Total Size
     ---        -----        ----     ----------
    1000   12,345,678      1,234       2.34 TiB
    1001    5,678,901        567     892.45 GiB
       0    2,345,678        234     456.78 GiB
...
```

### Usage by Group ID

```bash
nfs-walker stats scan.rocks --by-gid -n 10
```

Output:
```
Usage by Group ID (top 10):
─────────────────────────────────────────────────
     GID        Files        Dirs     Total Size
     ---        -----        ----     ----------
    1000   15,678,901      2,345       3.45 TiB
     100    8,901,234      1,234       1.23 TiB
       0    3,456,789        456     567.89 GiB
...
```

### Duplicate Files by Checksum

*Requires scan with `-c` (checksum) flag.*

```bash
nfs-walker stats scan.rocks --duplicates -n 10
nfs-walker stats scan.rocks --duplicates --min-size 1048576  # Only files > 1MB
```

Output:
```
Duplicate Files (top 10):
─────────────────────────────────────────────────
Checksum: a1b2c3d4e5f6...
  Size: 456.78 MiB × 3 copies (wasted: 913.56 MiB)
    /data/project-a/dataset.bin
    /data/project-b/dataset.bin
    /data/archive/dataset.bin

Checksum: f6e5d4c3b2a1...
  Size: 123.45 MiB × 2 copies (wasted: 123.45 MiB)
    /home/user1/report.pdf
    /home/user2/report.pdf
...
```

### File Type Distribution

*Requires scan with `-t` (file type) flag.*

```bash
nfs-walker stats scan.rocks --by-file-type -n 15
```

Output:
```
File Types (top 15):
─────────────────────────────────────────────────
MIME Type                    Count        Total Size
---------                    -----        ----------
application/octet-stream   345,678      234.56 GiB
image/jpeg                 123,456       45.67 GiB
application/pdf             45,678       12.34 GiB
video/mp4                   12,345       89.01 GiB
...
```

### Hard Link Groups

```bash
nfs-walker stats scan.rocks --hardlink-groups -n 10
```

Output:
```
Hard Link Groups (top 10):
─────────────────────────────────────────────────
Inode: 12345678  (nlink=5, size=1.23 GiB)
    /data/shared/common-lib.so
    /data/app1/lib/common-lib.so
    /data/app2/lib/common-lib.so
    /data/app3/lib/common-lib.so
    /data/backup/common-lib.so
...
```

---

## Combining Queries

Run multiple queries at once:

```bash
nfs-walker stats scan.rocks --by-extension --by-uid --largest-files -n 10
```

---

## Options Reference

| Option | Description |
|--------|-------------|
| `--by-extension` | Files grouped by extension with count/size |
| `--largest-files` | Biggest files by size |
| `--largest-dirs` | Directories with most direct children |
| `--oldest-files` | Files with oldest modification time |
| `--most-links` | Files with most hard links (nlink) |
| `--by-uid` | Usage breakdown by user ID |
| `--by-gid` | Usage breakdown by group ID |
| `--duplicates` | Files with identical checksums (requires `-c` scan) |
| `--by-file-type` | Files grouped by MIME type (requires `-t` scan) |
| `--hardlink-groups` | Groups of paths sharing the same inode |
| `--min-size N` | Minimum file size for `--duplicates` (default: 1024) |
| `-n N`, `--top N` | Limit results to top N (default: 20) |
| `--live` | Open in RocksDB secondary mode for live querying during an active scan |

---

## Live Querying During an Active Scan (`--live`)

By default `nfs-walker stats` opens the database in RocksDB **read-only**
mode. Read-only mode snapshots the SST file list at open time, which means
it cannot tolerate the writer running compactions while you query — you
will see errors like:

```
RocksDB error: IO error: No such file or directory:
While open a file for random read: /path/to/scan.rocks/005026.sst
```

That's a compaction having deleted an SST file out from under the
read-only view. It is not a corruption.

The `--live` flag opens the same database in RocksDB **secondary** mode,
which is designed for this case:

```bash
# Query a database that an active scan is still writing to
nfs-walker stats /mnt/local-nvme/figure.rocks --by-extension -n 15 --live
```

### How secondary mode works

A secondary instance reads the live primary's SST files but maintains its
own MANIFEST/WAL replay state in a separate directory, so concurrent
compactions on the primary do not break it. nfs-walker auto-derives the
secondary state directory:

```
${TMPDIR}/nfs-walker-secondary-<hash-of-canonical-primary-path>
```

Every `--live` invocation calls `try_catch_up_with_primary()` after open,
so each query reflects all data the primary had committed at open time.

### Caveats

- **Snapshot at open, not real-time.** A long query iterates a snapshot;
  data committed by the primary mid-query is not picked up until the next
  invocation.
- **Slightly slower to open** than read-only (one MANIFEST replay).
- **Secondary state dir is reusable.** Subsequent `--live` invocations
  pick up where they left off, which is faster. Safe to delete at any
  time — it will be recreated.
- **Final stats are always more accurate from a finished scan.** Once the
  scan completes, drop `--live` to use the faster read-only path.
- **One secondary per state dir.** Two concurrent `--live` queries on the
  same primary would collide on the auto-derived dir. If you need
  parallelism, run them serially or wait for the scan to finish.

---

## When to Use RocksDB vs SQLite

**Use RocksDB queries when:**
- You need quick answers to common questions
- Running on the same machine that did the scan
- Don't need complex joins or custom queries

**Convert to SQLite when:**
- You need complex SQL queries (joins, subqueries, CTEs)
- Sharing results with others who have SQLite
- Building custom reports or integrations
- Exporting to CSV/JSON

Convert with:
```bash
nfs-walker convert scan.rocks scan.db --progress
```

---

## Live DuckDB queries with `--stream-parquet`

For ad-hoc SQL on a scan that's still running, add `--stream-parquet` to
the scan command. The walker writes a parallel rolled Parquet directory
alongside the RocksDB:

```
/mnt/local-nvme/figure.rocks/             # the existing RocksDB
/mnt/local-nvme/figure.rocks.parquet/     # streamed Parquet sibling
└── scans/
    └── <scan_id>/                        # UUID generated at scan start
        ├── part-00000.parquet
        ├── part-00001.parquet
        └── ...
```

Each `part-NNNNN.parquet` is written as `.part-NNNNN.parquet.tmp` and
atomically renamed on close, so DuckDB glob queries naturally skip
in-progress files.

```bash
# Start a scan with streaming enabled
nfs-walker nfs://server/export -o /mnt/local-nvme/figure.rocks --stream-parquet

# In another terminal, query the live data with DuckDB
duckdb -c "SELECT extension, COUNT(*) AS files, SUM(size) AS bytes
           FROM read_parquet('/mnt/local-nvme/figure.rocks.parquet/scans/*/part-*.parquet')
           GROUP BY extension ORDER BY bytes DESC LIMIT 20"
```

### Caveats

- **Backpressure drops, not stalls.** If the Parquet writer can't keep up
  with the RocksDB writer, batches are dropped (a warning is logged at
  end-of-scan with the drop count). Ingest never blocks. Re-run
  `nfs-walker export-parquet <db> <out>` against the finished RocksDB if
  you need a complete export -- the post-scan converter reads the same
  `scan_id` from RocksDB metadata, so a clean output dir is required.
- **Many small parts at scan start.** The first few parts may be smaller
  than `target_file_size` (256 MB default) because the writer rotates on
  size, not on time.
- **`scan_id` is shared between paths.** A streamed scan and a later
  `export-parquet` use the same UUID, keeping the Parquet directory
  layout stable across both code paths.
