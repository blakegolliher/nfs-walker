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
| `-n N`, `--top N` | Limit results to top N (default: 20) |

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
