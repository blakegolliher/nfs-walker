# Case Studies

Two real-world uses of `nfs-walker` against multi-petabyte NFS filesystems. Both
ran on transfer hosts mounted to the source NFS and writing scan output to
local NVMe. Customer-identifying details have been anonymized; performance
numbers and analysis output are real.

- [Case 1: differential analysis between two scans of the same filesystem](#case-1-what-changed-between-two-scans)
- [Case 2: first-scan profiling of a new filesystem](#case-2-profiling-an-unknown-filesystem)

---

## Case 1: what changed between two scans

A 6.77 PiB production filesystem holding training data, video corpora, and
model checkpoints. Two scans were taken 6 days apart so the team could quantify
churn — what was added, deleted, modified, and where the growth was landing.

### Scan environment

| | |
|---|---|
| Source filesystem | 6.77 PiB used, ~4.9B files |
| Transfer host | 160 cores, 1.4 TiB RAM, local NVMe scratch |
| nfs-walker version | 0.1.0 |

### Running the walks

```bash
sudo ./nfs-walker nfs.example.internal:/volumes/<volume-uuid> \
  -w 1536 --pipeline-depth 16 --writer-shards 1 \
  -o /mnt/local-nvme/scan-v1.rocks
```

Output of v1 (older scan):

```
Walk Complete
──────────────────────────────────────────────────
  Directories: 84,702,112
  Files: 4,548,043,761
  Total Size: 6.29 PiB
  Duration: 16562.3s (274,601 files/sec)
  Errors: 4,721
  Database: /mnt/local-nvme/scan-v1.rocks (820.10 GiB)
```

Output of v2 (later scan, same parameters, 6 days later):

```
Walk Complete
──────────────────────────────────────────────────
  Directories: 88,135,200
  Files: 4,906,237,129
  Total Size: 6.77 PiB
  Duration: 16418.8s (298,818 files/sec)
  Database: /mnt/local-nvme/scan-v2.rocks (889.59 GiB)
```

Each walk took ~4.5 hours. The walker traversed nearly 5 billion entries via
parallel READDIRPLUS and wrote directly to RocksDB on local NVMe.

### Export to Parquet

```bash
./nfs-walker export-parquet /mnt/local-nvme/scan-v1.rocks /mnt/local-nvme/scan-v1.parquet --parallelism 160 -p
./nfs-walker export-parquet /mnt/local-nvme/scan-v2.rocks /mnt/local-nvme/scan-v2.parquet --parallelism 160 -p
```

About 10 minutes each. Output is a partitioned Parquet dataset (~96 GiB for 4.99B
entries, ~9× compression vs the RocksDB representation), directly queryable
by DuckDB, DataFusion, Spark, or any other Parquet-aware tool.

### Diff analysis with DuckDB

```sql
PRAGMA temp_directory='/mnt/local-nvme/duckdb-tmp';
CREATE VIEW v1 AS SELECT * FROM read_parquet('/mnt/local-nvme/scan-v1.parquet/scans/*/part-*.parquet');
CREATE VIEW v2 AS SELECT * FROM read_parquet('/mnt/local-nvme/scan-v2.parquet/scans/*/part-*.parquet');

-- 1. How far apart were the scans?
SELECT
  (SELECT max(scan_timestamp_us) FROM v2) - (SELECT max(scan_timestamp_us) FROM v1) AS gap_us;
-- 531,465,000,000 µs = 6 days 3h 37m

-- 2. Files touched (mtime advanced past v1 scan time) — single-pass, no join.
SELECT count(*), sum(size)/1024.0/1024/1024/1024 AS tib
FROM v2
WHERE file_type='file'
  AND mtime_us > (SELECT max(scan_timestamp_us) FROM v1);
-- 400,029,982 files / 545.29 TiB

-- 3. Inode-level diff: added (in v2, not in v1)
SELECT count(*), sum(size)/1024.0/1024/1024/1024 AS tib
FROM v2 WHERE file_type='file'
  AND inode NOT IN (SELECT inode FROM v1 WHERE file_type='file');
-- 399,384,002 files / 544.88 TiB

-- 4. Removed (in v1, not in v2)
SELECT count(*), sum(size)/1024.0/1024/1024/1024 AS tib
FROM v1 WHERE file_type='file'
  AND inode NOT IN (SELECT inode FROM v2 WHERE file_type='file');
-- 41,199,903 files / 47.97 TiB

-- 5. Where did the growth land? Top-2 path component, ranked by added bytes.
WITH added AS (
  SELECT v2.path, v2.size FROM v2
  WHERE v2.file_type='file'
    AND v2.inode NOT IN (SELECT inode FROM v1 WHERE file_type='file')
)
SELECT regexp_extract(path, '^(/[^/]+/[^/]+)', 1) AS top2,
       count(*) AS n,
       sum(size)/1024.0/1024/1024/1024 AS tib
FROM added GROUP BY 1 ORDER BY tib DESC LIMIT 10;
```

### Findings

| Question | Answer |
|---|---|
| Time between scans | 6 days 3h 37m |
| Files added | 399.4M (+544.88 TiB) |
| Files deleted | 41.2M (−47.97 TiB) |
| Files modified in place | ~646K (≈0.16% of total churn) |
| **Net growth** | **+358.2M files, +496.9 TiB (~0.49 PiB)** |
| Per-day rate | ~65M files/day, ~80.8 TiB/day net |

**~99.8% of churn is brand-new files, not edits.** This is a write-once /
append-mostly workload — the team can plan storage tiering and snapshot policy
around that fact rather than around in-place edits.

**78% of new bytes landed in three video dataset subtrees.** Two of these
landed >100 TiB each over the six-day window. That single insight let the team
target retention and replication policy at three specific paths instead of
applying broad rules across the whole filesystem.

The diff also confirmed the filer is healthy: net file count grew by 358.2M,
the inode-anti-join numbers reconciled with the top-level walker totals (within
sub-percent, attributable to unmounted submounts and momentary in-flight
deletes), and there were no surprises in the deletion side of the ledger.

### What this enabled

Before nfs-walker, the team had no way to answer "what changed and where" on
this filesystem short of running `find -mtime` against ~5 billion files —
which is impractical against an active NFS export. Two 4.5-hour walks plus
five DuckDB queries produced an authoritative, file-level differential audit
that could be diffed, archived, and re-run on any cadence.

---

## Case 2: profiling an unknown filesystem

A 1.13 PiB ML training scratch filesystem on a different NFS server. The team
wanted to know what was on it: what kind of data, who owned the bytes, where
the bytes were concentrated, how much of it was hot vs cold. None of these
questions had ever been answered comprehensively.

### Scan environment

| | |
|---|---|
| Source filesystem | 1.13 PiB used, ~141M entries |
| Transfer host | 160 cores, 1.4 TiB RAM, local NVMe scratch |
| Output disk | Single 11.6 TiB NVMe, XFS, mounted with `noatime,nodiratime` |

### Disk setup (single XFS mount on raw NVMe)

```bash
sudo mkfs.xfs -L scratch /dev/nvme0n1
sudo mkdir -p /mnt/local-nvme
sudo mount -o noatime,nodiratime /dev/nvme0n1 /mnt/local-nvme
echo "LABEL=scratch /mnt/local-nvme xfs noatime,nodiratime 0 0" | sudo tee -a /etc/fstab
```

No partition needed for a single-purpose scratch volume. `noatime` is important
when the workload is RocksDB — every SST read otherwise triggers an atime
metadata write.

### Worker tuning, the practical lesson

The first run used 2048 workers — the number that worked on the figure case.
That setup was network-latency-bound (load average ~40 of 160 cores), so it
made sense to push parallelism. On the new host, with a lower-latency filer
and `--writer-shards 4`, the configuration was wrong:

```text
After 38 minutes:
  load average: 1738
  CPU: 100% user, 0% idle
  Context switches: ~700K-900K/sec
  Walker rate: collapsing — 224K → 68K → 6.5K entries/sec
```

The signature is **CPU pegged with throughput collapsing**: scheduler
overhead from oversubscription. Each context switch is overhead, not work.
The fix was to drop workers until the system had headroom:

```bash
sudo ./nfs-walker nfs.example.internal:/volumes/<volume-uuid> \
  -w 512 --pipeline-depth 16 --writer-shards 4 \
  -o /mnt/local-nvme/scan.rocks
```

Result:

```
Walk Complete
──────────────────────────────────────────────────
  Directories: 27,857,101
  Files: 113,697,839
  Total Size: 1.13 PiB
  Duration: 645.3s (176,203 files/sec, 220K entries/sec)
  Database: /mnt/local-nvme/scan.rocks (20.91 GiB)
```

**10 minutes 45 seconds.** Same hardware as the failed run, same source
filesystem, just 4× fewer workers. With CPU now sitting at 50–77% user and
11–34% idle, the system was actually feeding workers efficiently instead of
spending cycles on scheduler thrash.

> Lesson: worker count is not portable across hosts and filers. The right
> number is "enough to keep cores busy without scheduler thrash." If you see
> 100% user CPU with throughput dropping, you have too many workers.

### Export to Parquet

```bash
./nfs-walker export-parquet /mnt/local-nvme/scan.rocks /mnt/local-nvme/scan.parquet --parallelism 160 -p
```

About 90 seconds. ~3 GiB of Parquet for 141M entries.

### Six profiling queries

The queries below produce a complete profile of an unknown filesystem in under
a minute on a moderately-sized box. All output uses DuckDB's `format()` for
human-readable numbers (thousand separators and 2-decimal floats).

```sql
PRAGMA temp_directory='/mnt/local-nvme/duckdb-tmp';
CREATE VIEW fs AS SELECT * FROM read_parquet('/mnt/local-nvme/scan.parquet/scans/*/part-*.parquet');

-- 1. Headline totals
SELECT
  format('{:,}',    count(*) FILTER (WHERE file_type='file'))                           AS files,
  format('{:,}',    count(*) FILTER (WHERE file_type='directory'))                      AS dirs,
  format('{:,}',    count(*) FILTER (WHERE file_type='symlink'))                        AS symlinks,
  format('{:,}',    count(*))                                                           AS total_entries,
  format('{:,.2f}', sum(size)    FILTER (WHERE file_type='file')/1024.0/1024/1024/1024) AS total_tib,
  format('{:,.2f}', avg(size)    FILTER (WHERE file_type='file')/1024.0/1024)           AS avg_file_mib,
  format('{:,.2f}', median(size) FILTER (WHERE file_type='file')/1024.0)                AS median_file_kib
FROM fs;
```

```text
┌─────────────┬────────────┬──────────┬───────────────┬───────────┬──────────────┬─────────────────┐
│    files    │    dirs    │ symlinks │ total_entries │ total_tib │ avg_file_mib │ median_file_kib │
├─────────────┼────────────┼──────────┼───────────────┼───────────┼──────────────┼─────────────────┤
│ 113,645,845 │ 27,857,100 │ 50,672   │ 141,554,939   │ 1,154.29  │ 10.65        │ 38.29           │
└─────────────┴────────────┴──────────┴───────────────┴───────────┴──────────────┴─────────────────┘
```

```sql
-- 2. Size distribution: where do the bytes actually live?
SELECT
  CASE
    WHEN size = 0          THEN '0:empty'
    WHEN size < 4096       THEN '1:<4K'
    WHEN size < 65536      THEN '2:<64K'
    WHEN size < 1048576    THEN '3:<1M'
    WHEN size < 16777216   THEN '4:<16M'
    WHEN size < 268435456  THEN '5:<256M'
    WHEN size < 4294967296 THEN '6:<4G'
    ELSE                        '7:>=4G'
  END                                                AS bucket,
  format('{:,}',    count(*))                        AS n,
  format('{:,.2f}', sum(size)/1024.0/1024/1024/1024) AS tib
FROM fs WHERE file_type='file'
GROUP BY bucket ORDER BY bucket;
```

```text
┌─────────┬────────────┬─────────┐
│ bucket  │     n      │   tib   │
├─────────┼────────────┼─────────┤
│ 0:empty │ 620,515    │ 0.00    │
│ 1:<4K   │ 35,829,757 │ 0.08    │
│ 2:<64K  │ 32,983,580 │ 1.00    │
│ 3:<1M   │ 36,617,966 │ 6.47    │
│ 4:<16M  │ 4,484,273  │ 13.91   │
│ 5:<256M │ 2,888,943  │ 127.90  │
│ 6:<4G   │ 145,561    │ 161.91  │
│ 7:>=4G  │ 75,250     │ 843.03  │   ← 0.07% of files = 73% of capacity
└─────────┴────────────┴─────────┘
```

This single table reframes every other question: the filesystem is a "fat
file" workload, not a "many small files" workload. 75K files (mostly checkpoints
and dataset shards) hold 73% of all bytes. Capacity decisions should target
those, not the 35M files in the <4K bucket.

```sql
-- 3. Top dirs by capacity (immediate children, ordered by bytes)
SELECT parent_path,
       format('{:,}',    count(*))                            AS n_files,
       format('{:,.2f}', sum(size)/1024.0/1024/1024/1024)     AS tib,
       format('{:,.2f}', avg(size)/1024.0/1024)               AS avg_mib
FROM fs WHERE file_type='file'
GROUP BY parent_path
ORDER BY sum(size) DESC LIMIT 10;
```

Sample output (paths anonymized):

```text
┌─────────────────────────────────────────────────────────────┬───────────┬─────────────┬──────────┐
│                        parent_path                          │  n_files  │     tib     │ avg_mib  │
├─────────────────────────────────────────────────────────────┼───────────┼─────────────┼──────────┤
│ /datasets/audio/training/<dataset_id>/audio                 │ 2,720,033 │     85.53   │      33  │
│ /datasets/audio/training/<dataset_id>/raw                   │ 5,529,450 │      1.30   │       0  │
│ /user-a/checkpoints/qwen-large/it100/hf                     │        12 │     27.50   │   2,343K │
│ /user-b/checkpoints/step_100                                │         4 │     22.40   │   5,734K │
└─────────────────────────────────────────────────────────────┴───────────┴─────────────┴──────────┘
```

Two distinct patterns visible: massive dataset directories with millions of
medium files, and tiny checkpoint directories with a handful of huge files.
Both kinds of "biggest" matter for different reasons.

```sql
-- 4. Largest individual files
SELECT path,
       format('{:,.2f}', size/1024.0/1024/1024)               AS gib,
       uid,
       to_timestamp(mtime_us/1000000)                         AS mtime
FROM fs WHERE file_type='file'
ORDER BY size DESC LIMIT 10;
```

```text
┌────────────────────────────────────────────────────────────────────────┬────────┬───────┐
│                                  path                                  │   gib  │  uid  │
├────────────────────────────────────────────────────────────────────────┼────────┼───────┤
│ /datasets/<dataset_id>/sources/sharegpt4v_images.zip                   │ 158.64 │ 20009 │
│ /datasets/<dataset_id>/dataset.jsonl                                   │ 109.90 │ 20013 │
│ /user-a/<run-id>/checkpoints/step_100/__0_0.distcp                     │  84.45 │ 20014 │
│ /user-a/<run-id-2>/checkpoints/step_100/__0_0.distcp                   │  84.45 │ 20014 │
│ /user-b/<run-id>/checkpoints/qwen-large/hf/model-00001.safetensors     │  64.56 │ 20012 │
└────────────────────────────────────────────────────────────────────────┴────────┴───────┘
```

```sql
-- 5. File workload signature: top extensions by total bytes
SELECT coalesce(extension, '(none)')                       AS ext,
       format('{:,}',    count(*))                          AS n,
       format('{:,.2f}', sum(size)/1024.0/1024/1024/1024)   AS tib
FROM fs WHERE file_type='file'
GROUP BY ext
ORDER BY sum(size) DESC LIMIT 10;
```

```text
┌─────────┬────────────┬─────────┐
│   ext   │     n      │   tib   │
├─────────┼────────────┼─────────┤
│ pt      │    199,239 │  529.51 │   PyTorch checkpoints
│ distcp  │    504,089 │  275.10 │   torch.distributed checkpoints
│ (none)  │    180,956 │   98.16 │
│ flac    │ 22,672,242 │   95.63 │   audio training data
│ parquet │    154,418 │   89.32 │   tabular data shards
│ tar     │     16,960 │   48.04 │
│ html    │  4,263,053 │    7.00 │
│ jpg     │ 10,814,115 │    1.63 │
│ jsonl   │    931,711 │    1.47 │
│ png     │  8,198,080 │    1.27 │
└─────────┴────────────┴─────────┘
```

The extension distribution is the workload's signature in one table:
`.pt + .distcp + .safetensors` (~1 PiB combined) plus `.flac + .mp3 + .wav`
(audio training data) plus `.parquet` (tabular shards). Without anyone
explaining the cluster, the data tells you it's a multi-modal ML training
workspace. No interviews needed.

```sql
-- 6. Where the bytes live by top-level subtree
SELECT regexp_extract(path, '^(/[^/]+/[^/]+)', 1)         AS top2,
       format('{:,}',    count(*))                         AS n_files,
       format('{:,.2f}', sum(size)/1024.0/1024/1024/1024)  AS tib
FROM fs WHERE file_type='file'
GROUP BY top2
ORDER BY sum(size) DESC LIMIT 10;
```

```text
┌──────────────────────────────────────┬───────────┬────────┐
│                 top2                 │  n_files  │  tib   │
├──────────────────────────────────────┼───────────┼────────┤
│ /user-c/<run-id>                     │    15,616 │ 156.58 │
│ /datasets/grooming                   │ 2,921,879 │  90.46 │
│ /user-d/checkpoints                  │   350,602 │  82.90 │
│ /user-d/multi_modal                  │    67,983 │  62.77 │
│ /models/Qwen3-30B-A3B_slime          │     6,705 │  57.35 │
│ /user-b/<run-id>                     │     4,960 │  49.21 │
│ /datasets/unsupervised_speech        │    33,730 │  43.08 │
│ /user-e/<job-id>                     │     6,490 │  28.62 │
└──────────────────────────────────────┴───────────┴────────┘
```

### Findings

After the export and the six queries — about two minutes of analysis on top of
the 11-minute walk:

- **Workload type**: ML training cluster, dominated by checkpoints and audio
  datasets. Single user's run accounted for 156 TiB of capacity in a single
  subtree.
- **Capacity profile**: 75,250 files (>=4 GiB each) hold 843 TiB of the total
  1,154 TiB. Optimizing the long tail of small files would yield negligible
  capacity relief; large-file lifecycle policy is the lever that matters.
- **Activity profile** (from the recency query, not shown above): 98% of bytes
  were touched in the last 90 days. This is an *active* working dataset, not
  an archive, so cold-tier proposals would target a vanishingly small fraction.
- **Operational anomaly**: 161 abandoned Python multiprocessing IPC sockets in
  one user's `/tmp/` subtree, clustered by date — an indicator of crashed jobs
  worth flagging back to that user.

### What this enabled

The team went from "we have no idea what's on this filer" to a complete,
queryable profile in under 15 minutes of compute time and roughly five
minutes of analyst time. The Parquet artifact is small (~3 GiB), retained as
a baseline, and any subsequent scan can be diffed against it with the same
queries from Case 1.

---

## Notes on the queries

- Both case studies use a single binary, `nfs-walker`, with subcommands for
  `walk` (default) and `export-parquet`.
- The Parquet schema is stable and documented in `src/parquet/schema.rs`. All
  queries here are portable to DataFusion, Spark, ClickHouse, Athena, BigQuery,
  or any other Parquet-aware engine.
- `PRAGMA temp_directory='/mnt/local-nvme/duckdb-tmp'` is recommended before
  running joins or anti-joins on multi-billion-row datasets so DuckDB's spill
  files land on fast local storage rather than `/tmp`.
- `format('{:,}', ...)` and `format('{:,.2f}', ...)` are DuckDB-specific. To
  preserve numeric ordering when displaying formatted columns, sort on the raw
  expression in `ORDER BY`, not on the formatted alias.
