# Querying RocksDB Scans

`nfs-walker` writes scans to a RocksDB directory. The `stats` subcommand
prints a fast scan overview directly from the database. For deeper analytics
(largest files, by-extension, duplicates, ownership breakdowns, etc.) export
to Parquet and query with DuckDB / DataFusion, or export to SQLite for
ad-hoc SQL.

## Overview

```bash
nfs-walker stats scan.rocks
```

Output:

```
Database Statistics
─────────────────────────────────────────────────
  Total entries:  1,234,567
  Files:          1,200,000
  Directories:    33,000
  Symlinks:       1,567
  Total size:     4.2 TiB
  Allocated:      4.5 TiB
  Max depth:      18
```

The summary is computed from the per-DB summary column family (instant on
finished scans) and falls back to a full path-CF iteration on legacy DBs
without a summary CF.

## Detailed analytics — go via Parquet

The previous per-flag helpers (`--by-extension`, `--largest-files`,
`--by-uid`, `--duplicates`, etc.) were removed. Export to Parquet and run
SQL — DuckDB chews through 4 B rows in seconds:

```bash
ulimit -n 1048576
nfs-walker export-parquet scan.rocks ./parquet-out -p --parallelism 64

duckdb -c "SELECT
             COALESCE(extension, '(none)') AS ext,
             COUNT(*)                       AS files,
             SUM(size)/1e9                  AS gb
           FROM read_parquet('parquet-out/scans/*/part-*.parquet')
           WHERE entry_type = 0
           GROUP BY 1
           ORDER BY gb DESC
           LIMIT 20"
```

Or, if you want a single-file SQLite database, use `nfs-walker export-sql`
and any SQLite client:

```bash
nfs-walker export-sql scan.rocks scan.db --progress
sqlite3 scan.db
```

See [QUERY_SQLITE.md](QUERY_SQLITE.md) for SQLite query examples.

## Live querying during an active scan (`--live`)

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
nfs-walker stats /mnt/local-nvme/scan.rocks --live
```

### How secondary mode works

A secondary instance reads the live primary's SST files but maintains its
own MANIFEST/WAL replay state in a separate directory, so concurrent
compactions on the primary do not break it. `nfs-walker` auto-derives the
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
