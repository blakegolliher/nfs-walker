# Multi-Shard RocksDB Writer Design

Status: design, not yet implemented.
Owner: -
Target: feature-flagged behind `--writer-shards N` (1 disables, current behavior).

## 1. Problem

After landing the writer-side throughput tweaks (commit `4297796`),
the new ceiling on a 1024-worker / `--pipeline-depth 8` walk against a
4.4B-entry NFS export is roughly **315 K entries/sec sustained**, ~5×
the prior single-threaded baseline. Per-thread CPU sampling at the
23-minute mark of that run shows where the cap is now:

```
rocks-writer        99.9 %    our writer thread
rocksdb+ × 9        99.9 %    background compaction (cap 32, ~9 active)
rocksdb+ × 5      8–92 %      compaction not pegged
walker-N × 1024     0.1–0.2 % blocked on entry_tx.send during writer stalls
```

The writer thread is back at 100 %. With pipelining, larger channels,
bigger batches, and more compaction parallelism all in place, the
single-threaded `put_cf` loop in `rocksdb_writer_loop`
(`src/walker/simple.rs:1393-1499`) is now the wall — one core
serializing every entry from 1 024 producers into one
`entries_by_path` column-family.

ETA at 315 K entries/sec is ~3 h 45 min for a 4.4 B-entry walk. Walker
threads sitting at 0.1 % CPU are the proof that we're throwing away
~98 % of the box's compute on the producer side too — every worker is
parked on `entry_tx.send` while one writer drains the channel.

The fix is to shard the writer: split the `entries_by_path` column
family into N shards, run N writer threads, each owning one shard
keyed by `hash(path) mod N`. RocksDB allows concurrent CF writes
(separate memtables, separate compaction state per CF), so each
writer scales nearly independently.

## 2. Goal

Push the entry-emission rate to **1.0–1.5 M entries/sec** on the
production target — i.e. **4–6× the post-tweaks baseline**, **20–30×
the original baseline**. Concretely: bring the same 4.4 B-entry walk
in under **2 hours wall-clock**.

Non-goals:

- Sharding the inode CF. The 8-byte fixed keys flush + compact
  cheaply; profiling has not shown the inode write path as a
  bottleneck. Worth re-checking after the path-CF shard lands, but
  out of scope for v1.
- Sharding any other CF (`metadata`, `big_directories`, `summary`).
  These see negligible write traffic.
- Multi-process writers. Stay in-process; the parallelism we need is
  thread-level.
- New compression / encoding tuning. The existing settings are
  already balanced for write throughput.

## 3. Key insight

RocksDB's design supports per-CF concurrency natively:

- Each CF has its own memtable, immutable memtable list, and SST
  file set.
- The compaction thread pool is shared, but jobs partition by CF;
  with `set_max_background_jobs(32)` (post-tweaks default) we have
  plenty of headroom for N ≤ 16 shards' parallel flushes/compactions.
- `allow_concurrent_memtable_write = true` (already set in
  `src/rocksdb/schema.rs:264`) lets multiple threads write to even
  *the same* CF without a per-write mutex; for N writer threads each
  on a *different* CF, the contention is zero.

So the architecture is just:

```
worker thread ─┐
worker thread ─┤
worker thread ─┤  hash(path) mod N
   ...         │ ──────────────────►  shard-0 channel ──► writer-0 ──► CF entries_by_path_0
                                       shard-1 channel ──► writer-1 ──► CF entries_by_path_1
                                       shard-2 channel ──► writer-2 ──► CF entries_by_path_2
                                       ...
                                       shard-N channel ──► writer-N ──► CF entries_by_path_N
                                                          ▲
                                              all writers share one
                                              CF entries_by_inode (concurrent memtable)
```

Reads that need ordered iteration over the path CF (the export-parquet
path, the stats path, the convert path) merge the N shards via a
k-way priority queue. With N=8, that's `log2(8) = 3` comparisons per
emitted row — negligible.

## 4. Architecture

Three layers. Each can be implemented and tested independently.

### Layer 1: schema — sharded CF discovery + creation

Today's CF list (`src/rocksdb/schema.rs:28-45`):

```rust
pub const CF_ENTRIES_BY_PATH: &str = "entries_by_path";
pub const CF_ENTRIES_BY_INODE: &str = "entries_by_inode";
pub const CF_METADATA: &str = "metadata";
pub const CF_BIG_DIRS: &str = "big_directories";
pub const CF_SUMMARY: &str = "summary";
```

Add:

```rust
/// Prefix for sharded path-CF names. Shard k's CF is
/// `format!("{}{}", CF_ENTRIES_BY_PATH_PREFIX, k)`, e.g.
/// `entries_by_path_0`, `entries_by_path_1`, ...
pub const CF_ENTRIES_BY_PATH_PREFIX: &str = "entries_by_path_";

/// Metadata key under which the shard count is persisted at scan
/// creation time. Readers consult it to know how many path CFs to
/// open and merge.
pub const META_PATH_SHARDS: &str = "path_cf_shards";
```

`RocksHandle` gains a `shards: usize` field populated at open time,
and a method `cf_entries_by_path_shard(idx: usize) -> &ColumnFamily`.

The legacy `cf_entries_by_path()` method stays — it's used by callers
that don't yet know about sharding (legacy DBs use `shards = 1` and
the single CF named `entries_by_path` without a suffix). The shim:

```rust
impl RocksHandle {
    pub fn cf_entries_by_path_shard(&self, idx: usize) -> &ColumnFamily {
        debug_assert!(idx < self.shards);
        if self.shards == 1 {
            // Legacy DB or shard-count-1 new DB: still uses the
            // unsuffixed CF name.
            return self.cf_entries_by_path();
        }
        let name = format!("{}{}", CF_ENTRIES_BY_PATH_PREFIX, idx);
        self.db
            .cf_handle(&name)
            .unwrap_or_else(|| panic!("missing path shard CF: {}", name))
    }

    /// Iterate every path-CF shard, returning (shard_idx, &CF) pairs
    /// in ascending shard order. Used by readers that need to merge
    /// across shards.
    pub fn cf_entries_by_path_all_shards(&self) -> impl Iterator<Item = (usize, &ColumnFamily)> {
        (0..self.shards).map(move |i| (i, self.cf_entries_by_path_shard(i)))
    }
}
```

`open_rocks_db` and `open_rocks_db_readonly` need to discover the
shard count. Two cases:

1. **Creating a fresh DB**: caller passes `shards: usize` to a new
   `open_rocks_db_with_shards(path, shards)`. The function builds
   `cf_descriptors` including N path-shard CFs and writes
   `META_PATH_SHARDS` to the metadata CF.
2. **Opening an existing DB**: read `META_PATH_SHARDS` from the
   metadata CF. If absent (legacy DB), default to `1` and use the
   unsuffixed `entries_by_path` CF. If present, use that count.

Today's `existing_known_cfs` helper at `src/rocksdb/schema.rs:228` is
already CF-discovery-tolerant; extend it to recognize
`entries_by_path_*` shard CFs.

### Layer 2: writer — N writer threads

`rocksdb_writer_loop` at `src/walker/simple.rs:1393` becomes
per-shard. New signature:

```rust
fn rocksdb_writer_loop_shard(
    handle: Arc<RocksHandle>,
    shard_idx: usize,
    entry_rx: Receiver<Vec<DbEntry>>,
    parquet_tx: Option<Sender<Vec<DbEntry>>>,
    batch_size: usize,
) -> Result<ShardWriterStats, RocksError>;
```

Body is mostly today's `rocksdb_writer_loop` minus the
summary-CF flush / FLUSH_INTERVAL bookkeeping (we centralize those —
see below). Each call to `write_rocks_batch` writes to:

- `handle.cf_entries_by_path_shard(shard_idx)` for the shard's
  partition of paths
- `handle.cf_entries_by_inode()` for the (still-shared) inode CF —
  `allow_concurrent_memtable_write` makes this safe.

The summary accumulator and periodic `db.flush()` move to a single
**coordinator thread** that:

- Receives per-shard `ShardWriterStats` snapshots periodically (every
  N writes or every T seconds).
- Updates the global `SummaryAccumulator` from the merged snapshots.
- Triggers `db.flush()` (still global — RocksDB flushes all dirty
  memtables across all CFs in one call).
- Forwards already-written batches to the streaming Parquet writer
  if `--stream-parquet` is enabled (note: streaming Parquet should be
  **disabled when shards > 1** in v1 — see §6 edge case 5).

Or simpler: skip the coordinator thread entirely in v1. Each writer
maintains its own slice of the summary accumulator; merge them once
at scan finalization. Periodic global `db.flush()` happens from the
last shard writer to cross a per-shard threshold (with a mutex to
avoid concurrent flush calls — RocksDB serializes them anyway, but
the mutex avoids redundant work).

I lean toward **no coordinator thread**; the bookkeeping fits cleanly
into per-shard state. The `summary_cf` write at scan finalization is
trivially cheap.

### Layer 3: routing — workers fan out per-shard

Today's worker holds one `Sender<Vec<DbEntry>>` and pushes a single
batch into it. New worker holds `Vec<Sender<Vec<DbEntry>>>` of length
N (one per shard) and N parallel batches:

```rust
struct ShardedSender {
    senders: Vec<Sender<Vec<DbEntry>>>,    // length N
    batches: Vec<Vec<DbEntry>>,            // length N, each cap = batch_size
    shards: usize,
    batch_size: usize,
}

impl ShardedSender {
    fn push(&mut self, entry: DbEntry) -> Result<(), SendError<Vec<DbEntry>>> {
        let shard = (gxhash::gxhash64(entry.path.as_bytes(), HASH_SEED) as usize) % self.shards;
        let batch = &mut self.batches[shard];
        batch.push(entry);
        if batch.len() >= self.batch_size {
            let full = std::mem::replace(batch, Vec::with_capacity(self.batch_size));
            self.senders[shard].send(full)?;
        }
        Ok(())
    }

    /// Drain all per-shard residual batches at end-of-walk.
    fn flush_all(&mut self) -> Result<(), SendError<Vec<DbEntry>>> {
        for (shard, batch) in self.batches.iter_mut().enumerate() {
            if !batch.is_empty() {
                let full = std::mem::take(batch);
                self.senders[shard].send(full)?;
            }
        }
        Ok(())
    }
}
```

Use `gxhash` because (a) it's already a dependency
(`crate::content::checksum::compute_gxhash`) and (b) it's one of the
fastest non-cryptographic hashes for short inputs. Pin a fixed seed
constant so shard assignment is deterministic across processes (we
need this for verification queries — see §6 edge case 4).

Per-worker memory: N shards × `batch_size` entries × ~280 bytes ≈
N × 1.4 MB. With N=8 and 1024 workers that's ~11 GB peak in the
worst case (every worker holds N nearly-full batches). On a 1.4 TiB
host, fine. On a smaller box, drop the default batch size.

Both `worker_loop` (legacy) and `worker_loop_pipelined` need to grow
a `ShardedSender` instead of a plain `Sender`. The construction
happens in `run_workers` (`src/walker/simple.rs:655`) — build N
channels up front, hand each worker N senders.

## 5. Memory safety / data-race contract

All shared state is `Send + Sync`. The contract is straightforward:

**Rule 1: each CF is owned by exactly one writer thread.** No two
writer threads ever call `put_cf` against the same path-shard CF.
This is the property that lets RocksDB's per-CF parallelism work
without per-shard mutexes.

**Rule 2: the shared inode CF tolerates concurrent writes.** Already
true today (single writer with `allow_concurrent_memtable_write =
true`); for N writers it relies on the same flag. RocksDB's
documentation explicitly supports this case.

**Rule 3: hash determinism.** The same `(path, shard_count)` pair
must always map to the same shard, across processes and across runs.
Pin `gxhash::gxhash64` seed = 0 (or another constant); document it.
If we ever change the hash function, bump the
`META_PATH_SHARDS_HASH_VERSION` metadata key so old DBs can be
detected and read with the right hash.

**Rule 4: shard count is immutable for the life of a DB.** Writing
the count to metadata at create-time and refusing to reopen with a
different count is sufficient. Resharding is a separate operation
not in scope.

## 6. Edge cases

1. **Legacy DB (`shards == 0` or unset metadata)**: open the
   unsuffixed `entries_by_path` CF, set `shards = 1`, and run with
   one writer (i.e. today's behavior). Reads use the single-CF path.

2. **Reader needs ordered iteration**: use `KMergeIterator` (k-way
   merge via `BinaryHeap<Reverse<(Vec<u8>, ShardIdx, ValueCursor)>>`)
   over `cf_entries_by_path_all_shards()`. Each shard yields keys in
   lexicographic order, the heap picks the global minimum. Cost
   `O(log N)` per emitted row, N small.

3. **Reader needs point lookup by path**: `get_by_path(path)` knows
   the path → can compute the shard via `gxhash(path) % shards` and
   issue a single `get_cf` against that shard's CF. No fan-out
   needed.

4. **Reader needs to verify a row exists with no path in hand**: this
   never happens in current code, but if it ever does, fall back to
   N parallel `get_cf` calls and take the first hit.

5. **`--stream-parquet` + `--writer-shards > 1`**: not supported in
   v1. The streaming Parquet writer is single-threaded and would
   become the new bottleneck. Validate at config parse time and
   refuse with a clear error: "streaming Parquet is not yet
   compatible with multi-shard writers; either drop --stream-parquet
   or set --writer-shards 1, then run export-parquet --parallelism
   after the scan." Sharded streaming Parquet is a follow-up.

6. **Batch full but channel full**: today's `Sender::send` blocks.
   With N writers each draining one channel of capacity 1024,
   blocking is per-shard — a slow shard does not block fast shards
   (because each worker holds N independent batches and routes per
   entry). A single hot shard (skewed input) will still cap at one
   writer's throughput; see risk #1 below.

7. **Worker shutdown / channel drop**: each worker drops its N
   senders at end-of-walk; each shard's writer sees `Err(Disconnected)`
   on `recv()` and exits. Coordinator (or scan finalization) joins
   all N writer handles before declaring "done".

8. **Big-dir-hunt mode**: writes only to `CF_BIG_DIRS`, which is not
   sharded. No change to that path. The big-dir worker still runs as
   today.

9. **Shutdown mid-scan**: same as today — `shutdown` flag flips,
   workers exit their loops, drop senders, writers see Disconnected,
   write their pending batches, drop CF handles. RocksDB's `Drop` for
   the shared `Arc<RocksHandle>` runs once when the last writer drops.

10. **Writer panic**: if one writer thread panics, the others keep
    running but the database becomes inconsistent (one shard missing
    its tail). Wrap each writer in a `catch_unwind`; on panic, set
    the global `shutdown` flag and surface an error. The walk is
    not partially-recoverable — the DB has to be discarded and
    re-walked. Same as today's single-writer behavior.

## 7. Configuration

Add to `src/config.rs`:

```rust
/// Number of writer-side shards for the entries_by_path CF.
/// 1 = legacy single-writer path. >1 splits the path CF into N
/// independent CFs each owned by its own writer thread, plus a
/// k-way merge for ordered reads. On a many-core box pointing at a
/// large NFS export, set to 8 for a strong baseline; 16 if profiling
/// shows lingering writer-thread saturation.
#[arg(long, default_value = "1", value_name = "N")]
pub writer_shards: usize,
```

Validation in `WalkConfig::from_args`: `1 ≤ N ≤ 32`. Above 32 is
wasteful (compaction thread pool starts to thrash). Reject combinations
that the v1 implementation can't handle:

```rust
if args.writer_shards > 1 && args.stream_parquet {
    return Err(ConfigError::Incompatible(
        "--writer-shards > 1 is not yet compatible with --stream-parquet".into()
    ));
}
```

`open_rocks_db` gets a new `shards: usize` parameter. The
`SimpleWalker::run_rocksdb` path passes `self.config.writer_shards`;
the convert / export-parquet / stats paths read it from metadata.

## 8. Testing

### Unit tests (`src/rocksdb/schema.rs` and `src/rocksdb/writer.rs`)

1. `sharded_cfs_created_at_open` — open a fresh DB with `shards=4`,
   verify all 4 path-CF names exist + metadata record is correct.
2. `legacy_db_opens_with_one_shard` — create a DB with the old code
   path (one CF named `entries_by_path`, no `META_PATH_SHARDS`
   metadata key), reopen with the new code, verify shards=1 and the
   unsuffixed CF is used.
3. `path_routes_to_correct_shard` — sample 10 000 random paths,
   verify each lands in `gxhash(path) % shards` consistently.
4. `kmerge_iterator_yields_ordered` — write 10 000 entries with
   shards=8, iterate the merged result, assert each emitted key is ≥
   the previous.
5. `point_lookup_routes_to_one_shard` — write a row, look it up by
   path, assert it was a single-CF `get_cf` call (mock or
   instrument).
6. `parallel_writers_isolated` — N writers each writing to its own
   shard; assert no contention via clean output (no error,
   no missing rows).

### Integration test (`tests/sharded_writer_test.rs`, new)

Walk a synthetic local tree (or a fixture rocksdb) with `writer_shards
= 1` and `writer_shards = 8`, then assert:

- Same row count.
- Same set of paths (k-merge sort vs single-CF iter).
- Same per-row attributes for a sampled set of paths.
- `writer_shards = 8` finishes in ≤ 0.4× the time of `writer_shards = 1`
  on a sufficiently large synthetic tree (loose threshold).

### Benchmark (must run before merge)

Add `benches/sharded_writer.rs`. Workloads:

1. **Wide shallow**: 100 K dirs × 50 files, depth 2. Targets
   `writer_shards=8` ≥ 4× `writer_shards=1` files/sec.
2. **Narrow deep**: 100 dirs deep × 10 children. Targets ≥ 1× (no
   regression — sharding doesn't help when there's no fan-out, but
   shouldn't hurt).
3. **Real production shape**: re-run the 4 B-entry NFS export walk
   with `writer_shards=8`, target wall-clock under 2 hours. Compare
   per-thread CPU breakdown to confirm 8 rocks-writer threads are
   each in the 70–95 % range and walker threads are above 30 % CPU
   (i.e. the channel is no longer the bottleneck).

If any target misses, do not merge — the design is wrong somewhere
and needs profiling.

## 9. Rollout

Phase 1 (this work): land behind `--writer-shards N` defaulting to 1.
Document in README.md under "Performance tuning". Reject
`--writer-shards > 1 + --stream-parquet` combinations explicitly.

Phase 2 (separate change, after a real production run validates the
gain): switch default to `--writer-shards 8` and lift the
streaming-Parquet incompatibility (sharded streaming Parquet writers
or coordinator that fans in to one rolling Parquet output).

Phase 3 (only if needed): `--writer-shards auto` that picks N based on
detected CPU count and prior scan size.

## 10. Files to touch

- `src/rocksdb/schema.rs`
  - Add `CF_ENTRIES_BY_PATH_PREFIX`, `META_PATH_SHARDS`,
    `META_PATH_SHARDS_HASH_VERSION` constants.
  - Add `shards: usize` field to `RocksHandle` and helpers
    `cf_entries_by_path_shard(idx)`, `cf_entries_by_path_all_shards()`.
  - Add `open_rocks_db_with_shards(path, shards)`. Both
    `open_rocks_db` and `open_rocks_db_readonly` now read the shard
    count from metadata when reopening.
  - Extend `existing_known_cfs` to recognize sharded path CFs.
  - Add `path_to_shard(path: &str, shards: usize) -> usize` (gxhash-
    based, fixed seed).

- `src/rocksdb/writer.rs`
  - `RocksWriter::write_batch` switches to writing the path entry
    via `cf_entries_by_path_shard(path_to_shard(...))`.
  - `RocksWriterConfig` gains a `shards` field.

- `src/walker/simple.rs`
  - `rocksdb_writer_loop` becomes `rocksdb_writer_loop_shard` with
    `shard_idx` parameter; `run_rocksdb` spawns N of them.
  - Workers gain a `ShardedSender` instead of plain `Sender`. Both
    `worker_loop` (legacy) and `worker_loop_pipelined` need the
    refactor.
  - `run_workers` builds N channels and routes per worker.
  - End-of-walk drain joins all N writer handles before returning.
  - Streaming-parquet path is gated to `shards == 1`.

- `src/parquet/convert.rs` and `src/parquet/parallel_convert.rs`
  - `iter_by_path()` becomes a k-way merge over shards. Both
    converters consume the merged iterator without other changes.
  - `parallel_convert.rs::compute_shard_ranges` (different concept —
    Parquet output shards) is unaffected.

- `src/rocksdb/reader.rs`
  - `iter_paths()` becomes the k-way merge.
  - `get_by_path()` routes to one shard.
  - `iter_paths_with_prefix()` runs N prefix iterators and k-way
    merges.

- `src/rocksdb/stats.rs`
  - All places that iterate `entries_by_path` use the merged
    iterator. Most stats already work off the summary CF when
    available; the fallback iteration path is the only one that
    needs the merge.

- `src/config.rs`
  - Add `writer_shards: usize` to `Args` and `WalkConfig`. Validation
    plus the `--stream-parquet` incompatibility check.

- `src/main.rs`
  - Pass `config.writer_shards` into the walker constructor; export-
    parquet / convert / stats discover the shard count from the DB.

- `tests/sharded_writer_test.rs` (new)
- `benches/sharded_writer.rs` (new)
- `README.md`: one paragraph under "Performance".

## 11. Out of scope

- Sharding the `entries_by_inode` CF. Profile after this lands.
- Sharded streaming Parquet writer. Single-writer streaming
  Parquet has its own bottleneck shape — separate design.
- Cross-shard transactional writes (e.g. atomically updating the
  same path in two shards). Doesn't apply: each path lives in
  exactly one shard.
- Re-sharding an existing database from N to M shards. If a future
  user wants to migrate, the easiest path is to dump → re-import.
- Adaptive shard count based on observed key distribution.
  `gxhash` is sufficiently uniform on real NFS path inputs that a
  fixed shard count gives near-equal-sized shards.

## 12. Risks

1. **Hot-shard skew**. `gxhash` is uniform on average, but a single
   pathological subtree (e.g. 80 % of files under one parent) can
   route most of the work to one shard. Empirically NFS exports have
   reasonable cardinality at the path prefix level, but check on the
   production target: distribution of `gxhash(path) % 8` across the
   entire walk should be within ±10 % of 1/8 per shard. If it isn't,
   bump shard count or switch to a hash that mixes the *full* path
   bytes more aggressively (the leading `/figure/checkpoints/...`
   prefix is identical across millions of files, so a hash that only
   considers the first ~32 bytes will skew). gxhash is not in that
   class — it processes the full input — but worth verifying.

2. **Compaction contention across shards**. With 32 background-job
   slots and 8 shards each compacting independently, RocksDB will
   keep ~24 cores busy on background work alone. On a smaller box
   (16 cores, 8 shards) compaction will queue. The CLI flag's
   `min(num_cpus / 2, 16)` default plus the existing
   `set_max_background_jobs((num_cpus/2).clamp(4, 32))` cap
   together keep this in line.

3. **Memtable memory pressure**. Each path-shard CF has its own
   memtable. With default ~64 MB write-buffer per CF × 8 shards =
   512 MB of memtable for path data alone. That's fine on production
   targets, but on a 16 GiB dev box with shards=16 it adds up. The
   CFs we don't shard (inode, summary, etc.) keep their existing
   write-buffer config. Document in the doc comment.

4. **Reader k-way merge correctness**. Subtle: each shard iterator
   must emit keys in lexicographic order, which RocksDB guarantees
   per CF. The merge picks the smallest pending head. Bug class to
   watch for: emitting a `(key, value)` where the value is from the
   wrong shard's iterator (i.e. heap state desync). Cover with
   property-based tests on shuffled inputs.

5. **Hash version drift**. If we ever change the hash function or
   the seed, an existing DB created under the old hash becomes
   un-readable (point lookups go to the wrong shard). The
   `META_PATH_SHARDS_HASH_VERSION` metadata field guards against
   this; on mismatch, refuse to open with a clear error.

6. **Inode CF write contention**. With N=8 writers all calling
   `put_cf` against `entries_by_inode`, even with
   `allow_concurrent_memtable_write` on, there is some lock-free
   contention overhead. Profile after v1 lands; if the inode CF
   becomes the new bottleneck, shard it the same way. The
   architecture above leaves room for this without major refactor.

## 13. Estimated size

- `schema.rs`: ~120 lines added (CF discovery, shard helpers,
  `path_to_shard`, metadata reads).
- `writer.rs`: ~30 lines (route writes per shard).
- `simple.rs`: ~250 lines (per-shard writer loop, `ShardedSender`,
  fan-out wiring in `run_workers`).
- `reader.rs` + `stats.rs` + `convert.rs`: ~150 lines (k-way merge
  + per-call routing).
- `config.rs` + `main.rs`: ~30 lines.
- Tests: ~300 lines.
- Total: roughly **900 lines of new code, no deletions** beyond the
  one-line refactor at the writer-loop entry point.

Implementation should fit in **one focused session** for a developer
already familiar with the code (the reviewer of this design should
budget two — one for the schema + writer pieces, one for the reader
merge and tests). Bench iteration may take a second session to chase
unexpected slowdowns in workload 2 (narrow deep) or in the inode-CF
contention path.

## 14. Expected throughput

Modeling against the post-tweaks baseline (315 K entries/sec sustained
on the 4 B-entry production walk, single writer at 99.9 % CPU,
walkers at 0.1 % CPU):

- **Linear scaling assumption (best case)**: 8 writers × 315 K =
  2.5 M entries/sec. Walker side becomes the cap; with current
  pipelining, 1024 workers × ~1 K entries/sec ≈ 1 M/sec is more
  realistic.
- **Realistic target**: **1.0–1.5 M entries/sec**. 4 B entries finishes
  in **45–70 minutes**.
- **Compaction-bound case**: if compaction can't keep up with 8
  writers' flush rate, throughput plateaus around 800 K/sec and
  we land at ~95 minutes.

Either way, **comfortably under the 2-hour target**.

## 15. Relationship to the streaming Parquet writer

`--stream-parquet` (`src/parquet/streaming.rs`) is single-threaded by
design — one rolling Parquet writer fed by a single channel from the
RocksDB writer. With sharded writers, each shard would need its own
Parquet stream, OR all shards funnel into one writer (defeating the
parallelism we just built).

The clean v1 answer: **disable `--stream-parquet` when
`--writer-shards > 1`**, validated at config parse time. Users who
need Parquet output run `nfs-walker export-parquet --parallelism N`
after the scan completes — that path is already parallel and is the
recommended workflow for analytics.

A v2 design for sharded streaming Parquet is sketched in §11 of
`docs/PIPELINED_READDIRPLUS_DESIGN.md`-style follow-ups; out of
scope here.
