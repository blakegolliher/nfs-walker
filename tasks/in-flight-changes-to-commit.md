# In-Flight Changes — Triage Before Starting Plan Work

This document captures the uncommitted state of the working tree at the
end of a debugging/feature session against a multi-billion-file scan.
**Read this first** before starting on `parquet-and-summaries-plan.md` so
you know what to commit, what to leave alone, and what to merge in
which order.

The working tree has THREE layers of uncommitted changes, mixed across
the same files:

1. **Pre-existing WIP** — was already dirty in the working tree before
   this session started. Not authored in this session. Touch only to
   review/commit; do not assume context.
2. **Session work — Group A: stats `--live` (RocksDB secondary mode)** —
   self-contained feature, ready to commit as one PR.
3. **Session work — Group B: FD limit + worker cap** — small operational
   fixes, separate PR.

The plan file (`parquet-and-summaries-plan.md`) builds on top of all
three layers. It does NOT depend on any specific commit ordering, but
it's much easier to review and roll back if these are committed first.

---

## Pre-existing WIP (not from this session)

These changes were already in the working tree when the session began.
Treat them as someone else's in-progress work — review, possibly commit
as their own PRs, but **do not bundle them with the session's work**.

| File | Pre-existing change |
|---|---|
| `Cargo.toml` | `default = ["rocksdb", "csv-export"]` (added csv-export to defaults) |
| `Makefile` | Help text updates with new example invocations |
| `src/nfs/connection.rs` | New `nfs3_status_to_string()` helper (~80 lines) translating libnfs status codes to readable strings |
| `src/walker/simple.rs` | Worker loops now log `Permission denied` errors at debug level (was already only suppressing `not found`) |
| `src/config.rs` | CLI description/help-text rewrite (the `about=`, `long_about=`, `after_help=` blocks) |

Use `git diff <file>` to inspect each one. Likely all safe to commit
on their own, but verify with the original author or by reading.

---

## Session work Group A — `stats --live` for RocksDB secondary mode

**Why:** users were getting `IO error: While open a file for random
read: /path/NNNNNN.sst: No such file or directory` when running `stats`
against an actively-being-written RocksDB. Read-only mode breaks under
concurrent compaction. This adds RocksDB secondary mode with a `--live`
flag that tolerates concurrent writers.

**Also bundled in this group:** a perf optimization that switches most
stats functions to iterate the `entries_by_inode` CF instead of
`entries_by_path` (smaller keys → denser blocks → ~30-50% less disk
I/O on full-scan queries). This came along because both changes touch
the same code path in `src/rocksdb/stats.rs`.

**Files touched (session work only — be careful, they also have
pre-existing WIP):**

- `src/rocksdb/schema.rs` — added: `OpenMode` enum,
  `open_rocks_db_secondary()`, `default_secondary_path()`,
  `get_query_options()` (sets `max_open_files=-1` to prevent LRU
  eviction during long queries), `RocksHandle::open_secondary()`,
  `RocksHandle::try_catch_up_with_primary()`, `RocksHandle::iter_by_inode()`.
- `src/rocksdb/stats.rs` — added `OpenMode` parameter to all 11 stats
  functions; new `open_query_handle()` helper that handles secondary-
  mode dir creation + catch-up; switched 9 of 11 functions from
  `iter_by_path()` to `iter_by_inode()` (the two exceptions —
  `largest_directories` and `find_hardlink_groups` — need path-per-name
  semantics).
- `src/rocksdb/mod.rs` — added `OpenMode` to public re-exports.
- `src/config.rs` — added `--live` flag on the `Stats` subcommand
  (the MAX_WORKERS bump is from Group B, see below).
- `src/main.rs` — threaded `OpenMode` through `run_stats` (the FD-limit
  function `raise_fd_limit()` is from Group B).
- `README.md` — added a one-line callout pointing at `--live` and
  the docs section.
- `docs/QUERY_ROCKSDB.md` — added "Live Querying During an Active Scan
  (`--live`)" section explaining the SST-deleted-by-compaction error,
  how secondary mode fixes it, and caveats (snapshot-at-open, single
  secondary per state dir, slightly slower open).

**Behavioral change worth flagging in the commit message:** the 9
stats functions now switched to inode-CF iteration count entries
*per unique inode* rather than *per path*. For non-hardlinked
filesystems the result is identical. For hardlink-heavy filesystems
(rsnapshot backups, time-machine, package mirrors), file counts and
total-bytes will be lower (more accurate disk usage; less accurate
for "how many names exist"). Documented in the `iter_by_inode` doc
comment in schema.rs.

**Suggested commit message:**

```
Add --live flag for stats during active scans + inode-CF perf win

stats commands previously crashed with "No such file or directory:
.../NNNNNN.sst" when run against a RocksDB while a scan was still
writing — read-only mode snapshots the SST list at open time and
breaks under concurrent compaction.

The --live flag opens the database in RocksDB secondary mode against
a separate state dir (auto-derived under TMPDIR), which tolerates
concurrent writers. Secondary handles are opened with
max_open_files=-1 so the LRU does not evict SSTs mid-iteration (the
root cause of a related bug where secondary mode itself errored on
long queries against actively-compacting primaries).

Also switches 9 of 11 stats functions to iterate the entries_by_inode
CF instead of entries_by_path. Smaller keys (8B vs ~80-150B) means
denser SST blocks and ~30-50% less disk I/O on full-scan queries.
Behavior change: counts are now per-unique-inode rather than per-path,
which differs from previous behavior on hardlink-heavy filesystems.
Two functions (largest_directories, find_hardlink_groups) keep using
the path CF because they specifically need name-per-inode semantics.
```

---

## Session work Group B — FD limit + worker cap

**Why:** the user's first PB-scale scan crashed at ~2.6B entries with
"Too many open files" because Ubuntu's default `ulimit -n` is 1024.
Manual `ulimit` workarounds are fragile; better to handle it in the
binary. Also, the hardcoded 512-worker cap was too tight for a 1.4 TiB
host scanning a server with a 65k-RPC limit.

**Files touched (session work only):**

- `src/main.rs` — added `raise_fd_limit()` function (Unix-gated) called
  early in `run()`. Reads current `RLIMIT_NOFILE`, raises the soft
  limit to 1M (or to the hard limit, whichever is lower), logs the
  change at info level, warns clearly with actionable advice if it
  can't get enough.
- `src/config.rs` — bumped `MAX_WORKERS` from 512 to 4096 (with
  comment explaining work-stealing diminishing returns past ~1000).

**Suggested commit message:**

```
Auto-raise RLIMIT_NOFILE and lift worker cap to 4096

Large RocksDB scans need many FDs (one per NFS worker socket + every
open SST file handle). Default Ubuntu soft limit of 1024 caused a
crash at ~2.6B entries with "Too many open files" mid-scan.

Auto-raise the soft limit to 1M (capped at the hard limit) at startup.
Logs the new value, warns with actionable instructions if it cannot
raise enough.

Also bump MAX_WORKERS from 512 to 4096. The previous cap was too
tight for high-RAM hosts scanning servers with high RPC capacity.
4096 catches fat-finger typos but allows realistic exploration —
work-stealing returns diminish sharply past ~1000-2000 anyway.
```

---

## Untracked: `tasks/`

The `tasks/` directory is new (not in the previous commit). Contains:

- `parquet-and-summaries-plan.md` — the implementation plan you are
  about to execute on.
- `in-flight-changes-to-commit.md` — this file.

Either commit `tasks/` as part of "kicking off the parquet work" or
add it to `.gitignore` if planning docs aren't supposed to live in
the repo. Check repo conventions (CLAUDE.md mentions `tasks/todo.md`
as an expected location, so it seems intended to be tracked).

---

## Recommended order of operations

1. **Review pre-existing WIP** (each file in the table above) — commit
   each separately, with appropriate authorship attribution. If the
   author isn't around, either leave as-is and `git stash` them, or
   commit with `WIP:` prefix and note source unknown.
2. **Commit Group A** (`--live` + inode-CF perf) — one commit, message
   suggested above.
3. **Commit Group B** (FD limit + worker cap) — one commit, message
   suggested above.
4. **Commit `tasks/`** — single commit adding the plan + this triage doc.
5. **Start work on PR #1 (Tier 1A)** from `parquet-and-summaries-plan.md`.
6. **Start work on PR #2 (Tier 2C)** after PR #1 lands.

Optional but recommended: tag the post-step-4 state as
`pre-summary-cf-work` (a lightweight git tag) so the new work has a
clear baseline to diff against.

---

## How to use `git add -p` to separate the layers

The cleanest way to split mixed changes in a single file:

```bash
git add -p src/config.rs
# Step through each hunk; press 'y' to stage, 'n' to skip,
# 's' to split into smaller hunks. Stage only the hunks belonging
# to one logical change, commit, then repeat for the next group.
```

For files where session work and pre-existing WIP are intermixed
(`src/config.rs`, `src/main.rs`), use `git add -p` to stage only the
session hunks for the Group A or Group B commit. The pre-existing
hunks will remain unstaged and can be committed separately afterward.

---

## Things NOT to do

- **Do not `git add -A`** — that bundles pre-existing WIP into the
  session's commits and muddies authorship/intent.
- **Do not amend or rebase** to combine these into one big commit.
  The cohesion of separate PRs is the point.
- **Do not start the parquet-and-summaries plan on top of an
  uncommitted working tree.** Get to a clean state first so the new
  work has a clear diff and is reviewable in isolation.
