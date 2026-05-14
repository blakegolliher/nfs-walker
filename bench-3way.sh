#!/bin/bash
#
# 3-way A/B for the direct-write parquet experiment.
#
# Runs four scans back-to-back against the same NFS target, dropping
# page cache between each, so we can attribute the next wall-clock
# delta to one specific change:
#
#   1. rocks         — rocksdb baseline (current main equivalent)
#   2. parquet-base  — tail-flush fix only (256K row groups, ZSTD-3,
#                      pipeline-depth 8, big-dir-split-after 1M)
#   3. parquet-conc  — adds concurrency tuning
#                      (pipeline-depth 16, big-dir-split-after 2000)
#   4. parquet-snappy — adds snappy compression on top of #3
#
# Run #4 matches the current shipped defaults.
#
# Usage:
#   sudo -E ./bench-3way.sh [nfs://url] [base-dir]
#
# Defaults:
#   nfs://main.selab-var202.selab.vastdata.com:/nfs-top-io
#   /tmp/bench-3way-<timestamp>
#
# Environment overrides:
#   WALKER=/path/to/nfs-walker          # default: ./target/release/nfs-walker
#   WORKERS=1024
#   ROCKS_SHARDS=8
#   PARQUET_SHARDS=32
#   NO_CACHE_DROP=1                     # skip page-cache flush
#   SKIP_ROCKS=1                        # skip step 1 (use prior baseline)
#   ARCHIVE_DIR=/path                   # default: /mnt/vamoose-dest/nfs-walker-bench
#                                       # set to "" to disable archival entirely
#   KEEP_LOCAL=1                        # don't delete /tmp output after archival
#                                       # (useful when the NFS archive is slow
#                                       # or you want to inspect locally first)

set -euo pipefail

NFS_URL="${1:-nfs://main.selab-var202.selab.vastdata.com:/nfs-top-io}"
BASE_DIR_DEFAULT="/tmp/bench-3way-$(date +%Y%m%d-%H%M%S)"
BASE_DIR="${2:-$BASE_DIR_DEFAULT}"

WALKER="${WALKER:-./target/release/nfs-walker}"
WORKERS="${WORKERS:-1024}"
ROCKS_SHARDS="${ROCKS_SHARDS:-8}"
PARQUET_SHARDS="${PARQUET_SHARDS:-32}"
NO_CACHE_DROP="${NO_CACHE_DROP:-0}"
SKIP_ROCKS="${SKIP_ROCKS:-0}"
ARCHIVE_DIR="${ARCHIVE_DIR-/mnt/vamoose-dest/nfs-walker-bench}"
KEEP_LOCAL="${KEEP_LOCAL:-0}"

if [ ! -x "$WALKER" ]; then
    echo "error: walker not at $WALKER — build first (cargo build --release)" >&2
    exit 1
fi
mkdir -p "$BASE_DIR"
LOG="$BASE_DIR/bench.log"
exec > >(tee -a "$LOG") 2>&1

G=$'\033[0;32m'; Y=$'\033[1;33m'; N=$'\033[0m'
hdr() { printf '\n%s=== %s ===%s\n' "$G" "$1" "$N"; }
note() { printf '%s%s%s\n' "$Y" "$1" "$N"; }

drop_caches() {
    if [ "$NO_CACHE_DROP" = "1" ]; then
        note "cache drop: skipped"
        return
    fi
    sync
    if echo 3 | tee /proc/sys/vm/drop_caches > /dev/null 2>&1; then
        note "cache drop: ok"
    elif echo 3 | sudo -n tee /proc/sys/vm/drop_caches > /dev/null 2>&1; then
        note "cache drop: ok (via sudo)"
    else
        note "cache drop: FAILED — second/third runs will favor warmed cache"
    fi
    sleep 2
}

# args: <run-name> <flag-list>
run_walker() {
    local name="$1"; shift
    local out_dir="$BASE_DIR/$name"
    local stdout="$BASE_DIR/$name.stdout"
    hdr "$name"
    drop_caches
    local t0 t1
    t0=$(date +%s.%N)
    set +e
    "$WALKER" "$NFS_URL" \
        -o "$out_dir" \
        -w "$WORKERS" \
        --log "$BASE_DIR/$name.scanlog" --log-fmt json --log-interval-secs 5 \
        "$@" \
        >"$stdout" 2>&1
    local rc=$?
    set -e
    t1=$(date +%s.%N)
    local wall
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.2f", b - a }')
    note "$name wall-clock: ${wall}s (rc=$rc)"
    grep -E "Duration|Throughput|files/sec|Total entries" "$stdout" 2>/dev/null | head -5 || true
    echo "$wall" > "$BASE_DIR/$name.wall"
    echo "$rc" > "$BASE_DIR/$name.rc"
}

count_parquet_rows() {
    # Run unguarded — partial scan dirs (failed walker) can return
    # non-zero from find/duckdb and `set -e + pipefail` would otherwise
    # propagate the failure all the way up and abort the bench.
    set +e
    local out_dir="$1"
    local scan_dir
    scan_dir=$(find "$out_dir/scans" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -n1)
    if [ -n "$scan_dir" ]; then
        local rows
        rows=$(duckdb -c "SELECT count(*) FROM '${scan_dir}/part-*.parquet'" 2>/dev/null \
            | awk '/^[0-9]/ { print $1 }' | head -n1)
        echo "$rows"
    fi
    set -e
}

# Capture per-run statistics from the output dir before we move/delete
# it, so the final summary doesn't depend on the data still being local.
# Best-effort: a partial output (failed walker) shouldn't abort the
# bench, so this runs with set +e.
record_stats() {
    set +e
    local name="$1"
    local out_dir="$BASE_DIR/$name"
    case "$name" in
        rocks)
            "$WALKER" stats "$out_dir" 2>/dev/null \
                | awk -F': +' '/Total entries/ { gsub(",","",$2); print $2 }' \
                | head -1 > "$BASE_DIR/$name.entries"
            ;;
        *)
            count_parquet_rows "$out_dir" > "$BASE_DIR/$name.entries"
            ;;
    esac
    du -sb "$out_dir" 2>/dev/null | awk '{print $1}' > "$BASE_DIR/$name.bytes"
    set -e
    return 0
}

# Move the run's bulky output directory off /tmp to the NFS archive so
# the next run starts with a clean local fs. Sidecar files (scanlog,
# stdout, wall, rc, entries, bytes) stay local for the summary; they're
# tiny.
#
# Falls back to leaving the data in place when ARCHIVE_DIR is empty or
# unreachable — the bench still completes, we just don't free disk.
archive_output_dir() {
    local name="$1"
    local out_dir="$BASE_DIR/$name"
    if [ ! -d "$out_dir" ]; then
        return
    fi
    if [ -z "$ARCHIVE_DIR" ]; then
        note "archive: ARCHIVE_DIR unset — $name stays at $out_dir"
        return
    fi
    local dest_root="$ARCHIVE_DIR/$(basename "$BASE_DIR")"
    if ! mkdir -p "$dest_root" 2>/dev/null; then
        note "archive: cannot create $dest_root — $name stays at $out_dir"
        return
    fi
    local t0 t1
    t0=$(date +%s)
    if [ "$KEEP_LOCAL" = "1" ]; then
        note "archive: copying $name -> $dest_root/ (KEEP_LOCAL=1)"
        cp -r "$out_dir" "$dest_root/" || {
            note "archive: copy failed; leaving $name local"
            return
        }
    else
        note "archive: moving $name -> $dest_root/ (frees local disk)"
        # mv across filesystems = cp+unlink; tolerate failure and leave
        # data in place rather than blow up the bench.
        if mv "$out_dir" "$dest_root/" 2>/dev/null; then
            :
        else
            cp -r "$out_dir" "$dest_root/" || {
                note "archive: copy fallback failed; leaving $name local"
                return
            }
            rm -rf "$out_dir"
        fi
    fi
    t1=$(date +%s)
    note "archive: $name done in $((t1 - t0))s"
}

# Each run's stats/archive helpers are best-effort: an individual run
# failing (or partially scanning) shouldn't abort the whole bench. We
# want all four numbers in the final summary, even if some are partial.
run_step() {
    local name="$1"; shift
    run_walker "$name" "$@" || true
    record_stats "$name" || true
    archive_output_dir "$name" || true
}

# Step 1: rocksdb baseline
if [ "$SKIP_ROCKS" != "1" ]; then
    run_step rocks \
        --output-format rocksdb \
        --writer-shards "$ROCKS_SHARDS" \
        --pipeline-depth 8
fi

# Channel buffer per shard. Default 1024 batches matches yesterday's
# 580 K/s validated config; total worst-case buffer ~ 32 GiB on the
# default 32-shard config which is fine on production transfer hosts
# (1.4 TiB+) and on this dev host once it's bumped to 256 GiB. On a
# tight-memory host (<=32 GiB total) export
# PARQUET_CHANNEL_DEPTH=256 to cap at ~8 GiB.
PARQUET_CHANNEL_DEPTH="${PARQUET_CHANNEL_DEPTH:-1024}"

# Step 2: parquet-base — direct-write Parquet, tail-flush fix only.
# Pipeline-depth 8 matches the rocks baseline; no big-dir-split.
run_step parquet-base \
    --output-format parquet \
    --writer-shards "$PARQUET_SHARDS" \
    --pipeline-depth 8 \
    --big-dir-split-after 1000000 \
    --parquet-row-group-size 256000 \
    --parquet-compression zstd3 \
    --parquet-channel-depth "$PARQUET_CHANNEL_DEPTH"

# Step 3: parquet-conc — adds the two concurrency levers we want to
# A/B against parquet-base:
#   - pipeline-depth 16: more in-flight RPCs per worker
#   - big-dir-split-after 2000: split giant flat dirs across workers
# These cost ~15 GiB libnfs response buffer + DirWork queue growth, so
# this step needs a roomy host (~64 GiB+).
run_step parquet-conc \
    --output-format parquet \
    --writer-shards "$PARQUET_SHARDS" \
    --pipeline-depth 16 \
    --big-dir-split-after 2000 \
    --parquet-row-group-size 256000 \
    --parquet-compression zstd3 \
    --parquet-channel-depth "$PARQUET_CHANNEL_DEPTH"

# Step 4: parquet-snappy — same as conc but with Snappy compression
# (= the shipped default). On the dev-host bench Snappy completed
# without OOM where ZSTD-3 (parquet-conc) OOM'd, so this is also a
# memory-pressure escape hatch.
run_step parquet-snappy \
    --output-format parquet \
    --writer-shards "$PARQUET_SHARDS" \
    --pipeline-depth 16 \
    --big-dir-split-after 2000 \
    --parquet-row-group-size 256000 \
    --parquet-compression snappy \
    --parquet-channel-depth "$PARQUET_CHANNEL_DEPTH"

# Summary — reads pre-archive sidecar files, so it works whether the
# data is still local or already pushed to $ARCHIVE_DIR.
hdr "Summary"
printf '%-18s %10s %5s %14s %14s\n' "run" "wall(s)" "rc" "entries" "bytes"
for r in rocks parquet-base parquet-conc parquet-snappy; do
    [ -f "$BASE_DIR/$r.wall" ] || continue
    wall=$(cat "$BASE_DIR/$r.wall")
    rc=$(cat "$BASE_DIR/$r.rc")
    entries=$(cat "$BASE_DIR/$r.entries" 2>/dev/null)
    bytes=$(cat "$BASE_DIR/$r.bytes" 2>/dev/null)
    printf '%-18s %10s %5s %14s %14s\n' "$r" "$wall" "$rc" "${entries:-?}" "${bytes:-?}"
done

echo
if [ -n "$ARCHIVE_DIR" ]; then
    note "archive:   $ARCHIVE_DIR/$(basename "$BASE_DIR")/"
fi
note "sidecars:  $BASE_DIR/  (scanlogs, stdouts, .wall/.rc/.entries/.bytes)"
note "log:       $LOG"

# Move the sidecar dir to the archive too so everything from this bench
# lives in one place. Done LAST so the bench.log captures the summary
# before being moved.
if [ -n "$ARCHIVE_DIR" ] && [ "$KEEP_LOCAL" != "1" ]; then
    dest_root="$ARCHIVE_DIR/$(basename "$BASE_DIR")"
    if mkdir -p "$dest_root" 2>/dev/null; then
        note "archive: moving sidecars + bench.log -> $dest_root/"
        find "$BASE_DIR" -maxdepth 1 -type f -exec mv {} "$dest_root/" \; 2>/dev/null || true
        rmdir "$BASE_DIR" 2>/dev/null || true
    fi
fi
