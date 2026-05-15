#!/bin/bash
#
# Side-by-side benchmark: --output-format rocksdb vs --output-format parquet.
#
# Drives the same NFS URL through both writer backends back-to-back with
# cache-drop between, captures wall-clock + throughput + per-shard scanlog,
# verifies parquet row count via duckdb, and prints a summary so the win
# (or non-win) is obvious at a glance.
#
# Usage:
#   ./bench-parquet-vs-rocks.sh [nfs://url] [output-base-dir]
#
# Defaults:
#   nfs://main.selab-var202.selab.vastdata.com:/nfs-top-io
#   /tmp/bench-pvr-<timestamp>
#
# Environment overrides:
#   WALKER=/path/to/nfs-walker      # default: ./target/release/nfs-walker
#   WORKERS=1024                    # default: 1024 (matches production profile)
#   PIPELINE_DEPTH=8                # default: 8 (matches production profile)
#   ROCKS_SHARDS=8                  # default: 8 (Profile B baseline)
#   PARQUET_SHARDS=32               # default: 32 (customer-tuning analogue)
#   SKIP_ROCKS=1                    # skip the rocks baseline (parquet-only)
#   SKIP_PARQUET=1                  # skip the parquet experiment
#   NO_CACHE_DROP=1                 # skip cache drop (no sudo) — only meaningful
#                                   # if the NFS server is what's bottlenecking,
#                                   # not local page cache
#   DROP_LOCAL_FS_CACHE=1           # additionally try `echo 3` on the NFS mount
#                                   # cache (server-side); ignored if not root.

set -euo pipefail

NFS_URL="${1:-nfs://main.selab-var202.selab.vastdata.com:/nfs-top-io}"
BASE_DIR_DEFAULT="/tmp/bench-pvr-$(date +%Y%m%d-%H%M%S)"
BASE_DIR="${2:-$BASE_DIR_DEFAULT}"

WALKER="${WALKER:-./target/release/nfs-walker}"
WORKERS="${WORKERS:-1024}"
PIPELINE_DEPTH="${PIPELINE_DEPTH:-8}"
ROCKS_SHARDS="${ROCKS_SHARDS:-8}"
PARQUET_SHARDS="${PARQUET_SHARDS:-32}"
SKIP_ROCKS="${SKIP_ROCKS:-0}"
SKIP_PARQUET="${SKIP_PARQUET:-0}"
NO_CACHE_DROP="${NO_CACHE_DROP:-0}"

if [ ! -x "$WALKER" ]; then
    echo "error: walker binary not at $WALKER — build first (cargo build --release)" >&2
    exit 1
fi
if [ ! -d "$(dirname "$BASE_DIR")" ]; then
    echo "error: parent of $BASE_DIR doesn't exist" >&2
    exit 1
fi
mkdir -p "$BASE_DIR"

LOG="$BASE_DIR/bench.log"
exec > >(tee -a "$LOG") 2>&1

GREEN=$'\033[0;32m'
YELLOW=$'\033[1;33m'
RED=$'\033[0;31m'
NC=$'\033[0m'

header() {
    printf '\n%s=== %s ===%s\n' "$GREEN" "$1" "$NC"
}

note() {
    printf '%s%s%s\n' "$YELLOW" "$1" "$NC"
}

# duckdb is only used for the post-run parquet row-count sanity check.
# Without it, the row count silently reports "unavailable" (line 177
# below) — which previously left operators wondering whether duckdb
# was missing or the scan was empty. Warn loudly at start instead.
if ! command -v duckdb >/dev/null 2>&1; then
    note "warning: duckdb not on PATH — parquet row count will be 'unavailable'"
fi

drop_caches() {
    if [ "$NO_CACHE_DROP" = "1" ]; then
        note "cache drop: skipped (NO_CACHE_DROP=1)"
        return
    fi
    sync
    if echo 3 | sudo -n tee /proc/sys/vm/drop_caches > /dev/null 2>&1; then
        note "cache drop: local page cache flushed"
    else
        note "cache drop: sudo unavailable — page cache NOT dropped (numbers will favor the second run)"
    fi
    sleep 2
}

# Wall-clock + throughput extraction from the walker's stdout.
#
# nfs-walker prints a final summary block of the form:
#   Total dirs:  ...
#   Total files: ...
#   Total bytes: ...
#   Throughput:  XXXX files/sec
#
# This helper parses out the lines we care about and exposes them as
# shell variables for the summary.
parse_walker_output() {
    local out="$1"
    grep -E "Total (dirs|files|bytes|errors)|Throughput|Duration|completed|Direct-write Parquet scan complete|RocksDB" "$out" || true
}

run_rocks() {
    header "RocksDB baseline (shards=$ROCKS_SHARDS, pipeline=$PIPELINE_DEPTH, workers=$WORKERS)"
    local out_dir="$BASE_DIR/rocks.db"
    local stdout="$BASE_DIR/rocks.stdout"
    drop_caches
    local t0 t1
    t0=$(date +%s.%N)
    set +e
    "$WALKER" "$NFS_URL" \
        -o "$out_dir" \
        -w "$WORKERS" \
        --writer-shards "$ROCKS_SHARDS" \
        --pipeline-depth "$PIPELINE_DEPTH" \
        --output-format rocksdb \
        --log "$BASE_DIR/rocks.scanlog" \
        --log-fmt json \
        --log-interval-secs 5 \
        >"$stdout" 2>&1
    local rc=$?
    set -e
    t1=$(date +%s.%N)
    local wall
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.2f", b - a }')
    note "rocks wall-clock: ${wall}s (rc=$rc)"
    parse_walker_output "$stdout"
    echo "$wall" > "$BASE_DIR/rocks.wall"
    echo "$rc" > "$BASE_DIR/rocks.rc"
}

run_parquet() {
    header "Parquet-direct (shards=$PARQUET_SHARDS, pipeline=$PIPELINE_DEPTH, workers=$WORKERS)"
    local out_dir="$BASE_DIR/parquet"
    local stdout="$BASE_DIR/parquet.stdout"
    drop_caches
    local t0 t1
    t0=$(date +%s.%N)
    set +e
    "$WALKER" "$NFS_URL" \
        -o "$out_dir" \
        -w "$WORKERS" \
        --writer-shards "$PARQUET_SHARDS" \
        --pipeline-depth "$PIPELINE_DEPTH" \
        --output-format parquet \
        --log "$BASE_DIR/parquet.scanlog" \
        --log-fmt json \
        --log-interval-secs 5 \
        >"$stdout" 2>&1
    local rc=$?
    set -e
    t1=$(date +%s.%N)
    local wall
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.2f", b - a }')
    note "parquet wall-clock: ${wall}s (rc=$rc)"
    parse_walker_output "$stdout"
    echo "$wall" > "$BASE_DIR/parquet.wall"
    echo "$rc" > "$BASE_DIR/parquet.rc"
}

verify_parquet_row_count() {
    local parquet_dir="$BASE_DIR/parquet"
    if [ ! -d "$parquet_dir" ]; then
        return
    fi
    local scans_dir
    scans_dir=$(find "$parquet_dir/scans" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -n1)
    if [ -z "$scans_dir" ]; then
        note "verify: no parquet scan dir found"
        return
    fi
    note "duckdb row count over $scans_dir/part-*.parquet ..."
    local rows
    rows=$(duckdb -c "SELECT count(*) FROM '${scans_dir}/part-*.parquet'" 2>/dev/null | awk '/^[0-9]/ { print $1 }' | head -n1)
    if [ -n "$rows" ]; then
        echo "$rows" > "$BASE_DIR/parquet.rows"
        note "parquet row count: $rows"
    else
        note "duckdb row count: unavailable"
    fi
    # File count, sizes
    local file_count total_bytes
    file_count=$(find "$scans_dir" -name 'part-*.parquet' | wc -l)
    total_bytes=$(du -sb "$scans_dir" 2>/dev/null | awk '{ print $1 }')
    note "parquet files: $file_count, on-disk: $total_bytes bytes"
}

print_rocks_row_count() {
    local rocks_dir="$BASE_DIR/rocks.db"
    if [ ! -d "$rocks_dir" ]; then
        return
    fi
    if "$WALKER" stats "$rocks_dir" 2>/dev/null | grep -E "Total (files|dirs|entries)" > "$BASE_DIR/rocks.stats" 2>&1; then
        note "rocks stats:"
        cat "$BASE_DIR/rocks.stats"
    fi
}

summary() {
    header "Summary"
    local rocks_wall parquet_wall rocks_rc parquet_rc
    rocks_wall=$(cat "$BASE_DIR/rocks.wall" 2>/dev/null || echo "n/a")
    parquet_wall=$(cat "$BASE_DIR/parquet.wall" 2>/dev/null || echo "n/a")
    rocks_rc=$(cat "$BASE_DIR/rocks.rc" 2>/dev/null || echo "n/a")
    parquet_rc=$(cat "$BASE_DIR/parquet.rc" 2>/dev/null || echo "n/a")
    echo "target:        $NFS_URL"
    echo "rocks (rc=$rocks_rc):    ${rocks_wall}s"
    echo "parquet (rc=$parquet_rc):  ${parquet_wall}s"
    if [ "$rocks_wall" != "n/a" ] && [ "$parquet_wall" != "n/a" ] \
       && [ "$rocks_rc" = "0" ] && [ "$parquet_rc" = "0" ]; then
        local speedup
        speedup=$(awk -v r="$rocks_wall" -v p="$parquet_wall" \
            'BEGIN { if (p > 0) printf "%.2fx", r / p; else print "n/a" }')
        echo "speedup:       $speedup (rocks / parquet)"
    fi
    if [ -f "$BASE_DIR/parquet.rows" ]; then
        echo "parquet rows:  $(cat "$BASE_DIR/parquet.rows")"
    fi
    echo "log:           $LOG"
    echo "outputs:       $BASE_DIR/{rocks.db,parquet}"
}

note "bench dir: $BASE_DIR"
note "binary:    $WALKER"
note "target:    $NFS_URL"

if [ "$SKIP_ROCKS" != "1" ]; then
    run_rocks
    print_rocks_row_count
fi
if [ "$SKIP_PARQUET" != "1" ]; then
    run_parquet
    verify_parquet_row_count
fi
summary
