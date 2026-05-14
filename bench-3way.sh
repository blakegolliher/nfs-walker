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
    local out_dir="$1"
    local scan_dir
    scan_dir=$(find "$out_dir/scans" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -n1)
    [ -z "$scan_dir" ] && return
    local rows
    rows=$(duckdb -c "SELECT count(*) FROM '${scan_dir}/part-*.parquet'" 2>/dev/null \
        | awk '/^[0-9]/ { print $1 }' | head -n1)
    echo "$rows"
}

# Step 1: rocksdb baseline
if [ "$SKIP_ROCKS" != "1" ]; then
    run_walker rocks \
        --output-format rocksdb \
        --writer-shards "$ROCKS_SHARDS" \
        --pipeline-depth 8
fi

# Step 2: parquet-base (tail-flush fix only)
run_walker parquet-base \
    --output-format parquet \
    --writer-shards "$PARQUET_SHARDS" \
    --pipeline-depth 8 \
    --big-dir-split-after 1000000 \
    --parquet-row-group-size 256000 \
    --parquet-compression zstd3

# Step 3: parquet-conc (+ concurrency tuning)
run_walker parquet-conc \
    --output-format parquet \
    --writer-shards "$PARQUET_SHARDS" \
    --pipeline-depth 16 \
    --big-dir-split-after 2000 \
    --parquet-row-group-size 256000 \
    --parquet-compression zstd3

# Step 4: parquet-snappy (+ snappy compression — current defaults)
run_walker parquet-snappy \
    --output-format parquet \
    --writer-shards "$PARQUET_SHARDS" \
    --pipeline-depth 16 \
    --big-dir-split-after 2000 \
    --parquet-row-group-size 256000 \
    --parquet-compression snappy

# Summary
hdr "Summary"
printf '%-18s %10s %10s %12s\n' "run" "wall(s)" "rc" "entries"
for r in rocks parquet-base parquet-conc parquet-snappy; do
    [ -f "$BASE_DIR/$r.wall" ] || continue
    wall=$(cat "$BASE_DIR/$r.wall")
    rc=$(cat "$BASE_DIR/$r.rc")
    case "$r" in
        rocks)
            entries=$("$WALKER" stats "$BASE_DIR/rocks" 2>/dev/null \
                | awk -F': +' '/Total entries/ { gsub(",","",$2); print $2 }' \
                | head -1)
            ;;
        *) entries=$(count_parquet_rows "$BASE_DIR/$r") ;;
    esac
    printf '%-18s %10s %10s %12s\n' "$r" "$wall" "$rc" "${entries:-?}"
done

echo
note "outputs:   $BASE_DIR/{rocks,parquet-base,parquet-conc,parquet-snappy}"
note "log:       $LOG"
