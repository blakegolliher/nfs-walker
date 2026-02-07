#!/bin/bash
#
# Benchmark: nfs-walker vs traditional filesystem tools
#

set -e

# Configuration
NFS_MOUNT="/mnt/syncengine-demo"
NFS_URL="nfs://main.selab-var203.selab.vastdata.com/syncengine-demo"
WALKER="./build/nfs-walker"
FD="/home/vastdata/.cargo/bin/fd"
WORKERS=32
OUTPUT_DIR="/tmp"
LOG_FILE="benchmark-$(date +%Y%m%d-%H%M%S).log"

# Log everything to file and screen
exec > >(tee -a "$LOG_FILE") 2>&1

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

clear_cache() {
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>&1
    sleep 1
}

print_header() {
    echo ""
    echo -e "${GREEN}=== $1 ===${NC}"
    echo ""
}

print_result() {
    echo -e "${YELLOW}$1${NC}"
}

# Check prerequisites
if [ ! -x "$WALKER" ]; then
    echo "Error: nfs-walker not found at $WALKER"
    echo "Run 'make build' first"
    exit 1
fi

if [ ! -d "$NFS_MOUNT" ]; then
    echo "Error: NFS mount not found at $NFS_MOUNT"
    exit 1
fi

echo "=============================================="
echo "  NFS Walker Benchmark"
echo "=============================================="
echo ""
echo "NFS Mount: $NFS_MOUNT"
echo "NFS URL:   $NFS_URL"
echo "Workers:   $WORKERS"
echo "Log file:  $LOG_FILE"
echo ""

# Cleanup
rm -f ${OUTPUT_DIR}/bench.rocks ${OUTPUT_DIR}/bench.db
rm -rf /tmp/rsync-null && mkdir -p /tmp/rsync-null

#############################################
# Traditional Tools (via kernel NFS client)
#############################################

print_header "1. fd-find (fast file search)"
clear_cache
time $FD . "$NFS_MOUNT" 2>/dev/null | wc -l

print_header "2. dust (fast du alternative)"
clear_cache
time dust -d 0 "$NFS_MOUNT" 2>/dev/null

print_header "3. rsync --dry-run (file enumeration)"
clear_cache
time rsync -an --stats "$NFS_MOUNT/" /tmp/rsync-null/ 2>&1 | grep -E "(files|bytes)"

print_header "4. find (count files)"
clear_cache
time find "$NFS_MOUNT" -type f 2>/dev/null | wc -l

print_header "5. du (disk usage)"
clear_cache
time du -sh "$NFS_MOUNT" 2>/dev/null

#############################################
# nfs-walker (direct NFS protocol)
#############################################

print_header "6. nfs-walker (SQLite output)"
clear_cache
time sudo "$WALKER" "$NFS_URL" --sqlite -o ${OUTPUT_DIR}/bench.db -w $WORKERS

print_header "7. nfs-walker (RocksDB output)"
clear_cache
time sudo "$WALKER" "$NFS_URL" -o ${OUTPUT_DIR}/bench.rocks -w $WORKERS

#############################################
# Summary
#############################################

print_header "Output File Sizes"
ls -lh ${OUTPUT_DIR}/bench.db 2>/dev/null
du -sh ${OUTPUT_DIR}/bench.rocks 2>/dev/null

echo ""
echo "=============================================="
echo "  Benchmark Complete"
echo "=============================================="
echo ""
echo "Log saved to: $LOG_FILE"
