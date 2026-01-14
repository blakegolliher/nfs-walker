#!/bin/bash
#
# Benchmark: nfs-walker vs traditional filesystem tools
#

set -e

# Configuration
NFS_MOUNT="/mnt/syncengine-demo"
NFS_URL="nfs://main.selab-var203.selab.vastdata.com/syncengine-demo"
WALKER="./build/nfs-walker"
CONNECTIONS=32
OUTPUT_DIR="/tmp"

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
echo "Connections: $CONNECTIONS"
echo ""

# Cleanup
rm -f ${OUTPUT_DIR}/bench.parquet ${OUTPUT_DIR}/bench.db

#############################################
# Traditional Tools (via kernel NFS client)
#############################################

print_header "1. find (count files only)"
clear_cache
time find "$NFS_MOUNT" -type f 2>/dev/null | wc -l

print_header "2. find with stat (path + size + mtime)"
clear_cache
time find "$NFS_MOUNT" -type f -printf '%s %T@ %p\n' 2>/dev/null | wc -l

print_header "3. ls -lR (recursive listing)"
clear_cache
time ls -lR "$NFS_MOUNT" 2>/dev/null | wc -l

print_header "4. du (disk usage)"
clear_cache
time du -s "$NFS_MOUNT" 2>/dev/null

print_header "5. tree (directory tree)"
clear_cache
time tree -a "$NFS_MOUNT" 2>/dev/null | tail -3

print_header "6. rsync --dry-run (file enumeration)"
clear_cache
time rsync -an --stats "$NFS_MOUNT/" /dev/null 2>/dev/null | tail -5

#############################################
# nfs-walker (direct NFS protocol)
#############################################

print_header "7. nfs-walker (SQLite output)"
clear_cache
time sudo "$WALKER" "$NFS_URL" -o ${OUTPUT_DIR}/bench.db --connections $CONNECTIONS

print_header "8. nfs-walker (Parquet output)"
clear_cache
time sudo "$WALKER" "$NFS_URL" --format parquet -o ${OUTPUT_DIR}/bench.parquet --connections $CONNECTIONS

#############################################
# Summary
#############################################

print_header "Output File Sizes"
ls -lh ${OUTPUT_DIR}/bench.db ${OUTPUT_DIR}/bench.parquet 2>/dev/null

echo ""
echo "=============================================="
echo "  Benchmark Complete"
echo "=============================================="
