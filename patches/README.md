# libnfs Patches

This directory contains patches for libnfs that enable high-performance directory enumeration.

## Applying Patches

```bash
# Clone libnfs
git clone https://github.com/sahlberg/libnfs.git
cd libnfs

# Apply our patches
patch -p1 < /path/to/nfs-walker/patches/libnfs-all-optimizations.patch

# Build and install
./bootstrap
./configure
make -j$(nproc)
sudo make install
sudo ldconfig
```

## What the Patches Add

### 1. `nfs_opendir_names_only_at_cookie()`
Fast directory enumeration using READDIR instead of READDIRPLUS:
- No server-side stat() calls
- Cookie-based streaming (batch processing)
- Cookie verifier support for strict NFS servers

### 2. `nfs_opendir_at_cookie()`
READDIRPLUS with cookie positioning and max_entries limit.

### 3. `nfs_opendir_names_only()`
Simple names-only read (loads all entries at once).

### 4. `nfs_readdir_get_cookieverf()`
Helper to retrieve cookie verifier from nfsdir handle.

### 5. Cookie Exposure
Adds `uint64_t cookie` field to `struct nfsdirent` to expose real NFS cookies.

## Performance Impact

| Directory | READDIRPLUS | Skinny Read | Speedup |
|-----------|-------------|-------------|---------|
| 48k files | 0.99s | 0.27s | 3.6x |
| 15M files | 364s | 84s | 4.3x |

## Source Repository

These patches were developed against libnfs commit `dc7e6f8` (branch: `expose-cookie`).

The development repo is at: `/home/vastdata/projects/libnfs-src`

## Upstream Submission

TODO: Submit these patches as PRs to https://github.com/sahlberg/libnfs
