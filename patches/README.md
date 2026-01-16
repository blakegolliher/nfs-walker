# libnfs Patches

> **STATUS**: These patches are vendored in the project at `vendor/libnfs/`. You don't need to apply them manually.
>
> **WARNING**: The skinny mode features added by these patches have known bugs and are disabled by default. The standard READDIRPLUS mode achieves 50,000+ files/sec and is recommended.

This directory contains patches for libnfs that were intended to enable high-performance directory enumeration for huge flat directories (millions of files in a single directory).

## Current Status

- **Vendored**: libnfs with patches is at `vendor/libnfs/`
- **Skinny mode**: Experimental, hidden, has enumeration bugs
- **Default mode**: READDIRPLUS (works correctly, 50K+ files/sec)

## What the Patches Add

### 1. `nfs_opendir_names_only_at_cookie()`
Fast directory enumeration using READDIR instead of READDIRPLUS:
- No server-side stat() calls
- Cookie-based streaming (batch processing)
- Cookie verifier support for strict NFS servers
- **STATUS**: Has enumeration bugs - doesn't return all entries

### 2. `nfs_opendir_at_cookie()`
READDIRPLUS with cookie positioning and max_entries limit.

### 3. `nfs_opendir_names_only()`
Simple names-only read (loads all entries at once).

### 4. `nfs_readdir_get_cookieverf()`
Helper to retrieve cookie verifier from nfsdir handle.

### 5. Cookie Exposure
Adds `uint64_t cookie` field to `struct nfsdirent` to expose real NFS cookies.

## Performance (When Working)

The original benchmarks showed promise, but integrated testing revealed bugs:

| Directory | READDIRPLUS | Skinny Read | Note |
|-----------|-------------|-------------|------|
| 48k files | 0.99s | 0.27s | Worked in isolated test |
| 15M files | 364s | 84s | Worked in isolated test |
| 10M files (integration) | 297s | **FAILS** | Only returns ~10K files |

## Future Work

The skinny mode concept is sound but the implementation needs debugging:
1. Investigate wrap-around detection logic
2. Verify cookie/verifier handling across batches
3. Test against different NFS server implementations

## Applying Patches (Not Needed)

The patches are already applied in `vendor/libnfs/`. If you need to apply them to a fresh libnfs:

```bash
git clone https://github.com/sahlberg/libnfs.git
cd libnfs
patch -p1 < /path/to/nfs-walker/patches/libnfs-all-optimizations.patch
./bootstrap
./configure
make -j$(nproc)
```

## Source Repository

These patches were developed against libnfs and are based on VAST Data's `vast-libnfs` repository.
