# Custom libnfs Dependency

**Important:** nfs-walker requires a patched version of libnfs with VAST extensions for high-performance directory scanning.

## VAST Extensions

The patch in `patches/libnfs-vast-extensions.patch` adds the following functions:

| Function | Purpose |
|----------|---------|
| `nfs_opendir_at_cookie_async()` | Start READDIRPLUS at specific cookie position with entry limit |
| `nfs_opendir_names_only_async()` | Use READDIR (no stat) - 10x+ faster for large directories |
| `nfs_opendir_names_only_at_cookie_async()` | Combined: fast enumeration + streaming + cookie positioning |
| `nfs_readdir_get_cookieverf()` | Get cookie verifier for resuming across batches |

These enable:
- **Parallel directory reading**: Start from different cookie positions
- **Streaming large directories**: Fetch in batches with entry limits
- **Fast enumeration**: READDIR without server-side stat() per file
- **Proper resume**: Cookie verifier handling for NFS3

## Building libnfs

```bash
# Clone upstream libnfs
git clone https://github.com/sahlberg/libnfs.git
cd libnfs

# Apply VAST extensions
patch -p1 < /path/to/nfs-walker/patches/libnfs-vast-extensions.patch

# Build
./bootstrap
./configure --prefix=/usr/local
make -j$(nproc)
sudo make install
sudo ldconfig
```

## Docker Builds

The Dockerfiles should be updated to apply the patch. Current builds use upstream without patches (may work for basic functionality but lack streaming optimizations).

## Verification

Check that the VAST extension functions are available:

```bash
nm -D /usr/local/lib/libnfs.so | grep nfs_opendir_at_cookie
```

Should show symbols like:
```
0000000000... T nfs_opendir_at_cookie
0000000000... T nfs_opendir_at_cookie_async
0000000000... T nfs_opendir_names_only
0000000000... T nfs_opendir_names_only_async
0000000000... T nfs_opendir_names_only_at_cookie
0000000000... T nfs_opendir_names_only_at_cookie_async
0000000000... T nfs_readdir_get_cookieverf
```

## Source Location

The patched libnfs source is at `/home/vastdata/projects/libnfs-src` with uncommitted changes. The patch was extracted from there on 2026-02-04.
