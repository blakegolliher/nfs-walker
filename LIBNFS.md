# Custom libnfs Dependency

**Important:** nfs-walker requires libnfs built from `master` branch (not the 5.0.3 release).

## Why Custom Build?

The `_task` async functions used by nfs-walker were added after the 5.0.3 release:
- `nfs_opendir_task()` / `nfs_opendir_task_async()`
- `nfs_readdirplus_task()` / `nfs_readdirplus_task_async()`
- `nfs_closedir_task()`

These are required for the async pipelining that gives nfs-walker its performance.

## Building libnfs

```bash
git clone https://github.com/sahlberg/libnfs.git
cd libnfs
git checkout master   # Must use master, not a release tag
./bootstrap
./configure --prefix=/usr/local
make -j$(nproc)
sudo make install
sudo ldconfig
```

## Docker Builds

The Docker builds (Dockerfile.rocky, Dockerfile.musl) automatically build libnfs from master, so no manual steps are needed when using:

```bash
make docker-rocky
```

## Verification

Check that the task functions are available:

```bash
nm -D /usr/local/lib/libnfs.so | grep nfs_opendir_task
```

Should show symbols like:
```
0000000000012345 T nfs_opendir_task
0000000000012350 T nfs_opendir_task_async
```
