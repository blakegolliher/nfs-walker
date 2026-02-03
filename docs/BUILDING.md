# Building nfs-walker

## Quick Start (Recommended)

Build a portable binary with RocksDB that works on Rocky/RHEL 9+, Ubuntu 22.04+, Debian 12+:

```bash
make docker-rocky
```

Output: `./build/nfs-walker-rocks`

This is the recommended method. It requires only Docker or Podman - no local dependencies needed.

**Build time:** ~5 minutes first build, ~1-2 minutes with cache

---

## Build Methods

### 1. Docker Build (Recommended)

Builds in a Rocky Linux 9 container. Produces a portable binary with RocksDB.

```bash
make docker-rocky
```

**Requirements:** Docker or Podman
**Build time:** ~5 minutes (first build), ~2 minutes (cached)
**Output:** `./build/nfs-walker-VERSION-el9-rocks`
**Compatible with:** Rocky/RHEL/Alma 9+, Ubuntu 22.04+, Debian 12+

### 2. Native Build

Build directly on your system. Requires local dependencies.

```bash
# Install dependencies (Ubuntu/Debian)
make install-deps

# Build with RocksDB
make build
```

**Requirements:** Rust 1.82+, libnfs-dev, libclang-dev, clang
**Output:** `./build/nfs-walker-rocks`

### 3. SQLite-Only Build

Smaller binary without RocksDB. Fully static, runs on any Linux.

```bash
make release
```

**Requirements:** Rust 1.82+, musl toolchain
**Output:** `./build/nfs-walker-static`
**Limitations:** No RocksDB output, no `convert` or `stats` commands

---

## Build Targets

| Target | Description | RocksDB | Portable |
|--------|-------------|---------|----------|
| `make docker-rocky` | Docker build for RHEL/Rocky 9+ | Yes | glibc 2.34+ |
| `make build` | Native build with RocksDB | Yes | No |
| `make release` | Static musl build | No | Any Linux |
| `make debug` | Debug build | Yes | No |

---

## Dependencies (Native Build)

### Ubuntu/Debian

```bash
# Core dependencies
sudo apt install build-essential pkg-config libsqlite3-dev libnfs-dev

# For RocksDB builds
sudo apt install libclang-dev clang

# Or use the Makefile
make install-deps
```

### From Source (libnfs)

If your distro doesn't have libnfs-dev:

```bash
git clone https://github.com/sahlberg/libnfs.git
cd libnfs
# Must use master branch - nfs-walker requires _task functions added after 5.0.3
git checkout master
./bootstrap
./configure --prefix=/usr/local
make -j$(nproc)
sudo make install
sudo ldconfig
```

---

## Development

```bash
# Run tests
make test

# Format code
make fmt

# Run clippy
make check

# Clean build artifacts
make clean

# List built binaries
make list
```

---

## Troubleshooting

### libnfs not found

```
error: could not find native static library `nfs`
```

Ensure libnfs is installed and pkg-config can find it:
```bash
pkg-config --libs libnfs
```

If installed to a non-standard location:
```bash
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH
```

### RocksDB build fails

RocksDB requires clang for compilation:
```bash
sudo apt install clang libclang-dev
```

If native builds are problematic, use the Docker build instead:
```bash
make docker-rocky
```

### Binary doesn't run on target system

```
./nfs-walker: /lib64/libc.so.6: version `GLIBC_2.38' not found
```

The binary was built on a newer system than the target. Use `make docker-rocky` which builds on Rocky 9 (glibc 2.34) for maximum compatibility.

### Docker/Podman build fails with disk space error

```
no space left on device
```

Clean up container images:
```bash
podman system prune -af
# or
docker system prune -af
```
