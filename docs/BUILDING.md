# Building nfs-walker

## Requirements

- Linux (Ubuntu 22.04+ recommended)
- Rust 1.82+
- libnfs-dev
- For RocksDB builds: libclang-dev, clang

## Quick Start

```bash
# Install dependencies
make install-deps

# Build with RocksDB (recommended)
make release-rocks

# Build without RocksDB (SQLite only, smaller binary)
make release
```

Binary output: `./build/nfs-walker`

---

## Build Targets

| Target | Description | Output |
|--------|-------------|--------|
| `make build` | Debug build with RocksDB | `target/debug/nfs-walker` |
| `make release` | Release build, SQLite only | `build/nfs-walker-VERSION` |
| `make release-rocks` | Release build with RocksDB | `build/nfs-walker-VERSION-rocks` |
| `make docker-release` | Static build via Docker | `build/nfs-walker-VERSION-rocks` |

---

## Dependencies

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
git checkout libnfs-5.0.3
./bootstrap
./configure --prefix=/usr/local
make -j$(nproc)
sudo make install
sudo ldconfig
```

---

## Build Configurations

### Standard Build (with RocksDB)

```bash
cargo build --release --features rocksdb
```

Produces a dynamically linked binary. Requires:
- libnfs.so
- librocksdb.so (or statically linked)
- glibc

### SQLite-Only Build

```bash
cargo build --release --no-default-features
```

Smaller binary, no RocksDB dependency. Limitations:
- No RocksDB output format
- No `stats` subcommand
- No `convert` subcommand

### Static Build (Docker)

For portable binaries that run on any Linux:

```bash
make docker-release
```

This builds in a Docker container with all dependencies statically linked.

---

## Cross-Compilation

### musl (Alpine Linux)

```bash
# Install musl toolchain
sudo apt install musl-tools

# Build libnfs for musl
CC=musl-gcc cmake -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX=/usr/local/musl ..

# Build nfs-walker
cargo build --release --target x86_64-unknown-linux-musl --no-default-features
```

Note: RocksDB doesn't compile easily with musl. Use `--no-default-features` for musl builds.

---

## Development

```bash
# Run tests
make test

# Run tests with RocksDB
cargo test --features rocksdb

# Format code
make fmt

# Run clippy
make check

# Clean build artifacts
make clean
```

---

## Makefile Reference

```makefile
install-deps    # Install build dependencies (Ubuntu/Debian)
build           # Debug build
release         # Release build (SQLite only)
release-rocks   # Release build with RocksDB
docker-release  # Static build via Docker
test            # Run tests
fmt             # Format code
check           # Run clippy
clean           # Clean build artifacts
list            # List built binaries
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

For musl targets, skip RocksDB:
```bash
cargo build --release --no-default-features --target x86_64-unknown-linux-musl
```

### libstdc++ not found

Add the GCC library path:
```bash
export LIBRARY_PATH=/usr/lib/gcc/x86_64-linux-gnu/13:$LIBRARY_PATH
```

Or add to `.cargo/config.toml`:
```toml
[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "link-arg=-L/usr/lib/gcc/x86_64-linux-gnu/13"]
```
