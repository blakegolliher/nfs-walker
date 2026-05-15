# Building nfs-walker

The binary streams scan output directly to sharded Parquet — there is
no RocksDB dependency anymore, so build prerequisites are lean: libnfs
and a Rust toolchain.

## Quick Start

```bash
# Native build
make build

# Static musl binary (works on any Linux ≥ glibc-anything)
make release
```

Output lands in `./build/`.

---

## Build methods

### 1. Native build

Builds against the system's libnfs and glibc. Best for development; the
resulting binary is tied to the build host's glibc.

```bash
sudo apt install build-essential pkg-config libnfs-dev      # Debian/Ubuntu
# or
sudo dnf install gcc make pkg-config libnfs-devel           # Rocky/RHEL

make build
```

**Output:** `./build/nfs-walker`

### 2. Static musl build (recommended for distributing)

Single-binary, no shared-library dependencies. We use `cargo-zigbuild`
to cross-compile against musl + a libnfs static archive.

```bash
make release
```

**Output:** `./build/nfs-walker-static`
**Compatible with:** any Linux distribution

Build sets `#[global_allocator] = mimalloc` to bypass Zig's
`SmpAllocator` returning NULL at high thread count — required for
scans with ≥250 total threads.

### 3. Docker / Podman build

If your host's libnfs is too old (see "libnfs version" below), build
inside a container with a known-good libnfs:

```bash
make docker-rocky
```

**Output:** `./build/nfs-walker-VERSION-el9`

---

## Build targets

| Target              | Description                          |
|---------------------|--------------------------------------|
| `make build`        | Native release build                 |
| `make release`      | Static musl build via cargo-zigbuild |
| `make docker-rocky` | Rocky-9-based container build        |
| `make debug`        | Native debug build                   |

---

## Dependencies

### Build-time

- **Rust 1.82+** — install via [rustup](https://rustup.rs/)
- **libnfs** (development headers) — see "libnfs version" below
- **pkg-config**

### Run-time

- libnfs shared library (for the native build)
- nothing else (for the static musl build)

### libnfs version

nfs-walker requires libnfs master (post-5.0.3) — it uses the `_task`
async API added after that release. If your distro's package is
older, build from source:

```bash
git clone https://github.com/sahlberg/libnfs.git
cd libnfs
git checkout master
./bootstrap
./configure --prefix=/usr/local
make -j$(nproc)
sudo make install
sudo ldconfig
```

### Cross-compilation (cargo-zigbuild)

For `make release`:

```bash
cargo install cargo-zigbuild
# Zig is a runtime dependency of cargo-zigbuild; the install picks it up.
```

---

## Development

```bash
# Run tests
cargo test

# Format
cargo fmt

# Lint
cargo clippy --all-targets

# Clean build artifacts
cargo clean
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

### Static musl build can't find libnfs

The musl build expects libnfs's static archive inside its sysroot.
Either build libnfs into that sysroot or use `make docker-rocky` and
let the container handle it.

### Binary doesn't run on target system

```
./nfs-walker: /lib64/libc.so.6: version `GLIBC_2.38' not found
```

The native binary was built against a newer glibc than the target.
Switch to `make release` (static musl) or `make docker-rocky`.

### `SmpAllocator: out of memory` / `bad_alloc` at high thread count

The static musl binary uses mimalloc as the global allocator
specifically to avoid Zig's `SmpAllocator` failing under high thread
fan-out. If you see this crash, confirm the binary actually has
mimalloc linked:

```bash
nm ./nfs-walker | grep -i mimalloc | head
```

Should show a handful of `mi_*` symbols.

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
