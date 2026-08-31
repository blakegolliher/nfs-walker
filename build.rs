//! Build script for nfs-walker
//!
//! Uses pkg-config to find libnfs and emit the correct linker flags.
//! The FFI bindings are pre-generated in src/nfs/bindings.rs.

fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();

    // Explicit override for embedding consumers (e.g. mongoose, which
    // compiles this crate in as a library and cross-links for an older
    // glibc): a directory holding the libnfs to link. `libnfs.a` is
    // preferred (fully static); `libnfs.so` is accepted. Checked before
    // every auto-detection path — the /usr/local probe below would
    // otherwise pick up a host-glibc build that poisons cross links.
    println!("cargo:rerun-if-env-changed=NFS_WALKER_LIBNFS_DIR");
    if let Some(dir) = std::env::var_os("NFS_WALKER_LIBNFS_DIR") {
        let dir = std::path::PathBuf::from(dir);
        println!("cargo:rustc-link-search=native={}", dir.display());
        if dir.join("libnfs.a").exists() {
            println!("cargo:rustc-link-lib=static=nfs");
            eprintln!("Using static libnfs from NFS_WALKER_LIBNFS_DIR={}", dir.display());
        } else if dir.join("libnfs.so").exists() {
            println!("cargo:rustc-link-lib=dylib=nfs");
            eprintln!("Using dynamic libnfs from NFS_WALKER_LIBNFS_DIR={}", dir.display());
        } else {
            panic!(
                "NFS_WALKER_LIBNFS_DIR={} contains neither libnfs.a nor libnfs.so",
                dir.display()
            );
        }
        return;
    }

    // For musl targets, use the musl-specific libnfs installation
    if target.contains("musl") {
        // Check for musl-specific libnfs installation
        let musl_lib_path = "/usr/local/musl/lib";
        let musl_include_path = "/usr/local/musl/include";

        if std::path::Path::new(&format!("{}/libnfs.a", musl_lib_path)).exists() {
            println!("cargo:rustc-link-search=native={}", musl_lib_path);
            println!("cargo:rustc-link-lib=static=nfs");
            println!("cargo:include={}", musl_include_path);
            eprintln!("Using musl libnfs from {}", musl_lib_path);
            return;
        }

        // Fallback: try to use pkg-config with musl sysroot
        if let Ok(lib) = pkg_config::Config::new()
            .statik(true)
            .probe("libnfs")
        {
            for path in &lib.link_paths {
                println!("cargo:rustc-link-search=native={}", path.display());
            }
            println!("cargo:rustc-link-lib=static=nfs");
            eprintln!("libnfs link_paths: {:?}", lib.link_paths);
            return;
        }

        panic!(
            "libnfs for musl not found. Expected static library at {}/libnfs.a\n\
             Build libnfs with musl: CC=musl-gcc ./configure --prefix=/usr/local/musl --enable-static --disable-shared",
            musl_lib_path
        );
    }

    // Check for static libnfs at common paths (Docker builds)
    // Rocky/RHEL may use lib64, others use lib
    for local_lib_path in &["/usr/local/lib", "/usr/local/lib64"] {
        if std::path::Path::new(&format!("{}/libnfs.a", local_lib_path)).exists() {
            println!("cargo:rustc-link-search=native={}", local_lib_path);
            println!("cargo:rustc-link-lib=static=nfs");
            eprintln!("Using static libnfs from {}", local_lib_path);
            return;
        }
    }

    // For non-musl targets, use pkg-config with dynamic linking
    // (static linking only works when libnfs.a is available)
    let lib = pkg_config::Config::new()
        .statik(false)  // Use dynamic linking
        .probe("libnfs")
        .expect(
            "libnfs not found. Please install libnfs development package \
             (e.g., libnfs-dev on Debian/Ubuntu, libnfs-devel on Fedora/RHEL)",
        );

    // Print what pkg-config found (for debugging)
    eprintln!("Using dynamic libnfs");
    eprintln!("libnfs link_paths: {:?}", lib.link_paths);
    eprintln!("libnfs libs: {:?}", lib.libs);
}
