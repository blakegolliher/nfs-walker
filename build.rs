//! Build script for nfs-walker
//!
//! Generates Rust bindings for libnfs using bindgen.
//! Uses the patched libnfs from vendor/libnfs.

use std::env;
use std::path::PathBuf;

fn main() {
    // Get the project root directory
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let vendor_libnfs = manifest_dir.join("vendor/libnfs");
    let vendor_build = vendor_libnfs.join("build");
    let vendor_include = vendor_libnfs.join("include");

    // Rerun if wrapper header or vendor sources change
    println!("cargo:rerun-if-changed=src/nfs/wrapper.h");
    println!(
        "cargo:rerun-if-changed={}",
        vendor_include.join("nfsc/libnfs.h").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        vendor_include.join("libnfs-private.h").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        vendor_libnfs.join("lib/libnfs.c").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        vendor_libnfs.join("lib/nfs_v3.c").display()
    );

    // Determine which build directory to use based on target
    let target = env::var("TARGET").unwrap_or_default();
    let build_dir = if target.contains("musl") {
        vendor_libnfs.join("build-musl")
    } else {
        vendor_build.clone()
    };

    // Check if vendor libnfs is built (static library)
    let vendor_lib = build_dir.join("lib/libnfs.a");
    if !vendor_lib.exists() {
        eprintln!("Error: Vendor libnfs not built for target {}. Please run:", target);
        if target.contains("musl") {
            eprintln!(
                "  cd {} && mkdir -p build-musl && cd build-musl && CC=musl-gcc cmake -DBUILD_SHARED_LIBS=OFF .. && make -j$(nproc)",
                vendor_libnfs.display()
            );
        } else {
            eprintln!(
                "  cd {} && mkdir -p build && cd build && cmake -DBUILD_SHARED_LIBS=OFF .. && make -j$(nproc)",
                vendor_libnfs.display()
            );
        }
        std::process::exit(1);
    }

    // Generate bindings from vendor headers
    let mut builder = bindgen::Builder::default()
        .header("src/nfs/wrapper.h")
        // Add vendor include path (must come first to override system headers)
        .clang_arg(format!("-I{}", vendor_include.display()));

    // When cross-compiling for musl, we need to tell clang where to find musl headers
    if target.contains("musl") {
        // Use musl headers instead of glibc headers
        builder = builder
            .clang_arg("--sysroot=/usr/lib/x86_64-linux-musl")
            .clang_arg("-I/usr/include/x86_64-linux-musl");
    }

    let builder = builder
        // Only generate bindings for nfs functions
        .allowlist_function("nfs_.*")
        .allowlist_function("rpc_.*")
        .allowlist_type("nfs_.*")
        .allowlist_type("nfsdir")
        .allowlist_type("nfsfh")
        .allowlist_type("nfsdirent")
        .allowlist_type("nfs_stat_64")
        .allowlist_type("nfs_context")
        .allowlist_type("nfs_cb")
        .allowlist_var("NFS.*")
        .allowlist_var("O_.*")
        .allowlist_var("POLLIN")
        .allowlist_var("POLLOUT")
        .allowlist_var("POLLERR")
        .allowlist_var("POLLHUP")
        // Generate Debug trait where possible
        .derive_debug(true)
        .derive_default(true)
        // Use core types
        .use_core()
        // Treat as opaque types we don't need to inspect
        .opaque_type("nfs_context")
        .opaque_type("nfsdir")
        .opaque_type("nfsfh")
        // Layout tests can be noisy
        .layout_tests(false)
        // Make it cleaner
        .generate_comments(true);

    // Generate bindings
    let bindings = builder
        .generate()
        .expect("Unable to generate libnfs bindings");

    // Write to OUT_DIR
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("nfs_bindings.rs"))
        .expect("Couldn't write bindings!");

    // Link against vendor libnfs (static)
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir.join("lib").display()
    );
    println!("cargo:rustc-link-lib=static=nfs");
}
