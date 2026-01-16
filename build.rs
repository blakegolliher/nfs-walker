//! Build script for nfs-walker
//!
//! Builds libnfs from the vendored submodule and generates Rust bindings.
//!
//! The vendored libnfs is in vendor/libnfs and includes VAST's optimizations
//! for high-performance directory scanning.

use std::env;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let libnfs_dir = manifest_dir.join("vendor").join("libnfs");

    // Check if vendored libnfs exists
    if !libnfs_dir.exists() {
        eprintln!("Error: Vendored libnfs not found at {}", libnfs_dir.display());
        eprintln!();
        eprintln!("Please initialize the submodule:");
        eprintln!("  git submodule update --init");
        eprintln!();
        eprintln!("Or clone it manually:");
        eprintln!("  git clone https://gitlab.vastdata.com/storage/vast-libnfs.git vendor/libnfs");
        std::process::exit(1);
    }

    // Rerun if libnfs sources change
    println!("cargo:rerun-if-changed=vendor/libnfs/lib");
    println!("cargo:rerun-if-changed=vendor/libnfs/include");
    println!("cargo:rerun-if-changed=src/nfs/wrapper.h");

    // Build libnfs using cmake
    let dst = cmake::Config::new(&libnfs_dir)
        .define("BUILD_SHARED_LIBS", "OFF")   // Static library
        .define("ENABLE_TESTS", "OFF")        // Skip tests
        .define("ENABLE_DOCUMENTATION", "OFF") // Skip docs
        .define("ENABLE_EXAMPLES", "OFF")     // Skip examples
        .build();

    let lib_dir = dst.join("lib");
    let lib64_dir = dst.join("lib64");
    let include_dir = dst.join("include");

    // Link paths (cmake might put libs in lib/ or lib64/)
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-search=native={}", lib64_dir.display());

    // Link libnfs statically
    println!("cargo:rustc-link-lib=static=nfs");

    // Generate bindings using bindgen
    let bindings = bindgen::Builder::default()
        .header("src/nfs/wrapper.h")
        // Point to vendored headers
        .clang_arg(format!("-I{}", include_dir.display()))
        .clang_arg(format!("-I{}", libnfs_dir.join("include").display()))
        // Only generate bindings for nfs functions
        .allowlist_function("nfs_.*")
        .allowlist_function("rpc_.*")
        .allowlist_type("nfs_.*")
        .allowlist_type("nfsdir")
        .allowlist_type("nfsfh")
        .allowlist_type("nfsdirent")
        .allowlist_type("nfs_stat_64")
        .allowlist_type("nfs_context")
        .allowlist_var("NFS.*")
        .allowlist_var("O_.*")
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
        .generate_comments(true)
        .generate()
        .expect("Unable to generate libnfs bindings");

    // Write bindings
    bindings
        .write_to_file(out_dir.join("nfs_bindings.rs"))
        .expect("Couldn't write bindings!");
}
