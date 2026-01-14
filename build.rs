//! Build script for nfs-walker
//!
//! Generates Rust bindings for libnfs using bindgen.
//! Requires libnfs-dev to be installed on the system.

use std::env;
use std::path::PathBuf;

fn main() {
    // Rerun if wrapper header changes
    println!("cargo:rerun-if-changed=src/nfs/wrapper.h");

    // Find libnfs using pkg-config
    let nfs_lib = match pkg_config::Config::new()
        .atleast_version("4.0.0")
        .probe("libnfs")
    {
        Ok(lib) => lib,
        Err(e) => {
            // Provide helpful error message
            eprintln!("Error: Could not find libnfs via pkg-config: {}", e);
            eprintln!();
            eprintln!("To install libnfs on Ubuntu/Debian:");
            eprintln!("  sudo apt install libnfs-dev");
            eprintln!();
            eprintln!("Or build from source:");
            eprintln!("  git clone https://github.com/sahlberg/libnfs.git");
            eprintln!("  cd libnfs && ./bootstrap && ./configure && make && sudo make install");
            std::process::exit(1);
        }
    };

    // Collect include paths for bindgen
    let mut builder = bindgen::Builder::default()
        .header("src/nfs/wrapper.h")
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
        .generate_comments(true);

    // Add include paths from pkg-config
    for path in &nfs_lib.include_paths {
        builder = builder.clang_arg(format!("-I{}", path.display()));
    }

    // Generate bindings
    let bindings = builder
        .generate()
        .expect("Unable to generate libnfs bindings");

    // Write to OUT_DIR
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("nfs_bindings.rs"))
        .expect("Couldn't write bindings!");

    // Link libraries
    for lib in &nfs_lib.libs {
        println!("cargo:rustc-link-lib={}", lib);
    }

    // Link paths
    for path in &nfs_lib.link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }
}
