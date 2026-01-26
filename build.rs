//! Build script for nfs-walker
//!
//! Uses pkg-config to find libnfs and emit the correct linker flags.
//! The FFI bindings are pre-generated in src/nfs/bindings.rs.

fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();

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

    // For non-musl targets, use pkg-config
    let lib = pkg_config::Config::new()
        .statik(true)  // Prefer static linking
        .probe("libnfs")
        .expect(
            "libnfs not found. Please install libnfs development package \
             (e.g., libnfs-dev on Debian/Ubuntu, libnfs-devel on Fedora/RHEL)",
        );

    // Print what pkg-config found (for debugging)
    eprintln!("libnfs link_paths: {:?}", lib.link_paths);
    eprintln!("libnfs libs: {:?}", lib.libs);

    // Explicitly emit static link directive
    for path in &lib.link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }

    // Force static linking
    println!("cargo:rustc-link-lib=static=nfs");
}
