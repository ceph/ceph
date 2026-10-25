/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! Build script for lancedb-rgw-store

fn main() {
    // The rgw_sal_wrapper_* functions this crate calls are provided by
    // rgw_sal_wrapper.cc, compiled into rgw_common. This crate is built
    // into librgw_lancedb.so (see the rgw-lancedb umbrella crate), which
    // leaves them undefined. The executables and shared libraries that
    // load it export them, because of the --undefined=rgw_sal_wrapper_version
    // flag in src/rgw/CMakeLists.txt.
    // Hence nothing needs to be linked here.
    println!("cargo:rerun-if-env-changed=CEPH_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=CEPH_SRC_DIR");
    println!("cargo:rerun-if-changed=build.rs");

    // A `cargo test` binary is an executable (unlike librgw_lancedb.so), so the
    // rgw_sal_wrapper symbols the FFI calls have to resolve here. The ceph
    // build sets RGW_SAL_TEST_ENV_DIR to the directory holding
    // libceph_rgw_sal_test_env.so, which exports both that API and the test
    // environment setup entry points (rgw_test_env_*).  It is unset for an
    // ordinary build, which therefore links exactly as before.
    println!("cargo:rerun-if-env-changed=RGW_SAL_TEST_ENV_DIR");
    println!("cargo:rerun-if-env-changed=RGW_SAL_TEST_ENV_RPATH");
    if let Ok(dir) = std::env::var("RGW_SAL_TEST_ENV_DIR") {
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=dylib=ceph_rgw_sal_test_env");
        // Use rustc-link-arg (not -tests): it covers the crate's own
        // #[cfg(test)] unit-test binary as well as the tests/ integration
        // binary.  It never reaches librgw_lancedb.so, both because
        // rustc-link-arg only applies to this package's own targets and
        // because RGW_SAL_TEST_ENV_DIR is only set for cargo test.
        if let Ok(rpath) = std::env::var("RGW_SAL_TEST_ENV_RPATH") {
            for path in rpath.split(':').filter(|p| !p.is_empty()) {
                println!("cargo:rustc-link-arg=-Wl,-rpath,{path}");
            }
        }
        // Keep this crate's rlib copy out of the tests/ binary's dynamic
        // symbols, so the test env's rgw_lancedb_store_create_provider()
        // binds to the copy in librgw_lancedb.so that its lancedb-c expects.
        println!("cargo:rustc-link-arg=-Wl,--exclude-libs,ALL");
    }
}
