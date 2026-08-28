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
    // rgw_sal_wrapper.cc, compiled into rgw_common. This crate is linked
    // into the executables as part of the librgw_lancedb.a staticlib (see
    // the rgw-lancedb umbrella crate), and the mutual symbol dependency
    // between the archive and rgw_common is resolved at link time by the
    // --undefined=rgw_sal_wrapper_version flag in src/rgw/CMakeLists.txt.
    // Hence nothing needs to be linked here.
    println!("cargo:rerun-if-env-changed=CEPH_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=CEPH_SRC_DIR");
    println!("cargo:rerun-if-changed=build.rs");

    // `cargo test` is different: a test binary is a real link, so the
    // rgw_sal_wrapper symbols have to resolve here.  RGW_SAL_TEST_ENV_DIR is
    // set by src/test/rgw/cargo_test_binary.cmake and points at the directory
    // holding libceph_rgw_sal_test_env.so, which exports both that API and the
    // test-environment setup entry points.  Unset for an ordinary build, which
    // therefore stays exactly as it was.
    println!("cargo:rerun-if-env-changed=RGW_SAL_TEST_ENV_DIR");
    println!("cargo:rerun-if-env-changed=RGW_SAL_TEST_ENV_RPATH");
    if let Ok(dir) = std::env::var("RGW_SAL_TEST_ENV_DIR") {
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=dylib=ceph_rgw_sal_test_env");
        // rustc-link-arg, not rustc-link-arg-tests: the latter covers only
        // tests/ targets, leaving the lib's own #[cfg(test)] unit-test binary
        // without an rpath.  Nothing here reaches the staticlib that radosgw
        // links, both because staticlib is not one of the target kinds
        // rustc-link-arg applies to and because RGW_SAL_TEST_ENV_DIR is only
        // ever set for a cargo test invocation.
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
        if let Ok(extra) = std::env::var("RGW_SAL_TEST_ENV_RPATH") {
            for path in extra.split(':').filter(|p| !p.is_empty()) {
                println!("cargo:rustc-link-arg=-Wl,-rpath,{path}");
            }
        }
    }
}
