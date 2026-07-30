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
}
