/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! Build script for lancedb-rgw-store

fn main() {
    // The Rust functions are provided by rgw_sal_wrapper.cc which
    // is compiled into libradosgw. Since this crate produces a shared library
    // (.so) that's loaded into the same process as radosgw, the symbols will
    // be available and resolved at runtime.
    //
    // Hence we do NOT statically link here to avoid circular dependency:
    // - lancedb-rgw-store depends on rgw_sal_wrapper symbols
    // - rgw_common depends on liblancedb_rgw_store.so
    println!("cargo:rerun-if-env-changed=CEPH_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=CEPH_SRC_DIR");
    println!("cargo:rerun-if-changed=build.rs");
}
