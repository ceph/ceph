/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! Umbrella crate producing a single staticlib that exports the C API of
//! both `lancedb-c` (the LanceDB C FFI bindings) and `lancedb-rgw-store`
//! (the RGW SAL backend).  See Cargo.toml for the rationale.

// Re-export both crates so their `#[no_mangle]` C symbols are compiled
// into the staticlib.
pub use lancedb;
pub use lancedb_rgw_store;
