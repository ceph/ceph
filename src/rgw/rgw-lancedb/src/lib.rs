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

// Both crates must be built against the same lance-io: the provider created
// by lancedb-rgw-store is handed to lancedb-c as an opaque pointer, and is
// read there as lancedb-c's own struct. With two lance-io versions in the
// build the trait objects differ, and the first use of the provider
// crashes. This does not compile unless the two `inner` fields have the
// same type.
const _: fn(lancedb_rgw_store::LanceDBObjectStoreProvider) -> lancedb::LanceDBObjectStoreProvider =
    |provider| lancedb::LanceDBObjectStoreProvider { inner: provider.inner };
