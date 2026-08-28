/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! Integration tests for [`RGWObjectStore`], exercising the ObjectStore trait
//! through the real FFI boundary against a live SAL driver.
//!
//! These are ordinary `cargo test` tests.  What they cannot do themselves is
//! bring up a ceph context and a SAL driver, so that happens in C++ behind the
//! `rgw_test_env_*` API declared below and implemented in
//! src/test/rgw/rgw_sal_test_env.cc.  The ceph build links the two together
//! into bin/ceph_test_rgw_lancedb_object_store; see
//! src/test/rgw/cargo_test_binary.cmake.
//!
//! Configuration comes from the environment rather than argv, which libtest
//! owns:
//!
//! ```text
//! CEPH_CONF=build/ceph.conf ./bin/ceph_test_rgw_lancedb_object_store
//! ```
//!
//! Every test gets its own bucket, which is what the arrow-rs conformance
//! suite assumes and what keeps libtest's thread-parallel execution safe.

use bytes::Bytes;
use futures::StreamExt;
use lancedb_rgw_store::ffi::{CRgwDoutPrefix, CRgwDriver};
use lancedb_rgw_store::RGWObjectStore;
use object_store::integration;
use object_store::path::Path;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

// The C test-environment API, from libceph_rgw_sal_test_env.so.
extern "C" {
    fn rgw_test_env_init() -> c_int;
    fn rgw_test_env_driver() -> *mut CRgwDriver;
    fn rgw_test_env_dpp() -> *const CRgwDoutPrefix;
    fn rgw_test_env_backend() -> *const c_char;
    fn rgw_test_env_create_bucket(name: *const c_char, tenant: *const c_char) -> c_int;
    fn rgw_test_env_remove_bucket(name: *const c_char, tenant: *const c_char) -> c_int;
}

/// Handles to the process-wide SAL driver.
struct SalEnv {
    driver: *mut CRgwDriver,
    dpp: *const CRgwDoutPrefix,
    #[allow(dead_code)]
    backend: String,
}

// Safety: the driver and DoutPrefixProvider are shared across RGW request
// threads in production, and rgw_test_env_init() hands out the same pair to
// every caller.  Same reasoning as the RGWObjectStore impls in store.rs.
unsafe impl Send for SalEnv {}
unsafe impl Sync for SalEnv {}

/// Bring up the SAL driver once, then hand the same handles to every test.
///
/// libtest has no global setup hook, so the first test to run pays for
/// initialization while the rest block on the OnceLock.
fn sal_env() -> &'static SalEnv {
    static ENV: OnceLock<SalEnv> = OnceLock::new();
    ENV.get_or_init(|| {
        let ret = unsafe { rgw_test_env_init() };
        assert_eq!(
            ret, 0,
            "rgw_test_env_init() failed ({ret}); is a cluster running and \
             is $CEPH_CONF pointing at it?"
        );
        let backend = unsafe { rgw_test_env_backend() };
        assert!(!backend.is_null(), "rgw_test_env_backend() returned null");
        SalEnv {
            driver: unsafe { rgw_test_env_driver() },
            dpp: unsafe { rgw_test_env_dpp() },
            backend: unsafe { std::ffi::CStr::from_ptr(backend) }
                .to_string_lossy()
                .into_owned(),
        }
    })
}

/// A bucket owned by a single test, removed when the test ends.
struct TestBucket {
    name: CString,
    store: RGWObjectStore,
}

impl TestBucket {
    fn new(tag: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let env = sal_env();

        // bucket names are restricted to lowercase alphanumerics and dashes,
        // so test names (which use underscores) have to be rewritten
        let tag: String = tag
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        let name = CString::new(format!(
            "lancedb-test-{}-{}-{tag}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ))
        .unwrap();

        let ret = unsafe { rgw_test_env_create_bucket(name.as_ptr(), std::ptr::null()) };
        assert_eq!(
            ret,
            0,
            "failed to create bucket {}: {ret}",
            name.to_string_lossy()
        );

        // Safety: the driver outlives every test -- it is torn down from an
        // atexit() handler registered by rgw_test_env_init().
        let store = unsafe {
            RGWObjectStore::new(
                env.driver,
                env.dpp,
                name.to_str().unwrap(),
                "", // tenant
                "", // prefix
            )
        };
        Self { name, store }
    }

    fn store(&self) -> &RGWObjectStore {
        &self.store
    }
}

impl Drop for TestBucket {
    fn drop(&mut self) {
        // Best effort: a bucket left behind by a panicking test is removed by
        // the atexit() handler instead.
        unsafe { rgw_test_env_remove_bucket(self.name.as_ptr(), std::ptr::null()) };
    }
}

// ---------------------------------------------------------------------------
// arrow-rs object-store conformance suite
//
// The formal conformance tests for a custom ObjectStore implementation:
// https://github.com/apache/arrow-rs-object-store/blob/main/src/integration.rs
//
// Every test function that module exports is called below, except the two
// that take a second trait object RGWObjectStore does not implement:
// `multipart` (needs MultipartStore) and `list_paginated` (needs
// PaginatedListStore).  Implementing those traits would extend coverage
// further.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aros_put_get_delete_list() {
    let b = TestBucket::new("put-get-delete-list");
    integration::put_get_delete_list(b.store()).await;
}

#[tokio::test]
async fn aros_get_nonexistent_object() {
    let b = TestBucket::new("get-nonexistent");
    let _ = integration::get_nonexistent_object(b.store(), None).await;
}

#[tokio::test]
async fn aros_list_uses_directories_correctly() {
    let b = TestBucket::new("list-uses-directories");
    integration::list_uses_directories_correctly(b.store()).await;
}

#[tokio::test]
async fn aros_list_with_delimiter() {
    let b = TestBucket::new("list-with-delimiter");
    integration::list_with_delimiter(b.store()).await;
}

#[tokio::test]
async fn aros_list_with_offset_exclusivity() {
    let b = TestBucket::new("list-with-offset");
    integration::list_with_offset_exclusivity(b.store()).await;
}

#[tokio::test]
async fn aros_rename_and_copy() {
    let b = TestBucket::new("rename-and-copy");
    integration::rename_and_copy(b.store()).await;
}

#[tokio::test]
async fn aros_copy_if_not_exists() {
    let b = TestBucket::new("copy-if-not-exists");
    integration::copy_if_not_exists(b.store()).await;
}

#[tokio::test]
async fn aros_copy_rename_nonexistent_object() {
    let b = TestBucket::new("copy-rename-nonexistent");
    integration::copy_rename_nonexistent_object(b.store()).await;
}

#[tokio::test]
async fn aros_get_opts() {
    let b = TestBucket::new("get-opts");
    integration::get_opts(b.store()).await;
}

#[tokio::test]
async fn aros_put_opts() {
    let b = TestBucket::new("put-opts");
    integration::put_opts(b.store(), true).await;
}

// Attributes (content-type, cache-control, user metadata) are not plumbed
// through: put_opts() drops opts.attributes on the floor and every read path
// reports Attributes::new().  Carrying them would mean extending the C wrapper
// ABI in rgw_sal_wrapper.h, which rgw_put_object() has no parameter for.
//
// The suite tolerates a store that returns Error::NotImplemented for
// attributes, so this test would pass once put_opts() rejects a non-empty
// opts.attributes instead of silently discarding it.
#[tokio::test]
#[ignore = "attributes are not implemented; see comment above"]
async fn aros_put_get_attributes() {
    let b = TestBucket::new("put-get-attributes");
    integration::put_get_attributes(b.store()).await;
}

#[tokio::test]
async fn aros_stream_get() {
    let b = TestBucket::new("stream-get");
    integration::stream_get(b.store()).await;
}

#[tokio::test]
async fn aros_multipart_out_of_order() {
    let b = TestBucket::new("multipart-out-of-order");
    integration::multipart_out_of_order(b.store()).await;
}

#[tokio::test]
async fn aros_multipart_race_condition() {
    let b = TestBucket::new("multipart-race");
    // RGW follows S3 semantics, where the last upload to complete wins
    integration::multipart_race_condition(b.store(), true).await;
}

// ---------------------------------------------------------------------------
// Coverage beyond the conformance suite
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_get_binary() {
    let b = TestBucket::new("put-get-binary");
    let key = Path::from("binary");
    let data: Vec<u8> = (0..=255u8).collect();

    b.store()
        .put(&key, PutPayload::from(Bytes::from(data.clone())))
        .await
        .unwrap();

    let got = b.store().get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), data.as_slice());
}

#[tokio::test]
async fn delete_non_existent() {
    let b = TestBucket::new("delete-non-existent");
    b.store().delete(&Path::from("already-gone")).await.unwrap();
}

#[tokio::test]
async fn delete_then_put() {
    let b = TestBucket::new("delete-then-put");
    let key = Path::from("del-reput");

    b.store().put(&key, "original".into()).await.unwrap();
    b.store().delete(&key).await.unwrap();
    b.store().put(&key, "recreated".into()).await.unwrap();

    let got = b.store().get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"recreated");
}

#[tokio::test]
async fn list_pagination() {
    let b = TestBucket::new("list-pagination");
    let prefix = Path::from("paginate");
    for i in 0..15 {
        b.store()
            .put(&prefix.clone().join(format!("obj-{i:02}")), "d".into())
            .await
            .unwrap();
    }

    let listed = b.store().list(Some(&prefix)).collect::<Vec<_>>().await;
    assert_eq!(listed.into_iter().filter(|r| r.is_ok()).count(), 15);
}

#[tokio::test]
async fn multipart_basic() {
    let b = TestBucket::new("multipart-basic");
    let key = Path::from("multipart");
    let mut upload = b.store().put_multipart(&key).await.unwrap();

    // S3 requires every part except the last to be at least 5MB
    let part1 = vec![0xAAu8; 5 * 1024 * 1024];
    let part2 = vec![0xBBu8; 1024];
    upload
        .put_part(PutPayload::from(Bytes::from(part1.clone())))
        .await
        .unwrap();
    upload
        .put_part(PutPayload::from(Bytes::from(part2.clone())))
        .await
        .unwrap();
    upload.complete().await.unwrap();

    let meta = b.store().head(&key).await.unwrap();
    assert_eq!(meta.size, (part1.len() + part2.len()) as u64);
}

#[tokio::test]
async fn multipart_abort() {
    let b = TestBucket::new("multipart-abort");
    let key = Path::from("multipart-abort");
    let mut upload = b.store().put_multipart(&key).await.unwrap();

    upload
        .put_part(PutPayload::from(Bytes::from(vec![0xCCu8; 1024])))
        .await
        .unwrap();
    upload.abort().await.unwrap();

    match b.store().head(&key).await {
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => panic!("expected NotFound, got: {e}"),
        Ok(_) => panic!("object exists after abort"),
    }
}
