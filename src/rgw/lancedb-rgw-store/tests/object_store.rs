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
//! libtest owns main() -- so that happens in C++ behind the `rgw_test_env_*` API declared
//! below and implemented in src/test/rgw/rgw_sal_test_env.cc.  The ceph build
//! links the two into bin/ceph_test_rgw_lancedb_object_store;  see
//! src/test/rgw/cargo_test_binary.cmake.
//!
//! Configuration comes from the environment, since libtest owns argv:
//!
//! ```text
//! CEPH_CONF=build/ceph.conf ./bin/ceph_test_rgw_lancedb_object_store
//! ```
//!
//! Each test gets its own bucket, which is what the arrow-rs conformance suite
//! assumes and what keeps libtest's thread-parallel execution safe.

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

// The C test-environment API, provided by libceph_rgw_sal_test_env.so.
extern "C" {
    fn rgw_test_env_init() -> c_int;
    fn rgw_test_env_driver() -> *mut CRgwDriver;
    fn rgw_test_env_dpp() -> *const CRgwDoutPrefix;
    fn rgw_test_env_backend() -> *const c_char;
    fn rgw_test_env_create_bucket(name: *const c_char, tenant: *const c_char) -> c_int;
    fn rgw_test_env_create_vector_bucket(name: *const c_char, tenant: *const c_char) -> c_int;
    fn rgw_test_env_remove_bucket(name: *const c_char, tenant: *const c_char) -> c_int;
}

/// Process-wide SAL driver handles, initialized once and shared by every test.
struct SalEnv {
    driver: *mut CRgwDriver,
    dpp: *const CRgwDoutPrefix,
    #[allow(dead_code)]
    backend: String,
}

// Safety: the driver and DoutPrefixProvider are shared across RGW request
// threads in production, and rgw_test_env_init() hands the same pair to every
// caller.  This matches the reasoning behind the RGWObjectStore Send/Sync impls
// in store.rs.
unsafe impl Send for SalEnv {}
unsafe impl Sync for SalEnv {}

/// Bring the SAL driver up once, then hand the same handles to every test.
///
/// libtest has no global setup hook, so the first test to run pays for
/// initialization while the rest wait on the OnceLock.
fn sal_env() -> &'static SalEnv {
    static ENV: OnceLock<SalEnv> = OnceLock::new();
    ENV.get_or_init(|| {
        let ret = unsafe { rgw_test_env_init() };
        assert_eq!(
            ret, 0,
            "rgw_test_env_init() failed ({ret}); is a cluster running and is \
             $CEPH_CONF pointing at it?"
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

/// A bucket owned by a single test and removed when the test finishes.
///
/// A bucket can be an ordinary S3 bucket or a vector bucket (a regular bucket in
/// a separate metadata namespace that holds its LanceDB data inside itself). The
/// `dual_mode_test!` macro runs each test against both so the FFI/wrapper regular
/// and vector-bucket paths are exercised identically.
struct TestBucket {
    name: CString,
    store: RGWObjectStore,
}

impl TestBucket {
    fn new(tag: &str, use_vector_bucket: bool) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let env = sal_env();

        // bucket names allow only lowercase alphanumerics and dashes, so the
        // test tag (which may contain other characters) is sanitized
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

        let ret = unsafe {
            if use_vector_bucket {
                rgw_test_env_create_vector_bucket(name.as_ptr(), std::ptr::null())
            } else {
                rgw_test_env_create_bucket(name.as_ptr(), std::ptr::null())
            }
        };
        assert_eq!(
            ret,
            0,
            "failed to create {} bucket {}: {ret}",
            if use_vector_bucket { "vector" } else { "regular" },
            name.to_string_lossy()
        );

        // Safety: the driver outlives every test -- it is torn down from the
        // atexit() handler registered by rgw_test_env_init().
        let store = unsafe {
            RGWObjectStore::new(
                env.driver,
                env.dpp,
                name.to_str().unwrap(),
                "", // tenant
                "", // prefix
                use_vector_bucket,
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
        // Best effort: a bucket left behind by a panicking test is instead
        // removed by the atexit() handler.
        unsafe { rgw_test_env_remove_bucket(self.name.as_ptr(), std::ptr::null()) };
    }
}

/// Define a test that runs against both a regular bucket and a vector bucket.
///
/// Expands to a module named after the test containing two `#[tokio::test]`s,
/// `regular` and `vector`, each running `$body` with `$b` bound to a freshly
/// created `TestBucket` of that type. Test IDs become e.g.
/// `aros_put_get_delete_list::regular` and `aros_put_get_delete_list::vector`.
macro_rules! dual_mode_test {
    ($name:ident, |$b:ident| $body:block) => {
        mod $name {
            use super::*;

            async fn run($b: TestBucket) $body

            #[tokio::test]
            async fn regular() {
                run(TestBucket::new(concat!(stringify!($name), "-reg"), false)).await;
            }

            #[tokio::test]
            async fn vector() {
                run(TestBucket::new(concat!(stringify!($name), "-vec"), true)).await;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// arrow-rs object-store conformance suite
//
// The formal conformance tests for a custom ObjectStore implementation:
// https://github.com/apache/arrow-rs-object-store/blob/main/src/integration.rs
//
// ---------------------------------------------------------------------------

dual_mode_test!(aros_put_get_delete_list, |b| {
    integration::put_get_delete_list(b.store()).await;
});

dual_mode_test!(aros_get_nonexistent_object, |b| {
    let _ = integration::get_nonexistent_object(b.store(), None).await;
});

dual_mode_test!(aros_list_uses_directories_correctly, |b| {
    integration::list_uses_directories_correctly(b.store()).await;
});

dual_mode_test!(aros_list_with_delimiter, |b| {
    integration::list_with_delimiter(b.store()).await;
});

dual_mode_test!(aros_rename_and_copy, |b| {
    integration::rename_and_copy(b.store()).await;
});

dual_mode_test!(aros_copy_if_not_exists, |b| {
    integration::copy_if_not_exists(b.store()).await;
});

dual_mode_test!(aros_copy_rename_nonexistent_object, |b| {
    integration::copy_rename_nonexistent_object(b.store()).await;
});

dual_mode_test!(aros_get_opts, |b| {
    integration::get_opts(b.store()).await;
});

dual_mode_test!(aros_put_opts, |b| {
    integration::put_opts(b.store(), true).await;
});

dual_mode_test!(aros_stream_get, |b| {
    integration::stream_get(b.store()).await;
});

// ----------------------------------------
// Coverage beyond the conformance suite
// ----------------------------------------

dual_mode_test!(put_get_binary, |b| {
    let key = Path::from("binary");
    let data: Vec<u8> = (0..=255u8).collect();

    b.store()
        .put(&key, PutPayload::from(Bytes::from(data.clone())))
        .await
        .unwrap();

    let got = b.store().get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), data.as_slice());
});

dual_mode_test!(delete_non_existent, |b| {
    b.store().delete(&Path::from("already-gone")).await.unwrap();
});

dual_mode_test!(delete_then_put, |b| {
    let key = Path::from("del-reput");

    b.store().put(&key, "original".into()).await.unwrap();
    b.store().delete(&key).await.unwrap();
    b.store().put(&key, "recreated".into()).await.unwrap();

    let got = b.store().get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"recreated");
});

dual_mode_test!(list_pagination, |b| {
    let prefix = Path::from("paginate");
    for i in 0..15 {
        b.store()
            .put(&prefix.clone().join(format!("obj-{i:02}")), "d".into())
            .await
            .unwrap();
    }

    let listed = b.store().list(Some(&prefix)).collect::<Vec<_>>().await;
    assert_eq!(listed.into_iter().filter(|r| r.is_ok()).count(), 15);
});

dual_mode_test!(multipart_basic, |b| {
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
});

dual_mode_test!(multipart_abort, |b| {
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
});
