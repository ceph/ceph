/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! Integration tests for RGWObjectStore.
//!
//! These tests exercise the ObjectStore trait through the real FFI boundary.
//! They are called from a C++ harness that provides an initialized SAL driver.

use crate::RGWObjectStore;
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};

struct TestRunner<'a> {
    store: &'a RGWObjectStore,
    failures: i32,
    passed: i32,
}

impl<'a> TestRunner<'a> {
    fn new(store: &'a RGWObjectStore) -> Self {
        Self {
            store,
            failures: 0,
            passed: 0,
        }
    }

    fn run<F>(&mut self, name: &str, f: F)
    where
        F: FnOnce(&RGWObjectStore) -> Result<(), String>,
    {
        eprint!("  rust test {name} ... ");
        match f(self.store) {
            Ok(()) => {
                eprintln!("PASSED");
                self.passed += 1;
            }
            Err(e) => {
                eprintln!("FAILED: {e}");
                self.failures += 1;
            }
        }
    }

    /// Run an upstream `object_store::integration` test function.
    ///
    /// These functions are async and assert/panic on failure, so we
    /// run them inside block_on() and catch_unwind() to integrate
    /// with our pass/fail reporting.
    fn run_async<F, Fut>(&mut self, name: &str, f: F)
    where
        F: FnOnce(&'a dyn ObjectStore) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        eprint!("  rust test {name} ... ");
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(f(self.store))));
        match result {
            Ok(()) => {
                eprintln!("PASSED");
                self.passed += 1;
            }
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "panic".to_string()
                };
                eprintln!("FAILED: {msg}");
                self.failures += 1;
            }
        }
    }

    fn summary(self) -> i32 {
        eprintln!(
            "  rust tests: {} passed, {} failed",
            self.passed, self.failures
        );
        self.failures
    }
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

pub fn run_all(store: &RGWObjectStore) -> i32 {
    let mut t = TestRunner::new(store);

    // Tests that add unique coverage beyond the aros_ conformance suite
    t.run("put_get_binary", test_put_get_binary);
    t.run("delete_non_existent", test_delete_non_existent);
    t.run("delete_then_put", test_delete_then_put);
    t.run("list_pagination", test_list_pagination);
    t.run("multipart_basic", test_multipart_basic);
    t.run("multipart_abort", test_multipart_abort);

    // cleanup test prefix
    let _ = block_on(cleanup(store, "rust-test/"));

    // aros_ = arrow-rs object-store conformance tests
    // (https://github.com/apache/arrow-rs-object-store/blob/main/src/integration.rs)
    //
    // These are the formal conformance tests for custom ObjectStore
    // implementations. They cover the same operations as our tests above
    // but are more thorough — unicode/emoji paths, conditional reads
    // (if-match, if-modified-since), attributes, concurrent updates, etc.
    //
    // NOTE: unlike upstream test suites (S3, GCS, Azure) which use a
    // separate bucket/container per test, all our tests share a single
    // RGW bucket. Some tests assume a clean store but don't call
    // delete_fixtures(), so we manually clean up between tests where
    // needed. Another way is to expose rgw_create_bucket /
    // rgw_delete_bucket via FFI and give each test its own bucket.
    use object_store::integration;
    t.run_async("aros_put_get_delete_list", integration::put_get_delete_list);
    t.run_async("aros_get_nonexistent", |s| async {
        let _ = integration::get_nonexistent_object(s, None).await;
    });
    t.run_async(
        "aros_list_uses_directories",
        integration::list_uses_directories_correctly,
    );
    t.run_async("aros_list_with_delimiter", integration::list_with_delimiter);
    t.run_async("aros_rename_and_copy", integration::rename_and_copy);
    t.run_async("aros_copy_if_not_exists", integration::copy_if_not_exists);
    t.run_async("aros_get_opts", integration::get_opts);
    // put_opts runs last — its concurrent race test may leave objects behind
    t.run_async("aros_put_opts", |s| integration::put_opts(s, true));

    // TODO: stream_get uses try_join_all for concurrent multipart uploads,
    // which requires a Tokio runtime. Our block_on uses futures::executor
    // (no Tokio reactor), so the test panics with "no reactor running".
    // t.run_async("aros_stream_get", integration::stream_get);

    t.run_async(
        "aros_copy_rename_nonexistent",
        integration::copy_rename_nonexistent_object,
    );

    t.summary()
}

fn put(store: &RGWObjectStore, key: &str, data: &[u8]) -> Result<(), String> {
    block_on(store.put(
        &Path::from(key),
        PutPayload::from(Bytes::copy_from_slice(data)),
    ))
    .map(|_| ())
    .map_err(|e| format!("put({key}): {e}"))
}

fn get(store: &RGWObjectStore, key: &str) -> Result<Bytes, String> {
    block_on(async {
        let result = store
            .get(&Path::from(key))
            .await
            .map_err(|e| format!("get({key}): {e}"))?;
        result
            .bytes()
            .await
            .map_err(|e| format!("get({key}) bytes: {e}"))
    })
}

fn del(store: &RGWObjectStore, key: &str) -> Result<(), String> {
    block_on(store.delete(&Path::from(key))).map_err(|e| format!("delete({key}): {e}"))
}

async fn cleanup(store: &RGWObjectStore, prefix: &str) -> Result<(), String> {
    let list = store
        .list(Some(&Path::from(prefix)))
        .collect::<Vec<_>>()
        .await;
    for meta in list.into_iter().flatten() {
        let _ = store.delete(&meta.location).await;
    }
    Ok(())
}

// --------------- Tests ---------------

fn test_put_get_binary(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/binary";
    let data: Vec<u8> = (0..=255u8).collect();
    put(store, key, &data)?;

    let got = get(store, key)?;
    if got.as_ref() != data.as_slice() {
        return Err(format!(
            "binary data mismatch: expected {} bytes, got {}",
            data.len(),
            got.len()
        ));
    }
    del(store, key)?;
    Ok(())
}

fn test_delete_non_existent(store: &RGWObjectStore) -> Result<(), String> {
    del(store, "rust-test/already-gone")
}

fn test_delete_then_put(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/del-reput";
    put(store, key, b"original")?;
    del(store, key)?;

    put(store, key, b"recreated")?;
    let got = get(store, key)?;
    if got.as_ref() != b"recreated" {
        del(store, key)?;
        return Err(format!(
            "expected 'recreated', got '{}'",
            String::from_utf8_lossy(&got)
        ));
    }
    del(store, key)
}

fn test_list_pagination(store: &RGWObjectStore) -> Result<(), String> {
    for i in 0..15 {
        put(store, &format!("rust-test/paginate/obj-{i:02}"), b"d")?;
    }

    let items = block_on(async {
        store
            .list(Some(&Path::from("rust-test/paginate/")))
            .collect::<Vec<_>>()
            .await
    });
    let count = items.iter().filter(|r| r.is_ok()).count();
    if count < 15 {
        return Err(format!("expected >= 15 items, got {count}"));
    }

    for i in 0..15 {
        del(store, &format!("rust-test/paginate/obj-{i:02}"))?;
    }
    Ok(())
}

fn test_multipart_basic(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/multipart";
    let result = block_on(async {
        let mut upload = store
            .put_multipart(&Path::from(key))
            .await
            .map_err(|e| format!("init: {e}"))?;

        // S3 requires parts >= 5MB except the last
        let part1 = vec![0xAAu8; 5 * 1024 * 1024];
        let part2 = vec![0xBBu8; 1024];

        upload
            .put_part(PutPayload::from(Bytes::from(part1.clone())))
            .await
            .map_err(|e| format!("put_part 1: {e}"))?;
        upload
            .put_part(PutPayload::from(Bytes::from(part2.clone())))
            .await
            .map_err(|e| format!("put_part 2: {e}"))?;

        upload
            .complete()
            .await
            .map_err(|e| format!("complete: {e}"))?;

        // verify via head
        let meta = store
            .head(&Path::from(key))
            .await
            .map_err(|e| format!("head after complete: {e}"))?;
        let expected_size = (part1.len() + part2.len()) as u64;
        if meta.size != expected_size {
            return Err(format!(
                "size mismatch: expected {expected_size}, got {}",
                meta.size
            ));
        }
        Ok::<(), String>(())
    });

    let _ = del(store, key);
    result
}

fn test_multipart_abort(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/multipart-abort";
    block_on(async {
        let mut upload = store
            .put_multipart(&Path::from(key))
            .await
            .map_err(|e| format!("init: {e}"))?;

        let part = vec![0xCCu8; 1024];
        upload
            .put_part(PutPayload::from(Bytes::from(part)))
            .await
            .map_err(|e| format!("put_part: {e}"))?;

        upload.abort().await.map_err(|e| format!("abort: {e}"))?;

        // object should not exist
        match store.head(&Path::from(key)).await {
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(format!("expected NotFound, got: {e}")),
            Ok(_) => Err("object exists after abort".into()),
        }
    })
}
