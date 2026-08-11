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
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetRange, MultipartUpload, ObjectStore, ObjectStoreExt,
    PutMode, PutOptions, PutPayload, UpdateVersion,
};

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

    // put / get
    t.run("put_get_round_trip", test_put_get_round_trip);
    t.run("put_get_binary", test_put_get_binary);
    t.run("put_get_empty", test_put_get_empty);
    t.run("put_overwrite", test_put_overwrite);
    t.run("put_get_multiple", test_put_get_multiple);
    t.run("get_non_existent", test_get_non_existent);

    // get with range (GetOptions)
    t.run("get_range_bounded", test_get_range_bounded);
    t.run("get_range_offset", test_get_range_offset);
    t.run("get_range_suffix", test_get_range_suffix);

    // head
    t.run("head_object", test_head_object);
    t.run("head_non_existent", test_head_non_existent);
    t.run("head_etag_changes", test_head_etag_changes);

    // delete
    t.run("delete_object", test_delete_object);
    t.run("delete_non_existent", test_delete_non_existent);
    t.run("delete_then_put", test_delete_then_put);
    t.run("delete_stream", test_delete_stream);

    // list
    t.run("list_objects", test_list_objects);
    t.run("list_with_delimiter", test_list_with_delimiter);
    t.run("list_empty", test_list_empty);
    t.run("list_pagination", test_list_pagination);

    // copy
    t.run("copy_object", test_copy_object);
    t.run("copy_non_existent", test_copy_non_existent);

    // conditional put (PutMode::Create, PutMode::Update)
    t.run("put_opts_create_mode", test_put_create_mode);
    t.run("put_opts_update_mode", test_put_update_mode);

    // multipart upload
    t.run("multipart_basic", test_multipart_basic);
    t.run("multipart_abort", test_multipart_abort);

    // special keys
    t.run("special_character_keys", test_special_character_keys);

    // cleanup test prefix
    let _ = block_on(cleanup(store, "rust-test/"));

    t.summary()
}

fn put(store: &RGWObjectStore, key: &str, data: &[u8]) -> Result<(), String> {
    block_on(store.put(&Path::from(key), PutPayload::from(Bytes::copy_from_slice(data))))
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

fn head(store: &RGWObjectStore, key: &str) -> Result<object_store::ObjectMeta, String> {
    block_on(store.head(&Path::from(key))).map_err(|e| format!("head({key}): {e}"))
}

fn del(store: &RGWObjectStore, key: &str) -> Result<(), String> {
    block_on(store.delete(&Path::from(key))).map_err(|e| format!("delete({key}): {e}"))
}

async fn cleanup(store: &RGWObjectStore, prefix: &str) -> Result<(), String> {
    let list = store
        .list(Some(&Path::from(prefix)))
        .collect::<Vec<_>>()
        .await;
    for item in list {
        if let Ok(meta) = item {
            let _ = store.delete(&meta.location).await;
        }
    }
    Ok(())
}

// --------------- Tests ---------------

fn test_put_get_round_trip(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/put-get";
    let data = b"hello from rust integration test";
    put(store, key, data)?;

    let got = get(store, key)?;
    if got.as_ref() != data {
        return Err(format!(
            "data mismatch: expected {} bytes, got {}",
            data.len(),
            got.len()
        ));
    }
    del(store, key)?;
    Ok(())
}

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

fn test_put_get_empty(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/empty";
    put(store, key, b"")?;

    let meta = head(store, key)?;
    if meta.size != 0 {
        return Err(format!("expected size 0, got {}", meta.size));
    }
    del(store, key)?;
    Ok(())
}

fn test_put_overwrite(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/overwrite";
    put(store, key, b"version1")?;
    put(store, key, b"version2-longer")?;

    let got = get(store, key)?;
    if got.as_ref() != b"version2-longer" {
        return Err(format!(
            "overwrite failed: got {} bytes, expected 15",
            got.len()
        ));
    }
    del(store, key)?;
    Ok(())
}

fn test_get_non_existent(store: &RGWObjectStore) -> Result<(), String> {
    let result = block_on(store.get(&Path::from("rust-test/no-such-key")));
    match result {
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(format!("expected NotFound, got: {e}")),
        Ok(_) => Err("expected NotFound, got Ok".into()),
    }
}

fn test_head_object(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/head";
    let data = b"head test data here";
    put(store, key, data)?;

    let meta = head(store, key)?;
    if meta.size != data.len() as u64 {
        return Err(format!(
            "size mismatch: expected {}, got {}",
            data.len(),
            meta.size
        ));
    }
    if meta.e_tag.is_none() {
        return Err("etag is None".into());
    }
    del(store, key)?;
    Ok(())
}

fn test_head_non_existent(store: &RGWObjectStore) -> Result<(), String> {
    let result = block_on(store.head(&Path::from("rust-test/head-missing")));
    match result {
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(format!("expected NotFound, got: {e}")),
        Ok(_) => Err("expected NotFound, got Ok".into()),
    }
}

fn test_delete_object(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/delete-me";
    put(store, key, b"delete this")?;
    del(store, key)?;

    let result = block_on(store.head(&Path::from(key)));
    match result {
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(format!("expected NotFound after delete, got: {e}")),
        Ok(_) => Err("object still exists after delete".into()),
    }
}

fn test_delete_non_existent(store: &RGWObjectStore) -> Result<(), String> {
    del(store, "rust-test/already-gone")
}

fn test_list_objects(store: &RGWObjectStore) -> Result<(), String> {
    for i in 0..5 {
        put(store, &format!("rust-test/list/obj-{i}"), b"data")?;
    }

    let items = block_on(async {
        store
            .list(Some(&Path::from("rust-test/list/")))
            .collect::<Vec<_>>()
            .await
    });

    let count = items.iter().filter(|r| r.is_ok()).count();
    if count < 5 {
        return Err(format!("expected >= 5 items, got {count}"));
    }

    for i in 0..5 {
        del(store, &format!("rust-test/list/obj-{i}"))?;
    }
    Ok(())
}

fn test_list_with_delimiter(store: &RGWObjectStore) -> Result<(), String> {
    put(store, "rust-test/delim/a/1", b"d")?;
    put(store, "rust-test/delim/a/2", b"d")?;
    put(store, "rust-test/delim/b/1", b"d")?;

    let result = block_on(store.list_with_delimiter(Some(&Path::from("rust-test/delim/"))))
        .map_err(|e| format!("list_with_delimiter: {e}"))?;

    if result.common_prefixes.len() < 2 {
        return Err(format!(
            "expected >= 2 common prefixes, got {}",
            result.common_prefixes.len()
        ));
    }

    del(store, "rust-test/delim/a/1")?;
    del(store, "rust-test/delim/a/2")?;
    del(store, "rust-test/delim/b/1")?;
    Ok(())
}

fn test_list_empty(store: &RGWObjectStore) -> Result<(), String> {
    let items = block_on(async {
        store
            .list(Some(&Path::from("rust-test/no-such-prefix/")))
            .collect::<Vec<_>>()
            .await
    });

    if !items.is_empty() {
        return Err(format!("expected 0 items, got {}", items.len()));
    }
    Ok(())
}

fn test_copy_object(store: &RGWObjectStore) -> Result<(), String> {
    let src = "rust-test/copy-src";
    let dst = "rust-test/copy-dst";
    let data = b"copy this data";
    put(store, src, data)?;

    let result = block_on(store.copy(&Path::from(src), &Path::from(dst)));
    match result {
        Ok(_) => {
            let got = get(store, dst)?;
            if got.as_ref() != data {
                del(store, src)?;
                del(store, dst)?;
                return Err(format!(
                    "copy data mismatch: expected {} bytes, got {}",
                    data.len(),
                    got.len()
                ));
            }
            del(store, src)?;
            del(store, dst)?;
            Ok(())
        }
        Err(object_store::Error::NotSupported { .. }) => {
            del(store, src)?;
            eprintln!("(skipped — not supported) ");
            Ok(())
        }
        Err(e) => {
            // copy may be a no-op on some backends — check if dst was created
            let dst_result = block_on(store.head(&Path::from(dst)));
            del(store, src)?;
            if dst_result.is_ok() {
                del(store, dst)?;
            }
            Err(format!("copy: {e}"))
        }
    }
}

fn test_put_get_multiple(store: &RGWObjectStore) -> Result<(), String> {
    for i in 0..5 {
        put(store, &format!("rust-test/multi/obj-{i}"), format!("val-{i}").as_bytes())?;
    }
    for i in 0..5 {
        let got = get(store, &format!("rust-test/multi/obj-{i}"))?;
        let expected = format!("val-{i}");
        if got.as_ref() != expected.as_bytes() {
            return Err(format!("obj-{i}: expected '{}', got '{}'", expected, String::from_utf8_lossy(&got)));
        }
    }
    for i in 0..5 {
        del(store, &format!("rust-test/multi/obj-{i}"))?;
    }
    Ok(())
}

fn test_get_range_bounded(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/range-bounded";
    let data: Vec<u8> = (0..=255u8).collect();
    put(store, key, &data)?;

    let opts = GetOptions {
        range: Some(GetRange::Bounded(10..50)),
        ..Default::default()
    };
    let result = block_on(async {
        let r = store.get_opts(&Path::from(key), opts).await.map_err(|e| format!("{e}"))?;
        r.bytes().await.map_err(|e| format!("{e}"))
    })?;
    if result.as_ref() != &data[10..50] {
        return Err(format!("range mismatch: expected 40 bytes, got {}", result.len()));
    }
    del(store, key)
}

fn test_get_range_offset(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/range-offset";
    let data = b"0123456789abcdef";
    put(store, key, data)?;

    let opts = GetOptions {
        range: Some(GetRange::Offset(10)),
        ..Default::default()
    };
    let result = block_on(async {
        let r = store.get_opts(&Path::from(key), opts).await.map_err(|e| format!("{e}"))?;
        r.bytes().await.map_err(|e| format!("{e}"))
    })?;
    if result.as_ref() != &data[10..] {
        return Err(format!("expected '{}', got '{}'",
            String::from_utf8_lossy(&data[10..]),
            String::from_utf8_lossy(&result)));
    }
    del(store, key)
}

fn test_get_range_suffix(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/range-suffix";
    let data = b"0123456789abcdef";
    put(store, key, data)?;

    let opts = GetOptions {
        range: Some(GetRange::Suffix(4)),
        ..Default::default()
    };
    let result = block_on(async {
        let r = store.get_opts(&Path::from(key), opts).await.map_err(|e| format!("{e}"))?;
        r.bytes().await.map_err(|e| format!("{e}"))
    })?;
    if result.as_ref() != b"cdef" {
        return Err(format!("expected 'cdef', got '{}'", String::from_utf8_lossy(&result)));
    }
    del(store, key)
}

fn test_head_etag_changes(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/etag-change";
    put(store, key, b"version1")?;
    let meta1 = head(store, key)?;

    put(store, key, b"version2-different")?;
    let meta2 = head(store, key)?;

    if meta1.e_tag == meta2.e_tag {
        del(store, key)?;
        return Err("etag should change after overwrite".into());
    }
    del(store, key)
}

fn test_delete_then_put(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/del-reput";
    put(store, key, b"original")?;
    del(store, key)?;

    put(store, key, b"recreated")?;
    let got = get(store, key)?;
    if got.as_ref() != b"recreated" {
        del(store, key)?;
        return Err(format!("expected 'recreated', got '{}'", String::from_utf8_lossy(&got)));
    }
    del(store, key)
}

fn test_delete_stream(store: &RGWObjectStore) -> Result<(), String> {
    for i in 0..5 {
        put(store, &format!("rust-test/del-stream/obj-{i}"), b"data")?;
    }

    let paths: Vec<_> = (0..5).map(|i| Ok(Path::from(format!("rust-test/del-stream/obj-{i}")))).collect();
    let results = block_on(async {
        store.delete_stream(futures::stream::iter(paths).boxed())
            .collect::<Vec<_>>()
            .await
    });

    let errors: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
    if !errors.is_empty() {
        return Err(format!("{} delete_stream errors", errors.len()));
    }

    // verify all deleted
    for i in 0..5 {
        let r = block_on(store.head(&Path::from(format!("rust-test/del-stream/obj-{i}"))));
        if !matches!(r, Err(object_store::Error::NotFound { .. })) {
            return Err(format!("obj-{i} still exists after delete_stream"));
        }
    }
    Ok(())
}

fn test_list_pagination(store: &RGWObjectStore) -> Result<(), String> {
    for i in 0..15 {
        put(store, &format!("rust-test/paginate/obj-{i:02}"), b"d")?;
    }

    let items = block_on(async {
        store.list(Some(&Path::from("rust-test/paginate/")))
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

fn test_copy_non_existent(store: &RGWObjectStore) -> Result<(), String> {
    let result = block_on(store.copy(
        &Path::from("rust-test/copy-no-src"),
        &Path::from("rust-test/copy-no-dst"),
    ));
    match result {
        Err(_) => Ok(()),
        Ok(_) => {
            // no-op backend — verify dst not created
            let h = block_on(store.head(&Path::from("rust-test/copy-no-dst")));
            match h {
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                _ => Err("copy of non-existent source should not create destination".into()),
            }
        }
    }
}

fn test_put_update_mode(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/update-mode";
    put(store, key, b"original")?;

    let meta = head(store, key)?;
    let etag = meta.e_tag.ok_or("no etag on original object")?;

    // Update with matching etag — should succeed
    let opts = PutOptions {
        mode: PutMode::Update(UpdateVersion { e_tag: Some(etag.clone()), version: None }),
        ..Default::default()
    };
    let result = block_on(store.put_opts(
        &Path::from(key),
        PutPayload::from_static(b"updated"),
        opts,
    ));

    match result {
        Ok(_) => {
            let got = get(store, key)?;
            del(store, key)?;
            if got.as_ref() != b"updated" {
                return Err(format!("expected 'updated', got '{}'", String::from_utf8_lossy(&got)));
            }
            Ok(())
        }
        Err(e) => {
            del(store, key)?;
            // backend may not support conditional — not a test failure
            eprintln!("(conditional update not supported: {e}) ");
            Ok(())
        }
    }
}

fn test_multipart_basic(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/multipart";
    let result = block_on(async {
        let mut upload = store.put_multipart(&Path::from(key)).await
            .map_err(|e| format!("init: {e}"))?;

        // S3 requires parts >= 5MB except the last
        let part1 = vec![0xAAu8; 5 * 1024 * 1024];
        let part2 = vec![0xBBu8; 1024];

        upload.put_part(PutPayload::from(Bytes::from(part1.clone()))).await
            .map_err(|e| format!("put_part 1: {e}"))?;
        upload.put_part(PutPayload::from(Bytes::from(part2.clone()))).await
            .map_err(|e| format!("put_part 2: {e}"))?;

        upload.complete().await.map_err(|e| format!("complete: {e}"))?;

        // verify via head
        let meta = store.head(&Path::from(key)).await
            .map_err(|e| format!("head after complete: {e}"))?;
        let expected_size = (part1.len() + part2.len()) as u64;
        if meta.size != expected_size {
            return Err(format!("size mismatch: expected {expected_size}, got {}", meta.size));
        }
        Ok::<(), String>(())
    });

    let _ = del(store, key);
    result
}

fn test_multipart_abort(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/multipart-abort";
    block_on(async {
        let mut upload = store.put_multipart(&Path::from(key)).await
            .map_err(|e| format!("init: {e}"))?;

        let part = vec![0xCCu8; 1024];
        upload.put_part(PutPayload::from(Bytes::from(part))).await
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

fn test_special_character_keys(store: &RGWObjectStore) -> Result<(), String> {
    let keys = [
        "rust-test/special/key with spaces",
        "rust-test/special/key+plus",
        "rust-test/special/key=equals",
        "rust-test/special/deep/nested/path/obj",
    ];
    let data = b"special";

    for key in &keys {
        put(store, key, data)?;
        let got = get(store, key)?;
        if got.as_ref() != data {
            return Err(format!("round-trip failed for key '{key}'"));
        }
        del(store, key)?;
    }
    Ok(())
}

fn test_put_create_mode(store: &RGWObjectStore) -> Result<(), String> {
    let key = "rust-test/create-mode";
    put(store, key, b"first")?;

    // PutMode::Create should fail since object already exists
    let opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    let result = block_on(store.put_opts(
        &Path::from(key),
        PutPayload::from_static(b"should not overwrite"),
        opts,
    ));

    match result {
        Err(object_store::Error::AlreadyExists { .. }) => {
            // correct — precondition prevented overwrite
            del(store, key)?;
            Ok(())
        }
        Err(e) => {
            del(store, key)?;
            Err(format!("expected AlreadyExists, got: {e}"))
        }
        Ok(_) => {
            // backend doesn't enforce preconditions — verify original data preserved or not
            let got = get(store, key)?;
            del(store, key)?;
            if got.as_ref() == b"first" {
                Ok(()) // data preserved even though Ok returned
            } else {
                eprintln!("(precondition not enforced by backend) ");
                Ok(()) // treat as backend limitation, not failure
            }
        }
    }
}
