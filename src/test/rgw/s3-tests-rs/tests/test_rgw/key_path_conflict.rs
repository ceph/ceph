//! On a filesystem backend a name in a bucket's tree holds either an
//! object (a file) or a directory, never both, so the keys `p` and `p/`,
//! or `p` and any key under `p/`, cannot exist together.  The one
//! written first holds the name until it is deleted, and a write of the
//! other returns 409 `KeyPathConflict`, in both directions
//! (DECISIONS.md 2026-10-24).  A 500 would tell the client to retry a
//! write that cannot succeed.
//!
//! A key without the trailing `/` that names a directory has no object,
//! so a read of it is 404, as NooBaa's `read_object_md()` returns.
//!
//! S3 keys are opaque strings, so on AWS and on RADOS both forms coexist;
//! `test_both_forms_coexist` holds them to that, and the rest are gated
//! off there.  librgw's `SETUP_HIER1` (`librgw_file_nfsns.cc`) asserts the
//! same coexistence and fails on nsfs for this reason.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, ObjectIdentifier};
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::{
    check_configure_versioning_retry, get_new_bucket, multipart_upload, TestGuard,
};

type S3Client = aws_sdk_s3::Client;

async fn put(client: &S3Client, b: &str, key: &str, data: &'static [u8]) {
    client.put_object().bucket(b).key(key)
        .body(ByteStream::from_static(data)).send().await
        .unwrap_or_else(|e| panic!("PUT {key:?} should succeed: {e:?}"));
}

async fn body(client: &S3Client, b: &str, key: &str) -> Vec<u8> {
    let got = client.get_object().bucket(b).key(key).send().await
        .unwrap_or_else(|e| panic!("GET {key:?} found nothing: {e:?}"));
    got.body.collect().await.expect("body").into_bytes().to_vec()
}

async fn keys(client: &S3Client, b: &str) -> Vec<String> {
    let mut listed: Vec<String> = client.list_objects_v2().bucket(b).send().await
        .expect("listing").contents().iter()
        .filter_map(|o| o.key().map(str::to_string)).collect();
    listed.sort();
    listed
}

/* PUT, CopyObject and CompleteMultipartUpload to `key` are each refused
 * with 409 KeyPathConflict */
async fn every_write_conflicts(client: &S3Client, b: &str, key: &str, src: &str) {
    let r = client.put_object().bucket(b).key(key)
        .body(ByteStream::from_static(b"put")).send().await;
    s3_tests_rs::assert_s3_err!(r, 409, "KeyPathConflict");

    let r = client.copy_object().bucket(b).key(key)
        .copy_source(format!("{b}/{src}")).send().await;
    s3_tests_rs::assert_s3_err!(r, 409, "KeyPathConflict");

    let mpu = multipart_upload(client, b, key, 5 * 1024 * 1024, None, None, None, None).await;
    let r = client.complete_multipart_upload().bucket(b).key(key)
        .upload_id(&mpu.upload_id)
        .multipart_upload(CompletedMultipartUpload::builder()
            .set_parts(Some(mpu.parts)).build())
        .send().await;
    s3_tests_rs::assert_s3_err!(r, 409, "KeyPathConflict");
    client.abort_multipart_upload().bucket(b).key(key)
        .upload_id(&mpu.upload_id).send().await
        .expect("the refused upload should abort");
}

#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: a name holds a file or a directory")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: untested")]
#[tokio::test]
async fn test_both_forms_coexist() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;

    put(&client, &b, "p", b"file").await;
    put(&client, &b, "p/", b"").await;
    put(&client, &b, "p/c", b"child").await;

    assert_eq!(body(&client, &b, "p").await, b"file");
    assert_eq!(body(&client, &b, "p/c").await, b"child");
    assert_eq!(keys(&client, &b).await, vec!["p", "p/", "p/c"]);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: untested")]
#[tokio::test]
async fn test_a_file_holds_its_name_against_keys_under_it() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;
    put(&client, &b, "src", b"source").await;
    put(&client, &b, "f", b"file").await;

    let r = client.put_object().bucket(&b).key("f/").send().await;
    s3_tests_rs::assert_s3_err!(r, 409, "KeyPathConflict");
    every_write_conflicts(&client, &b, "f/c", "src").await;

    /* the file is untouched, and nothing under it was made */
    assert_eq!(body(&client, &b, "f").await, b"file");
    assert_eq!(keys(&client, &b).await, vec!["f", "src"]);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: untested")]
#[tokio::test]
async fn test_a_directory_holds_its_name_against_the_key() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;
    put(&client, &b, "src", b"source").await;
    put(&client, &b, "d/", b"").await;
    put(&client, &b, "d/y", b"child").await;

    every_write_conflicts(&client, &b, "d", "src").await;

    /* `d` names no object:  a read of it, or a copy from it, is 404 */
    let r = client.head_object().bucket(&b).key("d").send().await;
    assert_eq!(s3_tests_rs::expect_s3_err!(r).status, 404, "HEAD d");
    let r = client.get_object().bucket(&b).key("d").send().await;
    s3_tests_rs::assert_s3_err!(r, 404, "NoSuchKey");
    let r = client.copy_object().bucket(&b).key("w")
        .copy_source(format!("{b}/d")).send().await;
    s3_tests_rs::assert_s3_err!(r, 404, "NoSuchKey");

    /* and a delete of it succeeds without touching the keys under it */
    client.delete_object().bucket(&b).key("d").send().await
        .expect("DELETE d should succeed");

    assert_eq!(body(&client, &b, "d/y").await, b"child");
    assert_eq!(keys(&client, &b).await, vec!["d/", "d/y", "src"]);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: untested")]
#[tokio::test]
async fn test_either_form_once_the_other_is_deleted() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;

    /* the directory, then the file */
    put(&client, &b, "d/y", b"child").await;
    client.delete_object().bucket(&b).key("d/y").send().await
        .expect("DELETE d/y");
    put(&client, &b, "d", b"now a file").await;
    assert_eq!(body(&client, &b, "d").await, b"now a file");

    /* the file, then the directory */
    put(&client, &b, "f", b"file").await;
    client.delete_object().bucket(&b).key("f").send().await
        .expect("DELETE f");
    put(&client, &b, "f/", b"").await;
    put(&client, &b, "f/c", b"child").await;

    assert_eq!(keys(&client, &b).await, vec!["d", "f/", "f/c"]);
}

/* In a versioned bucket the history of `d/y` is kept under `d`, so `d`
 * holds its name until that history is gone:  a delete marker leaves
 * it, and only deleting every version of every key under `d/` frees
 * the name for the file. */
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: untested")]
#[tokio::test]
async fn test_versioned_directory_holds_its_name_while_history_remains() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &b, "Enabled", "Enabled").await;

    put(&client, &b, "d/y", b"child").await;
    client.delete_object().bucket(&b).key("d/y").send().await
        .expect("DELETE d/y leaves a delete marker");
    let r = client.put_object().bucket(&b).key("d")
        .body(ByteStream::from_static(b"file")).send().await;
    s3_tests_rs::assert_s3_err!(r, 409, "KeyPathConflict");

    let listed = client.list_object_versions().bucket(&b).prefix("d/").send().await
        .expect("list versions");
    let ids: Vec<ObjectIdentifier> = listed.versions().iter()
        .map(|v| (v.key(), v.version_id()))
        .chain(listed.delete_markers().iter().map(|m| (m.key(), m.version_id())))
        .map(|(k, v)| ObjectIdentifier::builder()
            .key(k.expect("key")).set_version_id(v.map(str::to_string))
            .build().expect("identifier"))
        .collect();
    assert_eq!(ids.len(), 2, "one version and one delete marker");
    for id in &ids {
        client.delete_object().bucket(&b).key(id.key())
            .set_version_id(id.version_id().map(str::to_string)).send().await
            .expect("delete a version");
    }

    put(&client, &b, "d", b"file").await;
    assert_eq!(body(&client, &b, "d").await, b"file");
}

/* Every key a version listing holds under `prefix`, versions and delete
 * markers both */
async fn versioned_keys(client: &S3Client, b: &str, prefix: &str) -> Vec<String> {
    let listed = client.list_object_versions().bucket(b).prefix(prefix).send().await
        .expect("list versions");
    let mut keys: Vec<String> = listed.versions().iter().filter_map(|v| v.key())
        .chain(listed.delete_markers().iter().filter_map(|m| m.key()))
        .map(str::to_string).collect();
    keys.sort();
    keys
}

/* In a versioned bucket a directory holds its name against the key as
 * it does unversioned.  A delete of the key writes no delete marker:
 * `d` names no object and none can be written while `d/` holds the
 * name, so the delete returns 204 and changes nothing, as NooBaa's
 * delete_object() does (DECISIONS.md 2026-10-24).  A multi-object
 * delete of it does the same. */
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: untested")]
#[tokio::test]
async fn test_versioned_directory_holds_its_name_and_its_key_has_no_marker() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &b, "Enabled", "Enabled").await;
    put(&client, &b, "src", b"source").await;
    put(&client, &b, "d/", b"").await;
    put(&client, &b, "d/y", b"child").await;

    every_write_conflicts(&client, &b, "d", "src").await;

    let r = client.delete_object().bucket(&b).key("d").send().await
        .expect("DELETE d should succeed");
    assert!(!r.delete_marker().unwrap_or(false), "DELETE d wrote a delete marker");
    assert_eq!(r.version_id(), None, "DELETE d returned a version id");

    let delete = aws_sdk_s3::types::Delete::builder()
        .objects(ObjectIdentifier::builder().key("d").build().expect("identifier"))
        .build().expect("delete");
    let r = client.delete_objects().bucket(&b).delete(delete).send().await
        .expect("DeleteObjects d should succeed");
    assert!(r.errors().is_empty(), "DeleteObjects d: {:?}", r.errors());
    for d in r.deleted() {
        assert!(!d.delete_marker().unwrap_or(false),
                "DeleteObjects d wrote a delete marker");
    }

    assert_eq!(versioned_keys(&client, &b, "d").await, vec!["d/", "d/y"]);
    assert_eq!(body(&client, &b, "d/y").await, b"child");
}

/* A directory object is versioned as any object is:  each PUT of `d/`
 * keeps the one before as a version, a delete leaves a marker, and
 * the versions read back by id. */
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: untested")]
#[tokio::test]
async fn test_versioned_directory_object_keeps_its_versions() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &b, "Enabled", "Enabled").await;

    let v1 = client.put_object().bucket(&b).key("d/")
        .body(ByteStream::from_static(b"one")).send().await
        .expect("PUT d/ once").version_id().expect("a version id").to_string();
    put(&client, &b, "d/y", b"child").await;
    let v2 = client.put_object().bucket(&b).key("d/")
        .body(ByteStream::from_static(b"two")).send().await
        .expect("PUT d/ again").version_id().expect("a version id").to_string();
    assert_ne!(v1, v2);
    assert_eq!(body(&client, &b, "d/").await, b"two");

    let r = client.delete_object().bucket(&b).key("d/").send().await
        .expect("DELETE d/ should succeed");
    assert!(r.delete_marker().unwrap_or(false), "DELETE d/ wrote no delete marker");
    let r = client.get_object().bucket(&b).key("d/").send().await;
    s3_tests_rs::assert_s3_err!(r, 404, "NoSuchKey");

    for (v, want) in [(&v1, &b"one"[..]), (&v2, &b"two"[..])] {
        let got = client.get_object().bucket(&b).key("d/").version_id(v).send().await
            .unwrap_or_else(|e| panic!("GET d/ version {v}: {e:?}"));
        assert_eq!(got.body.collect().await.expect("body").into_bytes().as_ref(), want);
    }
    let listed = client.list_object_versions().bucket(&b).prefix("d/").send().await
        .expect("list versions");
    let dir_versions = listed.versions().iter().filter(|v| v.key() == Some("d/")).count();
    let dir_markers = listed.delete_markers().iter().filter(|m| m.key() == Some("d/")).count();
    assert_eq!((dir_versions, dir_markers), (2, 1), "d/:  two versions and a marker");
    assert_eq!(body(&client, &b, "d/y").await, b"child");
}

/* A directory object is one object.  Copying `d/` copies that one empty
 * object and nothing under it, as on every S3:  `d/a` is a key of its
 * own, and the copy has no business with it. */
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: untested")]
#[tokio::test]
async fn test_copying_a_directory_object_copies_only_it() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;
    put(&client, &b, "d/", b"").await;
    put(&client, &b, "d/a", b"child").await;

    client.copy_object().bucket(&b).key("e/")
        .copy_source(format!("{b}/d/")).send().await
        .expect("copying a directory object should succeed");

    assert_eq!(body(&client, &b, "e/").await, b"");
    assert_eq!(keys(&client, &b).await, vec!["d/", "d/a", "e/"],
        "the copy reached keys under the directory object");
}

/* And copying one onto a directory object that already exists replaces
 * that one object:  the keys under the destination are other objects,
 * and are left as they were. */
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: untested")]
#[tokio::test]
async fn test_copying_onto_a_directory_object_keeps_the_keys_under_it() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;
    put(&client, &b, "d/", b"").await;
    put(&client, &b, "e/", b"").await;
    put(&client, &b, "e/x", b"kept").await;

    client.copy_object().bucket(&b).key("e/")
        .copy_source(format!("{b}/d/")).send().await
        .expect("copying onto a directory object should succeed");

    assert_eq!(body(&client, &b, "e/x").await, b"kept",
        "the copy removed a key under its destination");
    assert_eq!(keys(&client, &b).await, vec!["d/", "e/", "e/x"]);
}
