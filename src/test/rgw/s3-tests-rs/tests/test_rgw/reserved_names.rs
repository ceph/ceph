//! A key that names nsfs's schema is no object, for every operation.
//!
//! `.folder` is where a directory object's content is stored -- the key
//! `x/` lives in `x/.folder` -- and `.versions` holds version entries.
//! Those names are schema, so no S3 operation reaches them by name
//! (DECISIONS.md 2026-10-12).  A lookup of such a key is NoSuchKey, a
//! delete deletes nothing, and a create is InvalidObjectName.  A key of
//! slashes alone is not one of them:  it is the root object
//! (`root_object.rs`).
//!
//! The reserved file is real:  PUT `x/` first and `x/.folder` exists on
//! disk as its storage, so a rejection here is the rule and not an absent
//! file.  `x/` itself is the control, and must survive everything.

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[cfg_attr(feature = "fails_on_noobaa", ignore = "fails on noobaa")]
#[tokio::test]
async fn test_reserved_names_are_not_objects() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;

    /* the control:  a directory object, whose storage is x/.folder */
    client.put_object().bucket(&b).key("x/")
        .body(ByteStream::from_static(b"directory object"))
        .send().await.expect("a directory object should write");

    /* creates are rejected */
    for k in [".folder", "x/.folder", ".versions/v"] {
        s3_tests_rs::assert_s3_err!(
            client.put_object().bucket(&b).key(k)
                .body(ByteStream::from_static(b"no")).send().await,
            400, "InvalidObjectName");
    }

    /* `.folder` is not an alias for the root object, which is stored in
     * the root's `.folder` */
    client.put_object().bucket(&b).key("/")
        .body(ByteStream::from_static(b"root object"))
        .send().await.expect("the root object should write");
    s3_tests_rs::assert_s3_err!(
        client.get_object().bucket(&b).key(".folder").send().await,
        404, "NoSuchKey");

    /* lookups find nothing, though x/.folder is on disk */
    s3_tests_rs::assert_s3_err!(
        client.get_object().bucket(&b).key("x/.folder").send().await,
        404, "NoSuchKey");
    let head = s3_tests_rs::expect_s3_err!(
        client.head_object().bucket(&b).key("x/.folder").send().await);
    assert_eq!(head.status, 404, "HEAD of x/.folder found something");

    /* copy:  from a reserved name finds nothing, to one is rejected */
    s3_tests_rs::assert_s3_err!(
        client.copy_object().bucket(&b).key("copied")
            .copy_source(format!("{b}/x/.folder")).send().await,
        404, "NoSuchKey");
    s3_tests_rs::assert_s3_err!(
        client.copy_object().bucket(&b).key(".folder")
            .copy_source(format!("{b}/x/")).send().await,
        400, "InvalidObjectName");

    /* a multipart upload to a reserved name cannot succeed:  rejected at
     * initiation, or else at completion, which is where the key becomes
     * a path.  Both cases assert, so neither can pass by being skipped. */
    let up = client.create_multipart_upload().bucket(&b).key(".folder")
        .send().await;
    if up.is_err() {
        s3_tests_rs::assert_s3_err!(up, 400, "InvalidObjectName");
        eprintln!("multipart to .folder:  rejected at initiation");
    }
    if let Ok(up) = up {
        eprintln!("multipart to .folder:  initiated;  completion must be rejected");
        let id = up.upload_id().unwrap_or_default().to_string();
        let part = client.upload_part().bucket(&b).key(".folder")
            .upload_id(&id).part_number(1)
            .body(ByteStream::from_static(b"part")).send().await
            .expect("a part should upload");
        let parts = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .parts(aws_sdk_s3::types::CompletedPart::builder()
                .part_number(1).e_tag(part.e_tag().unwrap_or_default()).build())
            .build();
        s3_tests_rs::assert_s3_err!(
            client.complete_multipart_upload().bucket(&b).key(".folder")
                .upload_id(&id).multipart_upload(parts).send().await,
            400, "InvalidObjectName");
        let _ = client.abort_multipart_upload().bucket(&b).key(".folder")
            .upload_id(&id).send().await;
    }

    /* a delete deletes nothing, and says so the way S3 does */
    client.delete_object().bucket(&b).key("x/.folder").send().await
        .expect("a delete of a missing key succeeds");

    /* the control survived all of it */
    let got = client.get_object().bucket(&b).key("x/").send().await
        .expect("the directory object is gone");
    let body = got.body.collect().await.expect("body").into_bytes();
    assert_eq!(&body[..], b"directory object");
    let listed: Vec<String> = client.list_objects_v2().bucket(&b).send().await
        .expect("listing").contents().iter()
        .filter_map(|o| o.key().map(str::to_string)).collect();
    assert_eq!(listed, vec!["x/".to_string()],
        "the listing shows something other than the directory object");
    client.delete_object().bucket(&b).key("x/").send().await
        .expect("the directory object should delete");
}
