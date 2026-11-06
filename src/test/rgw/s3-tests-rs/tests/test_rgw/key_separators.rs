//! On a filesystem backend '/' separates path components, so an empty
//! component is not a name:  a leading slash and a doubled one name the
//! same object as the key without them.  NooBaa maps a key onto a path
//! with `path.normalize` and behaves the same, and nsfs keeps that
//! (`docs/RGW_NSFS_VADE_MECUM.md` §14, key semantics:  `/y`, `a//b`).
//!
//! S3 itself does not normalize:  on AWS and on RADOS `/y` and `y` are
//! two objects, so those backends are gated.

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};

async fn body(client: &aws_sdk_s3::Client, b: &str, key: &str) -> String {
    let got = client.get_object().bucket(b).key(key).send().await
        .unwrap_or_else(|e| panic!("GET {key:?} found nothing: {e:?}"));
    String::from_utf8(got.body.collect().await.expect("body").into_bytes().to_vec())
        .expect("utf-8 body")
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: /y is not y")]
#[tokio::test]
async fn test_empty_key_components_are_not_names() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;

    /* a leading slash */
    client.put_object().bucket(&b).key("y")
        .body(ByteStream::from_static(b"plain")).send().await
        .expect("y should write");
    assert_eq!(body(&client, &b, "/y").await, "plain", "/y is not y");

    client.put_object().bucket(&b).key("/z")
        .body(ByteStream::from_static(b"lead")).send().await
        .expect("/z should write");
    assert_eq!(body(&client, &b, "z").await, "lead", "z is not /z");

    /* a doubled slash */
    client.put_object().bucket(&b).key("a//b")
        .body(ByteStream::from_static(b"doubled")).send().await
        .expect("a//b should write");
    assert_eq!(body(&client, &b, "a/b").await, "doubled", "a/b is not a//b");

    /* and the listing names each object once, without the empty
     * components */
    let mut listed: Vec<String> = client.list_objects_v2().bucket(&b).send().await
        .expect("listing").contents().iter()
        .filter_map(|o| o.key().map(str::to_string)).collect();
    listed.sort();
    assert_eq!(listed, vec!["a/b".to_string(), "y".to_string(), "z".to_string()]);
}
