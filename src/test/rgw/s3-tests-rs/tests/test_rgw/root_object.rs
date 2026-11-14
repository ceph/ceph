//! The root object:  a key of slashes alone, on nsfs.
//!
//! NooBaa stores a directory object `x/` at directory `x`, and applies
//! the same rule to the bucket's own directory, so the key `/` names an
//! object there.  It is served, every key made only of slashes reaches
//! it, and no listing shows it.  nsfs carries that forward for a client
//! that relies on it (DECISIONS.md 2026-10-13).  It also does not keep
//! its bucket alive, because NooBaa decides a bucket is empty by listing it.
//!
//! NooBaa's behaviour, so it passes there too.  Elsewhere `/` is an
//! ordinary key, and listed.  The control throughout is `x/`, an
//! ordinary directory object, which lists.  That `.folder` does not
//! reach the root object is `reserved_names.rs`'s, because NooBaa lets
//! it.

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_the_root_object() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;

    client.put_object().bucket(&b).key("x/")
        .body(ByteStream::from_static(b"directory object"))
        .send().await.expect("a directory object should write");

    client.put_object().bucket(&b).key("/")
        .content_type("text/x-root")
        .metadata("probe", "root")
        .body(ByteStream::from_static(b"root bytes"))
        .send().await.expect("the root object should write");

    /* every key of slashes alone reaches it, with its attributes */
    for k in ["/", "//", "///"] {
        let got = client.get_object().bucket(&b).key(k).send().await
            .unwrap_or_else(|e| panic!("GET {k:?} found nothing: {e:?}"));
        assert_eq!(got.content_type(), Some("text/x-root"), "content type of {k:?}");
        assert_eq!(got.metadata().and_then(|m| m.get("probe")).map(String::as_str),
                   Some("root"), "metadata of {k:?}");
        let body = got.body.collect().await.expect("body").into_bytes();
        assert_eq!(&body[..], b"root bytes", "bytes of {k:?}");
    }

    /* never listed;  the control is */
    let listed: Vec<String> = client.list_objects_v2().bucket(&b).send().await
        .expect("listing").contents().iter()
        .filter_map(|o| o.key().map(str::to_string)).collect();
    assert_eq!(listed, vec!["x/".to_string()],
        "the listing shows something other than the directory object");

    /* `//` overwrites the same object */
    client.put_object().bucket(&b).key("//")
        .body(ByteStream::from_static(b"again"))
        .send().await.expect("`//` should write the root object");
    let body = client.get_object().bucket(&b).key("/").send().await
        .expect("the root object is gone").body.collect().await
        .expect("body").into_bytes();
    assert_eq!(&body[..], b"again");

    /* a delete removes it, and leaves the control */
    client.delete_object().bucket(&b).key("/").send().await
        .expect("the root object should delete");
    s3_tests_rs::assert_s3_err!(
        client.get_object().bucket(&b).key("/").send().await,
        404, "NoSuchKey");
    client.get_object().bucket(&b).key("x/").send().await
        .expect("deleting the root object took the directory object");

    /* it does not keep the bucket alive */
    client.delete_object().bucket(&b).key("x/").send().await
        .expect("the directory object should delete");
    client.put_object().bucket(&b).key("/")
        .body(ByteStream::from_static(b"root bytes"))
        .send().await.expect("the root object should write");
    client.delete_bucket().bucket(&b).send().await
        .expect("the root object kept its bucket alive");
}
