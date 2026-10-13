//! nsfs's key limit, which is its listing cache's.
//!
//! A listing is served from an LMDB cache, and LMDB holds at most
//! `mdb_env_get_maxkeysize()` bytes of key -- 511 in a stock build,
//! against S3's 1024.  nsfs refuses a longer key with KeyTooLongError
//! rather than store an object no listing could show (`20651c8e79b`).
//!
//! This pins the refusal while the limit stands.  When LMDB is built with
//! a larger key size it fails, and so marks
//! `test_s3::object_ops::test_object_key_at_the_s3_maximum_is_stored_and_listed`
//! for ungating (PUNCHLIST.md, OPEN:  "one over-long name in a foreign
//! tree empties the whole listing").

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_noobaa", ignore = "fails on noobaa")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: such a PUT returns 500, not KeyTooLongError")]
#[tokio::test]
async fn test_a_key_over_the_listing_cache_is_refused() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let b = get_new_bucket(Some(&client)).await;

    /* well over the stock ceiling, each component under NAME_MAX */
    let key = format!("long/{}/{}/{}", "y".repeat(200), "y".repeat(200), "y".repeat(200));
    s3_tests_rs::assert_s3_err!(
        client.put_object().bucket(&b).key(&key)
            .body(ByteStream::from_static(b"over the ceiling"))
            .send().await,
        400, "KeyTooLongError");

    let listed = client.list_objects_v2().bucket(&b).send().await
        .expect("listing").key_count().unwrap_or(0);
    assert_eq!(listed, 0, "the refused key was stored anyway");
}
