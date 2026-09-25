//! Which buckets carry our extensions, and how one comes to.
//!
//! An nsfs bucket directory with no `user.nsfs.extensions` marker is read
//! as NooBaa wrote it, and nothing is added to it.  A marked bucket
//! carries the extensions -- the shadow subtree, positional layout, S3
//! ACLs.  The unmarked case is the default because a NooBaa tree has no
//! marker by construction, and because a tree we have not extended can
//! still be handed back.
//!
//! Marking is always a declared act:  at creation, or by adopting a named
//! bucket.  Never inferred on access, which would convert a tree an
//! operator was deliberately keeping reversible.

use s3_tests_rs::admin::{driver_hint_results, nsfs_adopt};
use s3_tests_rs::client::get_client;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};

/// `None` from either accessor means unknown, so skip rather than assert.
async fn nsfs_only() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs"),
        None => false,
    }
}

#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_extensions_are_reported() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let f = features().await.unwrap();

    let v = f.get_str("extensions")
        .expect("nsfs implements an extension set and should report which");
    let v: u32 = v.parse().expect("the extension set is not an integer");
    assert!(v > 0, "0 is the absence of a marker, not a version");

    assert!(f.get_bool("extensions_default").is_some(),
        "whether new buckets are marked is not reported");
}

/// Both polarities.  A bucket the gateway created is already marked, so
/// adopting it is a no-op;  the hint takes the marker away, and only then
/// does adoption have anything to do.  Without the unmarked leg this test
/// would pass against an adopt that did nothing at all.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_adopt_marks_an_unmarked_bucket() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let f = features().await.unwrap();
    if f.get_bool("extensions_default") != Some(true) {
        eprintln!("reversible mode;  adoption is refused by design");
        return;
    }
    let version = f.get_str("extensions").unwrap().to_string();

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    /* created here, so already marked:  adoption is a no-op and says so */
    let Some(first) = nsfs_adopt(&bucket).await else {
        eprintln!("adopt endpoint unavailable;  nothing to check");
        return;
    };
    assert_eq!(first.get("had").map(String::as_str), Some(version.as_str()),
        "a bucket this gateway created should already be marked");

    /* take the marker away, which is the only way to reach the base
     * profile from a suite -- everything a gateway creates is marked */
    let unmarked = driver_hint_results(
        "unmark-bucket", &[("bucket", &bucket)]).await
        .expect("unmark-bucket hint unavailable");
    assert_eq!(unmarked.get("unmarked").map(String::as_str), Some("true"),
        "the hint did not report the marker removed");

    let second = nsfs_adopt(&bucket).await
        .expect("adopt answered once and should answer again");
    assert_eq!(second.get("had").map(String::as_str), Some("0"),
        "the bucket was unmarked and adoption should have seen that");
    assert_eq!(second.get("extensions").map(String::as_str),
        Some(version.as_str()),
        "adoption did not mark the bucket at this gateway's version");

    /* and it is marked again, so the next adoption is a no-op */
    let third = nsfs_adopt(&bucket).await.expect("adopt should answer");
    assert_eq!(third.get("had").map(String::as_str), Some(version.as_str()),
        "adoption did not persist");
}

/// An unmarked bucket still serves S3.  The extensions are what it lacks,
/// not the object interface -- if this broke, the base profile would be
/// useless and the whole polarity would be wrong.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_unmarked_bucket_still_serves_s3() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    if driver_hint_results("unmark-bucket", &[("bucket", &bucket)]).await
        .is_none() {
        eprintln!("unmark-bucket hint unavailable;  nothing to check");
        return;
    }

    client.put_object()
        .bucket(&bucket).key("obj")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send().await.expect("put on an unmarked bucket");

    let got = client.get_object()
        .bucket(&bucket).key("obj")
        .send().await.expect("get on an unmarked bucket");
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"payload");

    let listed = client.list_objects_v2()
        .bucket(&bucket).send().await.expect("list on an unmarked bucket");
    assert_eq!(listed.contents().len(), 1);
}
