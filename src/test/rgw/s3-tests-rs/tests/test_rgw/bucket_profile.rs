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

use s3_tests_rs::admin::{nsfs_get_profile, nsfs_set_profile};
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

/// Both polarities.  A bucket the gateway created is already `strong`,
/// so setting it there is a no-op;  moving it to `base` is the only way
/// a suite reaches that profile, and only then does setting `strong`
/// have anything to do.  Without the base leg this would pass against a
/// setter that did nothing at all.
///
/// Reducing a profile requires the bucket to hold no content, so the
/// bucket is left empty here.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_profile_moves_between_base_and_strong() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let f = features().await.unwrap();
    if f.get_bool("extensions_default") != Some(true) {
        eprintln!("reversible mode;  extending a bucket is refused by design");
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    /* created here, so already strong */
    let Some(first) = nsfs_get_profile(&bucket).await else {
        eprintln!("profile endpoint unavailable;  nothing to check");
        return;
    };
    assert_eq!(first.get("profile").map(String::as_str), Some("strong"),
        "a bucket this gateway created should be in the strong profile");

    let down = nsfs_set_profile(&bucket, "base").await
        .expect("the profile endpoint answered once and should answer again");
    assert_eq!(down.get("had_profile").map(String::as_str), Some("strong"));

    let now = nsfs_get_profile(&bucket).await.expect("profile should answer");
    assert_eq!(now.get("profile").map(String::as_str), Some("base"),
        "the bucket was not reduced to base");
    assert_eq!(now.get("extensions").map(String::as_str), Some("0"),
        "base carries no extensions");

    let up = nsfs_set_profile(&bucket, "strong").await
        .expect("profile should answer");
    assert_eq!(up.get("had_profile").map(String::as_str), Some("base"),
        "the setter did not see the bucket at base");

    let back = nsfs_get_profile(&bucket).await.expect("profile should answer");
    assert_eq!(back.get("profile").map(String::as_str), Some("strong"),
        "the move to strong did not persist");
}

/// Shared sits between the two and is a profile in its own right, not an
/// absent marker.  A suite cannot otherwise reach it:  the gateway
/// creates every bucket strong.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_profile_shared_is_distinct_from_base() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let f = features().await.unwrap();
    if f.get_bool("extensions_default") != Some(true) {
        eprintln!("reversible mode;  extending a bucket is refused by design");
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    if nsfs_set_profile(&bucket, "shared").await.is_none() {
        eprintln!("profile endpoint unavailable;  nothing to check");
        return;
    }

    let got = nsfs_get_profile(&bucket).await.expect("profile should answer");
    assert_eq!(got.get("profile").map(String::as_str), Some("shared"),
        "shared did not read back as shared");

    /* the distinction the on-disk value has to carry:  shared is not the
     * absence of a marker, so its extension set is not zero */
    assert_ne!(got.get("extensions").map(String::as_str), Some("0"),
        "shared reported the same extension set as base");

    /* and base still reads as base, so the two are not aliases */
    nsfs_set_profile(&bucket, "base").await.expect("profile should answer");
    let base = nsfs_get_profile(&bucket).await.expect("profile should answer");
    assert_eq!(base.get("profile").map(String::as_str), Some("base"));
    assert_eq!(base.get("extensions").map(String::as_str), Some("0"));
}

/// Reducing a profile is refused once the bucket holds content, because
/// the structures the dropped extensions allowed stay in the tree.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_profile_cannot_be_reduced_with_content() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    /* empty, so the reduction is allowed:  the control for the refusal
     * below, which would otherwise pass against a setter that refused
     * everything */
    if nsfs_set_profile(&bucket, "base").await.is_none() {
        eprintln!("profile endpoint unavailable;  nothing to check");
        return;
    }
    nsfs_set_profile(&bucket, "strong").await.expect("profile should answer");

    client.put_object()
        .bucket(&bucket).key("obj")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send().await.expect("put on a strong bucket");

    assert!(nsfs_set_profile(&bucket, "base").await.is_none(),
        "a bucket holding content was reduced to base");

    let still = nsfs_get_profile(&bucket).await.expect("profile should answer");
    assert_eq!(still.get("profile").map(String::as_str), Some("strong"),
        "the refused reduction changed the profile anyway");
}

/// A base bucket still serves S3.  The extensions are what it lacks, not
/// the object interface -- if this broke, the base profile would be
/// useless and the whole polarity would be wrong.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_base_bucket_still_serves_s3() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    if nsfs_set_profile(&bucket, "base").await.is_none() {
        eprintln!("profile endpoint unavailable;  nothing to check");
        return;
    }

    client.put_object()
        .bucket(&bucket).key("obj")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send().await.expect("put on a base bucket");

    let got = client.get_object()
        .bucket(&bucket).key("obj")
        .send().await.expect("get on a base bucket");
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"payload");

    let listed = client.list_objects_v2()
        .bucket(&bucket).send().await.expect("list on a base bucket");
    assert_eq!(listed.contents().len(), 1);
}
