//! Which buckets carry our extensions, and how one comes to.
//!
//! An nsfs bucket directory with no `user.nsfs.extensions` marker is
//! NooBaa's:  a bucket RGW has not taken over, read as NooBaa wrote it
//! and not written (DECISIONS.md 2026-10-21).  A marked bucket carries
//! the extensions.  Every bucket this gateway creates is marked;  an
//! unmarked one comes from NooBaa's tree, or, for a test, from the
//! `unmarked-bucket` driver hint.
//!
//! Marking is always a declared act:  at creation, or by adopting a named
//! bucket.  Never inferred on access.  Adoption is one-way, so nothing
//! returns a bucket to base.

use s3_tests_rs::admin::{driver_hint_results, nsfs_get_profile, nsfs_set_profile};
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

    assert!(f.get_str("bucket_profile").is_some(),
        "the profile new buckets are marked with is not reported");
}

/* A bucket this gateway made, unmarked through the test hint -- the
 * only way an RGW bucket becomes unmarked.  None where the hint is not
 * reachable.  A test that leaves it unmarked adopts it before it ends:
 * cleanup's DeleteBucket is a client write, which the gate rejects on a
 * bucket not taken over. */
async fn unmarked_bucket() -> Option<String> {
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let r = driver_hint_results("unmarked-bucket", &[("bucket", &bucket)]).await?;
    assert_eq!(r.get("profile").map(String::as_str), Some("base"),
        "the hint did not leave the bucket unmarked");
    Some(bucket)
}

/// Both polarities.  A bucket the gateway created is in the profile the
/// gateway reports as `bucket_profile` (`rgw_nsfs_bucket_profile`,
/// default `shared`), and the endpoint will not take it back to `base`:
/// adoption is one-way.  An unmarked bucket, from the test hint, moves
/// up to `strong`.  Without the upward leg this would pass against a
/// setter that rejected everything.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_profile_moves_up_and_never_back() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let f = features().await.unwrap();

    let created = f.get_str("bucket_profile")
        .expect("the profile a new bucket is marked with is not reported");

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    /* created here, so in the reported profile */
    let Some(first) = nsfs_get_profile(&bucket).await else {
        eprintln!("profile endpoint unavailable;  nothing to check");
        return;
    };
    assert_eq!(first.get("profile").map(String::as_str), Some(created),
        "a bucket this gateway created is not in the profile it reports");

    assert!(nsfs_set_profile(&bucket, "base").await.is_none(),
        "a bucket was returned to base");
    let now = nsfs_get_profile(&bucket).await.expect("profile should answer");
    assert_eq!(now.get("profile").map(String::as_str), Some(created),
        "the rejected move changed the profile anyway");

    let Some(bucket) = unmarked_bucket().await else {
        eprintln!("unmarked-bucket hint unavailable;  the upward leg is not checked");
        return;
    };
    let base = nsfs_get_profile(&bucket).await.expect("profile should answer");
    assert_eq!(base.get("extensions").map(String::as_str), Some("0"),
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
/// absent marker.  Set explicitly, so the test does not depend on the
/// profile the gateway marks new buckets with.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_profile_shared_is_distinct_from_base() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
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

    /* and an unmarked bucket reads as base, so the two are not aliases */
    let Some(unmarked) = unmarked_bucket().await else {
        eprintln!("unmarked-bucket hint unavailable;  the base side is not checked");
        return;
    };
    let base = nsfs_get_profile(&unmarked).await.expect("profile should answer");
    assert_eq!(base.get("profile").map(String::as_str), Some("base"));
    assert_eq!(base.get("extensions").map(String::as_str), Some("0"));

    /* adopted, so cleanup can delete it */
    nsfs_set_profile(&unmarked, "shared").await.expect("adopting the empty bucket");
}

/// Reducing a profile is rejected once the bucket holds content, because
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

    /* empty, so the reduction is allowed:  the control for the rejection
     * below, which would otherwise pass against a setter that rejected
     * everything.  Strong down to shared, since nothing goes to base. */
    if nsfs_set_profile(&bucket, "strong").await.is_none() {
        eprintln!("profile endpoint unavailable;  nothing to check");
        return;
    }
    nsfs_set_profile(&bucket, "shared").await.expect("an empty bucket reduces");
    nsfs_set_profile(&bucket, "strong").await.expect("profile should answer");

    client.put_object()
        .bucket(&bucket).key("obj")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send().await.expect("put on a strong bucket");

    assert!(nsfs_set_profile(&bucket, "shared").await.is_none(),
        "a bucket holding content was reduced");

    let still = nsfs_get_profile(&bucket).await.expect("profile should answer");
    assert_eq!(still.get("profile").map(String::as_str), Some("strong"),
        "the rejected reduction changed the profile anyway");
}

/// An unmarked bucket is read and not written:  it has not been taken
/// over, so a client's PUT is rejected with InvalidBucketState and a
/// listing is served (DECISIONS.md 2026-10-21).
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_an_unmarked_bucket_takes_no_writes() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let Some(bucket) = unmarked_bucket().await else {
        eprintln!("unmarked-bucket hint unavailable;  nothing to check");
        return;
    };
    let client = get_client();

    let put = client.put_object()
        .bucket(&bucket).key("obj")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send().await;
    s3_tests_rs::assert_s3_err!(put, 409, "InvalidBucketState");

    let listed = client.list_objects_v2()
        .bucket(&bucket).send().await.expect("list on an unmarked bucket");
    assert_eq!(listed.contents().len(), 0, "the rejected PUT left an object");

    /* adopted, so cleanup can delete it */
    nsfs_set_profile(&bucket, "shared").await.expect("adopting the empty bucket");
}

/// Asking for the profile a bucket is already in changes nothing, and
/// reports nothing converted.
///
/// The no-op leg of the resume path:  the same call finishes a stopped
/// rewrite, so it has to be harmless on a bucket with nothing left to
/// do.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_reissuing_the_profile_is_a_no_op() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    client.put_object()
        .bucket(&bucket).key("obj")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send().await.expect("put on a bucket this gateway made");

    if nsfs_set_profile(&bucket, "strong").await.is_none() {
        eprintln!("profile endpoint unavailable;  nothing to check");
        return;
    }

    let again = nsfs_set_profile(&bucket, "strong").await
        .expect("asking for the profile it is already in");
    assert_eq!(again.get("had_profile").map(String::as_str), Some("strong"));
    assert_eq!(again.get("converting").map(String::as_str), Some("false"));
    assert_eq!(again.get("converted.objects").map(String::as_str), Some("0"),
        "a settled bucket was walked again");
}

