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

/// Upgrading a base bucket rewrites what is in it, and says how much.
///
/// The whole thing through the two interfaces an operator has:  S3 to
/// put the objects there, and the profile endpoint to move the bucket.
/// A base bucket's objects are written in NooBaa's format by the
/// driver, so this needs no tree laid down by hand -- the suite makes
/// the before state with ordinary PUTs.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_upgrade_reports_what_it_converted() {
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

    for k in ["flat.bin", "deep/nested.bin", "deep/other.bin"] {
        client.put_object()
            .bucket(&bucket).key(k)
            .content_type("text/plain")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
            .send().await.expect("put on a base bucket");
    }

    let before = nsfs_get_profile(&bucket).await.expect("profile read");
    assert_eq!(before.get("profile").map(String::as_str), Some("base"));
    assert_eq!(before.get("converting").map(String::as_str), Some("false"),
        "a base bucket is not converting;  it is simply in their format");

    let up = nsfs_set_profile(&bucket, "strong").await
        .expect("upgrading a base bucket");
    assert_eq!(up.get("had_profile").map(String::as_str), Some("base"));
    assert_eq!(up.get("profile").map(String::as_str), Some("strong"));
    assert_eq!(up.get("converting").map(String::as_str), Some("false"),
        "the upgrade did not report itself finished");
    assert_eq!(up.get("converted.objects").map(String::as_str), Some("3"),
        "the three objects were not counted");

    let after = nsfs_get_profile(&bucket).await.expect("profile read");
    assert_eq!(after.get("profile").map(String::as_str), Some("strong"));
    assert_eq!(after.get("converting").map(String::as_str), Some("false"));

    /* and the objects survived the rewrite, content type included */
    for k in ["flat.bin", "deep/nested.bin", "deep/other.bin"] {
        let got = client.get_object()
            .bucket(&bucket).key(k)
            .send().await.unwrap_or_else(|e| panic!("get {k} after upgrade: {e}"));
        assert_eq!(got.content_type(), Some("text/plain"),
            "{k} lost its content type in the rewrite");
        let body = got.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"payload", "{k}");
    }

    let listed = client.list_objects_v2()
        .bucket(&bucket).send().await.expect("list after upgrade");
    assert_eq!(listed.contents().len(), 3);
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

    if nsfs_set_profile(&bucket, "base").await.is_none() {
        eprintln!("profile endpoint unavailable;  nothing to check");
        return;
    }
    client.put_object()
        .bucket(&bucket).key("obj")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send().await.expect("put on a base bucket");

    let first = nsfs_set_profile(&bucket, "strong").await.expect("upgrade");
    assert_eq!(first.get("converted.objects").map(String::as_str), Some("1"));

    let again = nsfs_set_profile(&bucket, "strong").await
        .expect("asking for the profile it is already in");
    assert_eq!(again.get("had_profile").map(String::as_str), Some("strong"));
    assert_eq!(again.get("converting").map(String::as_str), Some("false"));
    assert_eq!(again.get("converted.objects").map(String::as_str), Some("0"),
        "a settled bucket was walked again");
}

/// Conversion must not lose an object's owner.
///
/// While a bucket is unmarked, an object with no recorded ACL answers
/// its owner from the inode -- `NooBaaXattrStrategy::object_owner()`
/// returns the file's uid, because NooBaa's format has nowhere to put
/// an ACL and their own gateway stores none.  The moment the bucket is
/// marked that reader is gone and the recorded ACL is the only source,
/// so conversion has to write down the answer it would have given.
///
/// The test measures both sides rather than naming a uid:  it reads the
/// owner before conversion and again after, and requires them to be
/// equal.  That makes it independent of which account runs the gateway.
///
/// **This is a control that can fail.**  A base bucket genuinely stores
/// no ACL -- `disk_name(RGW_ATTR_ACL)` returns the empty string on that
/// mapping -- so before the capture exists the object comes out of
/// conversion with no owner, and the two reads differ.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_conversion_keeps_an_objects_inode_owner() {
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

    /* Reduce while empty:  a profile cannot be reduced on a bucket that
     * holds content, and base is the only way to reach the state where
     * an object is written without an ACL. */
    if nsfs_set_profile(&bucket, "base").await.is_none() {
        eprintln!("profile endpoint unavailable;  nothing to check");
        return;
    }

    let key = "owner-survives-conversion";
    client.put_object()
        .bucket(&bucket)
        .key(key)
        .body(b"x".to_vec().into())
        .send()
        .await
        .expect("a base bucket should accept a write");

    let before = client.get_object_acl()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("an unmarked bucket answers the owner from the inode");
    let before_id = before.owner()
        .and_then(|o| o.id())
        .expect("the uid reader should have answered with an owner")
        .to_string();
    assert!(!before_id.is_empty(), "an empty owner is not an answer");

    nsfs_set_profile(&bucket, "shared").await
        .expect("profile should answer");

    let after = client.get_object_acl()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("a converted object should still answer its ACL");
    let after_id = after.owner()
        .and_then(|o| o.id())
        .map(|s| s.to_string())
        .unwrap_or_default();

    assert_eq!(after_id, before_id,
        "conversion lost the owner the inode was answering:  before {before_id:?}, after {after_id:?}");

    /* What it writes is RGW's default policy, not a bare owner.  An
     * owner with no grants is a shape RGW never produces, and it would
     * be more restrictive than the absence it replaced -- with no ACL
     * at all, authorization falls through to the bucket. */
    assert!(!after.grants().is_empty(),
        "the captured ACL grants nobody anything, which is more restrictive \
         than the no-ACL state it replaced");
}
