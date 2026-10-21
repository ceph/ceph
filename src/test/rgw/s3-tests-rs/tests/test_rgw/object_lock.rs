//! Object lock where the driver cannot read the lock.
//!
//! `test_s3/object_lock.rs` covers object lock where every read
//! succeeds.  These cover what a delete does when the attribute read
//! under it fails:  the lock lives in the object's attributes, so a
//! read that fails and is taken for "no attributes" is a lock that
//! does not hold.  Nothing in an ordinary setup makes that read fail,
//! so the failure is injected (`inject-attr-read-fail`, keyed by
//! bucket so the lock tests running beside it are not touched).

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ObjectLockLegalHold, ObjectLockLegalHoldStatus};

use s3_tests_rs::admin::driver_hint_results;
use s3_tests_rs::client::get_client;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket_name, TestGuard};
use s3_tests_rs::assert_s3_err;

fn fired(r: &std::collections::HashMap<String, String>) -> u64 {
    r.get("fired").and_then(|s| s.parse().ok())
        .expect("the hint returned no count")
}

/// A version under a legal hold survives a delete whose attribute
/// read fails.
///
/// Three steps.  The control:  with every read succeeding the hold
/// denies the delete, so the bucket, the hold and the request are
/// right.  The injected case:  the same delete with the attribute read
/// failing must fail as well, and the version must still be there --
/// the count shows the injection ran on this request rather than the
/// delete taking some other path.  Then the hold comes off and the
/// delete succeeds, so the object was not left undeletable.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_object_lock_holds_when_its_attributes_cannot_be_read() {
    let _guard = TestGuard::setup();

    let Some(f) = features().await else {
        eprintln!("no features returned;  nothing to check");
        return;
    };
    if f.backend.as_deref() != Some("nsfs") {
        eprintln!("inject-attr-read-fail is nsfs-only;  skipped");
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket()
        .bucket(&bucket)
        .object_lock_enabled_for_bucket(true)
        .send().await
        .expect("could not create a lock-enabled bucket");

    let key = "held";
    let put = client.put_object()
        .bucket(&bucket).key(key)
        .body(ByteStream::from_static(b"abc"))
        .send().await
        .expect("could not write the object");
    let version_id = put.version_id().unwrap_or_default().to_string();
    assert!(!version_id.is_empty(), "a lock-enabled bucket returned no version id");

    let hold = |s| ObjectLockLegalHold::builder().status(s).build();
    client.put_object_legal_hold()
        .bucket(&bucket).key(key)
        .legal_hold(hold(ObjectLockLegalHoldStatus::On))
        .send().await
        .expect("could not place the legal hold");

    /* the control */
    let denied = client.delete_object()
        .bucket(&bucket).key(key).version_id(&version_id)
        .send().await;
    assert_s3_err!(denied, 403, "AccessDenied");

    /* the injected case;  disarmed before anything is asserted, so a
     * failure cannot leave the bucket's reads failing */
    let Some(armed) = driver_hint_results(
        "inject-attr-read-fail",
        &[("enable", "true"), ("bucket", &bucket)]).await else {
        eprintln!("driver hint endpoint unavailable;  nothing to check");
        return;
    };
    let before = fired(&armed);
    let injected = client.delete_object()
        .bucket(&bucket).key(key).version_id(&version_id)
        .send().await;
    let disarmed = driver_hint_results(
        "inject-attr-read-fail",
        &[("enable", "false"), ("bucket", &bucket)]).await
        .expect("the hint returned once and should return again");
    let after = fired(&disarmed);

    assert!(after > before,
        "the attribute read was not injected on the delete ({before} -> \
         {after});  its outcome below shows nothing about a failed read");
    let survived = client.head_object()
        .bucket(&bucket).key(key).version_id(&version_id)
        .send().await;
    assert!(injected.is_err(),
        "the delete succeeded although the lock could not be read");
    assert!(survived.is_ok(),
        "the held version is gone after a delete that could not read its \
         lock: {survived:?}");

    /* the hold off, and the delete goes through */
    client.put_object_legal_hold()
        .bucket(&bucket).key(key)
        .legal_hold(hold(ObjectLockLegalHoldStatus::Off))
        .send().await
        .expect("could not lift the legal hold");
    client.delete_object()
        .bucket(&bucket).key(key).version_id(&version_id)
        .send().await
        .expect("the delete failed with the hold lifted");
}
