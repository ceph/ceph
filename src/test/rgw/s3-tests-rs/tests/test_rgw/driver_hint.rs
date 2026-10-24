//! The driver hint interface itself.
//!
//! `DELETE /admin/driver/hint` lets a test reach a driver-internal control
//! that has no S3 spelling -- arm a fallback path, drop a cache, inject a
//! failure.  Half of that is acting and half is answering:  a hint reports
//! results, and without them a test could arm the buffered copy and then
//! only assume it ran.
//!
//! These cover the wiring rather than any one hint's meaning.  The
//! endpoint is dev-gated (`rgw_driver_debug_apis`) and needs the
//! `driver-hint` capability, so against anything but a development
//! gateway it answers 403 and every test here skips -- the same rule the
//! features query follows, where absent means unknown and never false.

use serial_test::serial;

use aws_sdk_s3::primitives::ByteStream;

use s3_tests_rs::admin::driver_hint_results;
use s3_tests_rs::client::get_client;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};

/// Both surfaces read the same `get_features()`, so a disagreement means
/// one of them is rendering it wrong.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_driver_hint_results_round_trip() {
    let _guard = TestGuard::setup();

    let Some(results) = driver_hint_results("fs-features", &[]).await else {
        eprintln!("driver hint endpoint unavailable;  nothing to check");
        return;
    };

    assert!(!results.is_empty(),
        "fs-features answered with no results at all");

    let Some(f) = features().await else {
        eprintln!("no features reported;  cross-check skipped");
        return;
    };
    for (k, v) in &results {
        if let Some(fv) = f.get_str(k) {
            assert_eq!(fv, v,
                "/admin/features and the fs-features hint disagree on {k}");
        }
    }
}

/// The hint arms the buffered copy and reports how many bytes it moved.
/// Arming also resets the counter, so the second call reports exactly what
/// this test's copy cost.  Asserting `>=` rather than `==` keeps it honest
/// if another copy lands in the window.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[serial]
#[tokio::test]
async fn test_driver_hint_buffered_copy_accounts_bytes() {
    let _guard = TestGuard::setup();

    let Some(f) = features().await else {
        eprintln!("no features reported;  nothing to check");
        return;
    };
    if f.backend.as_deref() != Some("nsfs") {
        eprintln!("inject-buffered-copy is nsfs-only;  skipped");
        return;
    }

    /* arm, and zero the counter */
    let Some(armed) = driver_hint_results(
        "inject-buffered-copy", &[("enable", "true")]).await else {
        eprintln!("driver hint endpoint unavailable;  nothing to check");
        return;
    };
    assert_eq!(armed.get("enabled").map(String::as_str), Some("true"),
        "the hint did not report itself armed");
    assert!(armed.contains_key("buffered_bytes"),
        "the hint reported no byte count");

    /* The control.  Arming zeroes the counter, so arming again with no
     * copy in between has to report zero -- otherwise the assertion
     * below could pass on bytes this test did not cause, and would prove
     * nothing about which path the copy took. */
    let zeroed = driver_hint_results(
        "inject-buffered-copy", &[("enable", "true")]).await
        .expect("hint answered once and should answer again");
    assert_eq!(zeroed.get("buffered_bytes").map(String::as_str), Some("0"),
        "the byte count did not reset;  a nonzero result below would not \
         be evidence");

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    const SIZE: usize = 1 << 20;
    let body = vec![b'x'; SIZE];

    client.put_object()
        .bucket(&bucket).key("src")
        .body(ByteStream::from(body))
        .send().await.unwrap();

    client.copy_object()
        .bucket(&bucket).key("dst")
        .copy_source(format!("{bucket}/src"))
        .send().await.unwrap();

    let after = driver_hint_results(
        "inject-buffered-copy", &[("enable", "false")]).await
        .expect("hint answered once and should answer again");
    let moved: u64 = after.get("buffered_bytes")
        .expect("no byte count after the copy")
        .parse().expect("byte count is not a number");

    assert!(moved >= SIZE as u64,
        "buffered copy accounted {moved} bytes for a {SIZE}-byte object;  \
         the copy did not take the fallback");
    assert_eq!(after.get("enabled").map(String::as_str), Some("false"),
        "the hint did not report itself disarmed");
}
