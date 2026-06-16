use aws_sdk_s3::primitives::ByteStream;
use serial_test::serial;
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};
use s3_tests_rs::assert_s3_err;

use super::admin;

#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[serial]
#[tokio::test]
async fn test_user_quota_max_objects() {
    let _guard = TestGuard::setup();
    let cfg = get_config();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let uid = &cfg.main_user_id;

    admin::set_user_quota(uid, -1, 2, true).await;

    client.put_object()
        .bucket(&bucket_name).key("obj1")
        .body(ByteStream::from_static(b"data1"))
        .send().await.unwrap();

    client.put_object()
        .bucket(&bucket_name).key("obj2")
        .body(ByteStream::from_static(b"data2"))
        .send().await.unwrap();

    assert_s3_err!(
        client.put_object()
            .bucket(&bucket_name).key("obj3")
            .body(ByteStream::from_static(b"data3"))
            .send().await,
        403, "QuotaExceeded"
    );

    admin::disable_user_quota(uid).await;
}

#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[serial]
#[tokio::test]
async fn test_user_quota_max_size() {
    let _guard = TestGuard::setup();
    let cfg = get_config();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let uid = &cfg.main_user_id;

    // 1MB max — large enough that residual stats from prior tests
    // won't trip the limit, but small enough to test enforcement
    admin::set_user_quota(uid, 1_048_576, -1, true).await;

    let small_body = vec![0u8; 1024];
    client.put_object()
        .bucket(&bucket_name).key("small")
        .body(ByteStream::from(small_body))
        .send().await.unwrap();

    let big_body = vec![0u8; 2_097_152];
    assert_s3_err!(
        client.put_object()
            .bucket(&bucket_name).key("big")
            .body(ByteStream::from(big_body))
            .send().await,
        403, "QuotaExceeded"
    );

    admin::disable_user_quota(uid).await;
}

#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[serial]
#[tokio::test]
async fn test_user_quota_disabled() {
    let _guard = TestGuard::setup();
    let cfg = get_config();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let uid = &cfg.main_user_id;

    // set restrictive quota but keep it disabled
    admin::set_user_quota(uid, 1, 1, false).await;

    client.put_object()
        .bucket(&bucket_name).key("obj1")
        .body(ByteStream::from_static(b"data1"))
        .send().await.unwrap();

    client.put_object()
        .bucket(&bucket_name).key("obj2")
        .body(ByteStream::from_static(b"data2"))
        .send().await.unwrap();

    // both should succeed since quota is disabled
    admin::disable_user_quota(uid).await;
}

#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[serial]
#[tokio::test]
async fn test_user_quota_get_set_roundtrip() {
    let _guard = TestGuard::setup();
    let cfg = get_config();
    let uid = &cfg.main_user_id;

    admin::set_user_quota(uid, 1048576, 100, true).await;
    let (max_size, max_objects, enabled) = admin::get_user_quota(uid).await;
    assert_eq!(max_size, 1048576);
    assert_eq!(max_objects, 100);
    assert!(enabled);

    admin::disable_user_quota(uid).await;
    let (_, _, enabled) = admin::get_user_quota(uid).await;
    assert!(!enabled);
}

#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[serial]
#[tokio::test]
async fn test_user_quota_across_buckets() {
    let _guard = TestGuard::setup();
    let cfg = get_config();
    let client = get_client();
    let bucket1 = get_new_bucket(Some(&client)).await;
    let bucket2 = get_new_bucket(Some(&client)).await;
    let uid = &cfg.main_user_id;

    admin::set_user_quota(uid, -1, 3, true).await;

    client.put_object()
        .bucket(&bucket1).key("a")
        .body(ByteStream::from_static(b"1"))
        .send().await.unwrap();

    client.put_object()
        .bucket(&bucket2).key("b")
        .body(ByteStream::from_static(b"2"))
        .send().await.unwrap();

    client.put_object()
        .bucket(&bucket1).key("c")
        .body(ByteStream::from_static(b"3"))
        .send().await.unwrap();

    // 4th object across both buckets should fail
    assert_s3_err!(
        client.put_object()
            .bucket(&bucket2).key("d")
            .body(ByteStream::from_static(b"4"))
            .send().await,
        403, "QuotaExceeded"
    );

    admin::disable_user_quota(uid).await;
}
