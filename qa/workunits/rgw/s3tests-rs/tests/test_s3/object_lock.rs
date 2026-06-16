use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
    ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode, ObjectLockRule,
};
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::{check_configure_versioning_retry, get_new_bucket, get_new_bucket_name};
use s3_tests_rs::assert_s3_err;

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_lock() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build();

    client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf)
        .send()
        .await
        .unwrap();

    let conf2 = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Compliance)
                        .years(1)
                        .build(),
                )
                .build(),
        )
        .build();

    client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf2)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_versioning()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().unwrap().as_str(), "Enabled");
}

#[tokio::test]
async fn test_object_lock_put_obj_lock_invalid_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build();

    let result = client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf)
        .send()
        .await;
    assert_s3_err!(result, 409, "InvalidBucketState");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_get_obj_lock() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build();

    client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_lock_configuration()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let lock_conf = response.object_lock_configuration().unwrap();
    assert_eq!(lock_conf.object_lock_enabled().unwrap().as_str(), "Enabled");
    let retention = lock_conf.rule().unwrap().default_retention().unwrap();
    assert_eq!(retention.mode().unwrap().as_str(), "GOVERNANCE");
    assert_eq!(retention.days().unwrap(), 1);
}

#[tokio::test]
async fn test_object_lock_get_obj_lock_invalid_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let result = client
        .get_object_lock_configuration()
        .bucket(&bucket_name)
        .send()
        .await;
    assert_s3_err!(result, 404, "ObjectLockConfigurationNotFoundError");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_lock_with_days_and_years() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .years(1)
                        .build(),
                )
                .build(),
        )
        .build();

    let result = client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf)
        .send()
        .await;
    assert_s3_err!(result, 400, "MalformedXML");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_retention() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let retain_until = aws_smithy_types::DateTime::from_secs(5364662400); // 2140-01-01
    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(retain_until)
        .build();

    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.retention().unwrap().mode().unwrap().as_str(), "GOVERNANCE");

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_lock_put_obj_retention_invalid_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "file1";

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    let retain_until = aws_smithy_types::DateTime::from_secs(1893456000); // 2030-01-01
    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(retain_until)
        .build();

    let result = client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidRequest");
}

#[tokio::test]
async fn test_object_lock_get_obj_retention_invalid_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "file1";

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    let result = client
        .get_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidRequest");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_legal_hold() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    let legal_hold = aws_sdk_s3::types::ObjectLockLegalHold::builder()
        .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
        .build();
    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(legal_hold)
        .send()
        .await
        .unwrap();

    let legal_hold_off = aws_sdk_s3::types::ObjectLockLegalHold::builder()
        .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off)
        .build();
    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(legal_hold_off)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_get_legal_hold() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    let legal_hold = aws_sdk_s3::types::ObjectLockLegalHold::builder()
        .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
        .build();
    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(legal_hold)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.legal_hold().unwrap().status().unwrap().as_str(), "ON");

    let legal_hold_off = aws_sdk_s3::types::ObjectLockLegalHold::builder()
        .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off)
        .build();
    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(legal_hold_off)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.legal_hold().unwrap().status().unwrap().as_str(), "OFF");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_delete_object_with_legal_hold_on() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let legal_hold = aws_sdk_s3::types::ObjectLockLegalHold::builder()
        .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
        .build();
    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(legal_hold)
        .send()
        .await
        .unwrap();

    let result = client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let legal_hold_off = aws_sdk_s3::types::ObjectLockLegalHold::builder()
        .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off)
        .build();
    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(legal_hold_off)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_lock_put_obj_lock_enable_after_create() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build();

    let result = client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf.clone())
        .send()
        .await;
    assert_s3_err!(result, 409, "InvalidBucketState");

    check_configure_versioning_retry(&client, &bucket_name, "Suspended", "Suspended").await;
    let result = client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf.clone())
        .send()
        .await;
    assert_s3_err!(result, 409, "InvalidBucketState");

    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;
    client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_lock_invalid_days() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(0)
                        .build(),
                )
                .build(),
        )
        .build();

    let result = client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidRetentionPeriod");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_lock_invalid_years() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .years(-1)
                        .build(),
                )
                .build(),
        )
        .build();

    let result = client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidRetentionPeriod");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_suspend_versioning() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let result = client
        .put_bucket_versioning()
        .bucket(&bucket_name)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 409, "InvalidBucketState");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_get_obj_retention() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let retain_until = aws_smithy_types::DateTime::from_secs(1893456000);
    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(retain_until)
        .build();

    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.retention().unwrap().mode().unwrap().as_str(),
        "GOVERNANCE"
    );

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_retention_versionid() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let retain_until = aws_smithy_types::DateTime::from_secs(1893456000);
    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(retain_until)
        .build();

    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .retention(retention)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.retention().unwrap().mode().unwrap().as_str(),
        "GOVERNANCE"
    );

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_retention_increase_period() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let retention1 = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893456000))
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention1)
        .send()
        .await
        .unwrap();

    let retention2 = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893628800))
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention2)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.retention().unwrap().retain_until_date().unwrap().secs(),
        1893628800
    );

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_retention_shorten_period() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893628800))
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await
        .unwrap();

    let shorter = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893456000))
        .build();
    let result = client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(shorter)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_retention_shorten_period_bypass() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893628800))
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await
        .unwrap();

    let shorter = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893456000))
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(shorter)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.retention().unwrap().retain_until_date().unwrap().secs(),
        1893456000
    );

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_delete_object_with_retention() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893456000))
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await
        .unwrap();

    let result = client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_retention_override_default_retention() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build();

    client
        .put_object_lock_configuration()
        .bucket(&bucket_name)
        .object_lock_configuration(conf)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893456000))
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.retention().unwrap().mode().unwrap().as_str(),
        "GOVERNANCE"
    );
    assert_eq!(
        response.retention().unwrap().retain_until_date().unwrap().secs(),
        1893456000
    );

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_lock_put_legal_hold_invalid_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let key = "file1";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    let legal_hold = aws_sdk_s3::types::ObjectLockLegalHold::builder()
        .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
        .build();
    let result = client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(legal_hold)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidRequest");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_delete_object_with_retention_and_marker() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let obj_version_id = resp.version_id().unwrap_or_default().to_string();

    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893456000))
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await
        .unwrap();

    let del_resp = client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    let dm_version_id = del_resp.version_id().unwrap_or_default().to_string();

    let result = client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&obj_version_id)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&dm_version_id)
        .send()
        .await
        .unwrap();

    let result = client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&obj_version_id)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&obj_version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_multi_delete_object_with_retention() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key1 = "file1";
    let key2 = "file2";

    let resp1 = client
        .put_object()
        .bucket(&bucket_name)
        .key(key1)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let vid1 = resp1.version_id().unwrap_or_default().to_string();

    let resp2 = client
        .put_object()
        .bucket(&bucket_name)
        .key(key2)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let vid2 = resp2.version_id().unwrap_or_default().to_string();

    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_secs(1893456000))
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key1)
        .retention(retention)
        .send()
        .await
        .unwrap();

    let objects = vec![
        aws_sdk_s3::types::ObjectIdentifier::builder()
            .key(key1)
            .version_id(&vid1)
            .build()
            .unwrap(),
        aws_sdk_s3::types::ObjectIdentifier::builder()
            .key(key2)
            .version_id(&vid2)
            .build()
            .unwrap(),
    ];
    let delete = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects))
        .build()
        .unwrap();
    let response = client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete)
        .send()
        .await
        .unwrap();

    assert_eq!(response.deleted().len(), 1);
    assert_eq!(response.errors().len(), 1);

    assert_eq!(response.errors()[0].key().unwrap_or_default(), key1);
    assert_eq!(response.errors()[0].code().unwrap_or_default(), "AccessDenied");

    assert_eq!(response.deleted()[0].key().unwrap_or_default(), key2);

    let objects2 = vec![aws_sdk_s3::types::ObjectIdentifier::builder()
        .key(key1)
        .version_id(&vid1)
        .build()
        .unwrap()];
    let delete2 = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects2))
        .build()
        .unwrap();
    let response = client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete2)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();

    assert!(response.errors().is_empty());
    assert_eq!(response.deleted().len(), 1);
    assert_eq!(response.deleted()[0].key().unwrap_or_default(), key1);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_changing_mode_from_governance_with_bypass() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let retain_until = aws_smithy_types::DateTime::from_secs(
        aws_smithy_types::DateTime::from(std::time::SystemTime::now()).secs() + 10,
    );

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
        .object_lock_retain_until_date(retain_until)
        .send()
        .await
        .unwrap();

    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Compliance)
        .retain_until_date(retain_until)
        .build();
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_changing_mode_from_governance_without_bypass() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let retain_until = aws_smithy_types::DateTime::from_secs(
        aws_smithy_types::DateTime::from(std::time::SystemTime::now()).secs() + 10,
    );

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
        .object_lock_retain_until_date(retain_until)
        .send()
        .await
        .unwrap();

    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Compliance)
        .retain_until_date(retain_until)
        .build();
    let result = client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_changing_mode_from_compliance() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let retain_until = aws_smithy_types::DateTime::from_secs(
        aws_smithy_types::DateTime::from(std::time::SystemTime::now()).secs() + 10,
    );

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Compliance)
        .object_lock_retain_until_date(retain_until)
        .send()
        .await
        .unwrap();

    let retention = aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(retain_until)
        .build();
    let result = client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(retention)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[tokio::test]
async fn test_object_lock_get_legal_hold_invalid_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let key = "file1";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    let result = client
        .get_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidRequest");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_delete_object_with_legal_hold_off() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let response = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let version_id = response.version_id().unwrap().to_string();

    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off)
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_get_obj_metadata() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let retain_until = aws_smithy_types::DateTime::from_secs(1893456000); // 2030-01-01
    client
        .put_object_retention()
        .bucket(&bucket_name)
        .key(key)
        .retention(
            aws_sdk_s3::types::ObjectLockRetention::builder()
                .mode(ObjectLockRetentionMode::Governance)
                .retain_until_date(retain_until)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let response = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.object_lock_mode().unwrap().as_str(),
        "GOVERNANCE"
    );
    assert!(response.object_lock_retain_until_date().is_some());
    assert_eq!(
        response.object_lock_legal_hold_status().unwrap().as_str(),
        "ON"
    );

    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let version_id = response.version_id().unwrap().to_string();
    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_uploading_obj() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let retain_until = aws_smithy_types::DateTime::from_secs(1893456000); // 2030-01-01
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"abc"))
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
        .object_lock_retain_until_date(retain_until)
        .object_lock_legal_hold_status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
        .send()
        .await
        .unwrap();

    let response = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.object_lock_mode().unwrap().as_str(),
        "GOVERNANCE"
    );
    assert!(response.object_lock_retain_until_date().is_some());
    assert_eq!(
        response.object_lock_legal_hold_status().unwrap().as_str(),
        "ON"
    );

    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let version_id = response.version_id().unwrap().to_string();
    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_delete_multipart_object_with_legal_hold_on() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let mp = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .object_lock_legal_hold_status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
        .send()
        .await
        .unwrap();
    let upload_id = mp.upload_id().unwrap();

    let part = client
        .upload_part()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .parts(
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(part.e_tag().unwrap_or_default())
                .part_number(1)
                .build(),
        )
        .build();

    let response = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .unwrap();
    let version_id = response.version_id().unwrap().to_string();

    let result = client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    client
        .put_object_legal_hold()
        .bucket(&bucket_name)
        .key(key)
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_delete_multipart_object_with_retention() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let key = "file1";
    let retain_until = aws_smithy_types::DateTime::from_secs(1893456000); // 2030-01-01
    let mp = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
        .object_lock_retain_until_date(retain_until)
        .send()
        .await
        .unwrap();
    let upload_id = mp.upload_id().unwrap();

    let part = client
        .upload_part()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();

    let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .parts(
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(part.e_tag().unwrap_or_default())
                .part_number(1)
                .build(),
        )
        .build();

    let response = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .unwrap();
    let version_id = response.version_id().unwrap().to_string();

    let result = client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
}

// --- Invalid mode/status tests ---

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_lock_invalid_mode() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).object_lock_enabled_for_bucket(true).send().await.unwrap();

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(ObjectLockRule::builder().default_retention(
            DefaultRetention::builder().mode(ObjectLockRetentionMode::from("abc")).years(1).build(),
        ).build())
        .build();
    let result = client.put_object_lock_configuration().bucket(&bucket).object_lock_configuration(conf).send().await;
    assert_s3_err!(result, 400, "MalformedXML");

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(ObjectLockRule::builder().default_retention(
            DefaultRetention::builder().mode(ObjectLockRetentionMode::from("governance")).years(1).build(),
        ).build())
        .build();
    let result = client.put_object_lock_configuration().bucket(&bucket).object_lock_configuration(conf).send().await;
    assert_s3_err!(result, 400, "MalformedXML");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_lock_invalid_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).object_lock_enabled_for_bucket(true).send().await.unwrap();

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::from("Disabled"))
        .rule(ObjectLockRule::builder().default_retention(
            DefaultRetention::builder().mode(ObjectLockRetentionMode::Governance).years(1).build(),
        ).build())
        .build();
    let result = client.put_object_lock_configuration().bucket(&bucket).object_lock_configuration(conf).send().await;
    assert_s3_err!(result, 400, "MalformedXML");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_obj_retention_invalid_mode() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).object_lock_enabled_for_bucket(true).send().await.unwrap();

    let key = "file1";
    client.put_object().bucket(&bucket).key(key).body(ByteStream::from_static(b"abc")).send().await.unwrap();

    let retain_date = aws_smithy_types::DateTime::from_secs(1893456000);
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::from("governance"))
        .retain_until_date(retain_date)
        .build();
    let result = client.put_object_retention().bucket(&bucket).key(key).retention(retention).send().await;
    assert_s3_err!(result, 400, "MalformedXML");

    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::from("abc"))
        .retain_until_date(retain_date)
        .build();
    let result = client.put_object_retention().bucket(&bucket).key(key).retention(retention).send().await;
    assert_s3_err!(result, 400, "MalformedXML");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_get_obj_retention_iso8601() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).object_lock_enabled_for_bucket(true).send().await.unwrap();

    let key = "file1";
    let put_resp = client.put_object().bucket(&bucket).key(key).body(ByteStream::from_static(b"abc")).send().await.unwrap();
    let version_id = put_resp.version_id().unwrap().to_string();

    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
    let retain_date = aws_smithy_types::DateTime::from_secs(now + 365 * 24 * 3600);
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(retain_date)
        .build();
    client.put_object_retention().bucket(&bucket).key(key).retention(retention).send().await.unwrap();

    let head = client.head_object().bucket(&bucket).key(key).version_id(&version_id).send().await.unwrap();
    assert!(head.object_lock_retain_until_date().is_some());
    assert_eq!(head.object_lock_mode(), Some(&aws_sdk_s3::types::ObjectLockMode::Governance));

    client.delete_object().bucket(&bucket).key(key).version_id(&version_id).bypass_governance_retention(true).send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_lock_put_legal_hold_invalid_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).object_lock_enabled_for_bucket(true).send().await.unwrap();

    let key = "file1";
    client.put_object().bucket(&bucket).key(key).body(ByteStream::from_static(b"abc")).send().await.unwrap();

    let legal_hold = ObjectLockLegalHold::builder()
        .status(ObjectLockLegalHoldStatus::from("abc"))
        .build();
    let result = client.put_object_legal_hold().bucket(&bucket).key(key).legal_hold(legal_hold).send().await;
    assert_s3_err!(result, 400, "MalformedXML");
}
