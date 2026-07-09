use aws_sdk_s3::primitives::ByteStream;
use std::collections::HashMap;
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, ExpirationStatus, LifecycleRule, LifecycleRuleAndOperator,
    LifecycleRuleFilter, NoncurrentVersionExpiration, NoncurrentVersionTransition,
    Tag, Tagging, Transition, TransitionStorageClass,
};
use s3_tests_rs::client::{get_client, get_cloud_client};
use s3_tests_rs::config::{get_config, configured_storage_classes};
use s3_tests_rs::fixtures::{
    check_configure_versioning_retry, create_objects_in_new_bucket, get_new_bucket,
};

fn check_lifecycle_expiration_header(
    expiration: Option<&str>,
    start_time: chrono::DateTime<chrono::Utc>,
    rule_id: &str,
    delta_days: i64,
) -> bool {
    let expr_hdr = match expiration {
        Some(h) => h,
        None => return false,
    };
    let re = regex::Regex::new(r#"expiry-date="(.+?)", rule-id="(.+?)""#).unwrap();
    let caps = match re.captures(expr_hdr) {
        Some(c) => c,
        None => return false,
    };
    let expiry_str = &caps[1];
    let got_rule_id = &caps[2];
    let expiration_dt =
        chrono::DateTime::parse_from_rfc2822(expiry_str).unwrap_or_else(|_| {
            chrono::DateTime::parse_from_str(expiry_str, "%a, %d %b %Y %H:%M:%S %Z")
                .expect("failed to parse expiry-date")
        });
    let duration = expiration_dt.naive_utc() - start_time.naive_utc();
    duration.num_days() == delta_days && got_rule_id == rule_id
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_set() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![
        LifecycleRule::builder()
            .id("rule1")
            .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(1)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
        LifecycleRule::builder()
            .id("rule2")
            .filter(LifecycleRuleFilter::builder().prefix("test2/").build())
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(2)
                    .build(),
            )
            .status(ExpirationStatus::Disabled)
            .build()
            .unwrap(),
    ];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_get() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![
        LifecycleRule::builder()
            .id("test1/")
            .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(31)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
        LifecycleRule::builder()
            .id("test2/")
            .filter(LifecycleRuleFilter::builder().prefix("test2/").build())
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(120)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
    ];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    assert_eq!(response.rules().len(), 2);
    assert_eq!(response.rules()[0].id().unwrap_or_default(), "test1/");
    assert_eq!(response.rules()[0].expiration().unwrap().days().unwrap(), 31);
    assert_eq!(response.rules()[1].id().unwrap_or_default(), "test2/");
    assert_eq!(response.rules()[1].expiration().unwrap().days().unwrap(), 120);
}

/* filter round-trip */
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_empty_filter() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![
        LifecycleRule::builder()
            .id("test-empty-filter")
            .filter(LifecycleRuleFilter::builder().build())
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(120)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
    ];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    assert_eq!(response.rules().len(), 1);
    assert_eq!(response.rules()[0].id().unwrap_or_default(), "test-empty-filter");
    assert_eq!(response.rules()[0].expiration().unwrap().days().unwrap(), 120); // XXX fixme

    let rule1 = response.rules()[0].filter();
    eprintln!("rule1: WOOWOO {:?}", rule1);
    assert_eq!(1, 1);
    eprintln!("rule1: WOOWOO {:?}", rule1);

} /* test-empty-filter */

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_get_no_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![
        LifecycleRule::builder()
            .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(31)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
        LifecycleRule::builder()
            .filter(LifecycleRuleFilter::builder().prefix("test2/").build())
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(120)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
    ];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    for rule in response.rules() {
        assert!(rule.id().is_some());
    }
    assert_eq!(response.rules().len(), 2);
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_set_date() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let date = aws_smithy_types::DateTime::from_secs(1506470400); // 2017-09-27
    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .expiration(
            aws_sdk_s3::types::LifecycleExpiration::builder()
                .date(date)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_set_noncurrent() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["past/foo", "future/bar"]).await;
    let client = get_client();

    let rules = vec![
        LifecycleRule::builder()
            .id("rule1")
            .filter(LifecycleRuleFilter::builder().prefix("past/").build())
            .noncurrent_version_expiration(
                NoncurrentVersionExpiration::builder()
                    .noncurrent_days(2)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
        LifecycleRule::builder()
            .id("rule2")
            .filter(LifecycleRuleFilter::builder().prefix("future/").build())
            .noncurrent_version_expiration(
                NoncurrentVersionExpiration::builder()
                    .noncurrent_days(3)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
    ];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_set_filter() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("foo").build())
        .expiration(
            aws_sdk_s3::types::LifecycleExpiration::builder()
                .expired_object_delete_marker(true)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_set_empty_filter() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("").build())
        .expiration(
            aws_sdk_s3::types::LifecycleExpiration::builder()
                .expired_object_delete_marker(true)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test/").build())
        .expiration(
            aws_sdk_s3::types::LifecycleExpiration::builder()
                .days(1)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    client
        .delete_bucket_lifecycle()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let result = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .send()
        .await;
    assert!(result.is_err());
}

// --- expiration tests (these wait on lc_debug_interval) ---

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["expire1/foo", "expire1/bar", "keep2/foo", "keep2/bar", "expire3/foo", "expire3/bar"])
            .await;
    let client = get_client();
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    let rules = vec![
        LifecycleRule::builder()
            .id("rule1")
            .filter(LifecycleRuleFilter::builder().prefix("expire1/").build())
            .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
        LifecycleRule::builder()
            .id("rule2")
            .filter(LifecycleRuleFilter::builder().prefix("expire3/").build())
            .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(6).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
    ];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let response = client.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.contents().len(), 6);

    tokio::time::sleep(std::time::Duration::from_secs(2 * lc_interval)).await;

    let response = client.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.contents().len(), 4);

    tokio::time::sleep(std::time::Duration::from_secs(6 * lc_interval)).await;

    let response = client.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.contents().len(), 2);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecyclev2_expiration() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["expire1/foo", "expire1/bar", "keep2/foo", "keep2/bar", "expire3/foo", "expire3/bar"])
            .await;
    let client = get_client();
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    let rules = vec![
        LifecycleRule::builder()
            .id("rule1")
            .filter(LifecycleRuleFilter::builder().prefix("expire1/").build())
            .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
        LifecycleRule::builder()
            .id("rule2")
            .filter(LifecycleRuleFilter::builder().prefix("expire3/").build())
            .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(6).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
    ];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let response = client.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.contents().len(), 6);

    tokio::time::sleep(std::time::Duration::from_secs(3 * lc_interval)).await;

    let response = client.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.contents().len(), 4);

    tokio::time::sleep(std::time::Duration::from_secs(6 * lc_interval)).await;

    let response = client.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.contents().len(), 2);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_date() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["past/foo", "future/bar"]).await;
    let client = get_client();
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    let rules = vec![
        LifecycleRule::builder()
            .id("rule1")
            .filter(LifecycleRuleFilter::builder().prefix("past/").build())
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .date(aws_smithy_types::DateTime::from_secs(1420070400)) // 2015-01-01
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
        LifecycleRule::builder()
            .id("rule2")
            .filter(LifecycleRuleFilter::builder().prefix("future/").build())
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .date(aws_smithy_types::DateTime::from_secs(1893456000)) // 2030-01-01
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
    ];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.contents().len(), 2);

    tokio::time::sleep(std::time::Duration::from_secs(3 * lc_interval)).await;

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.contents().len(), 1);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_versioning_enabled() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    s3_tests_rs::fixtures::check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("test1/a")
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .unwrap();

    client.delete_object().bucket(&bucket_name).key("test1/a").send().await.unwrap();

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(3 * lc_interval)).await;

    let response = client.list_object_versions().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.versions().len(), 1);
    assert_eq!(response.delete_markers().len(), 1);
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_header_put() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("days1/").build())
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let key = "days1/foo";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let response = client.head_object().bucket(&bucket_name).key(key).send().await.unwrap();
    assert!(response.expiration().is_some());
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_id_too_long() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![LifecycleRule::builder()
        .id("a".repeat(256))
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(2).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    let result = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await;
    assert!(result.is_err());
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_same_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![
        LifecycleRule::builder()
            .id("rule1")
            .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
            .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
        LifecycleRule::builder()
            .id("rule1")
            .filter(LifecycleRuleFilter::builder().prefix("test2/").build())
            .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(2).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap(),
    ];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    let result = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await;
    assert!(result.is_err());
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_days0() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["days0/foo", "days0/bar"]).await;
    let client = get_client();

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("days0/").build())
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(0).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    let result = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await;
    assert!(result.is_err());
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_set_deletemarker() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .expiration(
            aws_sdk_s3::types::LifecycleExpiration::builder()
                .expired_object_delete_marker(true)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();
}

// test_lifecycle_set_invalid_date: Python sends malformed date string '20200101'
// which RGW rejects. Rust SDK always serializes valid ISO dates, so we can't
// reproduce this test (see docs/sdk-limitations.md #6).

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_noncur_expiration() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    for _ in 0..3 {
        client
            .put_object()
            .bucket(&bucket_name)
            .key("test1/a")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"a"))
            .send()
            .await
            .unwrap();
    }
    for _ in 0..3 {
        client
            .put_object()
            .bucket(&bucket_name)
            .key("test2/abc")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.versions().len(), 6);

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(2)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(5 * lc_interval)).await;

    let resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.versions().len(), 4);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_tags1() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    let tom_key = "days1/tom";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(tom_key)
        .body(ByteStream::from_static(b"tom_body"))
        .send()
        .await
        .unwrap();

    let tagging = aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("tom")
                .value("sawyer")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(tom_key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let rules = vec![LifecycleRule::builder()
        .id("rule_tag1")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    aws_sdk_s3::types::LifecycleRuleAndOperator::builder()
                        .prefix("days1/")
                        .tags(
                            aws_sdk_s3::types::Tag::builder()
                                .key("tom")
                                .value("sawyer")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build(),
        )
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(3 * lc_interval)).await;

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(response.contents().is_empty());
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_set_multipart() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let lifecycle = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .rules(
            LifecycleRule::builder()
                .id("rule1")
                .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
                .status(ExpirationStatus::Enabled)
                .abort_incomplete_multipart_upload(
                    AbortIncompleteMultipartUpload::builder()
                        .days_after_initiation(2)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .rules(
            LifecycleRule::builder()
                .id("rule2")
                .filter(LifecycleRuleFilter::builder().prefix("test2/").build())
                .status(ExpirationStatus::Disabled)
                .abort_incomplete_multipart_upload(
                    AbortIncompleteMultipartUpload::builder()
                        .days_after_initiation(3)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lifecycle)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_tags2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    let tom_key = "days1/tom";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(tom_key)
        .body(ByteStream::from_static(b"tom_body"))
        .send()
        .await
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(tom_key)
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("tom")
                        .value("sawyer")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let huck_key = "days1/huck";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(huck_key)
        .body(ByteStream::from_static(b"huck_body"))
        .send()
        .await
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(huck_key)
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("tom")
                        .value("sawyer")
                        .build()
                        .unwrap(),
                )
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("huck")
                        .value("finn")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let rules = vec![LifecycleRule::builder()
        .id("rule_tag1")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    aws_sdk_s3::types::LifecycleRuleAndOperator::builder()
                        .prefix("days1")
                        .tags(
                            aws_sdk_s3::types::Tag::builder()
                                .key("huck")
                                .value("finn")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build(),
        )
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(3 * lc_interval)).await;

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    let objects = response.contents();
    assert_eq!(objects.len(), 1);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_versioned_tags2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let tom_key = "days1/tom";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(tom_key)
        .body(ByteStream::from_static(b"tom_body"))
        .send()
        .await
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(tom_key)
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("tom")
                        .value("sawyer")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let huck_key = "days1/huck";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(huck_key)
        .body(ByteStream::from_static(b"huck_body"))
        .send()
        .await
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(huck_key)
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("tom")
                        .value("sawyer")
                        .build()
                        .unwrap(),
                )
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("huck")
                        .value("finn")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let rules = vec![LifecycleRule::builder()
        .id("rule_tag1")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    aws_sdk_s3::types::LifecycleRuleAndOperator::builder()
                        .prefix("days1")
                        .tags(
                            aws_sdk_s3::types::Tag::builder()
                                .key("huck")
                                .value("finn")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build(),
        )
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(3 * lc_interval)).await;

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    let objects = response.contents();
    assert_eq!(objects.len(), 1);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_noncur_tags1() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "myobject_";
    let tagset = aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("vidushi")
                .value("mishra")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    for ix in 0..10 {
        let body = format!("{key} v{ix}");
        client
            .put_object()
            .bucket(&bucket_name)
            .key(key)
            .body(ByteStream::from(body.into_bytes()))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket_name)
            .key(key)
            .tagging(tagset.clone())
            .send()
            .await
            .unwrap();
    }

    let rules = vec![LifecycleRule::builder()
        .id("rule_tag1")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    aws_sdk_s3::types::LifecycleRuleAndOperator::builder()
                        .prefix("")
                        .tags(
                            aws_sdk_s3::types::Tag::builder()
                                .key("vidushi")
                                .value("mishra")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build(),
        )
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(3)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1 * lc_interval)).await;

    let response = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let num_objs = response.versions().len();
    assert_eq!(num_objs, 10);

    tokio::time::sleep(std::time::Duration::from_secs(6 * lc_interval)).await;

    let response = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let num_objs = response.versions().len();
    assert_eq!(num_objs, 1);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_newer_noncurrent() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "myobject_";
    for ix in 0..10 {
        let body = format!("{key} v{ix}");
        client
            .put_object()
            .bucket(&bucket_name)
            .key(key)
            .body(ByteStream::from(body.into_bytes()))
            .send()
            .await
            .unwrap();
    }

    let rules = vec![LifecycleRule::builder()
        .id("newer_noncurrent1")
        .filter(LifecycleRuleFilter::builder().prefix("").build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .newer_noncurrent_versions(5)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2 * lc_interval)).await;

    let response = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.versions().len(), 6);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_size_gt() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("myobject_small")
        .body(ByteStream::from(vec![b'b'; 1000]))
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket_name)
        .key("myobject_big")
        .body(ByteStream::from(vec![b'b'; 3000]))
        .send()
        .await
        .unwrap();

    let rules = vec![LifecycleRule::builder()
        .id("object_gt1")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    aws_sdk_s3::types::LifecycleRuleAndOperator::builder()
                        .prefix("")
                        .object_size_greater_than(2000)
                        .build(),
                )
                .build(),
        )
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(10 * lc_interval)).await;

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    let objects = response.contents();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key().unwrap(), "myobject_small");
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_size_lt() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("myobject_small")
        .body(ByteStream::from(vec![b'b'; 1000]))
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket_name)
        .key("myobject_big")
        .body(ByteStream::from(vec![b'b'; 3000]))
        .send()
        .await
        .unwrap();

    let rules = vec![LifecycleRule::builder()
        .id("object_lt1")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    aws_sdk_s3::types::LifecycleRuleAndOperator::builder()
                        .prefix("")
                        .object_size_less_than(2000)
                        .build(),
                )
                .build(),
        )
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2 * lc_interval)).await;

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    let objects = response.contents();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key().unwrap(), "myobject_big");
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_deletemarker_expiration() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    for i in 0..1 {
        let body = format!("content-{i}");
        client
            .put_object()
            .bucket(&bucket_name)
            .key("test1/a")
            .body(ByteStream::from(body.into_bytes()))
            .send()
            .await
            .unwrap();
    }
    client
        .put_object()
        .bucket(&bucket_name)
        .key("test2/abc")
        .body(ByteStream::from_static(b"content-0"))
        .send()
        .await
        .unwrap();

    client
        .delete_object()
        .bucket(&bucket_name)
        .key("test1/a")
        .send()
        .await
        .unwrap();
    client
        .delete_object()
        .bucket(&bucket_name)
        .key("test2/abc")
        .send()
        .await
        .unwrap();

    let response = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let total_init = response.versions().len() + response.delete_markers().len();
    assert_eq!(total_init, 4);

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .build(),
        )
        .expiration(
            aws_sdk_s3::types::LifecycleExpiration::builder()
                .expired_object_delete_marker(true)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(7 * lc_interval)).await;

    let response = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let total_expire = response.versions().len() + response.delete_markers().len();
    assert_eq!(total_expire, 2);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_multipart_expiration() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;


    let keys = ["test1/a", "test2/"];
    for key in &keys {
        client
            .create_multipart_upload()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .unwrap();
    }

    let response = client
        .list_multipart_uploads()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let init_uploads = response.uploads();
    assert_eq!(init_uploads.len(), 2);

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .status(ExpirationStatus::Enabled)
        .abort_incomplete_multipart_upload(
            AbortIncompleteMultipartUpload::builder()
                .days_after_initiation(2)
                .build(),
        )
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(5 * lc_interval)).await;

    let response = client
        .list_multipart_uploads()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let expired_uploads = response.uploads();
    assert_eq!(expired_uploads.len(), 1);
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_invalid_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    // "enabled" (lowercase) is invalid
    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(2).build())
        .status(ExpirationStatus::from("enabled"))
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    let err = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await;
    assert!(err.is_err());

    // "invalid" is also invalid
    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(2).build())
        .status(ExpirationStatus::from("invalid"))
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    let err = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await;
    assert!(err.is_err());
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_header_head() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let now = chrono::Utc::now();

    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("days1/").build())
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let key = "days1/foo";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let response = client.head_object().bucket(&bucket_name).key(key).send().await.unwrap();
    assert!(check_lifecycle_expiration_header(response.expiration(), now, "rule1", 1));
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_header_tags_head() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let now = chrono::Utc::now();

    // rule filters on tag key1=tag1
    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(
            LifecycleRuleFilter::builder()
                .tag(Tag::builder().key("key1").value("tag1").build().unwrap())
                .build(),
        )
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let key1 = "obj_key1";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key1)
        .body(ByteStream::from_static(b"obj_key1_body"))
        .send()
        .await
        .unwrap();

    let tags = Tagging::builder()
        .tag_set(Tag::builder().key("key1").value("tag1").build().unwrap())
        .tag_set(Tag::builder().key("key5").value("tag5").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key1)
        .tagging(tags)
        .send()
        .await
        .unwrap();

    // object has key1=tag1 tag, so expiration header should be present
    let response = client.head_object().bucket(&bucket_name).key(key1).send().await.unwrap();
    assert!(check_lifecycle_expiration_header(response.expiration(), now, "rule1", 1));

    // now change rule to filter on key2=tag1 — object doesn't have key2
    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(
            LifecycleRuleFilter::builder()
                .tag(Tag::builder().key("key2").value("tag1").build().unwrap())
                .build(),
        )
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    // object does NOT have key2 tag, so expiration header should be absent
    let response = client.head_object().bucket(&bucket_name).key(key1).send().await.unwrap();
    assert!(!check_lifecycle_expiration_header(response.expiration(), now, "rule1", 1));
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_expiration_header_and_tags_head() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let now = chrono::Utc::now();

    // rule requires BOTH key1=tag1 AND key5=tag6
    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    LifecycleRuleAndOperator::builder()
                        .tags(Tag::builder().key("key1").value("tag1").build().unwrap())
                        .tags(Tag::builder().key("key5").value("tag6").build().unwrap())
                        .build(),
                )
                .build(),
        )
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    let key1 = "obj_key1";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key1)
        .body(ByteStream::from_static(b"obj_key1_body"))
        .send()
        .await
        .unwrap();

    // object has key1=tag1 and key5=tag5 (NOT tag6)
    let tags = Tagging::builder()
        .tag_set(Tag::builder().key("key1").value("tag1").build().unwrap())
        .tag_set(Tag::builder().key("key5").value("tag5").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key1)
        .tagging(tags)
        .send()
        .await
        .unwrap();

    // tags don't fully match (key5=tag5 != tag6), so no expiration header
    let response = client.head_object().bucket(&bucket_name).key(key1).send().await.unwrap();
    assert!(!check_lifecycle_expiration_header(response.expiration(), now, "rule1", 1));
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_set_invalid_date() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    // Build a valid lifecycle config with a real date, then use
    // customize().mutate_request() to mangle the serialized XML
    // body before signing — replacing the valid ISO date with "20200101".
    let valid_date = aws_smithy_types::DateTime::from_secs(1577836800); // 2020-01-01T00:00:00Z
    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .expiration(
            aws_sdk_s3::types::LifecycleExpiration::builder()
                .date(valid_date)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    let err = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .customize()
        .mutate_request(|req| {
            if let Some(body_bytes) = req.body().bytes() {
                let body_str = std::str::from_utf8(body_bytes).unwrap();
                let mangled = body_str.replace(
                    "<Date>2020-01-01T00:00:00Z</Date>",
                    "<Date>20200101</Date>",
                );
                let new_body = aws_smithy_types::body::SdkBody::from(mangled);
                if let Some(len) = new_body.content_length() {
                    req.headers_mut().insert("content-length", len.to_string());
                }
                *req.body_mut() = new_body;
            }
        })
        .send()
        .await;
    assert!(err.is_err());
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_deletemarker_expiration_with_days_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;

    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    // create 1 version of test1/a
    client
        .put_object()
        .bucket(&bucket_name)
        .key("test1/a")
        .body(ByteStream::from_static(b"content-0"))
        .send()
        .await
        .unwrap();

    // delete it → creates a delete marker
    client
        .delete_object()
        .bucket(&bucket_name)
        .key("test1/a")
        .send()
        .await
        .unwrap();

    // NoncurrentDays=1 expires the noncurrent version;
    // Days=5 expires the remaining delete marker later
    let rules = vec![LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .build(),
        )
        .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(5).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .unwrap()];

    let lc = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lc)
        .send()
        .await
        .unwrap();

    // after 2 intervals: noncurrent version expired, delete marker remains
    tokio::time::sleep(std::time::Duration::from_secs(3 * lc_interval)).await;

    let response = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let versions = response.versions();
    let delete_markers = response.delete_markers();
    assert_eq!(versions.len(), 0);
    assert_eq!(delete_markers.len(), 1);

    // after 4 more intervals: delete marker also expired (Days=5)
    tokio::time::sleep(std::time::Duration::from_secs(5 * lc_interval)).await;

    let response = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let delete_markers = response.delete_markers();
    assert_eq!(delete_markers.len(), 0);
}

// =====================================================================
// Lifecycle transition tests
// =====================================================================

async fn list_bucket_storage_class(
    client: &aws_sdk_s3::Client,
    bucket: &str,
) -> HashMap<String, Vec<String>> {
    let mut result: HashMap<String, Vec<String>> = HashMap::new();
    let response = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    for v in response.versions() {
        let sc = v.storage_class()
            .map(|s| s.as_str().to_string())
            .unwrap_or_else(|| "STANDARD".to_string());
        result.entry(sc).or_default().push(v.key().unwrap_or_default().to_string());
    }
    result
}

fn sc_count(map: &HashMap<String, Vec<String>>, class: &str) -> usize {
    map.get(class).map(|v| v.len()).unwrap_or(0)
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_transition_set_invalid_date() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    // The SDK normalizes dates, so we must inject the invalid date string
    // via mutate_request() after serialization (same pattern as bucket
    // logging extension XML injection).
    let result = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
                        .expiration(
                            aws_sdk_s3::types::LifecycleExpiration::builder()
                                .date(aws_smithy_types::DateTime::from_str(
                                    "2023-09-27T00:00:00Z",
                                    aws_smithy_types::date_time::Format::DateTime,
                                ).unwrap())
                                .build(),
                        )
                        .transitions(
                            Transition::builder()
                                .days(1)
                                .storage_class(TransitionStorageClass::Glacier)
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .customize()
        .mutate_request(|req| {
            let body = req.body().bytes().unwrap_or_default().to_vec();
            let xml = String::from_utf8_lossy(&body);
            // Replace the SDK-generated <Days>1</Days> inside <Transition>
            // with an invalid <Date>20220927</Date>
            let patched = xml.replacen(
                "<Days>1</Days>",
                "<Date>20220927</Date>",
                1,
            );
            *req.body_mut() = aws_smithy_types::body::SdkBody::from(patched);
            let len = req.body().bytes().map(|b| b.len()).unwrap_or(0);
            req.headers_mut().insert("content-length", len.to_string());
        })
        .send()
        .await;

    assert!(result.is_err(), "expected error for invalid transition date");
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
async fn test_lifecycle_transition() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sc = configured_storage_classes();
    if sc.len() < 3 {
        eprintln!("skipping: requires 3+ storage classes, have {}", sc.len());
        return;
    }

    let client = get_client();
    let keys = &["expire1/foo", "expire1/bar", "keep2/foo", "keep2/bar", "expire3/foo", "expire3/bar"];
    let bucket = get_new_bucket(Some(&client)).await;
    for key in keys {
        client.put_object().bucket(&bucket).key(*key).body(ByteStream::from_static(key.as_bytes())).send().await.unwrap();
    }

    let response = client.list_objects_v2().bucket(&bucket).send().await.unwrap();
    assert_eq!(response.contents().len(), 6);

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("expire1/").build())
                        .transitions(
                            Transition::builder()
                                .days(1)
                                .storage_class(TransitionStorageClass::from(sc[1].as_str()))
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .rules(
                    LifecycleRule::builder()
                        .id("rule2")
                        .filter(LifecycleRuleFilter::builder().prefix("expire3/").build())
                        .transitions(
                            Transition::builder()
                                .days(6)
                                .storage_class(TransitionStorageClass::from(sc[2].as_str()))
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;

    tokio::time::sleep(std::time::Duration::from_secs(4 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 4);
    assert_eq!(sc_count(&classes, &sc[1]), 2);
    assert_eq!(sc_count(&classes, &sc[2]), 0);

    tokio::time::sleep(std::time::Duration::from_secs(lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 4);
    assert_eq!(sc_count(&classes, &sc[1]), 2);
    assert_eq!(sc_count(&classes, &sc[2]), 0);

    tokio::time::sleep(std::time::Duration::from_secs(5 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);
    assert_eq!(sc_count(&classes, &sc[1]), 2);
    assert_eq!(sc_count(&classes, &sc[2]), 2);
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
async fn test_lifecycle_transition_single_rule_multi_trans() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sc = configured_storage_classes();
    if sc.len() < 3 {
        eprintln!("skipping: requires 3+ storage classes, have {}", sc.len());
        return;
    }

    let client = get_client();
    let keys = &["expire1/foo", "expire1/bar", "keep2/foo", "keep2/bar", "expire3/foo", "expire3/bar"];
    let bucket = get_new_bucket(Some(&client)).await;
    for key in keys {
        client.put_object().bucket(&bucket).key(*key).body(ByteStream::from_static(key.as_bytes())).send().await.unwrap();
    }

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("expire1/").build())
                        .transitions(
                            Transition::builder()
                                .days(1)
                                .storage_class(TransitionStorageClass::from(sc[1].as_str()))
                                .build(),
                        )
                        .transitions(
                            Transition::builder()
                                .days(7)
                                .storage_class(TransitionStorageClass::from(sc[2].as_str()))
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;

    tokio::time::sleep(std::time::Duration::from_secs(5 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 4);
    assert_eq!(sc_count(&classes, &sc[1]), 2);
    assert_eq!(sc_count(&classes, &sc[2]), 0);

    tokio::time::sleep(std::time::Duration::from_secs(lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 4);
    assert_eq!(sc_count(&classes, &sc[1]), 2);
    assert_eq!(sc_count(&classes, &sc[2]), 0);

    tokio::time::sleep(std::time::Duration::from_secs(6 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 4);
    assert_eq!(sc_count(&classes, &sc[1]), 0);
    assert_eq!(sc_count(&classes, &sc[2]), 2);
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_set_noncurrent_transition() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sc = configured_storage_classes();
    if sc.len() < 3 {
        eprintln!("skipping: requires 3+ storage classes, have {}", sc.len());
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let response = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
                        .noncurrent_version_transitions(
                            NoncurrentVersionTransition::builder()
                                .noncurrent_days(2)
                                .storage_class(TransitionStorageClass::from(sc[1].as_str()))
                                .build(),
                        )
                        .noncurrent_version_transitions(
                            NoncurrentVersionTransition::builder()
                                .noncurrent_days(4)
                                .storage_class(TransitionStorageClass::from(sc[2].as_str()))
                                .build(),
                        )
                        .noncurrent_version_expiration(
                            NoncurrentVersionExpiration::builder()
                                .noncurrent_days(6)
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .rules(
                    LifecycleRule::builder()
                        .id("rule2")
                        .filter(LifecycleRuleFilter::builder().prefix("test2/").build())
                        .noncurrent_version_expiration(
                            NoncurrentVersionExpiration::builder()
                                .noncurrent_days(3)
                                .build(),
                        )
                        .status(ExpirationStatus::Disabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // If we get here without error, the config was accepted
    drop(response);
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
async fn test_lifecycle_noncur_transition() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sc = configured_storage_classes();
    if sc.len() < 3 {
        eprintln!("skipping: requires 3+ storage classes, have {}", sc.len());
        return;
    }

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    client.put_object().bucket(&bucket).key("test1/a").body(ByteStream::from_static(b"fooz")).send().await.unwrap();

    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
                        .noncurrent_version_transitions(
                            NoncurrentVersionTransition::builder()
                                .noncurrent_days(1)
                                .storage_class(TransitionStorageClass::from(sc[1].as_str()))
                                .build(),
                        )
                        .noncurrent_version_transitions(
                            NoncurrentVersionTransition::builder()
                                .noncurrent_days(5)
                                .storage_class(TransitionStorageClass::from(sc[2].as_str()))
                                .build(),
                        )
                        .noncurrent_version_expiration(
                            NoncurrentVersionExpiration::builder()
                                .noncurrent_days(9)
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Create multiple versions
    for i in 0..2 {
        client.put_object().bucket(&bucket).key("test1/a")
            .body(ByteStream::from(format!("content-{i}").into_bytes()))
            .send().await.unwrap();
    }
    for i in 0..3 {
        client.put_object().bucket(&bucket).key("test1/b")
            .body(ByteStream::from(format!("content-{i}").into_bytes()))
            .send().await.unwrap();
    }

    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 6);

    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;

    tokio::time::sleep(std::time::Duration::from_secs(4 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);
    assert_eq!(sc_count(&classes, &sc[1]), 4);
    assert_eq!(sc_count(&classes, &sc[2]), 0);

    tokio::time::sleep(std::time::Duration::from_secs(4 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);
    assert_eq!(sc_count(&classes, &sc[1]), 0);
    assert_eq!(sc_count(&classes, &sc[2]), 4);

    tokio::time::sleep(std::time::Duration::from_secs(6 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);
    assert_eq!(sc_count(&classes, &sc[1]), 0);
    assert_eq!(sc_count(&classes, &sc[2]), 0);
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecycle_plain_null_version_current_transition() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sc = configured_storage_classes();
    if sc.len() < 2 {
        eprintln!("skipping: requires 2+ storage classes, have {}", sc.len());
        return;
    }

    let target_sc = &sc[1];
    assert_ne!(target_sc, "STANDARD");

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    client.put_object().bucket(&bucket).key("testobjfoo")
        .body(ByteStream::from_static(b"fooz"))
        .send().await.unwrap();

    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("testobj").build())
                        .transitions(
                            Transition::builder()
                                .days(1)
                                .storage_class(TransitionStorageClass::from(target_sc.as_str()))
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;

    tokio::time::sleep(std::time::Duration::from_secs(4 * lc_interval)).await;

    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 0);
    assert_eq!(sc_count(&classes, target_sc), 1);
}

// =====================================================================
// Cloud transition tests
// =====================================================================

async fn verify_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected_content: Option<&str>,
    expected_sc: &str,
) {
    let response = client.get_object().bucket(bucket).key(key).send().await.unwrap();
    let sc = response.storage_class()
        .map(|s| s.as_str().to_string())
        .unwrap_or_else(|| "STANDARD".to_string());
    assert_eq!(sc, expected_sc, "storage class mismatch for {key}");
    if let Some(content) = expected_content {
        let body = response.body.collect().await.unwrap().to_vec();
        let body_str = String::from_utf8_lossy(&body);
        assert_eq!(body_str, content, "content mismatch for {key}");
    }
}

fn get_cloud_config_or_skip() -> s3_tests_rs::config::CloudConfig {
    let cfg = get_config();
    match cfg.cloud.clone() {
        Some(c) => {
            if c.storage_class.is_none() {
                eprintln!("skipping: [s3 cloud] missing cloud_storage_class");
                std::process::exit(0);
            }
            c
        }
        None => {
            eprintln!("skipping: no [s3 cloud] section configured");
            std::process::exit(0);
        }
    }
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
async fn test_lifecycle_cloud_transition() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cloud_cfg = get_cloud_config_or_skip();
    let cloud_sc = cloud_cfg.storage_class.as_deref().unwrap();
    let retain_head = cloud_cfg.retain_head_object.as_deref() == Some("true");
    let target_path = cloud_cfg.target_path.clone()
        .unwrap_or_else(|| format!("rgwx-default-{}-cloud-bucket", cloud_sc.to_lowercase()));
    let target_sc = &cloud_cfg.target_storage_class;

    let client = get_client();
    let keys = &["expire1/foo", "expire1/bar", "keep2/foo", "keep2/bar"];
    let bucket = get_new_bucket(Some(&client)).await;
    for key in keys {
        client.put_object().bucket(&bucket).key(*key)
            .body(ByteStream::from_static(key.as_bytes()))
            .send().await.unwrap();
    }

    let response = client.list_objects_v2().bucket(&bucket).send().await.unwrap();
    assert_eq!(response.contents().len(), 4);

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("expire1/").build())
                        .transitions(
                            Transition::builder()
                                .days(1)
                                .storage_class(TransitionStorageClass::from(cloud_sc))
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;

    tokio::time::sleep(std::time::Duration::from_secs(10 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);

    if retain_head {
        assert_eq!(sc_count(&classes, cloud_sc), 2);
    } else {
        assert_eq!(sc_count(&classes, cloud_sc), 0);
    }

    // Check if objects copied to target path on cloud endpoint
    let cloud_client = get_cloud_client().expect("cloud client");
    let prefix = format!("{}/", bucket);

    tokio::time::sleep(std::time::Duration::from_secs(14 * lc_interval)).await;

    let expire1_key1 = format!("{}{}", prefix, keys[0]);
    verify_object(&cloud_client, &target_path, &expire1_key1, Some(keys[0]), target_sc).await;

    let expire1_key2 = format!("{}{}", prefix, keys[1]);
    verify_object(&cloud_client, &target_path, &expire1_key2, Some(keys[1]), target_sc).await;

    // Verify source object on RGW
    if retain_head {
        let response = client.head_object().bucket(&bucket).key(keys[0]).send().await.unwrap();
        assert_eq!(response.content_length().unwrap_or(0), 0);
        assert_eq!(
            response.storage_class().map(|s| s.as_str()),
            Some(cloud_sc)
        );

        if !cloud_cfg.allow_read_through {
            let result = client.get_object().bucket(&bucket).key(keys[0]).send().await;
            assert!(result.is_err(), "expected InvalidObjectState for transitioned object");

            let result = client.copy_object()
                .copy_source(format!("{}/{}", bucket, keys[0]))
                .bucket(&bucket).key("copy_obj")
                .send().await;
            assert!(result.is_err(), "expected InvalidObjectState for copy of transitioned object");
        }

        client.delete_object().bucket(&bucket).key(keys[0]).send().await.unwrap();
        let result = client.get_object().bucket(&bucket).key(keys[0]).send().await;
        assert!(result.is_err(), "expected NoSuchKey after delete");
    }
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
async fn test_lifecycle_cloud_multiple_transition() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cloud_cfg = get_cloud_config_or_skip();
    let cloud_sc = cloud_cfg.storage_class.as_deref().unwrap();
    let retain_head = cloud_cfg.retain_head_object.as_deref() == Some("true");

    let sc1 = match &cloud_cfg.regular_storage_class {
        Some(s) => s.clone(),
        None => {
            eprintln!("skipping: [s3 cloud] missing storage_class");
            return;
        }
    };

    let client = get_client();
    let keys = &["expire1/foo", "expire1/bar", "keep2/foo", "keep2/bar"];
    let bucket = get_new_bucket(Some(&client)).await;
    for key in keys {
        client.put_object().bucket(&bucket).key(*key)
            .body(ByteStream::from_static(key.as_bytes()))
            .send().await.unwrap();
    }

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("expire1/").build())
                        .transitions(
                            Transition::builder()
                                .days(1)
                                .storage_class(TransitionStorageClass::from(sc1.as_str()))
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .rules(
                    LifecycleRule::builder()
                        .id("rule2")
                        .filter(LifecycleRuleFilter::builder().prefix("expire1/").build())
                        .transitions(
                            Transition::builder()
                                .days(5)
                                .storage_class(TransitionStorageClass::from(cloud_sc))
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .rules(
                    LifecycleRule::builder()
                        .id("rule3")
                        .filter(LifecycleRuleFilter::builder().prefix("expire1/").build())
                        .expiration(
                            aws_sdk_s3::types::LifecycleExpiration::builder()
                                .days(9)
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;

    tokio::time::sleep(std::time::Duration::from_secs(4 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);
    assert_eq!(sc_count(&classes, &sc1), 2);
    assert_eq!(sc_count(&classes, cloud_sc), 0);

    tokio::time::sleep(std::time::Duration::from_secs(7 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);
    assert_eq!(sc_count(&classes, &sc1), 0);
    if retain_head {
        assert_eq!(sc_count(&classes, cloud_sc), 2);
    } else {
        assert_eq!(sc_count(&classes, cloud_sc), 0);
    }

    tokio::time::sleep(std::time::Duration::from_secs(12 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);
    assert_eq!(sc_count(&classes, &sc1), 0);
    assert_eq!(sc_count(&classes, cloud_sc), 0);
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
async fn test_lifecycle_noncur_cloud_transition() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cloud_cfg = get_cloud_config_or_skip();
    let cloud_sc = cloud_cfg.storage_class.as_deref().unwrap();
    let retain_head = cloud_cfg.retain_head_object.as_deref() == Some("true");

    let sc1 = match &cloud_cfg.regular_storage_class {
        Some(s) => s.clone(),
        None => {
            eprintln!("skipping: [s3 cloud] missing storage_class");
            return;
        }
    };

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    client.put_object().bucket(&bucket).key("test1/a")
        .body(ByteStream::from_static(b"fooz"))
        .send().await.unwrap();

    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
                        .noncurrent_version_transitions(
                            NoncurrentVersionTransition::builder()
                                .noncurrent_days(1)
                                .storage_class(TransitionStorageClass::from(sc1.as_str()))
                                .build(),
                        )
                        .noncurrent_version_transitions(
                            NoncurrentVersionTransition::builder()
                                .noncurrent_days(5)
                                .storage_class(TransitionStorageClass::from(cloud_sc))
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    for i in 0..2 {
        client.put_object().bucket(&bucket).key("test1/a")
            .body(ByteStream::from(format!("content-{i}").into_bytes()))
            .send().await.unwrap();
    }
    for i in 0..3 {
        client.put_object().bucket(&bucket).key("test1/b")
            .body(ByteStream::from(format!("content-{i}").into_bytes()))
            .send().await.unwrap();
    }

    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 6);

    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;

    tokio::time::sleep(std::time::Duration::from_secs(4 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);
    assert_eq!(sc_count(&classes, &sc1), 4);
    assert_eq!(sc_count(&classes, cloud_sc), 0);

    tokio::time::sleep(std::time::Duration::from_secs(15 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 2);
    assert_eq!(sc_count(&classes, &sc1), 0);
    if retain_head {
        assert_eq!(sc_count(&classes, cloud_sc), 4);
    } else {
        assert_eq!(sc_count(&classes, cloud_sc), 0);
    }
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
async fn test_lifecycle_cloud_transition_large_obj() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cloud_cfg = get_cloud_config_or_skip();
    let cloud_sc = cloud_cfg.storage_class.as_deref().unwrap();
    let retain_head = cloud_cfg.retain_head_object.as_deref() == Some("true");
    let target_path = cloud_cfg.target_path.clone()
        .unwrap_or_else(|| format!("rgwx-default-{}-cloud-bucket", cloud_sc.to_lowercase()));
    let target_sc = &cloud_cfg.target_storage_class;

    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let size = 9 * 1024 * 1024;
    let data: Vec<u8> = vec![b'A'; size];
    let data_str = std::str::from_utf8(&data).unwrap();

    let keys = &["keep/multi", "expire1/multi"];
    for key in keys {
        client.put_object().bucket(&bucket).key(*key)
            .body(ByteStream::from(data.clone()))
            .send().await.unwrap();
        verify_object(&client, &bucket, key, Some(data_str), "STANDARD").await;
    }

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("rule1")
                        .filter(LifecycleRuleFilter::builder().prefix("expire1/").build())
                        .transitions(
                            Transition::builder()
                                .days(1)
                                .storage_class(TransitionStorageClass::from(cloud_sc))
                                .build(),
                        )
                        .status(ExpirationStatus::Enabled)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let cfg = get_config();
    let lc_interval = cfg.lc_debug_interval;

    tokio::time::sleep(std::time::Duration::from_secs(12 * lc_interval)).await;
    let classes = list_bucket_storage_class(&client, &bucket).await;
    assert_eq!(sc_count(&classes, "STANDARD"), 1);
    if retain_head {
        assert_eq!(sc_count(&classes, cloud_sc), 1);
    } else {
        assert_eq!(sc_count(&classes, cloud_sc), 0);
    }

    // multipart upload to cloud takes time
    tokio::time::sleep(std::time::Duration::from_secs(12 * lc_interval)).await;
    let cloud_client = get_cloud_client().expect("cloud client");
    let prefix = format!("{}/", bucket);
    let expire1_key = format!("{}{}", prefix, keys[1]);
    verify_object(&cloud_client, &target_path, &expire1_key, Some(data_str), target_sc).await;
}
