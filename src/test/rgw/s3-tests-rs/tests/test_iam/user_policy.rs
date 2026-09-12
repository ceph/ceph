use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::{get_alt_client, get_iam_client, get_policy_only_client, get_iam_s3client};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::get_new_bucket;
use s3_tests_rs::{assert_s3_err, expect_s3_err};

fn allow_all_policy() -> String {
    serde_json::to_string(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*"
        }
    }))
    .unwrap()
}

#[tokio::test]
async fn test_put_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();
    let user_name = &cfg.alt_user_id;

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    client
        .delete_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_put_user_policy_invalid_user() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();

    let result = client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllAccessPolicy")
        .user_name("some-non-existing-user-id")
        .send()
        .await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 404);
}

#[tokio::test]
async fn test_put_user_policy_parameter_limit() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();

    let big_policy = serde_json::to_string(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": (0..1000).map(|_| serde_json::json!({
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*"
        })).collect::<Vec<_>>()
    }))
    .unwrap();

    let result = client
        .put_user_policy()
        .policy_document(big_policy)
        .policy_name(&"AllAccessPolicy".repeat(10))
        .user_name(&cfg.alt_user_id)
        .send()
        .await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 400);
}

#[tokio::test]
async fn test_put_existing_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();
    let user_name = &cfg.alt_user_id;

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    // put again — should succeed (overwrite)
    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    client
        .delete_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_list_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();
    let user_name = &cfg.alt_user_id;

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    let response = client
        .list_user_policies()
        .user_name(user_name)
        .send()
        .await
        .unwrap();
    assert!(!response.policy_names().is_empty());

    client
        .delete_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_list_user_policy_invalid_user() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();

    let result = client
        .list_user_policies()
        .user_name("some-non-existing-user-id")
        .send()
        .await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 404);
}

#[tokio::test]
async fn test_get_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();
    let user_name = &cfg.alt_user_id;

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    client
        .get_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    client
        .delete_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_get_user_policy_invalid_user() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();
    let user_name = &cfg.alt_user_id;

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    let result = client
        .get_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name("some-non-existing-user-id")
        .send()
        .await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 404);

    client
        .delete_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_get_user_policy_from_multiple_policies() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();
    let user_name = &cfg.alt_user_id;

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllowAccessPolicy1")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllowAccessPolicy2")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    client
        .get_user_policy()
        .policy_name("AllowAccessPolicy2")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy1")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy2")
        .user_name(user_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_delete_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();
    let user_name = &cfg.alt_user_id;

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllowAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_delete_user_policy_invalid_user() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();
    let user_name = &cfg.alt_user_id;

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllowAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    let result = client
        .delete_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name("some-non-existing-user-id")
        .send()
        .await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 404);

    client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_delete_user_policy_invalid_policy_name() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_client();
    let cfg = get_config();
    let user_name = &cfg.alt_user_id;

    client
        .put_user_policy()
        .policy_document(allow_all_policy())
        .policy_name("AllowAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();

    let result = client
        .delete_user_policy()
        .policy_name("non-existing-policy-name")
        .user_name(user_name)
        .send()
        .await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 404);

    client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy")
        .user_name(user_name)
        .send()
        .await
        .unwrap();
}

// --- allow/deny action tests ---

#[tokio::test]
async fn test_allow_bucket_actions_in_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let s3_client_iam = get_iam_s3client();
    let s3_client_alt = get_alt_client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client_iam)).await;
    s3_client_iam
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let policy = serde_json::to_string(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:DeleteBucket"],
            "Resource": format!("arn:aws:s3:::{bucket}")
        }
    }))
    .unwrap();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let response = s3_client_alt.list_objects().bucket(&bucket).send().await.unwrap();
    assert!(!response.contents().is_empty());

    s3_client_iam
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await
        .unwrap();

    s3_client_alt.delete_bucket().bucket(&bucket).send().await.unwrap();

    iam_client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_deny_bucket_actions_in_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let s3_client_alt = get_alt_client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client_alt)).await;

    let policy = serde_json::to_string(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Deny",
            "Action": ["s3:ListAllMyBuckets", "s3:DeleteBucket"],
            "Resource": "arn:aws:s3:::*"
        }
    }))
    .unwrap();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let result = s3_client_alt.list_buckets().send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = s3_client_alt.delete_bucket().bucket(&bucket).send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    iam_client
        .delete_user_policy()
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    s3_client_alt.delete_bucket().bucket(&bucket).send().await.unwrap();
}

#[tokio::test]
async fn test_allow_object_actions_in_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let s3_client_iam = get_iam_s3client();
    let s3_client_alt = get_alt_client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client_iam)).await;

    let policy = serde_json::to_string(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
            "Resource": format!("arn:aws:s3:::{bucket}/*")
        }
    }))
    .unwrap();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    s3_client_alt
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let resp = s3_client_alt
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");

    s3_client_alt
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await
        .unwrap();

    s3_client_iam.delete_bucket().bucket(&bucket).send().await.unwrap();

    iam_client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_deny_object_actions_in_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let s3_client_alt = get_alt_client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client_alt)).await;
    s3_client_alt
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let policy = serde_json::to_string(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
            "Resource": format!("arn:aws:s3:::{bucket}/*")
        }, {
            "Effect": "Allow",
            "Action": ["s3:DeleteBucket"],
            "Resource": format!("arn:aws:s3:::{bucket}")
        }]
    }))
    .unwrap();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let result = s3_client_alt
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"baz"))
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = s3_client_alt.get_object().bucket(&bucket).key("foo").send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = s3_client_alt.delete_object().bucket(&bucket).key("foo").send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    iam_client
        .delete_user_policy()
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[ignore = "VERIFY: RGW accepts invalid policy version string (also fails in Python)"]
#[tokio::test]
async fn test_put_user_policy_invalid_element() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let cfg = get_config();

    // Wrong version
    let policy = serde_json::json!({
        "Version": "2010-10-17",
        "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]
    })
    .to_string();
    let result = iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("AllAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await;
    assert!(result.is_err());

    // No Statement
    let policy = serde_json::json!({"Version": "2012-10-17"}).to_string();
    let result = iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("AllAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await;
    assert!(result.is_err());

    // Duplicate Sid
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": "98AB54CF", "Effect": "Allow", "Action": "*", "Resource": "*"},
            {"Sid": "98AB54CF", "Effect": "Allow", "Action": "*", "Resource": "*"}
        ]
    })
    .to_string();
    let result = iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("AllAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await;
    assert!(result.is_err());

    // With Principal (not allowed in identity policies)
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*",
            "Principal": "arn:aws:iam:::username"
        }]
    })
    .to_string();
    let result = iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("AllAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_user_policy_invalid_policy_name() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let cfg = get_config();

    iam_client
        .put_user_policy()
        .policy_document(&allow_all_policy())
        .policy_name("AllAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let result = iam_client
        .get_user_policy()
        .policy_name("non-existing-policy-name")
        .user_name(&cfg.alt_user_id)
        .send()
        .await;
    assert!(result.is_err());

    iam_client
        .delete_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[tokio::test]
async fn test_get_deleted_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let cfg = get_config();

    iam_client
        .put_user_policy()
        .policy_document(&allow_all_policy())
        .policy_name("AllAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    iam_client
        .delete_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let result = iam_client
        .get_user_policy()
        .policy_name("AllAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_delete_user_policy_from_multiple_policies() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let cfg = get_config();

    for name in ["AllowAccessPolicy1", "AllowAccessPolicy2", "AllowAccessPolicy3"] {
        iam_client
            .put_user_policy()
            .policy_document(&allow_all_policy())
            .policy_name(name)
            .user_name(&cfg.alt_user_id)
            .send()
            .await
            .unwrap();
    }

    iam_client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy1")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    iam_client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy2")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    iam_client
        .get_user_policy()
        .policy_name("AllowAccessPolicy3")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    iam_client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy3")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_allow_multipart_actions_in_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let s3_client_alt = get_alt_client();
    let s3_client_iam = get_iam_s3client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client_iam)).await;
    let key = "mymultipart";

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Action": ["s3:PutObject"],
            "Resource": format!("arn:aws:s3:::{bucket}/*")
        }
    })
    .to_string();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let mp = s3_client_alt
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = mp.upload_id().unwrap();

    let part = s3_client_alt
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(vec![b'a'; 5 * 1024 * 1024]))
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

    s3_client_alt
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .unwrap();

    s3_client_iam
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();

    iam_client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_deny_multipart_actions_in_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let s3_client_alt = get_alt_client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client_alt)).await;
    let key = "mymultipart";

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Deny",
            "Action": ["s3:PutObject"],
            "Resource": format!("arn:aws:s3:::{bucket}/*")
        }
    })
    .to_string();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let result = s3_client_alt
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    iam_client
        .delete_user_policy()
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_allow_tagging_actions_in_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let s3_client_alt = get_alt_client();
    let s3_client_iam = get_iam_s3client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client_iam)).await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Action": ["s3:PutBucketTagging", "s3:GetBucketTagging",
                       "s3:PutObjectTagging", "s3:GetObjectTagging"],
            "Resource": "arn:aws:s3:::*"
        }
    })
    .to_string();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("Hello")
                .value("World")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    s3_client_alt
        .put_bucket_tagging()
        .bucket(&bucket)
        .tagging(tags.clone())
        .send()
        .await
        .unwrap();

    let response = s3_client_alt
        .get_bucket_tagging()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set()[0].key(), "Hello");
    assert_eq!(response.tag_set()[0].value(), "World");

    let obj_key = "obj";
    s3_client_iam
        .put_object()
        .bucket(&bucket)
        .key(obj_key)
        .body(ByteStream::from_static(b"obj_body"))
        .send()
        .await
        .unwrap();

    s3_client_alt
        .put_object_tagging()
        .bucket(&bucket)
        .key(obj_key)
        .tagging(tags)
        .send()
        .await
        .unwrap();

    let response = s3_client_alt
        .get_object_tagging()
        .bucket(&bucket)
        .key(obj_key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set()[0].key(), "Hello");
    assert_eq!(response.tag_set()[0].value(), "World");

    s3_client_iam
        .delete_object()
        .bucket(&bucket)
        .key(obj_key)
        .send()
        .await
        .unwrap();

    iam_client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_deny_tagging_actions_in_user_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let s3_client_alt = get_alt_client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client_alt)).await;

    /* Scoped to this test's own bucket, not arn:aws:s3:::*.
     *
     * The policy is attached to the *shared* alt user, so a wildcard
     * resource denies tagging for every other test using that identity
     * while it is attached -- and an explicit deny in a user policy
     * overrides a bucket policy allow, which is exactly what those tests
     * rely on.  That made tagging::test_put_tags_acl_public and
     * tagging::test_delete_tags_obj_public fail intermittently under
     * parallel runs, whichever happened to overlap this window.
     *
     * Both forms are needed:  the bare bucket ARN for the bucket-level
     * actions, and the wildcard-suffixed one for the object-level
     * actions. */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Deny",
            "Action": ["s3:PutBucketTagging", "s3:GetBucketTagging",
                       "s3:PutObjectTagging", "s3:DeleteObjectTagging"],
            "Resource": [format!("arn:aws:s3:::{bucket}"),
                         format!("arn:aws:s3:::{bucket}/*")]
        }
    })
    .to_string();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("Hello")
                .value("World")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let result = s3_client_alt
        .put_bucket_tagging()
        .bucket(&bucket)
        .tagging(tags.clone())
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = s3_client_alt
        .get_bucket_tagging()
        .bucket(&bucket)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let obj_key = "obj";
    s3_client_alt
        .put_object()
        .bucket(&bucket)
        .key(obj_key)
        .body(ByteStream::from_static(b"obj_body"))
        .send()
        .await
        .unwrap();

    let result = s3_client_alt
        .put_object_tagging()
        .bucket(&bucket)
        .key(obj_key)
        .tagging(tags)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = s3_client_alt
        .delete_object_tagging()
        .bucket(&bucket)
        .key(obj_key)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    iam_client
        .delete_user_policy()
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_deny_tagging_actions_wildcard_resource() {
    /* The sibling above scopes its Deny to its own bucket, so it can run
     * beside anything.  This one keeps Resource arn:aws:s3:::* , which is
     * the case worth covering separately -- that a wildcard resource in a
     * user policy matches at all.
     *
     * It therefore runs against the policy-only user, whose permissions
     * nothing else depends on.  Attaching this to the shared alt user is
     * what made the tagging tests fail intermittently:  an explicit deny
     * in an identity policy overrides a bucket-policy allow, so any
     * concurrent test tagging as that identity got a correct 403 that
     * looked like a driver bug. */
    if !s3_tests_rs::client::policy_only_configured() {
        eprintln!("skipping: no [s3 policy only] user configured");
        return;
    }

    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_client();
    let s3_client = get_policy_only_client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client)).await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Deny",
            "Action": ["s3:PutBucketTagging", "s3:GetBucketTagging",
                       "s3:PutObjectTagging", "s3:DeleteObjectTagging"],
            "Resource": "arn:aws:s3:::*"
        }
    })
    .to_string();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("DenyTaggingWildcard")
        .user_name(&cfg.policy_only_user_id)
        .send()
        .await
        .unwrap();

    let tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("Hello")
                .value("World")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let result = s3_client
        .put_bucket_tagging()
        .bucket(&bucket)
        .tagging(tags)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = s3_client
        .get_bucket_tagging()
        .bucket(&bucket)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    iam_client
        .delete_user_policy()
        .policy_name("DenyTaggingWildcard")
        .user_name(&cfg.policy_only_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_verify_conflicting_user_policy_statements() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let s3_client = get_alt_client();
    let iam_client = get_iam_client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client)).await;
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": "98AB54CG", "Effect": "Allow", "Action": "s3:ListBucket",
             "Resource": format!("arn:aws:s3:::{bucket}")},
            {"Sid": "98AB54CA", "Effect": "Deny", "Action": "s3:ListBucket",
             "Resource": format!("arn:aws:s3:::{bucket}")}
        ]
    })
    .to_string();

    iam_client
        .put_user_policy()
        .policy_document(&policy)
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let result = s3_client
        .list_objects()
        .bucket(&bucket)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    iam_client
        .delete_user_policy()
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_verify_conflicting_user_policies() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let s3_client = get_alt_client();
    let iam_client = get_iam_client();
    let cfg = get_config();

    let bucket = get_new_bucket(Some(&s3_client)).await;

    let policy_allow = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Sid": "98AB54CG", "Effect": "Allow", "Action": "s3:ListBucket",
                      "Resource": format!("arn:aws:s3:::{bucket}")}
    })
    .to_string();
    let policy_deny = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Sid": "98AB54CGZ", "Effect": "Deny", "Action": "s3:ListBucket",
                      "Resource": format!("arn:aws:s3:::{bucket}")}
    })
    .to_string();

    iam_client
        .put_user_policy()
        .policy_document(&policy_allow)
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
    iam_client
        .put_user_policy()
        .policy_document(&policy_deny)
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();

    let result = s3_client
        .list_objects()
        .bucket(&bucket)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    iam_client
        .delete_user_policy()
        .policy_name("AllowAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
    iam_client
        .delete_user_policy()
        .policy_name("DenyAccessPolicy")
        .user_name(&cfg.alt_user_id)
        .send()
        .await
        .unwrap();
}
