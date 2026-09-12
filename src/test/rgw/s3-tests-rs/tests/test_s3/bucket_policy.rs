use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::{get_alt_client, get_client, get_iam_root_client, get_iam_root_s3client, build_s3_client, get_unauthenticated_client};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{create_objects_in_new_bucket, get_new_bucket};
use s3_tests_rs::policy::{make_arn_resource, make_json_policy};
use s3_tests_rs::assert_s3_err;
use serde_json::json;

async fn get_body(response: aws_sdk_s3::operation::get_object::GetObjectOutput) -> String {
    let bytes = response.body.collect().await.unwrap().into_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn test_bucket_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("asdf")
        .body(ByteStream::from_static(b"asdf"))
        .send()
        .await
        .unwrap();

    let resource1 = format!("arn:aws:s3:::{bucket_name}");
    let resource2 = format!("arn:aws:s3:::{bucket_name}/*");
    let policy_document = serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2]
        }]
    }))
    .unwrap();

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    let response = alt_client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 1);
}

#[tokio::test]
async fn test_bucket_policy_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("asdf")
        .body(ByteStream::from_static(b"asdf"))
        .send()
        .await
        .unwrap();

    let resource1 = format!("arn:aws:s3:::{bucket_name}");
    let resource2 = format!("arn:aws:s3:::{bucket_name}/*");
    let policy_document = serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2]
        }]
    }))
    .unwrap();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::AuthenticatedRead)
        .send()
        .await
        .unwrap();

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    let result = alt_client.list_objects().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    client
        .delete_bucket_policy()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bucket_policy_another_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket1 = get_new_bucket(Some(&client)).await;
    let bucket2 = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket1)
        .key("asdf")
        .body(ByteStream::from_static(b"asdf"))
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket2)
        .key("abcd")
        .body(ByteStream::from_static(b"abcd"))
        .send()
        .await
        .unwrap();

    let policy_document = serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": ["arn:aws:s3:::*", "arn:aws:s3:::*/*"]
        }]
    }))
    .unwrap();

    client
        .put_bucket_policy()
        .bucket(&bucket1)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_policy()
        .bucket(&bucket1)
        .send()
        .await
        .unwrap();
    let response_policy = response.policy().unwrap_or_default().to_string();

    client
        .put_bucket_policy()
        .bucket(&bucket2)
        .policy(&response_policy)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    let response = alt_client.list_objects().bucket(&bucket1).send().await.unwrap();
    assert_eq!(response.contents().len(), 1);

    let response = alt_client.list_objects().bucket(&bucket2).send().await.unwrap();
    assert_eq!(response.contents().len(), 1);
}

#[ignore = "VERIFY: also fails in Python s3-tests on this cluster — RGW tag-conditional policy"]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_policy_get_obj_existing_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["publictag", "privatetag", "invalidtag"]).await;
    let client = get_client();

    let tag_conditional = json!({
        "StringEquals": {
            "s3:ExistingObjectTag/security": "public"
        }
    });
    let resource = make_arn_resource(&format!("{bucket_name}/*"));
    let policy_document = make_json_policy("s3:GetObject", &resource, None, None, Some(tag_conditional));

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    // allow policy to propagate
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let public_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security").value("public").build().unwrap())
        .tag_set(aws_sdk_s3::types::Tag::builder().key("foo").value("bar").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key("publictag")
        .tagging(public_tags)
        .send()
        .await
        .unwrap();

    let private_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security").value("private").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key("privatetag")
        .tagging(private_tags)
        .send()
        .await
        .unwrap();

    let invalid_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security1").value("public").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key("invalidtag")
        .tagging(invalid_tags)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("publictag")
        .send()
        .await
        .unwrap();

    let result = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("privatetag")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("invalidtag")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[tokio::test]
async fn test_get_publicpolicy_acl_bucket_policy_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(!resp.policy_status().unwrap().is_public().unwrap_or(true));

    let resource1 = format!("arn:aws:s3:::{bucket_name}");
    let resource2 = format!("arn:aws:s3:::{bucket_name}/*");
    let policy_document = serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2]
        }]
    }))
    .unwrap();

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.policy_status().unwrap().is_public().unwrap_or(false));
}

#[tokio::test]
async fn test_bucketv2_policy_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("asdf")
        .body(ByteStream::from_static(b"asdf"))
        .send()
        .await
        .unwrap();

    let resource1 = format!("arn:aws:s3:::{bucket_name}");
    let resource2 = format!("arn:aws:s3:::{bucket_name}/*");
    let policy_document = serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2]
        }]
    }))
    .unwrap();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::AuthenticatedRead)
        .send()
        .await
        .unwrap();

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    let result = alt_client.list_objects_v2().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    client
        .delete_bucket_policy()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bucket_policy_multipart() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let alt_client = get_alt_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mpobj";

    let result = alt_client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await;
    assert!(result.is_err());

    let policy_document = serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:PutObject",
            "Resource": format!("arn:aws:s3:::{bucket_name}")
        }]
    }))
    .unwrap();
    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let result = alt_client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await;
    assert!(result.is_err());

    let policy_document = serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:PutObject",
            "Resource": format!("arn:aws:s3:::{bucket_name}/{key}")
        }]
    }))
    .unwrap();
    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    alt_client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_policy_put_obj_copy_source() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let keys = &["public/foo", "public/bar", "private/foo"];
    let bucket_name = create_objects_in_new_bucket(keys).await;
    let client = get_client();

    let src_resource = make_arn_resource(&format!("{bucket_name}/*"));
    let policy_document = make_json_policy("s3:GetObject", &src_resource, None, None, None);

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let bucket_name2 = get_new_bucket(Some(&client)).await;

    let tag_conditional = json!({
        "StringLike": {
            "s3:x-amz-copy-source": format!("{bucket_name}/public/*")
        }
    });
    let resource = make_arn_resource(&format!("{bucket_name2}/*"));
    let policy_document =
        make_json_policy("s3:PutObject", &resource, None, None, Some(tag_conditional));

    client
        .put_bucket_policy()
        .bucket(&bucket_name2)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();

    alt_client
        .copy_object()
        .bucket(&bucket_name2)
        .key("new_foo")
        .copy_source(format!("{bucket_name}/public/foo"))
        .send()
        .await
        .unwrap();

    let response = alt_client
        .get_object()
        .bucket(&bucket_name2)
        .key("new_foo")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "public/foo");

    alt_client
        .copy_object()
        .bucket(&bucket_name2)
        .key("new_foo2")
        .copy_source(format!("{bucket_name}/public/bar"))
        .send()
        .await
        .unwrap();

    let response = alt_client
        .get_object()
        .bucket(&bucket_name2)
        .key("new_foo2")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "public/bar");

    let result = alt_client
        .copy_object()
        .bucket(&bucket_name2)
        .key("new_foo2")
        .copy_source(format!("{bucket_name}/private/foo"))
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_bucket_policy_allow_notprincipal() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resource1 = format!("arn:aws:s3:::{bucket_name}");
    let resource2 = format!("arn:aws:s3:::{bucket_name}/*");
    let policy_document = serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "NotPrincipal": {"AWS": "arn:aws:iam::s3tenant1:root"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2]
        }]
    }))
    .unwrap();

    let result = client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await;
    assert!(result.is_err());
}

// --- public access block tests ---

#[tokio::test]
async fn test_put_public_block() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let access_conf = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .ignore_public_acls(true)
        .block_public_policy(true)
        .restrict_public_buckets(false)
        .build();

    client
        .put_public_access_block()
        .bucket(&bucket_name)
        .public_access_block_configuration(access_conf)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_public_access_block()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let conf = resp.public_access_block_configuration().unwrap();
    assert!(conf.block_public_acls().unwrap_or(false));
    assert!(conf.ignore_public_acls().unwrap_or(false));
    assert!(conf.block_public_policy().unwrap_or(false));
    assert!(!conf.restrict_public_buckets().unwrap_or(true));
}

#[tokio::test]
async fn test_get_undefined_public_block() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let _ = client
        .delete_public_access_block()
        .bucket(&bucket_name)
        .send()
        .await;

    let result = client
        .get_public_access_block()
        .bucket(&bucket_name)
        .send()
        .await;
    assert!(result.is_err());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_block_public_put_bucket_acls() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let access_conf = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .ignore_public_acls(false)
        .block_public_policy(true)
        .restrict_public_buckets(false)
        .build();

    client
        .put_public_access_block()
        .bucket(&bucket_name)
        .public_access_block_configuration(access_conf)
        .send()
        .await
        .unwrap();

    let result = client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await;
    assert!(result.is_err());

    let result = client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicReadWrite)
        .send()
        .await;
    assert!(result.is_err());

    let result = client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::AuthenticatedRead)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_block_public_object_canned_acls() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let access_conf = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .ignore_public_acls(false)
        .block_public_policy(false)
        .restrict_public_buckets(false)
        .build();

    client
        .put_public_access_block()
        .bucket(&bucket_name)
        .public_access_block_configuration(access_conf)
        .send()
        .await
        .unwrap();

    let result = client
        .put_object()
        .bucket(&bucket_name)
        .key("foo1")
        .body(ByteStream::from_static(b""))
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .send()
        .await;
    assert!(result.is_err());

    let result = client
        .put_object()
        .bucket(&bucket_name)
        .key("foo2")
        .body(ByteStream::from_static(b""))
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicReadWrite)
        .send()
        .await;
    assert!(result.is_err());

    let result = client
        .put_object()
        .bucket(&bucket_name)
        .key("foo3")
        .body(ByteStream::from_static(b""))
        .acl(aws_sdk_s3::types::ObjectCannedAcl::AuthenticatedRead)
        .send()
        .await;
    assert!(result.is_err());

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo4")
        .body(ByteStream::from_static(b""))
        .acl(aws_sdk_s3::types::ObjectCannedAcl::Private)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_block_public_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let access_conf = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
        .block_public_acls(false)
        .ignore_public_acls(false)
        .block_public_policy(true)
        .restrict_public_buckets(false)
        .build();

    client
        .put_public_access_block()
        .bucket(&bucket_name)
        .public_access_block_configuration(access_conf)
        .send()
        .await
        .unwrap();

    let resource = make_arn_resource(&format!("{bucket_name}/*"));
    let policy_document = make_json_policy("s3:GetObject", &resource, None, None, None);

    let result = client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_block_public_policy_with_principal() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let access_conf = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
        .block_public_acls(false)
        .ignore_public_acls(false)
        .block_public_policy(true)
        .restrict_public_buckets(false)
        .build();

    client
        .put_public_access_block()
        .bucket(&bucket_name)
        .public_access_block_configuration(access_conf)
        .send()
        .await
        .unwrap();

    let resource = make_arn_resource(&format!("{bucket_name}/*"));
    let policy_document = make_json_policy(
        "s3:GetObject",
        &resource,
        Some(serde_json::json!({"AWS": "arn:aws:iam::s3tenant1:root"})),
        None,
        None,
    );

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();
}

// --- bucket ownership controls ---

#[tokio::test]
async fn test_create_bucket_no_ownership_controls() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let result = client
        .get_bucket_ownership_controls()
        .bucket(&bucket_name)
        .send()
        .await;
    assert_s3_err!(result, 404, "OwnershipControlsNotFoundError");
}

#[tokio::test]
async fn test_bucket_create_delete_bucket_ownership() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let ownership = aws_sdk_s3::types::OwnershipControls::builder()
        .rules(
            aws_sdk_s3::types::OwnershipControlsRule::builder()
                .object_ownership(aws_sdk_s3::types::ObjectOwnership::BucketOwnerEnforced)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    client
        .put_bucket_ownership_controls()
        .bucket(&bucket_name)
        .ownership_controls(ownership)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_ownership_controls()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let rules = resp.ownership_controls().unwrap().rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(
        rules[0].object_ownership().as_str(),
        "BucketOwnerEnforced"
    );

    client
        .delete_bucket_ownership_controls()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let result = client
        .get_bucket_ownership_controls()
        .bucket(&bucket_name)
        .send()
        .await;
    assert!(result.is_err());

    client
        .delete_bucket_ownership_controls()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_get_public_block_deny_bucket_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let access_conf = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .ignore_public_acls(true)
        .block_public_policy(true)
        .restrict_public_buckets(false)
        .build();

    client
        .put_public_access_block()
        .bucket(&bucket_name)
        .public_access_block_configuration(access_conf)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_public_access_block()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let conf = resp.public_access_block_configuration().unwrap();
    assert!(conf.block_public_acls().unwrap_or(false));
    assert!(conf.block_public_policy().unwrap_or(false));

    let resource = make_arn_resource(&bucket_name);
    let policy_document =
        make_json_policy("s3:GetBucketPublicAccessBlock", &resource, None, Some("Deny"), None);

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let result = client
        .get_public_access_block()
        .bucket(&bucket_name)
        .send()
        .await;
    assert!(result.is_err());
}

// --- bucket policy status tests ---

#[tokio::test]
async fn test_get_bucket_policy_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(!resp.policy_status().unwrap().is_public().unwrap_or(true));
}

#[tokio::test]
async fn test_get_public_acl_bucket_policy_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.policy_status().unwrap().is_public().unwrap_or(false));
}

#[tokio::test]
async fn test_get_authpublic_acl_bucket_policy_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::AuthenticatedRead)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.policy_status().unwrap().is_public().unwrap_or(false));
}

#[tokio::test]
async fn test_set_get_del_bucket_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "asdf";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"asdf"))
        .send()
        .await
        .unwrap();

    let resource1 = make_arn_resource(&bucket_name);
    let resource2 = make_arn_resource(&format!("{bucket_name}/*"));
    let policy_document = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2]
        }]
    })
    .to_string();

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy_document)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_policy()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let returned_policy = response.policy().unwrap_or_default();
    let expected: serde_json::Value = serde_json::from_str(&policy_document).unwrap();
    let actual: serde_json::Value = serde_json::from_str(returned_policy).unwrap();
    assert_eq!(expected, actual);

    client
        .delete_bucket_policy()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let result = client
        .get_bucket_policy()
        .bucket(&bucket_name)
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchBucketPolicy");
}

#[tokio::test]
async fn test_block_public_restrict_public_buckets() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .delete_public_access_block()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let resource = make_arn_resource(&format!("{bucket_name}/*"));
    let policy = make_json_policy("s3:GetObject", &resource, None, None, None);
    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let unauth_client = get_unauthenticated_client();
    let resp = unauth_client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");

    let access_conf = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
        .block_public_acls(false)
        .ignore_public_acls(false)
        .block_public_policy(false)
        .restrict_public_buckets(true)
        .build();
    client
        .put_public_access_block()
        .bucket(&bucket_name)
        .public_access_block_configuration(access_conf)
        .send()
        .await
        .unwrap();

    let result = unauth_client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await;
    assert!(result.is_err());

    client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_ignore_public_acls() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let alt_client = get_alt_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    alt_client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket_name)
        .key("key1")
        .body(ByteStream::from_static(b"abcde"))
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let resp = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("key1")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"abcde");

    let access_conf = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
        .block_public_acls(false)
        .ignore_public_acls(true)
        .block_public_policy(false)
        .restrict_public_buckets(false)
        .build();
    client
        .put_public_access_block()
        .bucket(&bucket_name)
        .public_access_block_configuration(access_conf)
        .send()
        .await
        .unwrap();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let result = alt_client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await;
    assert!(result.is_err());

    let result = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("key1")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_put_get_delete_public_block() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let access_conf = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .ignore_public_acls(true)
        .block_public_policy(true)
        .restrict_public_buckets(false)
        .build();
    client
        .put_public_access_block()
        .bucket(&bucket_name)
        .public_access_block_configuration(access_conf)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_public_access_block()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let conf = resp.public_access_block_configuration().unwrap();
    assert!(conf.block_public_acls().unwrap_or(false));
    assert!(conf.ignore_public_acls().unwrap_or(false));
    assert!(conf.block_public_policy().unwrap_or(false));
    assert!(!conf.restrict_public_buckets().unwrap_or(true));

    client
        .delete_public_access_block()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let result = client
        .get_public_access_block()
        .bucket(&bucket_name)
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchPublicAccessBlockConfiguration");
}

async fn setup_tags_for_policy_test(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
) {
    let public_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security").value("public").build().unwrap())
        .tag_set(aws_sdk_s3::types::Tag::builder().key("foo").value("bar").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(bucket_name)
        .key("publictag")
        .tagging(public_tags)
        .send()
        .await
        .unwrap();

    let private_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security").value("private").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(bucket_name)
        .key("privatetag")
        .tagging(private_tags)
        .send()
        .await
        .unwrap();

    let invalid_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security1").value("public").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(bucket_name)
        .key("invalidtag")
        .tagging(invalid_tags)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_policy_get_obj_tagging_existing_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["publictag", "privatetag", "invalidtag"]).await;
    let client = get_client();

    let tag_condition = json!({"StringEquals": {"s3:ExistingObjectTag/security": "public"}});
    let resource = make_arn_resource(&format!("{bucket_name}/*"));
    let policy = make_json_policy(
        "s3:GetObjectTagging",
        &resource,
        None,
        None,
        Some(tag_condition),
    );
    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    setup_tags_for_policy_test(&client, &bucket_name).await;

    let alt_client = get_alt_client();
    alt_client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key("publictag")
        .send()
        .await
        .unwrap();

    let result = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("publictag")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = alt_client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key("privatetag")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = alt_client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key("invalidtag")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_policy_get_obj_acl_existing_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["publictag", "privatetag", "invalidtag"]).await;
    let client = get_client();

    let tag_condition = json!({"StringEquals": {"s3:ExistingObjectTag/security": "public"}});
    let resource = make_arn_resource(&format!("{bucket_name}/*"));
    let policy = make_json_policy(
        "s3:GetObjectAcl",
        &resource,
        None,
        None,
        Some(tag_condition),
    );
    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    setup_tags_for_policy_test(&client, &bucket_name).await;

    let alt_client = get_alt_client();
    alt_client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("publictag")
        .send()
        .await
        .unwrap();

    let result = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("publictag")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = alt_client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("privatetag")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = alt_client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("invalidtag")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[tokio::test]
async fn test_bucketv2_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("asdf")
        .body(ByteStream::from_static(b"asdf"))
        .send()
        .await
        .unwrap();

    let resource1 = make_arn_resource(&bucket_name);
    let resource2 = make_arn_resource(&format!("{bucket_name}/*"));
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2]
        }]
    })
    .to_string();

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    let response = alt_client
        .list_objects_v2()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 1);
}

#[tokio::test]
async fn test_bucketv2_policy_another_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let bucket_name2 = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("asdf")
        .body(ByteStream::from_static(b"asdf"))
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket_name2)
        .key("abcd")
        .body(ByteStream::from_static(b"abcd"))
        .send()
        .await
        .unwrap();

    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": ["arn:aws:s3:::*", "arn:aws:s3:::*/*"]
        }]
    })
    .to_string();

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_policy()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let response_policy = response.policy().unwrap_or_default().to_string();

    client
        .put_bucket_policy()
        .bucket(&bucket_name2)
        .policy(&response_policy)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    let response = alt_client
        .list_objects_v2()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 1);

    let response = alt_client
        .list_objects_v2()
        .bucket(&bucket_name2)
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 1);
}

#[tokio::test]
async fn test_get_nonpublicpolicy_acl_bucket_policy_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(!resp.policy_status().unwrap().is_public().unwrap_or(true));

    let resource1 = make_arn_resource(&bucket_name);
    let resource2 = make_arn_resource(&format!("{bucket_name}/*"));
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2],
            "Condition": {
                "IpAddress": {"aws:SourceIp": "10.0.0.0/32"}
            }
        }]
    })
    .to_string();

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(!resp.policy_status().unwrap().is_public().unwrap_or(true));
}

#[tokio::test]
async fn test_get_nonpublicpolicy_principal_bucket_policy_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resource1 = make_arn_resource(&bucket_name);
    let resource2 = make_arn_resource(&format!("{bucket_name}/*"));
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::s3tenant1:root"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2]
        }]
    })
    .to_string();

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(!resp.policy_status().unwrap().is_public().unwrap_or(true));
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_policy_put_obj_tagging_existing_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["publictag", "privatetag"]).await;
    let client = get_client();

    let tag_condition = json!({"StringEquals": {"s3:ExistingObjectTag/security": "public"}});
    let resource = make_arn_resource(&format!("{bucket_name}/*"));
    let policy = make_json_policy(
        "s3:PutObjectTagging",
        &resource,
        None,
        None,
        Some(tag_condition),
    );
    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let public_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security").value("public").build().unwrap())
        .tag_set(aws_sdk_s3::types::Tag::builder().key("foo").value("bar").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key("publictag")
        .tagging(public_tags)
        .send()
        .await
        .unwrap();

    let private_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security").value("private").build().unwrap())
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key("privatetag")
        .tagging(private_tags)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();

    let new_public_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security").value("public").build().unwrap())
        .tag_set(aws_sdk_s3::types::Tag::builder().key("foo").value("bar").build().unwrap())
        .build()
        .unwrap();
    alt_client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key("publictag")
        .tagging(new_public_tags.clone())
        .send()
        .await
        .unwrap();

    let result = alt_client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key("privatetag")
        .tagging(new_public_tags)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let override_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security").value("private").build().unwrap())
        .build()
        .unwrap();
    alt_client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key("publictag")
        .tagging(override_tags)
        .send()
        .await
        .unwrap();

    let retry_tags = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("security").value("public").build().unwrap())
        .tag_set(aws_sdk_s3::types::Tag::builder().key("foo").value("bar").build().unwrap())
        .build()
        .unwrap();
    let result = alt_client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key("publictag")
        .tagging(retry_tags)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[ignore = "requires SSE configuration"]
#[tokio::test]
async fn test_bucket_policy_put_obj_s3_noenc() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resource = make_arn_resource(&format!("{bucket_name}/*"));
    let deny_condition = json!({"Null": {"s3:x-amz-server-side-encryption": "true"}});
    let policy = make_json_policy(
        "s3:PutObject",
        &resource,
        None,
        Some("Deny"),
        Some(deny_condition),
    );

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let result = client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"testobj"))
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"testobj"))
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: lifecycle not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_lifecyclev2_expiration() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let rules = vec![
        aws_sdk_s3::types::LifecycleRule::builder()
            .id("rule1")
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(1)
                    .build(),
            )
            .filter(aws_sdk_s3::types::LifecycleRuleFilter::builder().prefix("test1/").build())
            .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
            .build()
            .unwrap(),
        aws_sdk_s3::types::LifecycleRule::builder()
            .id("rule2")
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(2)
                    .build(),
            )
            .filter(aws_sdk_s3::types::LifecycleRuleFilter::builder().prefix("test2/").build())
            .status(aws_sdk_s3::types::ExpirationStatus::Disabled)
            .build()
            .unwrap(),
    ];

    let lifecycle = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .lifecycle_configuration(lifecycle)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let rules = response.rules();
    assert_eq!(rules.len(), 2);
}

// VERIFY: RGW rejects Allow+NotPrincipal ("Allow with NotPrincipal is not allowed")
#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[cfg_attr(feature = "fails_on_posix", ignore = "rgw core: Allow+NotPrincipal rejected")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "rgw core: Allow+NotPrincipal rejected")]
#[tokio::test]
async fn test_get_nonpublicpolicy_deny_bucket_policy_status() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.policy_status().unwrap().is_public(), Some(false));

    let resource1 = format!("arn:aws:s3:::{bucket_name}");
    let resource2 = format!("arn:aws:s3:::{bucket_name}/*");
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "NotPrincipal": {"AWS": "arn:aws:iam::s3tenant1:root"},
            "Action": "s3:ListBucket",
            "Resource": [resource1, resource2],
        }]
    });

    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(policy.to_string())
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_policy_status()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.policy_status().unwrap().is_public(), Some(true));
}

#[ignore = "RGW returns 411 MissingContentLength on upload_part_copy"]
#[tokio::test]
async fn test_bucket_policy_upload_part_copy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = create_objects_in_new_bucket(&["public/foo", "public/bar", "private/foo"]).await;

    let resource = make_arn_resource(&format!("{}/public/*", bucket));
    let policy = make_json_policy("s3:GetObject", &resource, None, None, None);
    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    let bucket2 = get_new_bucket(Some(&alt_client)).await;

    // copy public/foo via upload_part_copy
    let copy_source = format!("{}/public/foo", bucket);
    let mpu = alt_client
        .create_multipart_upload()
        .bucket(&bucket2)
        .key("new_foo")
        .send()
        .await
        .unwrap();
    let upload_id = mpu.upload_id().unwrap().to_string();

    let copy_resp = alt_client
        .upload_part_copy()
        .bucket(&bucket2)
        .key("new_foo")
        .part_number(1)
        .upload_id(&upload_id)
        .copy_source(&copy_source)
        .send()
        .await
        .unwrap();

    let etag = copy_resp
        .copy_part_result()
        .and_then(|r| r.e_tag())
        .unwrap();
    alt_client
        .complete_multipart_upload()
        .bucket(&bucket2)
        .key("new_foo")
        .upload_id(&upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(etag)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = alt_client
        .get_object()
        .bucket(&bucket2)
        .key("new_foo")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(resp).await, "public/foo");

    // copy public/bar via upload_part_copy
    let copy_source2 = format!("{}/public/bar", bucket);
    let mpu2 = alt_client
        .create_multipart_upload()
        .bucket(&bucket2)
        .key("new_foo2")
        .send()
        .await
        .unwrap();
    let upload_id2 = mpu2.upload_id().unwrap().to_string();

    let copy_resp2 = alt_client
        .upload_part_copy()
        .bucket(&bucket2)
        .key("new_foo2")
        .part_number(1)
        .upload_id(&upload_id2)
        .copy_source(&copy_source2)
        .send()
        .await
        .unwrap();

    let etag2 = copy_resp2
        .copy_part_result()
        .and_then(|r| r.e_tag())
        .unwrap();
    alt_client
        .complete_multipart_upload()
        .bucket(&bucket2)
        .key("new_foo2")
        .upload_id(&upload_id2)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(etag2)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = alt_client
        .get_object()
        .bucket(&bucket2)
        .key("new_foo2")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(resp).await, "public/bar");

    // copy private/foo should be denied
    let copy_source3 = format!("{}/private/foo", bucket);
    let mpu3 = alt_client
        .create_multipart_upload()
        .bucket(&bucket2)
        .key("new_foo3")
        .send()
        .await
        .unwrap();
    let upload_id3 = mpu3.upload_id().unwrap().to_string();

    let result = alt_client
        .upload_part_copy()
        .bucket(&bucket2)
        .key("new_foo3")
        .part_number(1)
        .upload_id(&upload_id3)
        .copy_source(&copy_source3)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    alt_client
        .abort_multipart_upload()
        .bucket(&bucket2)
        .key("new_foo3")
        .upload_id(&upload_id3)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bucket_policy_put_obj_copy_source_meta() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let src_bucket = create_objects_in_new_bucket(&["public/foo", "public/bar"]).await;

    let src_resource = make_arn_resource(&format!("{src_bucket}/*"));
    let src_policy = make_json_policy("s3:GetObject", &src_resource, None, None, None);
    client
        .put_bucket_policy()
        .bucket(&src_bucket)
        .policy(src_policy)
        .send()
        .await
        .unwrap();

    let dest_bucket = get_new_bucket(Some(&client)).await;
    let tag_conditional = json!({"StringEquals": {
        "s3:x-amz-metadata-directive": "COPY"
    }});
    let resource = make_arn_resource(&format!("{dest_bucket}/*"));
    let policy = make_json_policy("s3:PutObject", &resource, None, None, Some(tag_conditional));
    client
        .put_bucket_policy()
        .bucket(&dest_bucket)
        .policy(policy)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();

    // copy with x-amz-metadata-directive: COPY → should succeed
    alt_client
        .copy_object()
        .bucket(&dest_bucket)
        .key("new_foo")
        .copy_source(format!("{src_bucket}/public/foo"))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-metadata-directive", "COPY");
        })
        .send()
        .await
        .unwrap();

    let response = alt_client
        .get_object()
        .bucket(&dest_bucket)
        .key("new_foo")
        .send()
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body, "public/foo");

    // copy without the directive header → should be denied
    let result = alt_client
        .copy_object()
        .bucket(&dest_bucket)
        .key("new_foo2")
        .copy_source(format!("{src_bucket}/public/bar"))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .metadata("foo", "bar")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_bucket_policy_put_obj_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let conditional = json!({"StringLike": {
        "s3:x-amz-acl": "public*"
    }});
    let resource = make_arn_resource(&format!("{bucket}/*"));

    // allow PutObject, deny PutObject with public* ACL
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:PutObject",
                "Resource": resource,
            },
            {
                "Effect": "Deny",
                "Principal": {"AWS": "*"},
                "Action": "s3:PutObject",
                "Resource": resource,
                "Condition": conditional,
            },
        ],
    });
    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(serde_json::to_string(&policy).unwrap())
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();

    // put without x-amz-acl → allowed
    alt_client
        .put_object()
        .bucket(&bucket)
        .key("private-key")
        .body(ByteStream::from_static(b"private-key"))
        .send()
        .await
        .unwrap();

    // put with x-amz-acl: public-read → denied
    let result = alt_client
        .put_object()
        .bucket(&bucket)
        .key("public-key")
        .body(ByteStream::from_static(b"public-key"))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-acl", "public-read");
        })
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[tokio::test]
async fn test_bucket_policy_put_obj_grant() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket1 = get_new_bucket(Some(&client)).await;
    let bucket2 = get_new_bucket(Some(&client)).await;

    let owner_id_str = format!("id={}", cfg.main_user_id);
    let s3_conditional = json!({"StringEquals": {
        "s3:x-amz-grant-full-control": owner_id_str
    }});

    let resource1 = make_arn_resource(&format!("{bucket1}/*"));
    let policy1 = make_json_policy("s3:PutObject", &resource1, None, None, Some(s3_conditional));

    let resource2 = make_arn_resource(&format!("{bucket2}/*"));
    let policy2 = make_json_policy("s3:PutObject", &resource2, None, None, None);

    client.put_bucket_policy().bucket(&bucket1).policy(policy1).send().await.unwrap();
    client.put_bucket_policy().bucket(&bucket2).policy(policy2).send().await.unwrap();

    let alt_client = get_alt_client();

    // put to bucket1 with grant-full-control → allowed
    let owner_id_clone = owner_id_str.clone();
    alt_client
        .put_object()
        .bucket(&bucket1)
        .key("key1")
        .body(ByteStream::from_static(b"key1"))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-grant-full-control", owner_id_clone.clone());
        })
        .send()
        .await
        .unwrap();

    // put to bucket2 without grant header → allowed (no condition)
    alt_client
        .put_object()
        .bucket(&bucket2)
        .key("key2")
        .body(ByteStream::from_static(b"key2"))
        .send()
        .await
        .unwrap();

    // main user can read ACL for key1 (was granted full control)
    let acl1 = client.get_object_acl().bucket(&bucket1).key("key1").send().await.unwrap();
    assert_eq!(acl1.grants()[0].grantee().unwrap().id().unwrap(), &cfg.main_user_id);

    // main user cannot read ACL for key2 (ownership not transferred)
    let result = client.get_object_acl().bucket(&bucket2).key("key2").send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    // alt user can read ACL for key2 (they own it)
    let acl2 = alt_client.get_object_acl().bucket(&bucket2).key("key2").send().await.unwrap();
    assert_eq!(acl2.grants()[0].grantee().unwrap().id().unwrap(), &cfg.alt_user_id);
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[cfg_attr(feature = "fails_on_posix", ignore = "rgw core: IfExists condition modifier not enforced")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "rgw core: IfExists condition modifier not enforced")]
#[tokio::test]
async fn test_bucket_policy_set_condition_operator_end_with_if_exists() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "foo";
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();

    let policy = format!(r#"{{
      "Version":"2012-10-17",
      "Statement": [{{
        "Sid": "Allow Public Access to All Objects",
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Condition": {{
            "StringLikeIfExists": {{
                "aws:Referer": "http://www.example.com/*"
            }}
        }},
        "Resource": "arn:aws:s3:::{}/*"
      }}]
    }}"#, bucket);

    client.put_bucket_policy().bucket(&bucket).policy(policy).send().await.unwrap();

    // matching referer → 200
    client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("referer", "http://www.example.com/");
        })
        .send()
        .await
        .unwrap();

    // matching referer with path → 200
    client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("referer", "http://www.example.com/index.html");
        })
        .send()
        .await
        .unwrap();

    // non-matching referer → 403
    let result = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("referer", "http://example.com");
        })
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[tokio::test]
async fn test_bucket_policy_put_obj_request_obj_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let tag_conditional = json!({"StringEquals": {
        "s3:RequestObjectTag/security": "public"
    }});
    let resource = make_arn_resource(&format!("{bucket}/*"));
    let policy = make_json_policy("s3:PutObject", &resource, None, None, Some(tag_conditional));
    client.put_bucket_policy().bucket(&bucket).policy(policy).send().await.unwrap();

    let alt_client = get_alt_client();

    // put without tag → denied
    let result = alt_client
        .put_object()
        .bucket(&bucket)
        .key("testobj")
        .body(ByteStream::from_static(b"testobj"))
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    // put with matching x-amz-tagging header → allowed
    alt_client
        .put_object()
        .bucket(&bucket)
        .key("testobj")
        .body(ByteStream::from_static(b"testobj"))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-tagging", "security=public");
        })
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_policy_deny_self_denied_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let root_client = get_iam_root_s3client();
    let iam_client = get_iam_root_client();
    let bucket = get_new_bucket(Some(&root_client)).await;

    let resource1 = format!("arn:aws:s3:::{}", bucket);
    let resource2 = format!("arn:aws:s3:::{}/*", bucket);
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": [
                "s3:PutBucketPolicy",
                "s3:GetBucketPolicy",
                "s3:DeleteBucketPolicy",
            ],
            "Resource": [resource1, resource2]
        }]
    });
    let policy_str = serde_json::to_string(&policy).unwrap();

    root_client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(&policy_str)
        .send()
        .await
        .unwrap();

    // create an IAM user under the same account
    let user_name = format!("policyuser-{}", rand::random::<u32>());
    iam_client.create_user().user_name(&user_name).send().await.unwrap();
    let keys = iam_client
        .create_access_key()
        .user_name(&user_name)
        .send()
        .await
        .unwrap();
    let ak = keys.access_key().unwrap();
    let user_s3 = build_s3_client(
        ak.access_key_id(),
        ak.secret_access_key(),
    );

    // non-root user should be denied
    let result = user_s3.get_bucket_policy().bucket(&bucket).send().await;
    assert_s3_err!(result, 403, "AccessDenied");
    let result = user_s3.delete_bucket_policy().bucket(&bucket).send().await;
    assert_s3_err!(result, 403, "AccessDenied");
    let result = user_s3.put_bucket_policy().bucket(&bucket).policy(&policy_str).send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    // root account should still be able to manage policy
    let resp = root_client.get_bucket_policy().bucket(&bucket).send().await.unwrap();
    assert!(!resp.policy().unwrap_or_default().is_empty());
    root_client.delete_bucket_policy().bucket(&bucket).send().await.unwrap();
    root_client.put_bucket_policy().bucket(&bucket).policy(&policy_str).send().await.unwrap();

    // cleanup
    iam_client.delete_access_key().user_name(&user_name).access_key_id(ak.access_key_id()).send().await.unwrap();
    iam_client.delete_user().user_name(&user_name).send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_policy_deny_self_denied_policy_confirm_header() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let root_client = get_iam_root_s3client();
    let iam_client = get_iam_root_client();
    let bucket = get_new_bucket(Some(&root_client)).await;

    let resource1 = format!("arn:aws:s3:::{}", bucket);
    let resource2 = format!("arn:aws:s3:::{}/*", bucket);
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": [
                "s3:PutBucketPolicy",
                "s3:GetBucketPolicy",
                "s3:DeleteBucketPolicy",
            ],
            "Resource": [resource1, resource2]
        }]
    });
    let policy_str = serde_json::to_string(&policy).unwrap();

    // put with ConfirmRemoveSelfBucketAccess
    root_client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(&policy_str)
        .confirm_remove_self_bucket_access(true)
        .send()
        .await
        .unwrap();

    // create an IAM user under the same account
    let user_name = format!("policyuser-{}", rand::random::<u32>());
    iam_client.create_user().user_name(&user_name).send().await.unwrap();
    let keys = iam_client
        .create_access_key()
        .user_name(&user_name)
        .send()
        .await
        .unwrap();
    let ak = keys.access_key().unwrap();
    let user_s3 = build_s3_client(
        ak.access_key_id(),
        ak.secret_access_key(),
    );

    // non-root user should be denied
    let result = user_s3.get_bucket_policy().bucket(&bucket).send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    // with ConfirmRemoveSelfBucketAccess, root should ALSO be denied
    let result = root_client.get_bucket_policy().bucket(&bucket).send().await;
    assert_s3_err!(result, 403, "AccessDenied");
    let result = root_client.delete_bucket_policy().bucket(&bucket).send().await;
    assert_s3_err!(result, 403, "AccessDenied");

    // cleanup
    iam_client.delete_access_key().user_name(&user_name).access_key_id(ak.access_key_id()).send().await.unwrap();
    iam_client.delete_user().user_name(&user_name).send().await.unwrap();
}
