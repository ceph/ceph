use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::{get_alt_client, get_client};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{check_configure_versioning_retry, get_new_bucket, get_new_bucket_name};
use s3_tests_rs::assert_s3_err;

async fn get_body(response: aws_sdk_s3::operation::get_object::GetObjectOutput) -> String {
    let bytes = response.body.collect().await.unwrap().into_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_copy_zero_size() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "foo123bar";

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b""))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(format!("{bucket_name}/{key}"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_length().unwrap_or(-1), 0);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_copy_same_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(format!("{bucket_name}/foo123bar"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "foo");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_copy_verify_contenttype() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let content_type = "text/bla";

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .body(ByteStream::from_static(b"foo"))
        .content_type(content_type)
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(format!("{bucket_name}/foo123bar"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "foo");

    let head = client
        .head_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_type().unwrap_or_default(), content_type);
}

#[tokio::test]
async fn test_object_copy_to_itself() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();

    let result = client
        .copy_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .copy_source(format!("{bucket_name}/foo123bar"))
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidRequest");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_copy_to_itself_with_metadata() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .copy_source(format!("{bucket_name}/foo123bar"))
        .metadata("foo", "bar")
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .send()
        .await
        .unwrap();
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("foo").map(|s| s.as_str()), Some("bar"));
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_copy_diff_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket1 = get_new_bucket(Some(&client)).await;
    let bucket2 = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket1)
        .key("foo123bar")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket2)
        .key("bar321foo")
        .copy_source(format!("{bucket1}/foo123bar"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket2)
        .key("bar321foo")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "foo");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_copy_16m() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let size = 16 * 1024 * 1024;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("obj1")
        .body(ByteStream::from(vec![0u8; size]))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("obj2")
        .copy_source(format!("{bucket_name}/obj1"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("obj2")
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_length().unwrap_or(0), size as i64);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_copy_retaining_metadata() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let content_type = "audio/ogg";

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .body(ByteStream::from(vec![0u8; 1024]))
        .content_type(content_type)
        .metadata("key1", "value1")
        .metadata("key2", "value2")
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(format!("{bucket_name}/foo123bar"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_type().unwrap_or_default(), content_type);
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("key1").map(|s| s.as_str()), Some("value1"));
    assert_eq!(metadata.get("key2").map(|s| s.as_str()), Some("value2"));
    assert_eq!(response.content_length().unwrap_or(0), 1024);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_copy_replacing_metadata() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .body(ByteStream::from(vec![0u8; 1024]))
        .content_type("audio/ogg")
        .metadata("key1", "value1")
        .metadata("key2", "value2")
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(format!("{bucket_name}/foo123bar"))
        .metadata("key3", "value3")
        .metadata("key2", "value2")
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .content_type("audio/mpeg")
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_type().unwrap_or_default(), "audio/mpeg");
    let metadata = response.metadata().unwrap();
    assert!(metadata.get("key1").is_none());
    assert_eq!(metadata.get("key3").map(|s| s.as_str()), Some("value3"));
    assert_eq!(metadata.get("key2").map(|s| s.as_str()), Some("value2"));
    assert_eq!(response.content_length().unwrap_or(0), 1024);
}

#[ignore = "VERIFY: also fails in Python s3-tests on this cluster — RGW cross-user copy access"]
#[tokio::test]
async fn test_object_copy_not_owned_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let alt_client = s3_tests_rs::client::get_alt_client();

    let bucket1 = get_new_bucket(Some(&client)).await;
    let bucket2 = get_new_bucket(Some(&alt_client)).await;

    // make bucket1 explicitly private
    client
        .put_bucket_acl()
        .bucket(&bucket1)
        .acl(aws_sdk_s3::types::BucketCannedAcl::Private)
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket1)
        .key("foo123bar")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();

    let result = alt_client
        .copy_object()
        .bucket(&bucket2)
        .key("bar321foo")
        .copy_source(format!("{bucket1}/foo123bar"))
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_object_copy_canned_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let alt_client = get_alt_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(format!("{bucket_name}/foo123bar"))
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .copy_source(format!("{bucket_name}/bar321foo"))
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .metadata("abc", "def")
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .send()
        .await
        .unwrap();

    alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_object_copy_versioned_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let data = vec![0u8; 5];
    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let copy_source = format!("{bucket_name}/foo123bar?versionId={version_id}");
    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(&copy_source)
        .send()
        .await
        .unwrap();
    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_length().unwrap_or(0), 5);

    let version_id2 = resp.version_id().unwrap_or_default().to_string();
    let copy_source2 = format!("{bucket_name}/bar321foo?versionId={version_id2}");
    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo2")
        .copy_source(&copy_source2)
        .send()
        .await
        .unwrap();
    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar321foo2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_length().unwrap_or(0), 5);

    let bucket2 = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket2, "Enabled", "Enabled").await;
    let copy_source3 = format!("{bucket_name}/foo123bar?versionId={version_id}");
    client
        .copy_object()
        .bucket(&bucket2)
        .key("bar321foo3")
        .copy_source(&copy_source3)
        .send()
        .await
        .unwrap();
    let resp = client
        .get_object()
        .bucket(&bucket2)
        .key("bar321foo3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_length().unwrap_or(0), 5);

    let bucket3 = get_new_bucket(Some(&client)).await;
    client
        .copy_object()
        .bucket(&bucket3)
        .key("bar321foo4")
        .copy_source(&copy_source3)
        .send()
        .await
        .unwrap();
    let resp = client
        .get_object()
        .bucket(&bucket3)
        .key("bar321foo4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_length().unwrap_or(0), 5);

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("foo123bar2")
        .copy_source(format!("{bucket3}/bar321foo4"))
        .send()
        .await
        .unwrap();
    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo123bar2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_length().unwrap_or(0), 5);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_object_copy_versioned_url_encoding() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let src_key = "foo?bar";
    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(src_key)
        .body(ByteStream::from_static(b""))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    client
        .head_object()
        .bucket(&bucket_name)
        .key(src_key)
        .send()
        .await
        .unwrap();

    let dst_key = "bar&foo";
    let encoded_key = src_key.replace('?', "%3F");
    let copy_source = format!(
        "{}/{}?versionId={}",
        bucket_name, encoded_key, version_id
    );
    client
        .copy_object()
        .bucket(&bucket_name)
        .key(dst_key)
        .copy_source(&copy_source)
        .send()
        .await
        .unwrap();

    client
        .head_object()
        .bucket(&bucket_name)
        .key(dst_key)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_copy_object_ifmatch_good() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();
    let etag = resp.e_tag().unwrap_or_default().to_string();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar")
        .copy_source(format!("{bucket_name}/foo"))
        .copy_source_if_match(&etag)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "bar");
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[tokio::test]
async fn test_copy_object_ifmatch_failed() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let result = client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar")
        .copy_source(format!("{bucket_name}/foo"))
        .copy_source_if_match("ABCORZ")
        .send()
        .await;
    assert_s3_err!(result, 412, "PreconditionFailed");
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[tokio::test]
async fn test_copy_object_ifnonematch_good() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();
    let etag = resp.e_tag().unwrap_or_default().to_string();

    let result = client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar")
        .copy_source(format!("{bucket_name}/foo"))
        .copy_source_if_none_match(&etag)
        .send()
        .await;
    assert_s3_err!(result, 412, "PreconditionFailed");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_copy_object_ifnonematch_failed() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar")
        .copy_source(format!("{bucket_name}/foo"))
        .copy_source_if_none_match("ABCORZ")
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "bar");
}

#[tokio::test]
async fn test_object_copy_not_owned_object_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let alt_client = get_alt_client();
    let cfg = get_config();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();

    let obj_acl = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo123bar")
        .send()
        .await
        .unwrap();
    let mut obj_grants = obj_acl.grants().to_vec();
    obj_grants.push(
        aws_sdk_s3::types::Grant::builder()
            .grantee(
                aws_sdk_s3::types::Grantee::builder()
                    .r#type(aws_sdk_s3::types::Type::CanonicalUser)
                    .id(&cfg.alt_user_id)
                    .build()
                    .unwrap(),
            )
            .permission(aws_sdk_s3::types::Permission::FullControl)
            .build(),
    );
    let obj_acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(obj_acl.owner().unwrap().clone())
        .set_grants(Some(obj_grants))
        .build();
    client
        .put_object_acl()
        .bucket(&bucket_name)
        .key("foo123bar")
        .access_control_policy(obj_acp)
        .send()
        .await
        .unwrap();

    let bucket_acl = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let mut bucket_grants = bucket_acl.grants().to_vec();
    bucket_grants.push(
        aws_sdk_s3::types::Grant::builder()
            .grantee(
                aws_sdk_s3::types::Grantee::builder()
                    .r#type(aws_sdk_s3::types::Type::CanonicalUser)
                    .id(&cfg.alt_user_id)
                    .build()
                    .unwrap(),
            )
            .permission(aws_sdk_s3::types::Permission::FullControl)
            .build(),
    );
    let bucket_acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(bucket_acl.owner().unwrap().clone())
        .set_grants(Some(bucket_grants))
        .build();
    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(bucket_acp)
        .send()
        .await
        .unwrap();

    alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("foo123bar")
        .send()
        .await
        .unwrap();

    alt_client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(format!("{bucket_name}/foo123bar"))
        .send()
        .await
        .unwrap();
}
