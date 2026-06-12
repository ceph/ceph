use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::{check_configure_versioning_retry, get_new_bucket};
use s3_tests_rs::{assert_s3_err, expect_s3_err};

async fn get_body(response: aws_sdk_s3::operation::get_object::GetObjectOutput) -> String {
    let bytes = response.body.collect().await.unwrap().into_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn test_get_object_ifmatch_good() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let put_resp = client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();
    let etag = put_resp.e_tag().unwrap_or_default().to_string();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .if_match(&etag)
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "bar");
}

#[tokio::test]
async fn test_get_object_ifmatch_failed() {
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
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .if_match("\"ABCORZ\"")
        .send()
        .await;
    assert_s3_err!(result, 412, "PreconditionFailed");
}

#[tokio::test]
async fn test_get_object_ifnonematch_good() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let put_resp = client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();
    let etag = put_resp.e_tag().unwrap_or_default().to_string();

    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .if_none_match(&etag)
        .send()
        .await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 304);
}

#[tokio::test]
async fn test_get_object_ifnonematch_failed() {
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

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .if_none_match("ABCORZ")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "bar");
}

#[tokio::test]
async fn test_get_object_ifmodifiedsince_good() {
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

    let past = aws_smithy_types::DateTime::from_secs(783459811); // 1994-10-29
    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .if_modified_since(past)
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "bar");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_get_object_ifunmodifiedsince_good() {
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

    let past = aws_smithy_types::DateTime::from_secs(783459811); // 1994-10-29
    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .if_unmodified_since(past)
        .send()
        .await;
    assert_s3_err!(result, 412, "PreconditionFailed");
}

#[tokio::test]
async fn test_get_object_ifunmodifiedsince_failed() {
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

    let future = aws_smithy_types::DateTime::from_secs(4121020411); // 2100-07-29
    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .if_unmodified_since(future)
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(response).await, "bar");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_get_object_ifmodifiedsince_failed() {
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

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    let last_modified = response.last_modified().unwrap();
    let after = aws_smithy_types::DateTime::from_secs(last_modified.secs() + 1);

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .if_modified_since(after)
        .send()
        .await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 304);
}

// --- Conditional write helpers ---

async fn prepare_multipart_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &[u8],
) -> (String, Vec<aws_sdk_s3::types::CompletedPart>) {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_string();
    let resp = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap();
    let parts = vec![aws_sdk_s3::types::CompletedPart::builder()
        .e_tag(resp.e_tag().unwrap())
        .part_number(1)
        .build()];
    (upload_id, parts)
}

async fn successful_conditional_multipart_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &[u8],
    if_match: Option<&str>,
    if_none_match: Option<&str>,
) -> aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput {
    let (upload_id, parts) = prepare_multipart_upload(client, bucket, key, body).await;
    let mpu = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let mut req = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(mpu);
    if let Some(v) = if_match {
        req = req.if_match(v);
    }
    if let Some(v) = if_none_match {
        req = req.if_none_match(v);
    }
    req.send().await.unwrap()
}

async fn failing_conditional_multipart_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &[u8],
    if_match: Option<&str>,
    if_none_match: Option<&str>,
    expected_status: u16,
    expected_code: &str,
) {
    let (upload_id, parts) = prepare_multipart_upload(client, bucket, key, body).await;
    let mpu = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let mut req = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(mpu);
    if let Some(v) = if_match {
        req = req.if_match(v);
    }
    if let Some(v) = if_none_match {
        req = req.if_none_match(v);
    }
    let result = req.send().await;
    assert_s3_err!(result, expected_status, expected_code);
}

// --- Conditional PUT tests ---

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_put_object_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "obj";

    let etag = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .send()
        .await
        .unwrap()
        .e_tag()
        .unwrap()
        .to_string();

    let result = client.put_object().bucket(&bucket).key(key).if_none_match("*").send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");
    let result = client.put_object().bucket(&bucket).key(key).if_none_match(&etag).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");
    client.put_object().bucket(&bucket).key(key).if_none_match("badetag").send().await.unwrap();

    client.put_object().bucket(&bucket).key(key).if_match(&etag).send().await.unwrap();

    client.delete_object().bucket(&bucket).key(key).send().await.unwrap();

    let result = client.put_object().bucket(&bucket).key(key).if_match("*").send().await;
    assert_s3_err!(result, 404, "NoSuchKey");
    let result = client.put_object().bucket(&bucket).key(key).if_match("badetag").send().await;
    assert_s3_err!(result, 404, "NoSuchKey");

    client.put_object().bucket(&bucket).key(key).if_none_match(&etag).send().await.unwrap();
    client.put_object().bucket(&bucket).key(key).if_match("*").send().await.unwrap();
    let result = client.put_object().bucket(&bucket).key(key).if_match("badetag").send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");
    client.put_object().bucket(&bucket).key(key).if_match(&etag).send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_put_object_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "obj";
    let body = b"abc";

    let resp = successful_conditional_multipart_upload(&client, &bucket, key, body, None, Some("*")).await;
    let etag = resp.e_tag().unwrap().to_string();

    failing_conditional_multipart_upload(&client, &bucket, key, body, None, Some("*"), 412, "PreconditionFailed").await;
    failing_conditional_multipart_upload(&client, &bucket, key, body, None, Some(&etag), 412, "PreconditionFailed").await;

    let resp = successful_conditional_multipart_upload(&client, &bucket, key, body, None, Some("badetag")).await;
    let etag = resp.e_tag().unwrap().to_string();

    let resp = successful_conditional_multipart_upload(&client, &bucket, key, body, Some(&etag), None).await;
    let _etag = resp.e_tag().unwrap().to_string();

    client.delete_object().bucket(&bucket).key(key).send().await.unwrap();

    failing_conditional_multipart_upload(&client, &bucket, key, body, Some("*"), None, 404, "NoSuchKey").await;
    failing_conditional_multipart_upload(&client, &bucket, key, body, Some("badetag"), None, 404, "NoSuchKey").await;

    let resp = successful_conditional_multipart_upload(&client, &bucket, key, body, None, Some(&etag)).await;
    let _etag2 = resp.e_tag().unwrap().to_string();
    successful_conditional_multipart_upload(&client, &bucket, key, body, Some("*"), None).await;
    failing_conditional_multipart_upload(&client, &bucket, key, body, Some("badetag"), None, 412, "PreconditionFailed").await;
    let resp = successful_conditional_multipart_upload(&client, &bucket, key, body, Some(&etag), None).await;
    assert!(resp.e_tag().is_some());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_put_current_object_if_none_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"data1"))
        .if_none_match("*")
        .send()
        .await
        .unwrap();
    let etag = resp.e_tag().unwrap().to_string();

    let resp2 = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"data2"))
        .send()
        .await
        .unwrap();
    let etag2 = resp2.e_tag().unwrap().to_string();

    let result = client.put_object().bucket(&bucket).key(key).if_none_match("*").send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");
    let result = client.put_object().bucket(&bucket).key(key).if_none_match(&etag2).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    // can't specify a version, only checks against current
    client.put_object().bucket(&bucket).key(key).if_none_match(&etag).send().await.unwrap();
    client.put_object().bucket(&bucket).key(key).if_none_match("badetag").send().await.unwrap();

    client.delete_object().bucket(&bucket).key(key).send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_multipart_put_current_object_if_none_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let resp = successful_conditional_multipart_upload(&client, &bucket, key, b"data1", None, Some("*")).await;
    let etag = resp.e_tag().unwrap().to_string();

    let resp2 = successful_conditional_multipart_upload(&client, &bucket, key, b"data2", None, None).await;
    let etag2 = resp2.e_tag().unwrap().to_string();

    failing_conditional_multipart_upload(&client, &bucket, key, b"abc", None, Some("*"), 412, "PreconditionFailed").await;
    failing_conditional_multipart_upload(&client, &bucket, key, b"abc", None, Some(&etag2), 412, "PreconditionFailed").await;

    successful_conditional_multipart_upload(&client, &bucket, key, b"abc", None, Some(&etag)).await;
    successful_conditional_multipart_upload(&client, &bucket, key, b"abc", None, Some("badetag")).await;

    client.delete_object().bucket(&bucket).key(key).send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_put_current_object_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let result = client.put_object().bucket(&bucket).key(key).if_match("*").send().await;
    assert_s3_err!(result, 404, "NoSuchKey");
    let result = client.put_object().bucket(&bucket).key(key).if_match("badetag").send().await;
    assert_s3_err!(result, 404, "NoSuchKey");

    let resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"data1"))
        .if_none_match("deadbeef")
        .send()
        .await
        .unwrap();
    let etag = resp.e_tag().unwrap().to_string();

    let resp2 = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"data2"))
        .if_match("*")
        .send()
        .await
        .unwrap();
    let etag2 = resp2.e_tag().unwrap().to_string();

    let result = client.put_object().bucket(&bucket).key(key).if_match("badetag").send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");
    let result = client.put_object().bucket(&bucket).key(key).if_match(&etag).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");
    client.put_object().bucket(&bucket).key(key).if_match(&etag2).send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_multipart_put_current_object_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    failing_conditional_multipart_upload(&client, &bucket, key, b"abc", Some("*"), None, 404, "NoSuchKey").await;
    failing_conditional_multipart_upload(&client, &bucket, key, b"abc", Some("badetag"), None, 404, "NoSuchKey").await;

    let resp = successful_conditional_multipart_upload(&client, &bucket, key, b"data1", None, Some("deadbeef")).await;
    let etag = resp.e_tag().unwrap().to_string();

    let resp2 = successful_conditional_multipart_upload(&client, &bucket, key, b"data2", Some("*"), None).await;
    let etag2 = resp2.e_tag().unwrap().to_string();

    failing_conditional_multipart_upload(&client, &bucket, key, b"abc", Some("badetag"), None, 412, "PreconditionFailed").await;
    failing_conditional_multipart_upload(&client, &bucket, key, b"abc", Some(&etag), None, 412, "PreconditionFailed").await;
    successful_conditional_multipart_upload(&client, &bucket, key, b"abc", Some(&etag2), None).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_put_object_current_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let etag = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .send()
        .await
        .unwrap()
        .e_tag()
        .unwrap()
        .to_string();

    let result = client.put_object().bucket(&bucket).key(key).if_none_match("*").send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");
    let result = client.put_object().bucket(&bucket).key(key).if_none_match(&etag).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    client.put_object().bucket(&bucket).key(key).if_match(&etag).send().await.unwrap();

    let del_resp = client.delete_object().bucket(&bucket).key(key).send().await.unwrap();
    assert_eq!(del_resp.delete_marker(), Some(true));

    let result = client.put_object().bucket(&bucket).key(key).if_match("*").send().await;
    assert_s3_err!(result, 404, "NoSuchKey");
    let result = client.put_object().bucket(&bucket).key(key).if_match("badetag").send().await;
    assert_s3_err!(result, 404, "NoSuchKey");

    client.put_object().bucket(&bucket).key(key).if_none_match(&etag).send().await.unwrap();
}

// --- Conditional DELETE (single object) tests ---

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_object_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "obj";

    let etag = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap()
        .e_tag()
        .unwrap()
        .to_string();

    let result = client.delete_object().bucket(&bucket).key(key).if_match("badetag").send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    client.delete_object().bucket(&bucket).key(key).if_match(&etag).send().await.unwrap();

    // -ENOENT doesn't raise error in delete op
    let _resp = client.delete_object().bucket(&bucket).key(key).if_match("*").send().await.unwrap();
    let _resp = client.delete_object().bucket(&bucket).key(key).if_match("badetag").send().await.unwrap();

    // recreate to test IfMatch='*'
    client.put_object().bucket(&bucket).key(key).send().await.unwrap();
    client.delete_object().bucket(&bucket).key(key).if_match("*").send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_delete_object_current_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    // -ENOENT doesn't raise error in delete op
    let _resp = client.delete_object().bucket(&bucket).key(key).if_match("*").send().await.unwrap();
    let _resp = client.delete_object().bucket(&bucket).key(key).if_match("badetag").send().await.unwrap();

    let put_resp = client.put_object().bucket(&bucket).key(key).send().await.unwrap();
    let version = put_resp.version_id().unwrap().to_string();
    let etag = put_resp.e_tag().unwrap().to_string();

    let result = client.delete_object().bucket(&bucket).key(key).if_match("badetag").send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    let del_resp = client.delete_object().bucket(&bucket).key(key).if_match(&etag).send().await.unwrap();
    assert_eq!(del_resp.delete_marker(), Some(true));

    // -ENOENT doesn't raise error in delete op
    client.delete_object().bucket(&bucket).key(key).if_match("*").send().await.unwrap();
    let result = client.delete_object().bucket(&bucket).key(key).if_match("badetag").send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    // remove delete marker to retest IfMatch='*'
    client.delete_object().bucket(&bucket).key(key).version_id(&version).send().await.unwrap();
    client.delete_object().bucket(&bucket).key(key).if_match("*").send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_object_version_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let put_resp = client.put_object().bucket(&bucket).key(key).send().await.unwrap();
    let version = put_resp.version_id().unwrap().to_string();
    let etag = put_resp.e_tag().unwrap().to_string();

    let result = client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match("badetag").send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    let del_resp = client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match(&etag).send().await.unwrap();
    assert!(del_resp.delete_marker().is_none() || del_resp.delete_marker() == Some(false));

    // -ENOENT doesn't raise error in delete op
    client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match("*").send().await.unwrap();
    client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match("badetag").send().await.unwrap();

    // recreate to test IfMatch='*'
    let put_resp = client.put_object().bucket(&bucket).key(key).send().await.unwrap();
    let version = put_resp.version_id().unwrap().to_string();
    client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match("*").send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_object_if_match_last_modified_time() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "obj";

    let badmtime = aws_smithy_types::DateTime::from_secs(1420070400); // 2015-01-01

    client.put_object().bucket(&bucket).key(key).send().await.unwrap();
    let head = client.head_object().bucket(&bucket).key(key).send().await.unwrap();
    let mtime = *head.last_modified().unwrap();

    let result = client.delete_object().bucket(&bucket).key(key).if_match_last_modified_time(badmtime).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    client.delete_object().bucket(&bucket).key(key).if_match_last_modified_time(mtime).send().await.unwrap();

    // -ENOENT doesn't raise error
    let _resp = client.delete_object().bucket(&bucket).key(key).if_match_last_modified_time(badmtime).send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_delete_object_current_if_match_last_modified_time() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let badmtime = aws_smithy_types::DateTime::from_secs(1420070400);

    client.put_object().bucket(&bucket).key(key).send().await.unwrap();
    let head = client.head_object().bucket(&bucket).key(key).send().await.unwrap();
    let mtime = *head.last_modified().unwrap();

    let result = client.delete_object().bucket(&bucket).key(key).if_match_last_modified_time(badmtime).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    let del_resp = client.delete_object().bucket(&bucket).key(key).if_match_last_modified_time(mtime).send().await.unwrap();
    assert_eq!(del_resp.delete_marker(), Some(true));

    // object is marked as deleted but still exists
    let result = client.delete_object().bucket(&bucket).key(key).if_match_last_modified_time(badmtime).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_object_version_if_match_last_modified_time() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let badmtime = aws_smithy_types::DateTime::from_secs(1420070400);

    let version = client.put_object().bucket(&bucket).key(key).send().await.unwrap().version_id().unwrap().to_string();
    let head = client.head_object().bucket(&bucket).key(key).version_id(&version).send().await.unwrap();
    let mtime = *head.last_modified().unwrap();

    let result = client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match_last_modified_time(badmtime).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    let del_resp = client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match_last_modified_time(mtime).send().await.unwrap();
    assert!(del_resp.delete_marker().is_none() || del_resp.delete_marker() == Some(false));

    // -ENOENT doesn't raise error
    let _resp = client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match_last_modified_time(badmtime).send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_object_if_match_size() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "obj";

    let size: i64 = 0;
    let badsize: i64 = 9999;

    client.put_object().bucket(&bucket).key(key).send().await.unwrap();

    let result = client.delete_object().bucket(&bucket).key(key).if_match_size(badsize).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    client.delete_object().bucket(&bucket).key(key).if_match_size(size).send().await.unwrap();

    // -ENOENT doesn't raise error
    let _resp = client.delete_object().bucket(&bucket).key(key).if_match_size(badsize).send().await.unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_delete_object_current_if_match_size() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let size: i64 = 0;
    let badsize: i64 = 9999;

    client.put_object().bucket(&bucket).key(key).send().await.unwrap();

    let result = client.delete_object().bucket(&bucket).key(key).if_match_size(badsize).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    let del_resp = client.delete_object().bucket(&bucket).key(key).if_match_size(size).send().await.unwrap();
    assert!(del_resp.delete_marker().is_some());

    // object exists but marked as deleted
    let result = client.delete_object().bucket(&bucket).key(key).if_match_size(badsize).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_object_version_if_match_size() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let size: i64 = 0;
    let badsize: i64 = 9999;

    let version = client.put_object().bucket(&bucket).key(key).send().await.unwrap().version_id().unwrap().to_string();

    let result = client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match_size(badsize).send().await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    let del_resp = client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match_size(size).send().await.unwrap();
    assert!(del_resp.delete_marker().is_none() || del_resp.delete_marker() == Some(false));

    // -ENOENT doesn't raise error
    let _resp = client.delete_object().bucket(&bucket).key(key).version_id(&version).if_match_size(badsize).send().await.unwrap();
}

// --- Conditional multi-DELETE tests ---

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_objects_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "obj";

    let etag = client.put_object().bucket(&bucket).key(key).send().await.unwrap().e_tag().unwrap().to_string();

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).e_tag("badetag").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).e_tag(&etag).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().first().unwrap().key(), Some(key));

    // -ENOENT doesn't raise error
    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).e_tag("badetag").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.errors().is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_delete_objects_current_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let etag = client.put_object().bucket(&bucket).key(key).send().await.unwrap().e_tag().unwrap().to_string();

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).e_tag("badetag").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).e_tag(&etag).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().first().unwrap().key(), Some(key));
    assert_eq!(resp.deleted().first().unwrap().delete_marker(), Some(true));

    // object is marked as deleted but still exists
    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).e_tag("badetag").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_objects_version_if_match() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let put_resp = client.put_object().bucket(&bucket).key(key).send().await.unwrap();
    let etag = put_resp.e_tag().unwrap().to_string();
    let version = put_resp.version_id().unwrap().to_string();

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).version_id(&version).e_tag("badetag").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).version_id(&version).e_tag(&etag).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().first().unwrap().key(), Some(key));
    assert!(resp.deleted().first().unwrap().delete_marker().is_none() || !resp.deleted().first().unwrap().delete_marker().unwrap());

    // -ENOENT
    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).version_id(&version).e_tag("badetag").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.errors().is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_objects_if_match_last_modified_time() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "obj";

    let badmtime = aws_smithy_types::DateTime::from_secs(1420070400);

    client.put_object().bucket(&bucket).key(key).send().await.unwrap();
    let head = client.head_object().bucket(&bucket).key(key).send().await.unwrap();
    let mtime = *head.last_modified().unwrap();

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).last_modified_time(badmtime).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).last_modified_time(mtime).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().first().unwrap().key(), Some(key));

    // -ENOENT
    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).last_modified_time(badmtime).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.errors().is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_delete_objects_current_if_match_last_modified_time() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let badmtime = aws_smithy_types::DateTime::from_secs(1420070400);

    client.put_object().bucket(&bucket).key(key).send().await.unwrap();
    let head = client.head_object().bucket(&bucket).key(key).send().await.unwrap();
    let mtime = *head.last_modified().unwrap();

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).last_modified_time(badmtime).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).last_modified_time(mtime).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().first().unwrap().key(), Some(key));
    assert_eq!(resp.deleted().first().unwrap().delete_marker(), Some(true));

    // object exists, but marked as deleted
    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).last_modified_time(badmtime).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_delete_objects_version_if_match_last_modified_time() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let badmtime = aws_smithy_types::DateTime::from_secs(1420070400);

    let version = client.put_object().bucket(&bucket).key(key).send().await.unwrap().version_id().unwrap().to_string();
    let head = client.head_object().bucket(&bucket).key(key).version_id(&version).send().await.unwrap();
    let mtime = *head.last_modified().unwrap();

    // create a different version as current
    client.put_object().bucket(&bucket).key(key).send().await.unwrap();

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).version_id(&version).last_modified_time(badmtime).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).version_id(&version).last_modified_time(mtime).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().first().unwrap().key(), Some(key));

    // -ENOENT
    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).version_id(&version).last_modified_time(badmtime).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.errors().is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_objects_if_match_size() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "obj";

    let size: i64 = 0;
    let badsize: i64 = 9999;

    client.put_object().bucket(&bucket).key(key).send().await.unwrap();

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).size(badsize).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).size(size).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().first().unwrap().key(), Some(key));

    // -ENOENT
    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).size(badsize).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.errors().is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: requires versioning")]
async fn test_delete_objects_current_if_match_size() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let size: i64 = 0;
    let badsize: i64 = 9999;

    client.put_object().bucket(&bucket).key(key).send().await.unwrap();

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).size(badsize).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).size(size).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().first().unwrap().key(), Some(key));
    assert_eq!(resp.deleted().first().unwrap().delete_marker(), Some(true));

    // object exists, but marked as deleted
    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).size(badsize).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_objects_version_if_match_size() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let size: i64 = 0;
    let badsize: i64 = 9999;

    let version = client.put_object().bucket(&bucket).key(key).send().await.unwrap().version_id().unwrap().to_string();

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).version_id(&version).size(badsize).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.errors().first().unwrap().code(), Some("PreconditionFailed"));

    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).version_id(&version).size(size).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().first().unwrap().key(), Some(key));

    // -ENOENT
    let resp = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).version_id(&version).size(badsize).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.errors().is_empty());
}
