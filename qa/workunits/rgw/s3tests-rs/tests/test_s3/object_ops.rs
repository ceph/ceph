use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::{get_alt_client, get_client, get_unauthenticated_client};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{
    check_configure_versioning_retry, create_objects_in_new_bucket, get_new_bucket,
    get_new_bucket_name,
};
use s3_tests_rs::assert_s3_err;

async fn get_body(
    response: aws_sdk_s3::operation::get_object::GetObjectOutput,
) -> String {
    let bytes = response.body.collect().await.unwrap().into_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn test_object_write_read_update_read_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    // Write
    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    // Read
    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(resp).await, "bar");

    // Update
    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"soup"))
        .send()
        .await
        .unwrap();

    // Read
    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(resp).await, "soup");

    // Delete
    client
        .delete_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_write_to_nonexist_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let result = client
        .put_object()
        .bucket("whatchutalkinboutwillis")
        .key("foo")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_object_read_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key("bar")
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchKey");
}

#[tokio::test]
async fn test_object_head_zero_bytes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b""))
        .send()
        .await
        .unwrap();

    let response = client
        .head_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_length().unwrap_or(-1), 0);
}

#[tokio::test]
async fn test_object_write_check_etag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let response = client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();
    let etag = response.e_tag().unwrap_or_default();
    assert_eq!(etag, "\"37b51d194a7513e45b56f6524f2d51f2\"");
}

#[tokio::test]
async fn test_object_write_cache_control() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let cache_control = "public, max-age=14400";
    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .cache_control(cache_control)
        .send()
        .await
        .unwrap();

    let response = client
        .head_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.cache_control().unwrap_or_default(), cache_control);
}

#[tokio::test]
async fn test_object_set_get_metadata_none_to_good() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .metadata("meta1", "mymeta")
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
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("meta1").map(|s| s.as_str()), Some("mymeta"));
}

#[tokio::test]
async fn test_object_set_get_metadata_none_to_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .metadata("meta1", "")
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
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("meta1").map(|s| s.as_str()), Some(""));
}

#[tokio::test]
async fn test_object_set_get_metadata_overwrite_to_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .metadata("meta1", "oldmeta")
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
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("meta1").map(|s| s.as_str()), Some("oldmeta"));

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .metadata("meta1", "")
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
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("meta1").map(|s| s.as_str()), Some(""));
}

#[tokio::test]
async fn test_object_metadata_replaced_on_put() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .metadata("meta1", "bar")
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .metadata("meta2", "baz")
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
    let metadata = response.metadata().unwrap();
    assert!(metadata.get("meta1").is_none());
    assert_eq!(metadata.get("meta2").map(|s| s.as_str()), Some("baz"));
}

#[tokio::test]
async fn test_multi_object_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let keys = &["key0", "key1", "key2"];
    let bucket_name = create_objects_in_new_bucket(keys).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 3);

    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = keys
        .iter()
        .map(|k| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(*k)
                .build()
                .unwrap()
        })
        .collect();

    let delete = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects.clone()))
        .build()
        .unwrap();

    let response = client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete)
        .send()
        .await
        .unwrap();
    assert_eq!(response.deleted().len(), 3);
    assert!(response.errors().is_empty());

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(response.contents().is_empty());
}

#[tokio::test]
async fn test_object_write_file() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let data = "bar";
    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from(data.as_bytes().to_vec()))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(resp).await, data);
}

// --- range request tests ---

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_ranged_request_response_code() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let content = "testcontent";

    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from_static(b"testcontent"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .range("bytes=4-7")
        .send()
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body, &content[4..8]);
}

#[tokio::test]
async fn test_ranged_request_skip_leading_bytes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let content = "testcontent";

    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from_static(b"testcontent"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .range("bytes=4-")
        .send()
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body, &content[4..]);
}

#[tokio::test]
async fn test_ranged_request_return_trailing_bytes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let content = "testcontent";

    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from_static(b"testcontent"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .range("bytes=-7")
        .send()
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body, &content[content.len() - 7..]);
}

#[tokio::test]
async fn test_ranged_request_invalid_range() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from_static(b"testcontent"))
        .send()
        .await
        .unwrap();

    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .range("bytes=40-50")
        .send()
        .await;
    assert_s3_err!(result, 416, "InvalidRange");
}

#[tokio::test]
async fn test_ranged_request_empty_object() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from_static(b""))
        .send()
        .await
        .unwrap();

    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .range("bytes=40-50")
        .send()
        .await;
    assert_s3_err!(result, 416, "InvalidRange");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_ranged_big_request_response_code() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let size = 8 * 1024 * 1024;
    let content: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from(content.clone()))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .range("bytes=3145728-5242880")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_length().unwrap(), 5242880 - 3145728 + 1);
    let body = resp.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(body, content[3145728..5242881]);
}

#[tokio::test]
async fn test_object_copy_bucket_not_found() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let result = client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(format!("{bucket_name}-fake/foo123bar"))
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_object_copy_key_not_found() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let result = client
        .copy_object()
        .bucket(&bucket_name)
        .key("bar321foo")
        .copy_source(format!("{bucket_name}/foo123bar"))
        .send()
        .await;
    assert!(result.is_err());
}

// --- bucket creation tests ---

#[tokio::test]
async fn test_bucket_create_naming_dns_underscore() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let result = client.create_bucket().bucket("foo_bar").send().await;
    assert_s3_err!(result, 400, "InvalidBucketName");
}

#[tokio::test]
async fn test_bucket_create_naming_dns_dash_at_end() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let result = client.create_bucket().bucket("foo-").send().await;
    assert_s3_err!(result, 400, "InvalidBucketName");
}

#[tokio::test]
async fn test_bucket_create_naming_dns_dot_dot() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let result = client.create_bucket().bucket("foo..bar").send().await;
    assert_s3_err!(result, 400, "InvalidBucketName");
}

#[tokio::test]
async fn test_bucket_create_naming_dns_dot_dash() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let result = client.create_bucket().bucket("foo.-bar").send().await;
    assert_s3_err!(result, 400, "InvalidBucketName");
}

#[tokio::test]
async fn test_bucket_create_naming_dns_dash_dot() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let result = client.create_bucket().bucket("foo-.bar").send().await;
    assert_s3_err!(result, 400, "InvalidBucketName");
}

#[tokio::test]
async fn test_bucket_create_naming_bad_short_one() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let result = client.create_bucket().bucket("a").send().await;
    assert_s3_err!(result, 400, "InvalidBucketName");
}

#[tokio::test]
async fn test_bucket_create_naming_bad_short_two() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let result = client.create_bucket().bucket("aa").send().await;
    assert_s3_err!(result, 400, "InvalidBucketName");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_create_exists_nonowner() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let alt_client = get_alt_client();
    let bucket_name = get_new_bucket_name();

    client.create_bucket().bucket(&bucket_name).send().await.unwrap();

    let result = alt_client.create_bucket().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 409, "BucketAlreadyExists");
}

#[tokio::test]
async fn test_bucket_list_return_data() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "foo"]).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    let objs = response.contents();
    assert_eq!(objs.len(), 3);

    for obj in objs {
        let key_name = obj.key().unwrap_or_default();
        assert!(obj.e_tag().is_some());
        assert!(obj.size().unwrap_or(0) > 0);
        assert!(obj.last_modified().is_some());

        let head = client
            .head_object()
            .bucket(&bucket_name)
            .key(key_name)
            .send()
            .await
            .unwrap();
        assert_eq!(obj.e_tag().unwrap_or_default(), head.e_tag().unwrap_or_default());
        assert_eq!(obj.size().unwrap_or(0), head.content_length().unwrap_or(0));
    }
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_create_naming_dns_long() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let prefix = &cfg.bucket_prefix;
    assert!(prefix.len() < 50);
    let num = 63 - prefix.len();
    let name = format!("{}{}", prefix, "a".repeat(num));
    client.create_bucket().bucket(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_bucket_concurrent_set_canned_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let num_tasks = 50;

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..num_tasks {
        let c = client.clone();
        let bn = bucket_name.clone();
        set.spawn(async move {
            c.put_bucket_acl()
                .bucket(&bn)
                .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
                .send()
                .await
                .is_ok()
        });
    }

    while let Some(result) = set.join_next().await {
        assert!(result.unwrap(), "Concurrent ACL set failed");
    }
}

// --- bucket delete tests ---

#[tokio::test]
async fn test_bucket_delete_notexist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    let result = client.delete_bucket().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_bucket_delete_nonempty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo"]).await;
    let client = get_client();
    let result = client.delete_bucket().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 409, "BucketNotEmpty");
}

// --- object write/header tests ---

#[tokio::test]
async fn test_object_write_expires() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let expires = aws_smithy_types::DateTime::from_secs(
        aws_smithy_types::DateTime::from(std::time::SystemTime::now()).secs() + 6000,
    );

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .expires(expires)
        .send()
        .await
        .unwrap();

    let response = client
        .head_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert!(response.expires_string().is_some());
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[tokio::test]
async fn test_object_set_get_unicode_metadata() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .metadata("meta1", "Hello World\u{e9}")
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
    let metadata = response.metadata().unwrap();
    let got = metadata.get("meta1").unwrap();
    assert!(got.contains("Hello World"));
}

#[tokio::test]
async fn test_object_content_encoding_aws_chunked() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "encoding";

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .content_encoding("gzip")
        .send()
        .await
        .unwrap();
    let resp = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_encoding().unwrap_or_default(), "gzip");

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .content_encoding("deflate, gzip")
        .send()
        .await
        .unwrap();
    let resp = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.content_encoding().unwrap_or_default(),
        "deflate, gzip"
    );

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .content_encoding("gzip, aws-chunked")
        .send()
        .await
        .unwrap();
    let resp = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_encoding().unwrap_or_default(), "gzip");

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .content_encoding("aws-chunked, gzip")
        .send()
        .await
        .unwrap();
    let resp = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_encoding().unwrap_or_default(), "gzip");

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .content_encoding("aws-chunked")
        .send()
        .await
        .unwrap();
    let resp = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert!(resp.content_encoding().is_none() || resp.content_encoding() == Some(""));

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .content_encoding("aws-chunked, aws-chunked")
        .send()
        .await
        .unwrap();
    let resp = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert!(resp.content_encoding().is_none() || resp.content_encoding() == Some(""));
}

// --- multi-object delete variants ---

#[tokio::test]
async fn test_multi_objectv2_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let keys = &["key0", "key1", "key2"];
    let bucket_name = create_objects_in_new_bucket(keys).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 3);

    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = keys
        .iter()
        .map(|k| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(*k)
                .build()
                .unwrap()
        })
        .collect();

    let delete = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects.clone()))
        .build()
        .unwrap();

    let response = client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete)
        .send()
        .await
        .unwrap();
    assert_eq!(response.deleted().len(), 3);
    assert!(response.errors().is_empty());

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(response.contents().is_empty());

    let objects2: Vec<aws_sdk_s3::types::ObjectIdentifier> = keys
        .iter()
        .map(|k| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(*k)
                .build()
                .unwrap()
        })
        .collect();
    let delete2 = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects2))
        .build()
        .unwrap();
    let response = client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete2)
        .send()
        .await
        .unwrap();
    assert_eq!(response.deleted().len(), 3);
    assert!(response.errors().is_empty());

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(response.contents().is_empty());
}

#[tokio::test]
async fn test_multi_object_delete_key_limit() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names: Vec<String> = (0..1001).map(|i| format!("key-{i}")).collect();
    let key_refs: Vec<&str> = key_names.iter().map(|s| s.as_str()).collect();
    let bucket_name = create_objects_in_new_bucket(&key_refs).await;
    let client = get_client();

    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = key_names
        .iter()
        .map(|k| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(k.as_str())
                .build()
                .unwrap()
        })
        .collect();

    let delete = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects))
        .build()
        .unwrap();

    let result = client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete)
        .send()
        .await;
    assert!(result.is_err());
}

// --- anonymous/unauthenticated access tests ---

async fn setup_bucket_object_acl(
    bucket_acl: aws_sdk_s3::types::BucketCannedAcl,
    object_acl: aws_sdk_s3::types::ObjectCannedAcl,
) -> String {
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(bucket_acl)
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .acl(object_acl)
        .send()
        .await
        .unwrap();
    bucket_name
}

#[tokio::test]
async fn test_object_raw_get() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::PublicRead,
        aws_sdk_s3::types::ObjectCannedAcl::PublicRead,
    )
    .await;

    let unauth = get_unauthenticated_client();
    unauth
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_raw_get_bucket_gone() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::PublicRead,
        aws_sdk_s3::types::ObjectCannedAcl::PublicRead,
    )
    .await;
    let client = get_client();

    client
        .delete_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    client
        .delete_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let unauth = get_unauthenticated_client();
    let result = unauth
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_object_delete_key_bucket_gone() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::PublicRead,
        aws_sdk_s3::types::ObjectCannedAcl::PublicRead,
    )
    .await;
    let client = get_client();

    client
        .delete_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    client
        .delete_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let unauth = get_unauthenticated_client();
    let result = unauth
        .delete_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_object_raw_get_object_gone() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::PublicRead,
        aws_sdk_s3::types::ObjectCannedAcl::PublicRead,
    )
    .await;
    let client = get_client();

    client
        .delete_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();

    let unauth = get_unauthenticated_client();
    let result = unauth
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchKey");
}

#[tokio::test]
async fn test_object_raw_get_bucket_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::Private,
        aws_sdk_s3::types::ObjectCannedAcl::PublicRead,
    )
    .await;

    let unauth = get_unauthenticated_client();
    unauth
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_raw_get_object_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::PublicRead,
        aws_sdk_s3::types::ObjectCannedAcl::Private,
    )
    .await;

    let unauth = get_unauthenticated_client();
    let result = unauth
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[tokio::test]
async fn test_object_raw_response_headers() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::Private,
        aws_sdk_s3::types::ObjectCannedAcl::Private,
    )
    .await;

    let client = get_client();
    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .response_cache_control("no-cache")
        .response_content_disposition("bla")
        .response_content_encoding("aaa")
        .response_content_language("esperanto")
        .response_content_type("foo/bar")
        .response_expires(aws_smithy_types::DateTime::from_secs(123))
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_type().unwrap_or_default(), "foo/bar");
    assert_eq!(
        response.content_disposition().unwrap_or_default(),
        "bla"
    );
    assert_eq!(
        response.content_language().unwrap_or_default(),
        "esperanto"
    );
    assert_eq!(response.content_encoding().unwrap_or_default(), "aaa");
    assert_eq!(response.cache_control().unwrap_or_default(), "no-cache");
}

#[tokio::test]
async fn test_object_anon_put() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();

    let unauth = get_unauthenticated_client();
    let result = unauth
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[tokio::test]
async fn test_object_anon_put_write_access() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();

    let unauth = get_unauthenticated_client();
    unauth
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_put_authenticated() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();
}

// --- ACL mtime preservation ---

#[tokio::test]
async fn test_object_put_acl_mtime() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"foooz"))
        .send()
        .await
        .unwrap();

    let head_resp = client
        .head_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    let create_mtime = head_resp.last_modified().unwrap().clone();

    let list_resp = client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let list_mtime = list_resp.contents()[0].last_modified().unwrap().clone();
    assert_eq!(create_mtime, list_mtime);

    let ver_resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let ver_mtime = ver_resp.versions()[0].last_modified().unwrap().clone();
    assert_eq!(create_mtime, ver_mtime);

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    client
        .put_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .acl(aws_sdk_s3::types::ObjectCannedAcl::Private)
        .send()
        .await
        .unwrap();

    let head_resp2 = client
        .head_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(create_mtime, head_resp2.last_modified().unwrap().clone());

    let list_resp2 = client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(
        create_mtime,
        list_resp2.contents()[0].last_modified().unwrap().clone()
    );

    let ver_resp2 = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(
        create_mtime,
        ver_resp2.versions()[0].last_modified().unwrap().clone()
    );
}

// --- bucket lifecycle tests ---

#[tokio::test]
async fn test_bucket_create_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .delete_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let result = client.delete_bucket().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_bucket_head() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .head_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bucket_head_notexist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    let result = client.head_bucket().bucket(&bucket_name).send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_bucket_recreate_not_overriding() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let keys = &["mykey1", "mykey2"];
    let bucket_name = create_objects_in_new_bucket(keys).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let obj_keys: Vec<&str> = response
        .contents()
        .iter()
        .map(|o| o.key().unwrap_or_default())
        .collect();
    assert_eq!(obj_keys, vec!["mykey1", "mykey2"]);

    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let obj_keys: Vec<&str> = response
        .contents()
        .iter()
        .map(|o| o.key().unwrap_or_default())
        .collect();
    assert_eq!(obj_keys, vec!["mykey1", "mykey2"]);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_create_special_key_names() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    // " " (space-only key) omitted — SDK rejects it (see docs/sdk-limitations.md #8)
    let key_names = [
        "\"", "$", "%", "&", "'", "<", ">", "_", "_ ", "_ _", "__",
    ];
    let key_refs: Vec<&str> = key_names.to_vec();
    let bucket_name = create_objects_in_new_bucket(&key_refs).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let obj_keys: Vec<String> = response
        .contents()
        .iter()
        .map(|o| o.key().unwrap_or_default().to_string())
        .collect();
    assert_eq!(obj_keys.len(), key_names.len());

    for name in &key_names {
        assert!(obj_keys.contains(&name.to_string()));
        let resp = client
            .get_object()
            .bucket(&bucket_name)
            .key(*name)
            .send()
            .await
            .unwrap();
        let body = get_body(resp).await;
        assert_eq!(body, *name);
    }
}

#[tokio::test]
async fn test_bucket_notexist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    let result = client.list_objects().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_bucketv2_notexist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();
    let result = client.list_objects_v2().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_bucket_list_distinct() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket1 = create_objects_in_new_bucket(&["asdf"]).await;
    let bucket2 = get_new_bucket(Some(&get_client())).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket2)
        .send()
        .await
        .unwrap();
    assert!(response.contents().is_empty());

    let response = client
        .list_objects()
        .bucket(&bucket1)
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 1);
}

#[tokio::test]
async fn test_bucket_list_many() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo", "bar", "baz"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .max_keys(2)
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = response
        .contents()
        .iter()
        .map(|o| o.key().unwrap_or_default())
        .collect();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys, vec!["bar", "baz"]);
    assert!(response.is_truncated().unwrap_or(false));
}

#[tokio::test]
async fn test_multi_objectv2_delete_key_limit() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names: Vec<String> = (0..1001).map(|i| format!("key-{i}")).collect();
    let key_refs: Vec<&str> = key_names.iter().map(|s| s.as_str()).collect();
    let bucket_name = create_objects_in_new_bucket(&key_refs).await;
    let client = get_client();

    let mut total_keys = 0;
    let mut continuation_token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(&bucket_name).max_keys(1000);
        if let Some(ref token) = continuation_token {
            req = req.continuation_token(token);
        }
        let response = req.send().await.unwrap();
        total_keys += response.contents().len();
        if response.is_truncated().unwrap_or(false) {
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }
    assert_eq!(total_keys, 1001);

    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = key_names
        .iter()
        .map(|k| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(k.as_str())
                .build()
                .unwrap()
        })
        .collect();
    let delete = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects))
        .build()
        .unwrap();
    let result = client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete)
        .send()
        .await;
    let err = s3_tests_rs::expect_s3_err!(result);
    assert_eq!(err.status, 400);
}

#[tokio::test]
async fn test_object_raw_authenticated() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::PublicRead,
        aws_sdk_s3::types::ObjectCannedAcl::PublicRead,
    )
    .await;

    let client = get_client();
    client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_raw_authenticated_bucket_gone() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::PublicRead,
        aws_sdk_s3::types::ObjectCannedAcl::PublicRead,
    )
    .await;
    let client = get_client();

    client
        .delete_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    client
        .delete_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_object_raw_authenticated_object_gone() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::PublicRead,
        aws_sdk_s3::types::ObjectCannedAcl::PublicRead,
    )
    .await;
    let client = get_client();

    client
        .delete_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();

    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchKey");
}

#[tokio::test]
async fn test_bucket_create_exists() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    match client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
    {
        Ok(_) => {}
        Err(ref e) => {
            let err = s3_tests_rs::error::extract_s3_error(e);
            assert_eq!(err.status, 409);
            assert_eq!(err.error_code, "BucketAlreadyOwnedByYou");
        }
    }
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_recreate_overwrite_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let result = client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await;
    assert_s3_err!(result, 409, "BucketAlreadyExists");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_recreate_new_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let result = client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await;
    assert_s3_err!(result, 409, "BucketAlreadyExists");
}

async fn test_bucket_create_naming_good_long(length: usize) {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    let prefix = &cfg.bucket_prefix;
    assert!(prefix.len() < 50);
    let num = length - prefix.len();
    let name = format!("{}{}", prefix, "a".repeat(num));
    let client = get_client();
    client.create_bucket().bucket(&name).send().await.unwrap();
    client.delete_bucket().bucket(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_bucket_create_naming_good_long_60() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_bucket_create_naming_good_long(60).await;
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_create_naming_good_long_61() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_bucket_create_naming_good_long(61).await;
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_create_naming_good_long_62() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_bucket_create_naming_good_long(62).await;
}

#[tokio::test]
async fn test_bucket_create_naming_good_long_63() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_bucket_create_naming_good_long(63).await;
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_create_naming_bad_ip() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let result = client
        .create_bucket()
        .bucket("192.168.5.123")
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidBucketName");
}

#[tokio::test]
async fn test_object_checksum_sha256() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "myobj";
    let body = "A".repeat(1024);
    let sha256sum = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

    let response = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from(body.clone().into_bytes()))
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
        .checksum_sha256(sha256sum)
        .send()
        .await
        .unwrap();
    assert_eq!(response.checksum_sha256().unwrap(), sha256sum);

    let head = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert!(head.checksum_sha256().is_none());

    let head_cksum = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(head_cksum.checksum_sha256().unwrap(), sha256sum);

    let result = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from(body.into_bytes()))
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
        .checksum_sha256("bad")
        .send()
        .await;
    assert_s3_err!(result, 400, "BadDigest");
}

#[tokio::test]
async fn test_object_checksum_crc64nvme() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "myobj";
    let body = "A".repeat(1024);
    let crc64sum = "Qeh8oXvGiSo=";

    let response = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from(body.clone().into_bytes()))
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc64Nvme)
        .checksum_crc64_nvme(crc64sum)
        .send()
        .await
        .unwrap();
    assert_eq!(response.checksum_crc64_nvme().unwrap(), crc64sum);

    let head = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert!(head.checksum_crc64_nvme().is_none());

    let head_cksum = client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(head_cksum.checksum_crc64_nvme().unwrap(), crc64sum);

    let result = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from(body.into_bytes()))
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc64Nvme)
        .checksum_crc64_nvme("bad")
        .send()
        .await;
    assert_s3_err!(result, 400, "BadDigest");
}

#[tokio::test]
async fn test_get_object_attributes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "obj";

    let put_resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();
    let etag = put_resp.e_tag().unwrap().trim_matches('"').to_string();
    assert!(!etag.is_empty());

    let attrs = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize";
    let response = client
        .get_object_attributes()
        .bucket(&bucket_name)
        .key(key)
        .object_attributes(aws_sdk_s3::types::ObjectAttributes::Etag)
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-object-attributes", attrs);
        })
        .send()
        .await
        .unwrap();

    assert_eq!(response.object_size().unwrap(), 3);
    assert_eq!(response.e_tag().unwrap().trim_matches('"'), etag);
    assert_eq!(
        response.storage_class().unwrap(),
        &aws_sdk_s3::types::StorageClass::Standard
    );
    assert!(response.object_parts().is_none());
}

#[tokio::test]
async fn test_expected_bucket_owner() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicReadWrite)
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

    let unauth_client = get_unauthenticated_client();
    let cfg = get_config();
    let incorrect_owner = format!("{}foo", cfg.main_user_id);

    let result = unauth_client
        .list_objects()
        .bucket(&bucket_name)
        .expected_bucket_owner(&incorrect_owner)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let result = unauth_client
        .put_object()
        .bucket(&bucket_name)
        .key("bar")
        .body(ByteStream::from_static(b"coffee"))
        .expected_bucket_owner(&incorrect_owner)
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_get_location() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    if cfg.main_api_name.is_empty() {
        return;
    }
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .create_bucket_configuration(
            aws_sdk_s3::types::CreateBucketConfiguration::builder()
                .location_constraint(
                    cfg.main_api_name
                        .as_str()
                        .parse::<aws_sdk_s3::types::BucketLocationConstraint>()
                        .unwrap_or(aws_sdk_s3::types::BucketLocationConstraint::from(
                            cfg.main_api_name.as_str(),
                        )),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_location()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let location = response
        .location_constraint()
        .map(|l| l.as_str())
        .unwrap_or("");
    assert_eq!(location, cfg.main_api_name);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_create_naming_bad_starts_nonalpha() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = get_new_bucket_name();
    let bad_name = format!("_{bucket_name}");
    let client = get_client();
    let result = client.create_bucket().bucket(&bad_name).send().await;
    assert_s3_err!(result, 400, "InvalidBucketName");
}

#[ignore = "VERIFY: RGW returns 404 instead of 400 for unreadable key (also fails in Python)"]
#[tokio::test]
async fn test_object_read_unreadable() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key("\u{ae}\u{8a}-")
        .send()
        .await;
    let err = s3_tests_rs::expect_s3_err!(result);
    assert_eq!(err.status, 400);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_list_long_name() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    let prefix = &cfg.bucket_prefix;
    let num = 61 - prefix.len();
    let name = format!("{}{}", prefix, "a".repeat(num));
    let client = get_client();
    client.create_bucket().bucket(&name).send().await.unwrap();

    let response = client.list_objects().bucket(&name).send().await.unwrap();
    assert!(response.contents().is_empty());

    client.delete_bucket().bucket(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_object_raw_authenticated_bucket_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::Private,
        aws_sdk_s3::types::ObjectCannedAcl::PublicRead,
    )
    .await;

    let client = get_client();
    client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_raw_authenticated_object_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = setup_bucket_object_acl(
        aws_sdk_s3::types::BucketCannedAcl::PublicRead,
        aws_sdk_s3::types::ObjectCannedAcl::Private,
    )
    .await;

    let client = get_client();
    client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
}

// VERIFY: RGW returns AccessDenied for put_bucket_logging (also not in active Python suite)
#[ignore = "requires bucket logging support"]
#[tokio::test]
async fn test_logging_toggle() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let status = aws_sdk_s3::types::BucketLoggingStatus::builder()
        .logging_enabled(
            aws_sdk_s3::types::LoggingEnabled::builder()
                .target_bucket(&bucket_name)
                .target_prefix("foologgingprefix")
                .target_grants(
                    aws_sdk_s3::types::TargetGrant::builder()
                        .grantee(
                            aws_sdk_s3::types::Grantee::builder()
                                .display_name(&cfg.main_display_name)
                                .id(&cfg.main_user_id)
                                .r#type(aws_sdk_s3::types::Type::CanonicalUser)
                                .build()
                                .unwrap(),
                        )
                        .permission(aws_sdk_s3::types::BucketLogsPermission::FullControl)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .build();

    client
        .put_bucket_logging()
        .bucket(&bucket_name)
        .bucket_logging_status(status)
        .send()
        .await
        .unwrap();

    client
        .get_bucket_logging()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let empty_status = aws_sdk_s3::types::BucketLoggingStatus::builder().build();
    client
        .put_bucket_logging()
        .bucket(&bucket_name)
        .bucket_logging_status(empty_status)
        .send()
        .await
        .unwrap();
}

// --- Atomic read/write tests ---
// Note: Python originals use FakeWriteFile with mid-stream callbacks to test
// concurrent read/write atomicity. These simplified translations verify the
// sequential write-read-overwrite-verify behavior without interleaving.

fn make_fill(size: usize, ch: u8) -> Vec<u8> {
    vec![ch; size]
}

async fn verify_key_data(client: &aws_sdk_s3::Client, bucket: &str, key: &str, size: usize, ch: u8) {
    let resp = client.get_object().bucket(bucket).key(key).send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.len(), size);
    assert!(body.iter().all(|&b| b == ch));
}

async fn test_atomic_read(file_size: usize) {
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    // write A's
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(make_fill(file_size, b'A')))
        .send()
        .await
        .unwrap();

    // overwrite with B's
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(make_fill(file_size, b'B')))
        .send()
        .await
        .unwrap();

    // verify B's
    verify_key_data(&client, &bucket, key, file_size, b'B').await;
}

#[tokio::test]
async fn test_atomic_read_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_atomic_read(1024 * 1024).await;
}

#[tokio::test]
async fn test_atomic_read_4mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_atomic_read(4 * 1024 * 1024).await;
}

#[tokio::test]
async fn test_atomic_read_8mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_atomic_read(8 * 1024 * 1024).await;
}

async fn test_atomic_write(file_size: usize) {
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    // write A's, verify A's
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(make_fill(file_size, b'A')))
        .send()
        .await
        .unwrap();
    verify_key_data(&client, &bucket, key, file_size, b'A').await;

    // overwrite with B's, verify B's
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(make_fill(file_size, b'B')))
        .send()
        .await
        .unwrap();
    verify_key_data(&client, &bucket, key, file_size, b'B').await;
}

#[tokio::test]
async fn test_atomic_write_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_atomic_write(1024 * 1024).await;
}

#[tokio::test]
async fn test_atomic_write_4mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_atomic_write(4 * 1024 * 1024).await;
}

#[tokio::test]
async fn test_atomic_write_8mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_atomic_write(8 * 1024 * 1024).await;
}

async fn test_atomic_dual_write(file_size: usize) {
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(make_fill(file_size, b'A')))
        .send()
        .await
        .unwrap();

    // overwrite with B's
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(make_fill(file_size, b'B')))
        .send()
        .await
        .unwrap();

    // verify the final content is consistent (all one character)
    let resp = client.get_object().bucket(&bucket).key(key).send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.len(), file_size);
    let first = body[0];
    assert!(first == b'A' || first == b'B');
    assert!(body.iter().all(|&b| b == first));
}

#[tokio::test]
async fn test_atomic_dual_write_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_atomic_dual_write(1024 * 1024).await;
}

#[tokio::test]
async fn test_atomic_dual_write_4mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_atomic_dual_write(4 * 1024 * 1024).await;
}

#[tokio::test]
async fn test_atomic_dual_write_8mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_atomic_dual_write(8 * 1024 * 1024).await;
}

// --- Object attributes tests ---

#[tokio::test]
async fn test_get_checksum_object_attributes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "myobj";
    let size = 1024;
    let body = vec![b'A'; size];
    let sha256sum = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

    let put_resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(body))
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
        .checksum_sha256(sha256sum)
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp.checksum_sha256().unwrap(), sha256sum);
    let etag = put_resp.e_tag().unwrap().trim_matches('"').to_string();
    assert!(!etag.is_empty());

    let attrs = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize";
    let resp = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(aws_sdk_s3::types::ObjectAttributes::Etag)
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-object-attributes", attrs);
        })
        .send()
        .await
        .unwrap();

    assert_eq!(resp.object_size(), Some(size as i64));
    assert_eq!(resp.e_tag().unwrap(), &etag);
    assert_eq!(*resp.storage_class().unwrap(), aws_sdk_s3::types::StorageClass::Standard);
    assert_eq!(resp.checksum().unwrap().checksum_sha256().unwrap(), sha256sum);
    assert!(resp.object_parts().is_none());
}

#[tokio::test]
async fn test_get_versioned_object_attributes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket, "Enabled", "Enabled").await;
    let key = "obj";

    let put_resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
        .unwrap();
    let etag = put_resp.e_tag().unwrap().trim_matches('"').to_string();
    let version = put_resp.version_id().unwrap().to_string();
    assert!(!version.is_empty());

    let attrs = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize";
    let resp = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(aws_sdk_s3::types::ObjectAttributes::Etag)
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-object-attributes", attrs);
        })
        .send()
        .await
        .unwrap();

    assert!(resp.delete_marker().is_none() || !resp.delete_marker().unwrap());
    assert_eq!(resp.version_id().unwrap(), &version);
    assert_eq!(resp.object_size(), Some(3));
    assert_eq!(resp.e_tag().unwrap(), &etag);
    assert_eq!(*resp.storage_class().unwrap(), aws_sdk_s3::types::StorageClass::Standard);
    assert!(resp.object_parts().is_none());

    // write a new current version
    client.put_object().bucket(&bucket).key(key).body(ByteStream::from_static(b"foo")).send().await.unwrap();

    // ask for the original version
    let attrs2 = "ETag,ObjectSize,StorageClass";
    let resp = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .version_id(&version)
        .object_attributes(aws_sdk_s3::types::ObjectAttributes::Etag)
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-object-attributes", attrs2);
        })
        .send()
        .await
        .unwrap();

    assert!(resp.delete_marker().is_none() || !resp.delete_marker().unwrap());
    assert_eq!(resp.version_id().unwrap(), &version);
    assert_eq!(resp.object_size(), Some(3));
    assert_eq!(resp.e_tag().unwrap(), &etag);
    assert_eq!(*resp.storage_class().unwrap(), aws_sdk_s3::types::StorageClass::Standard);
}

#[tokio::test]
async fn test_100_continue() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = s3_tests_rs::config::get_config();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    let objname = "testobj";

    let host = &cfg.default_host;
    let port = cfg.default_port;

    async fn send_100_cont(host: &str, port: u16, method: &str, resource: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let req = format!(
            "{method} {resource} HTTP/1.1\r\nHost: {host}\r\nAccept-Encoding: identity\r\nContent-Length: 123\r\nExpect: 100-continue\r\n\r\n"
        );
        let mut stream = tokio::net::TcpStream::connect(format!("{host}:{port}"))
            .await
            .expect("TCP connect failed");
        stream.write_all(req.as_bytes()).await.expect("write failed");
        let mut buf = vec![0u8; 1024];
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let n = stream.read(&mut buf).await.expect("read failed");
            String::from_utf8_lossy(&buf[..n]).to_string()
        })
        .await
        .unwrap_or_default()
    }

    let resource = format!("/{bucket}/{objname}");

    // without public access → 403
    let resp = send_100_cont(host, port, "PUT", &resource).await;
    let status = resp.split(' ').nth(1).unwrap_or("");
    assert_eq!(status, "403");

    client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    // with public access → 100
    let resp = send_100_cont(host, port, "PUT", &resource).await;
    let status = resp.split(' ').nth(1).unwrap_or("");
    assert_eq!(status, "100");
}

#[ignore = "Rust SDK uses aws-chunked signing which conflicts with Transfer-Encoding: chunked"]
#[tokio::test]
async fn test_object_write_with_chunked_transfer_encoding() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    #[derive(Debug)]
    struct AddTransferEncoding;
    impl aws_smithy_runtime_api::client::interceptors::Intercept for AddTransferEncoding {
        fn name(&self) -> &'static str { "AddTransferEncoding" }
        fn modify_before_transmit(
            &self,
            context: &mut aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut<'_>,
            _runtime_components: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
            _cfg: &mut aws_smithy_types::config_bag::ConfigBag,
        ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
            context.request_mut().headers_mut().insert("transfer-encoding", "chunked");
            Ok(())
        }
    }

    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .customize()
        .interceptor(AddTransferEncoding)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw — https://tracker.ceph.com/issues/64841")]
#[tokio::test]
async fn test_100_continue_error_retry() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let not_bucket = format!("{}-but-doesnt-exist", bucket);
    let result = client
        .put_object()
        .bucket(&not_bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchBucket");

    // second request on same client should still work
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_atomic_conditional_write_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";
    let file_size = 1024 * 1024;

    let data_a = vec![b'A'; file_size];
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data_a))
        .send()
        .await
        .unwrap();

    // verify A
    let resp = client.get_object().bucket(&bucket).key(key).send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert!(body.iter().all(|&b| b == b'A'));

    // overwrite with B using If-Match: *
    let data_b = vec![b'B'; file_size];
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data_b))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("If-Match", "*");
        })
        .send()
        .await
        .unwrap();

    // verify B
    let resp = client.get_object().bucket(&bucket).key(key).send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert!(body.iter().all(|&b| b == b'B'));
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[tokio::test]
async fn test_atomic_dual_conditional_write_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";
    let file_size = 1024 * 1024;

    // write A
    let data_a = vec![b'A'; file_size];
    let resp_a = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data_a))
        .send()
        .await
        .unwrap();
    let etag_a = resp_a.e_tag().unwrap().trim_matches('"').to_string();

    // write B with If-Match on A's etag — should succeed
    let data_b = vec![b'B'; file_size];
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data_b))
        .if_match(&etag_a)
        .send()
        .await
        .unwrap();

    // write C with If-Match on A's stale etag — should fail (B replaced A)
    let data_c = vec![b'C'; file_size];
    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data_c))
        .if_match(&etag_a)
        .send()
        .await;
    assert_s3_err!(result, 412, "PreconditionFailed");

    // verify B won
    let resp = client.get_object().bucket(&bucket).key(key).send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert!(body.iter().all(|&b| b == b'B'));
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[tokio::test]
async fn test_atomic_write_bucket_gone() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    // delete the bucket first
    client.delete_bucket().bucket(&bucket).send().await.unwrap();

    // PutObject to deleted bucket → 404
    let result = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from(vec![b'A'; 1024 * 1024]))
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_account_usage() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let resp = s3_tests_rs::http::signed_request(
        reqwest::Method::GET,
        "",
        "usage",
        None,
    )
    .await;
    assert_eq!(resp.status, 200);
    let body = resp.body;
    assert!(body.contains("QuotaMaxBytes") || body.contains("Usage"));
}
