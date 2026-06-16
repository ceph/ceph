use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumType, CompletedMultipartUpload, CompletedPart,
    ObjectAttributes,
};
use s3_tests_rs::client::{get_alt_client, get_client};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{
    check_configure_versioning_retry, create_key_with_random_content, get_new_bucket,
    multipart_copy, multipart_upload,
};
use s3_tests_rs::assert_s3_err;

async fn get_body(response: aws_sdk_s3::operation::get_object::GetObjectOutput) -> Vec<u8> {
    response.body.collect().await.unwrap().into_bytes().to_vec()
}

#[tokio::test]
async fn test_multipart_upload_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    let result = multipart_upload(&client, &bucket_name, key, 0, None, None, None, None).await;

    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(result.parts))
        .build();
    let res = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(mp)
        .send()
        .await;
    assert_s3_err!(res, 400, "MalformedXML");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_upload_small() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    let result = multipart_upload(&client, &bucket_name, key, 1, None, None, None, None).await;

    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(result.parts.clone()))
        .build();
    client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(mp)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_length().unwrap_or(-1), 1);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_upload_contents() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let content_type = "text/bla";
    let objlen = 30 * 1024 * 1024;
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("foo".to_string(), "bar".to_string());

    let result = multipart_upload(
        &client,
        &bucket_name,
        key,
        objlen,
        None,
        Some(content_type),
        Some(metadata.clone()),
        None,
    )
    .await;

    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(result.parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(mp)
        .send()
        .await
        .unwrap();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .prefix(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 1);
    assert_eq!(response.contents()[0].size().unwrap_or(0), objlen as i64);

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_type().unwrap_or_default(), content_type);
    assert_eq!(
        response.metadata().unwrap().get("foo").map(|s| s.as_str()),
        Some("bar")
    );
    let body = get_body(response).await;
    assert_eq!(body.len(), objlen);
    assert_eq!(body, result.data);
}

#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: vstart uses rgw_multipart_min_part_size=32, parts exceed that")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: multipart listing or size check")]
async fn test_multipart_upload_size_too_small() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let size = 100 * 1024;

    let result =
        multipart_upload(&client, &bucket_name, key, size, Some(10 * 1024), None, None, None)
            .await;

    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(result.parts))
        .build();
    let res = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(mp)
        .send()
        .await;
    assert_s3_err!(res, 400, "EntityTooSmall");
}

// Validates EntityTooSmall with 1-byte parts, which are below any
// reasonable rgw_multipart_min_part_size (nsfs vstart uses 32).
#[tokio::test]
async fn test_multipart_upload_size_too_small_1byte() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart_tiny";

    let result =
        multipart_upload(&client, &bucket_name, key, 3, Some(1), None, None, None)
            .await;

    let mp = CompletedMultipartUpload::builder()
        .set_parts(Some(result.parts))
        .build();
    let res = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(mp)
        .send()
        .await;
    assert_s3_err!(res, 400, "EntityTooSmall");
}

#[tokio::test]
async fn test_multipart_upload_multiple_sizes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    for objlen in [
        5 * 1024 * 1024,
        5 * 1024 * 1024 + 100 * 1024,
        5 * 1024 * 1024 + 600 * 1024,
        10 * 1024 * 1024 + 100 * 1024,
        10 * 1024 * 1024 + 600 * 1024,
        10 * 1024 * 1024,
    ] {
        let result =
            multipart_upload(&client, &bucket_name, key, objlen, None, None, None, None).await;
        let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(result.parts))
            .build();
        client
            .complete_multipart_upload()
            .bucket(&bucket_name)
            .key(key)
            .upload_id(&result.upload_id)
            .multipart_upload(mp)
            .send()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_abort_multipart_upload() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    let result =
        multipart_upload(&client, &bucket_name, key, 10 * 1024 * 1024, None, None, None, None)
            .await;

    client
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .send()
        .await
        .unwrap();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(response.contents().is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: multipart listing or size check")]
async fn test_list_multipart_upload() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    let resp = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = resp.upload_id().unwrap().to_string();

    let response = client
        .list_multipart_uploads()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.uploads().len(), 1);
    assert_eq!(
        response.uploads()[0].upload_id().unwrap_or_default(),
        upload_id
    );

    client
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_multipart_upload_missing_part() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    let resp = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = resp.upload_id().unwrap().to_string();

    let part_resp = client
        .upload_part()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"\0"))
        .send()
        .await
        .unwrap();

    let parts = vec![aws_sdk_s3::types::CompletedPart::builder()
        .e_tag(part_resp.e_tag().unwrap_or_default())
        .part_number(9999)
        .build()];

    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let result = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(mp)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidPart");
}

#[tokio::test]
async fn test_multipart_upload_incorrect_etag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    let resp = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = resp.upload_id().unwrap().to_string();

    client
        .upload_part()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"\0"))
        .send()
        .await
        .unwrap();

    let parts = vec![aws_sdk_s3::types::CompletedPart::builder()
        .e_tag("\"ffffffffffffffffffffffffffffffff\"")
        .part_number(1)
        .build()];

    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let result = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(mp)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidPart");
}

#[tokio::test]
async fn test_multipart_upload_overwrite_existing_object() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let payload = vec![b'x'; 5 * 1024 * 1024];

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(payload.clone()))
        .send()
        .await
        .unwrap();

    let result = multipart_upload(&client, &bucket_name, key, 10 * 1024 * 1024, None, None, None, None).await;
    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(result.parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(mp)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_length().unwrap_or(0), 10 * 1024 * 1024);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_copy_small() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let src_key = "foo";
    let src_bucket = create_key_with_random_content(&client, src_key, None, None).await;
    let dest_bucket = get_new_bucket(Some(&client)).await;
    let dest_key = "mymultipart";
    let size = 1;

    let resp = client
        .create_multipart_upload()
        .bucket(&dest_bucket)
        .key(dest_key)
        .send()
        .await
        .unwrap();
    let upload_id = resp.upload_id().unwrap().to_string();

    let copy_resp = client
        .upload_part_copy()
        .bucket(&dest_bucket)
        .key(dest_key)
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source(format!("{src_bucket}/{src_key}"))
        .copy_source_range(format!("bytes=0-{}", size - 1))
        .customize()
        .mutate_request(|req| { req.headers_mut().insert("content-length", "0"); })
        .send()
        .await
        .unwrap();

    let parts = vec![aws_sdk_s3::types::CompletedPart::builder()
        .e_tag(copy_resp.copy_part_result().unwrap().e_tag().unwrap_or_default())
        .part_number(1)
        .build()];

    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(&dest_bucket)
        .key(dest_key)
        .upload_id(&upload_id)
        .multipart_upload(mp)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&dest_bucket)
        .key(dest_key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_length().unwrap_or(-1), size);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_copy_without_range() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let src_key = "source";
    let src_size: i64 = 10 * 1024 * 1024;
    let src_bucket = create_key_with_random_content(&client, src_key, None, Some(src_size as usize)).await;
    let dest_bucket = get_new_bucket(Some(&client)).await;
    let dest_key = "mymultipartcopy";

    let resp = client
        .create_multipart_upload()
        .bucket(&dest_bucket)
        .key(dest_key)
        .send()
        .await
        .unwrap();
    let upload_id = resp.upload_id().unwrap().to_string();

    let copy_resp = client
        .upload_part_copy()
        .bucket(&dest_bucket)
        .key(dest_key)
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source(format!("{src_bucket}/{src_key}"))
        .customize()
        .mutate_request(|req| { req.headers_mut().insert("content-length", "0"); })
        .send()
        .await
        .unwrap();

    let parts = vec![aws_sdk_s3::types::CompletedPart::builder()
        .e_tag(copy_resp.copy_part_result().unwrap().e_tag().unwrap_or_default())
        .part_number(1)
        .build()];

    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(&dest_bucket)
        .key(dest_key)
        .upload_id(&upload_id)
        .multipart_upload(mp)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&dest_bucket)
        .key(dest_key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_length().unwrap_or(-1), src_size);
}

#[tokio::test]
async fn test_multipart_copy_invalid_range() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let src_key = "source";
    let bucket_name =
        create_key_with_random_content(&client, src_key, None, Some(5 * 1024 * 1024)).await;

    let resp = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key("dest")
        .send()
        .await
        .unwrap();
    let upload_id = resp.upload_id().unwrap().to_string();

    let result = client
        .upload_part_copy()
        .bucket(&bucket_name)
        .key("dest")
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source(format!("{bucket_name}/{src_key}"))
        .copy_source_range("bytes=0-536870911")
        .customize()
        .mutate_request(|req| { req.headers_mut().insert("content-length", "0"); })
        .send()
        .await;
    let err = s3_tests_rs::expect_s3_err!(result);
    assert!(
        err.status == 400 || err.status == 416,
        "Expected 400 or 416, got {}",
        err.status
    );
    assert_eq!(err.error_code, "InvalidRange");
}

#[tokio::test]
async fn test_multipart_upload_with_metadata() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let content_type = "text/bla";
    let objlen = 30 * 1024 * 1024;

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("foo".to_string(), "bar".to_string());

    let result = multipart_upload(
        &client,
        &bucket_name,
        key,
        objlen,
        None,
        Some(content_type),
        Some(metadata),
        None,
    )
    .await;

    client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(result.parts.clone()))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_type().unwrap_or_default(), content_type);
    let md = response.metadata().unwrap();
    assert_eq!(md.get("foo").map(|s| s.as_str()), Some("bar"));
    assert_eq!(response.content_length().unwrap_or(0), objlen as i64);
}

#[tokio::test]
async fn test_abort_multipart_upload_not_found() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    let result = client
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id("56788")
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchUpload");
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: multipart listing or size check")]
async fn test_list_multipart_upload_multiple() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let mb = 1024 * 1024;

    let result1 = multipart_upload(&client, &bucket_name, key, 5 * mb, None, None, None, None).await;
    let result2 = multipart_upload(&client, &bucket_name, key, 6 * mb, None, None, None, None).await;

    let key2 = "mymultipart2";
    let result3 = multipart_upload(&client, &bucket_name, key2, 5 * mb, None, None, None, None).await;

    let response = client
        .list_multipart_uploads()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let upload_ids: Vec<&str> = response
        .uploads()
        .iter()
        .map(|u| u.upload_id().unwrap_or_default())
        .collect();

    assert!(upload_ids.contains(&result1.upload_id.as_str()));
    assert!(upload_ids.contains(&result2.upload_id.as_str()));
    assert!(upload_ids.contains(&result3.upload_id.as_str()));

    client
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result1.upload_id)
        .send()
        .await
        .unwrap();
    client
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result2.upload_id)
        .send()
        .await
        .unwrap();
    client
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key(key2)
        .upload_id(&result3.upload_id)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_atomic_multipart_upload_write() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let resp = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    let upload_id = resp.upload_id().unwrap().to_string();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body, b"bar");

    client
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key("foo")
        .upload_id(&upload_id)
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
    let body = get_body(response).await;
    assert_eq!(body, b"bar");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: multipart listing or size check")]
async fn test_list_multipart_upload_owner() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client1 = get_client();
    let bucket_name = get_new_bucket(Some(&client1)).await;
    let cfg = get_config();

    client1
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let client2 = get_alt_client();
    let key1 = "multipart1";
    let key2 = "multipart2";

    let upload1 = client1
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key1)
        .send()
        .await
        .unwrap();
    let upload_id1 = upload1.upload_id().unwrap().to_string();

    let upload2 = client2
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key2)
        .send()
        .await
        .unwrap();
    let upload_id2 = upload2.upload_id().unwrap().to_string();

    let response = client1
        .list_multipart_uploads()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let uploads = response.uploads();
    assert_eq!(uploads.len(), 2);

    assert_eq!(uploads[0].key().unwrap(), key1);
    assert_eq!(uploads[0].upload_id().unwrap(), upload_id1);
    assert_eq!(uploads[0].initiator().unwrap().id().unwrap(), cfg.main_user_id);
    assert_eq!(uploads[0].owner().unwrap().id().unwrap(), cfg.main_user_id);

    assert_eq!(uploads[1].key().unwrap(), key2);
    assert_eq!(uploads[1].upload_id().unwrap(), upload_id2);
    assert_eq!(uploads[1].initiator().unwrap().id().unwrap(), cfg.alt_user_id);
    assert_eq!(uploads[1].owner().unwrap().id().unwrap(), cfg.alt_user_id);

    client2
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key(key2)
        .upload_id(&upload_id2)
        .send()
        .await
        .unwrap();
    client1
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key(key1)
        .upload_id(&upload_id1)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_multipart_copy_improper_range() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let src_key = "source";
    let src_bucket = create_key_with_random_content(&client, src_key, None, Some(5)).await;

    let mp = client
        .create_multipart_upload()
        .bucket(&src_bucket)
        .key("dest")
        .send()
        .await
        .unwrap();
    let upload_id = mp.upload_id().unwrap();

    let bad_ranges = [
        "0-2",
        "bytes=0",
        "bytes=hello-world",
        "bytes=0-bar",
        "bytes=hello-",
        "bytes=0-2,3-5",
    ];

    for range in bad_ranges {
        let result = client
            .upload_part_copy()
            .bucket(&src_bucket)
            .key("dest")
            .upload_id(upload_id)
            .copy_source(format!("{src_bucket}/{src_key}"))
            .copy_source_range(range)
            .part_number(1)
            .send()
            .await;
        assert_s3_err!(result, 400, "InvalidArgument");
    }
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_get_part() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    let part_size = 5 * 1024 * 1024;
    let total_size = 3 * part_size + 1024 * 1024;
    let part_sizes = [part_size, part_size, part_size, 1024 * 1024];
    let part_count = part_sizes.len() as i32;

    let result = multipart_upload(&client, &bucket_name, key, total_size, Some(part_size), None, None, None).await;

    let err = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .part_number(1)
        .send()
        .await;
    assert_s3_err!(err, 404, "NoSuchKey");

    client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(result.parts.clone()))
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(result.parts.len() as i32, part_count);

    for (part, &size) in result.parts.iter().zip(part_sizes.iter()) {
        let response = client
            .head_object()
            .bucket(&bucket_name)
            .key(key)
            .part_number(part.part_number().unwrap_or_default())
            .send()
            .await
            .unwrap();
        assert_eq!(response.parts_count(), Some(part_count));

        let response = client
            .get_object()
            .bucket(&bucket_name)
            .key(key)
            .part_number(part.part_number().unwrap_or_default())
            .send()
            .await
            .unwrap();
        assert_eq!(response.parts_count(), Some(part_count));
        assert_eq!(response.content_length(), Some(size as i64));
    }

    let err = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .part_number(5)
        .send()
        .await;
    assert_s3_err!(err, 400, "InvalidPart");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_non_multipart_get_part() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "singlepart";

    let response = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"body"))
        .send()
        .await
        .unwrap();
    let etag = response.e_tag().unwrap().to_string();

    let err = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .part_number(2)
        .send()
        .await;
    assert_s3_err!(err, 400, "InvalidPart");

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .part_number(1)
        .send()
        .await
        .unwrap();
    assert_eq!(response.e_tag().unwrap(), &etag);
    let body = get_body(response).await;
    assert_eq!(body, b"body");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_upload() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let content_type = "text/bla";
    let objlen = 30 * 1024 * 1024;
    let metadata: std::collections::HashMap<String, String> =
        [("foo".to_string(), "bar".to_string())].into();

    let result = multipart_upload(
        &client,
        &bucket_name,
        key,
        objlen,
        None,
        Some(content_type),
        Some(metadata.clone()),
        None,
    )
    .await;

    client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(result.parts.clone()))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .prefix(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 1);
    assert_eq!(response.contents()[0].size(), Some(objlen as i64));

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.content_type().unwrap(), content_type);
    let resp_metadata = response.metadata().unwrap();
    assert_eq!(resp_metadata.get("foo").map(|s| s.as_str()), Some("bar"));
    let body = get_body(response).await;
    assert_eq!(body.len(), objlen);
    assert_eq!(body, result.data);
}

// --- Get-part tests ---

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_single_get_part() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";

    let part_size = 5 * 1024 * 1024;
    let result = multipart_upload(&client, &bucket, key, part_size, Some(part_size), None, None, None).await;

    // request part before complete
    let res = client.get_object().bucket(&bucket).key(key).part_number(1).send().await;
    assert_s3_err!(res, 404, "NoSuchKey");

    let complete_resp = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(result.parts.clone())).build())
        .send()
        .await
        .unwrap();
    let complete_etag = complete_resp.e_tag().unwrap().to_string();
    assert_eq!(result.parts.len(), 1);

    let head = client.head_object().bucket(&bucket).key(key).part_number(1).send().await.unwrap();
    assert_eq!(head.parts_count(), Some(1));

    let get_resp = client.get_object().bucket(&bucket).key(key).part_number(1).send().await.unwrap();
    assert_eq!(get_resp.parts_count(), Some(1));
    assert_eq!(get_resp.content_length().unwrap(), part_size as i64);
    let body = get_body(get_resp).await;
    assert_eq!(body, result.data);

    // request PartNumber out of range
    let res = client.get_object().bucket(&bucket).key(key).part_number(5).send().await;
    assert_s3_err!(res, 400, "InvalidPart");
}

/* url_decode() returns "" on invalid percent sequences, silently dropping
 * the copy source; the request degrades to a plain UploadPart with empty
 * body and succeeds.  Likely an op-layer issue, not driver-specific. */
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: url_decode drops invalid percent-encoded copy source")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: multipart listing or size check")]
async fn test_upload_part_copy_percent_encoded_key() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "anyfile.txt";
    let encoded_key = "anyfilename%25.txt";
    let raw_key = "anyfilename%.txt";

    client
        .put_object()
        .bucket(&bucket)
        .key(encoded_key)
        .body(ByteStream::from_static(b"foo"))
        .content_type("text/plain")
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"foo"))
        .content_type("text/plain")
        .send()
        .await
        .unwrap();

    let mpu = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = mpu.upload_id().unwrap().to_string();

    let copy_source = format!("{}/{}", bucket, raw_key);
    let result = client
        .upload_part_copy()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .upload_id(&upload_id)
        .copy_source(&copy_source)
        .customize()
        .mutate_request(|req| { req.headers_mut().insert("content-length", "0"); })
        .send()
        .await;
    assert!(result.is_err());

    let resp = client.get_object().bucket(&bucket).key(key).send().await.unwrap();
    let body = get_body(resp).await;
    assert_eq!(body, b"foo");
}

// --- Multipart copy variant tests ---

#[tokio::test]
async fn test_multipart_copy_special_names() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let src_bucket = get_new_bucket(Some(&client)).await;
    let dest_bucket = get_new_bucket(Some(&client)).await;
    let dest_key = "mymultipart";

    // SDK rejects whitespace-only keys (MissingField: key), so skip " "
    for src_key in ["_", "__", "?versionId"] {
        client
            .put_object()
            .bucket(&src_bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();

        let copy_result = multipart_copy(&client, &src_bucket, src_key, &dest_bucket, dest_key, 1, None, None).await;
        client
            .complete_multipart_upload()
            .bucket(&dest_bucket)
            .key(dest_key)
            .upload_id(&copy_result.upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(copy_result.parts)).build())
            .send()
            .await
            .unwrap();

        let resp = client.get_object().bucket(&dest_bucket).key(dest_key).send().await.unwrap();
        assert_eq!(resp.content_length().unwrap(), 1);
    }
}

#[tokio::test]
async fn test_multipart_copy_multiple_sizes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let src_key = "foo";
    let src_bucket = create_key_with_random_content(&client, src_key, None, Some(12 * 1024 * 1024)).await;
    let dest_bucket = get_new_bucket(Some(&client)).await;
    let dest_key = "mymultipart";

    for size in [
        5 * 1024 * 1024,
        5 * 1024 * 1024 + 100 * 1024,
        5 * 1024 * 1024 + 600 * 1024,
        10 * 1024 * 1024 + 100 * 1024,
        10 * 1024 * 1024 + 600 * 1024,
        10 * 1024 * 1024,
    ] {
        let copy_result = multipart_copy(&client, &src_bucket, src_key, &dest_bucket, dest_key, size, None, None).await;
        client
            .complete_multipart_upload()
            .bucket(&dest_bucket)
            .key(dest_key)
            .upload_id(&copy_result.upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(copy_result.parts)).build())
            .send()
            .await
            .unwrap();

        let src_resp = client
            .get_object()
            .bucket(&src_bucket)
            .key(src_key)
            .range(format!("bytes=0-{}", size - 1))
            .send()
            .await
            .unwrap();
        let src_body = get_body(src_resp).await;

        let dest_resp = client.get_object().bucket(&dest_bucket).key(dest_key).send().await.unwrap();
        let dest_body = get_body(dest_resp).await;

        assert_eq!(src_body, dest_body);
    }
}

// --- Checksum tests ---

fn make_repeated_body(size: usize, ch: u8) -> Vec<u8> {
    vec![ch; size]
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_checksum_sha256() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let size = 1024;

    let key = "mymultipart";
    let create_resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.checksum_algorithm(), Some(&ChecksumAlgorithm::Sha256));
    let upload_id = create_resp.upload_id().unwrap().to_string();

    let body = make_repeated_body(size, b'A');
    let part_sha256 = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";
    let upload_resp = client
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body.clone()))
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(part_sha256)
        .send()
        .await
        .unwrap();

    // bad composite checksum should be rejected
    let result = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .checksum_sha256("bad")
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .e_tag(upload_resp.e_tag().unwrap())
                        .checksum_sha256(upload_resp.checksum_sha256().unwrap())
                        .part_number(1)
                        .build(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 400, "BadDigest");

    // use a fresh key for the missing-part-checksum test
    let key2 = "mymultipart2";
    let create_resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key2)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .unwrap();
    let upload_id2 = create_resp.upload_id().unwrap().to_string();

    let upload_resp2 = client
        .upload_part()
        .bucket(&bucket)
        .key(key2)
        .upload_id(&upload_id2)
        .part_number(1)
        .body(ByteStream::from(body.clone()))
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(part_sha256)
        .send()
        .await
        .unwrap();

    // missing part checksum should be rejected
    let result = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key2)
        .upload_id(&upload_id2)
        .checksum_sha256("bad")
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .e_tag(upload_resp2.e_tag().unwrap())
                        .part_number(1)
                        .build(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 400, "BadDigest");

    // valid composite checksum should succeed
    let key3 = "mymultipart3";
    let create_resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key3)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .unwrap();
    let upload_id3 = create_resp.upload_id().unwrap().to_string();

    let upload_resp3 = client
        .upload_part()
        .bucket(&bucket)
        .key(key3)
        .upload_id(&upload_id3)
        .part_number(1)
        .body(ByteStream::from(body))
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(part_sha256)
        .send()
        .await
        .unwrap();

    let composite_sha256 = "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1";
    let complete_resp = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key3)
        .upload_id(&upload_id3)
        .checksum_sha256(composite_sha256)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .e_tag(upload_resp3.e_tag().unwrap())
                        .checksum_sha256(upload_resp3.checksum_sha256().unwrap())
                        .part_number(1)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(complete_resp.checksum_sha256().unwrap(), composite_sha256);

    let head = client.head_object().bucket(&bucket).key(key3).send().await.unwrap();
    assert!(head.checksum_sha256().is_none());

    let head = client
        .head_object()
        .bucket(&bucket)
        .key(key3)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(head.checksum_sha256().unwrap(), composite_sha256);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_reupload_checksum_and_etag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let size = 5 * 1024 * 1024;

    let create_resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .unwrap();
    let upload_id = create_resp.upload_id().unwrap().to_string();

    let body1 = make_repeated_body(size, b'A');
    let body2 = make_repeated_body(size, b'B');
    let body3 = make_repeated_body(size, b'C');

    let part1_sha256 = "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=";
    let part2_sha256 = "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=";
    let part3_sha256 = "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=";
    let composite_etag = "b2add96cc9702bbf4efb0ccdfc6b7747-3";
    let composite_sha256 = "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3";

    let r1 = client.upload_part().bucket(&bucket).key(key).upload_id(&upload_id).part_number(1)
        .body(ByteStream::from(body1)).checksum_algorithm(ChecksumAlgorithm::Sha256).checksum_sha256(part1_sha256)
        .send().await.unwrap();
    let r2 = client.upload_part().bucket(&bucket).key(key).upload_id(&upload_id).part_number(2)
        .body(ByteStream::from(body2)).checksum_algorithm(ChecksumAlgorithm::Sha256).checksum_sha256(part2_sha256)
        .send().await.unwrap();
    let r3 = client.upload_part().bucket(&bucket).key(key).upload_id(&upload_id).part_number(3)
        .body(ByteStream::from(body3)).checksum_algorithm(ChecksumAlgorithm::Sha256).checksum_sha256(part3_sha256)
        .send().await.unwrap();

    let mpu = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().e_tag(r1.e_tag().unwrap()).checksum_sha256(r1.checksum_sha256().unwrap()).part_number(1).build())
        .parts(CompletedPart::builder().e_tag(r2.e_tag().unwrap()).checksum_sha256(r2.checksum_sha256().unwrap()).part_number(2).build())
        .parts(CompletedPart::builder().e_tag(r3.e_tag().unwrap()).checksum_sha256(r3.checksum_sha256().unwrap()).part_number(3).build())
        .build();

    let res1 = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .checksum_sha256(composite_sha256)
        .multipart_upload(mpu.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(res1.e_tag().unwrap().trim_matches('"'), composite_etag);
    assert_eq!(res1.checksum_sha256().unwrap(), composite_sha256);

    // idempotent retry should return matching etag and checksum
    let res2 = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .checksum_sha256(composite_sha256)
        .multipart_upload(mpu)
        .send()
        .await
        .unwrap();

    assert_eq!(res1.e_tag(), res2.e_tag());
    assert_eq!(res1.checksum_sha256(), res2.checksum_sha256());
}

// --- Checksum helper tests (3-part uploads with various algorithms) ---

struct CksumTestParams {
    algo: ChecksumAlgorithm,
    cksum_type: ChecksumType,
    part1_cksum: &'static str,
    part2_cksum: &'static str,
    part3_cksum: &'static str,
    composite_cksum: &'static str,
}

async fn multipart_checksum_3parts_helper(params: &CksumTestParams) {
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "mymultipart3";
    let size = 5 * 1024 * 1024;

    let create_resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(params.algo.clone())
        .checksum_type(params.cksum_type.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.checksum_algorithm(), Some(&params.algo));
    let upload_id = create_resp.upload_id().unwrap().to_string();

    let body1 = make_repeated_body(size, b'A');
    let body2 = make_repeated_body(size, b'B');
    let body3 = make_repeated_body(size, b'C');

    let upload_part = |pn: i32, body: Vec<u8>, cksum: &str| {
        let mut req = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(pn)
            .body(ByteStream::from(body))
            .checksum_algorithm(params.algo.clone());
        match &params.algo {
            a if *a == ChecksumAlgorithm::Sha256 => { req = req.checksum_sha256(cksum); }
            a if *a == ChecksumAlgorithm::Sha1 => { req = req.checksum_sha1(cksum); }
            a if *a == ChecksumAlgorithm::Crc32 => { req = req.checksum_crc32(cksum); }
            a if *a == ChecksumAlgorithm::Crc32C => { req = req.checksum_crc32_c(cksum); }
            a if *a == ChecksumAlgorithm::Crc64Nvme => { req = req.checksum_crc64_nvme(cksum); }
            _ => {}
        }
        req.send()
    };

    let r1 = upload_part(1, body1, params.part1_cksum).await.unwrap();
    let r2 = upload_part(2, body2, params.part2_cksum).await.unwrap();
    let r3 = upload_part(3, body3, params.part3_cksum).await.unwrap();

    let build_part = |resp: &aws_sdk_s3::operation::upload_part::UploadPartOutput, pn: i32| -> CompletedPart {
        let mut b = CompletedPart::builder()
            .e_tag(resp.e_tag().unwrap())
            .part_number(pn);
        match &params.algo {
            a if *a == ChecksumAlgorithm::Sha256 => { b = b.checksum_sha256(resp.checksum_sha256().unwrap()); }
            a if *a == ChecksumAlgorithm::Sha1 => { b = b.checksum_sha1(resp.checksum_sha1().unwrap()); }
            a if *a == ChecksumAlgorithm::Crc32 => { b = b.checksum_crc32(resp.checksum_crc32().unwrap()); }
            a if *a == ChecksumAlgorithm::Crc32C => { b = b.checksum_crc32_c(resp.checksum_crc32_c().unwrap()); }
            a if *a == ChecksumAlgorithm::Crc64Nvme => { b = b.checksum_crc64_nvme(resp.checksum_crc64_nvme().unwrap()); }
            _ => {}
        }
        b.build()
    };

    let mpu = CompletedMultipartUpload::builder()
        .parts(build_part(&r1, 1))
        .parts(build_part(&r2, 2))
        .parts(build_part(&r3, 3))
        .build();

    let mut complete_req = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(mpu);
    match &params.algo {
        a if *a == ChecksumAlgorithm::Sha256 => { complete_req = complete_req.checksum_sha256(params.composite_cksum); }
        a if *a == ChecksumAlgorithm::Sha1 => { complete_req = complete_req.checksum_sha1(params.composite_cksum); }
        a if *a == ChecksumAlgorithm::Crc32 => { complete_req = complete_req.checksum_crc32(params.composite_cksum); }
        a if *a == ChecksumAlgorithm::Crc32C => { complete_req = complete_req.checksum_crc32_c(params.composite_cksum); }
        a if *a == ChecksumAlgorithm::Crc64Nvme => { complete_req = complete_req.checksum_crc64_nvme(params.composite_cksum); }
        _ => {}
    }
    let complete_resp = complete_req.send().await.unwrap();

    let resp_cksum = match &params.algo {
        a if *a == ChecksumAlgorithm::Sha256 => complete_resp.checksum_sha256().unwrap(),
        a if *a == ChecksumAlgorithm::Sha1 => complete_resp.checksum_sha1().unwrap(),
        a if *a == ChecksumAlgorithm::Crc32 => complete_resp.checksum_crc32().unwrap(),
        a if *a == ChecksumAlgorithm::Crc32C => complete_resp.checksum_crc32_c().unwrap(),
        a if *a == ChecksumAlgorithm::Crc64Nvme => complete_resp.checksum_crc64_nvme().unwrap(),
        _ => panic!("unknown algo"),
    };
    assert_eq!(resp_cksum, params.composite_cksum);

    let head1 = client.head_object().bucket(&bucket).key(key).send().await.unwrap();
    assert!(head1.checksum_sha256().is_none() && head1.checksum_crc32().is_none()
        && head1.checksum_crc32_c().is_none() && head1.checksum_sha1().is_none()
        && head1.checksum_crc64_nvme().is_none());

    let head2 = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    let attrs = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await
        .unwrap();
    let cksum = attrs.checksum().unwrap();
    assert!(cksum.checksum_type().is_some());

    for pn in [1, 2, 3] {
        let resp = client.get_object().bucket(&bucket).key(key).part_number(pn).send().await.unwrap();
        let got = match &params.algo {
            a if *a == ChecksumAlgorithm::Sha256 => resp.checksum_sha256().map(|s| s.to_string()),
            a if *a == ChecksumAlgorithm::Sha1 => resp.checksum_sha1().map(|s| s.to_string()),
            a if *a == ChecksumAlgorithm::Crc32 => resp.checksum_crc32().map(|s| s.to_string()),
            a if *a == ChecksumAlgorithm::Crc32C => resp.checksum_crc32_c().map(|s| s.to_string()),
            a if *a == ChecksumAlgorithm::Crc64Nvme => resp.checksum_crc64_nvme().map(|s| s.to_string()),
            _ => panic!("unknown algo"),
        };
        assert!(got.is_some(), "expected checksum on part {pn}");
    }
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_use_cksum_helper_sha256() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    multipart_checksum_3parts_helper(&CksumTestParams {
        algo: ChecksumAlgorithm::Sha256,
        cksum_type: ChecksumType::Composite,
        part1_cksum: "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=",
        part2_cksum: "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=",
        part3_cksum: "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=",
        composite_cksum: "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
    })
    .await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_use_cksum_helper_crc64nvme() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    multipart_checksum_3parts_helper(&CksumTestParams {
        algo: ChecksumAlgorithm::Crc64Nvme,
        cksum_type: ChecksumType::FullObject,
        part1_cksum: "L/E4WYn8v98=",
        part2_cksum: "xW1l19VobYM=",
        part3_cksum: "cK5MnNaWrW4=",
        composite_cksum: "i+6LR0y3eFo=",
    })
    .await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_use_cksum_helper_crc32() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    multipart_checksum_3parts_helper(&CksumTestParams {
        algo: ChecksumAlgorithm::Crc32,
        cksum_type: ChecksumType::FullObject,
        part1_cksum: "JRTCyQ==",
        part2_cksum: "QoZTGg==",
        part3_cksum: "YAgjqw==",
        composite_cksum: "WgDhBQ==",
    })
    .await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_use_cksum_helper_crc32c() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    multipart_checksum_3parts_helper(&CksumTestParams {
        algo: ChecksumAlgorithm::Crc32C,
        cksum_type: ChecksumType::FullObject,
        part1_cksum: "MDaLrw==",
        part2_cksum: "TH4EZg==",
        part3_cksum: "Z7mBIQ==",
        composite_cksum: "xU+Krw==",
    })
    .await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_use_cksum_helper_sha1() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    multipart_checksum_3parts_helper(&CksumTestParams {
        algo: ChecksumAlgorithm::Sha1,
        cksum_type: ChecksumType::Composite,
        part1_cksum: "iIaTCGbm+vdVjNqIMF2S0T7ibMk=",
        part2_cksum: "LS/TJ32bAVKEwRu+sE3X7awh/lk=",
        part3_cksum: "6DDwovUaHwrKNXDMzOGbuvj9kxI=",
        composite_cksum: "sizjvY4eud3MrcHdZM3cQ/ol39o=-3",
    })
    .await;
}

// --- Remaining multipart tests ---

#[tokio::test]
async fn test_multipart_upload_on_a_bucket_with_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "*",
            "Resource": [
                format!("arn:aws:s3:::{bucket}"),
                format!("arn:aws:s3:::{bucket}/*")
            ]
        }]
    })
    .to_string();
    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let key = "foo";
    let objlen = 10 * 1024 * 1024;
    let result = multipart_upload(&client, &bucket, key, objlen, None, None, None, None).await;
    let mp = CompletedMultipartUpload::builder()
        .set_parts(Some(result.parts))
        .build();
    let resp = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(mp)
        .send()
        .await
        .unwrap();
    assert!(resp.e_tag().is_some());
}

#[tokio::test]
async fn test_multipart_copy_versioned() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let src_bucket = get_new_bucket(Some(&client)).await;
    let dest_bucket = get_new_bucket(Some(&client)).await;
    let dest_key = "mymultipart";
    let src_key = "foo";
    let size = 15 * 1024 * 1024;

    check_configure_versioning_retry(&client, &src_bucket, "Enabled", "Enabled").await;

    // create 3 versions
    for _ in 0..3 {
        create_key_with_random_content(&client, src_key, Some(&src_bucket), Some(size)).await;
    }

    let versions_resp = client.list_object_versions().bucket(&src_bucket).send().await.unwrap();
    let version_ids: Vec<String> = versions_resp
        .versions()
        .iter()
        .map(|v| v.version_id().unwrap_or_default().to_string())
        .collect();

    for vid in &version_ids {
        let copy_result = multipart_copy(&client, &src_bucket, src_key, &dest_bucket, dest_key, size, None, Some(vid)).await;
        client
            .complete_multipart_upload()
            .bucket(&dest_bucket)
            .key(dest_key)
            .upload_id(&copy_result.upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(copy_result.parts)).build())
            .send()
            .await
            .unwrap();

        let resp = client.get_object().bucket(&dest_bucket).key(dest_key).send().await.unwrap();
        assert_eq!(resp.content_length().unwrap(), size as i64);
    }
}

#[tokio::test]
async fn test_get_single_multipart_object_attributes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "multipart";
    let part_size = 5 * 1024 * 1024;

    let result = multipart_upload(&client, &bucket, key, part_size, Some(part_size), None, None, None).await;
    let complete_resp = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(result.parts)).build())
        .send()
        .await
        .unwrap();
    let etag = complete_resp.e_tag().unwrap().trim_matches('"').to_string();
    assert!(!etag.is_empty());

    let attrs = "ETag,ObjectParts,StorageClass,ObjectSize";
    let resp = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-object-attributes", attrs);
        })
        .send()
        .await
        .unwrap();

    assert_eq!(resp.object_size(), Some(part_size as i64));
    assert_eq!(resp.e_tag().unwrap(), &etag);
    assert_eq!(*resp.storage_class().unwrap(), aws_sdk_s3::types::StorageClass::Standard);

    let obj_parts = resp.object_parts().unwrap();
    assert_eq!(obj_parts.parts().len(), 1);
    let part = &obj_parts.parts()[0];
    assert_eq!(part.part_number(), Some(1));
    assert_eq!(part.size(), Some(part_size as i64));
}

#[tokio::test]
async fn test_get_multipart_object_attributes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "multipart";
    let part_size = 5 * 1024 * 1024;
    let objlen = 30 * 1024 * 1024;
    let nparts = objlen / part_size;

    let result = multipart_upload(&client, &bucket, key, objlen, Some(part_size), None, None, None).await;
    let complete_resp = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(result.parts)).build())
        .send()
        .await
        .unwrap();
    let etag = complete_resp.e_tag().unwrap().trim_matches('"').to_string();
    assert!(!etag.is_empty());

    let attrs = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize";
    let resp = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-object-attributes", attrs);
        })
        .send()
        .await
        .unwrap();

    assert_eq!(resp.object_size(), Some(objlen as i64));
    assert_eq!(resp.e_tag().unwrap(), &etag);
    assert_eq!(*resp.storage_class().unwrap(), aws_sdk_s3::types::StorageClass::Standard);

    let obj_parts = resp.object_parts().unwrap();
    assert_eq!(obj_parts.total_parts_count(), Some(nparts as i32));
    assert_eq!(obj_parts.is_truncated(), Some(false));
    assert_eq!(obj_parts.parts().len(), nparts);

    for (i, part) in obj_parts.parts().iter().enumerate() {
        assert_eq!(part.part_number(), Some((i + 1) as i32));
        assert_eq!(part.size(), Some(part_size as i64));
        assert!(part.checksum_sha256().is_none());
    }
}

#[tokio::test]
async fn test_get_paginated_multipart_object_attributes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "multipart";
    let part_size = 5 * 1024 * 1024;
    let objlen = 30 * 1024 * 1024;
    let nparts = objlen / part_size;

    let result = multipart_upload(&client, &bucket, key, objlen, Some(part_size), None, None, None).await;
    let complete_resp = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(result.parts)).build())
        .send()
        .await
        .unwrap();
    let etag = complete_resp.e_tag().unwrap().trim_matches('"').to_string();
    assert!(!etag.is_empty());

    let attrs = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize";
    let resp = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .max_parts(1)
        .part_number_marker("3")
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-object-attributes", attrs);
        })
        .send()
        .await
        .unwrap();

    assert_eq!(resp.object_size(), Some(objlen as i64));
    assert_eq!(resp.e_tag().unwrap(), &etag);
    assert_eq!(*resp.storage_class().unwrap(), aws_sdk_s3::types::StorageClass::Standard);

    let obj_parts = resp.object_parts().unwrap();
    assert_eq!(obj_parts.total_parts_count(), Some(nparts as i32));
    assert_eq!(obj_parts.max_parts(), Some(1));
    assert_eq!(obj_parts.part_number_marker(), Some("3"));
    assert_eq!(obj_parts.is_truncated(), Some(true));
    assert_eq!(obj_parts.next_part_number_marker(), Some("4"));
    assert_eq!(obj_parts.parts().len(), 1);
    let part = &obj_parts.parts()[0];
    assert_eq!(part.part_number(), Some(4));
    assert_eq!(part.size(), Some(part_size as i64));
    assert!(part.checksum_sha256().is_none());

    let attrs2 = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize";
    let resp2 = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .max_parts(10)
        .part_number_marker("4")
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-object-attributes", attrs2);
        })
        .send()
        .await
        .unwrap();

    assert_eq!(resp2.object_size(), Some(objlen as i64));
    assert_eq!(resp2.e_tag().unwrap(), &etag);
    assert_eq!(*resp2.storage_class().unwrap(), aws_sdk_s3::types::StorageClass::Standard);

    let obj_parts2 = resp2.object_parts().unwrap();
    assert_eq!(obj_parts2.total_parts_count(), Some(nparts as i32));
    assert_eq!(obj_parts2.max_parts(), Some(10));
    assert_eq!(obj_parts2.is_truncated(), Some(false));
    assert_eq!(obj_parts2.part_number_marker(), Some("4"));
    assert_eq!(obj_parts2.parts().len(), 2);

    for (i, part) in obj_parts2.parts().iter().enumerate() {
        assert_eq!(part.part_number(), Some((5 + i) as i32));
        assert_eq!(part.size(), Some(part_size as i64));
        assert!(part.checksum_sha256().is_none());
    }
}

#[tokio::test]
async fn test_multipart_upload_complete_without_create() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let result = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("mymultipart")
        .upload_id("abc1234def")
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .e_tag("1234")
                        .part_number(1)
                        .build(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchUpload");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_upload_resend_part() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let part_size = 5 * 1024 * 1024;
    let objlen = 30 * 1024 * 1024;
    let nparts = objlen / part_size;
    let content_type = "text/bla";
    let metadata = std::collections::HashMap::from([("foo".to_string(), "bar".to_string())]);

    let resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .content_type(content_type)
        .metadata("foo", "bar")
        .send()
        .await
        .unwrap();
    let upload_id = resp.upload_id().unwrap().to_string();

    let mut data = Vec::with_capacity(objlen);
    let mut parts = Vec::new();

    for pn in 1..=nparts {
        let part_data: Vec<u8> = (0..part_size).map(|i| (((pn - 1) * part_size + i) % 256) as u8).collect();
        data.extend_from_slice(&part_data);

        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(pn as i32)
            .body(ByteStream::from(part_data.clone()))
            .send()
            .await
            .unwrap();

        parts.push(
            CompletedPart::builder()
                .e_tag(resp.e_tag().unwrap_or_default())
                .part_number(pn as i32)
                .build(),
        );

        // resend part 1 with same data — last writer should win
        if pn == 1 {
            let resend_resp = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(1)
                .body(ByteStream::from(part_data))
                .send()
                .await
                .unwrap();
            parts[0] = CompletedPart::builder()
                .e_tag(resend_resp.e_tag().unwrap_or_default())
                .part_number(1)
                .build();
        }
    }

    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
        .send()
        .await
        .unwrap();

    let resp = client.get_object().bucket(&bucket).key(key).send().await.unwrap();
    assert_eq!(resp.content_type().unwrap(), content_type);
    assert_eq!(resp.metadata().unwrap().get("foo").unwrap(), "bar");
    let body = get_body(resp).await;
    assert_eq!(body.len(), objlen);
    assert_eq!(body, data);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_multipart_resend_first_finishes_last() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let file_size = 8;

    let resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = resp.upload_id().unwrap().to_string();

    let body_a = vec![b'A'; file_size];
    let body_b = vec![b'B'; file_size];

    // upload part 1 with content A
    client
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body_a))
        .send()
        .await
        .unwrap();

    // re-upload part 1 with content B — should replace A
    let resp_b = client
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body_b.clone()))
        .send()
        .await
        .unwrap();

    let parts = vec![
        CompletedPart::builder()
            .e_tag(resp_b.e_tag().unwrap_or_default())
            .part_number(1)
            .build(),
    ];

    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
        .send()
        .await
        .unwrap();

    let resp = client.get_object().bucket(&bucket).key(key).send().await.unwrap();
    let body = get_body(resp).await;
    assert_eq!(body, body_b);
}
