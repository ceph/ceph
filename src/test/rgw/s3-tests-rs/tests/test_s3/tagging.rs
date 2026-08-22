use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::{get_alt_client, get_client};
use s3_tests_rs::fixtures::{create_key_with_random_content, get_new_bucket};
use s3_tests_rs::policy::{make_arn_resource, make_json_policy};
use s3_tests_rs::assert_s3_err;

fn create_simple_tagset(count: usize) -> Vec<aws_sdk_s3::types::Tag> {
    (0..count)
        .map(|i| {
            aws_sdk_s3::types::Tag::builder()
                .key(i.to_string())
                .value(i.to_string())
                .build()
                .unwrap()
        })
        .collect()
}

#[tokio::test]
async fn test_set_bucket_tagging() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let result = client
        .get_bucket_tagging()
        .bucket(&bucket_name)
        .send()
        .await;
    let err = s3_tests_rs::expect_s3_err!(result);
    assert_eq!(err.status, 404);
    assert_eq!(err.error_code, "NoSuchTagSet");

    let tagging = aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("Hello")
                .value("World")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    client
        .put_bucket_tagging()
        .bucket(&bucket_name)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_tagging()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 1);
    assert_eq!(response.tag_set()[0].key(), "Hello");
    assert_eq!(response.tag_set()[0].value(), "World");

    client
        .delete_bucket_tagging()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let result = client
        .get_bucket_tagging()
        .bucket(&bucket_name)
        .send()
        .await;
    let err = s3_tests_rs::expect_s3_err!(result);
    assert_eq!(err.status, 404);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_get_obj_tagging() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputtags";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let input_tagset = create_simple_tagset(2);
    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(input_tagset.clone()))
        .build()
        .unwrap();

    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    assert_eq!(response.tag_set().len(), 2);
    for (i, tag) in response.tag_set().iter().enumerate() {
        assert_eq!(tag.key(), i.to_string());
        assert_eq!(tag.value(), i.to_string());
    }
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_put_max_tags() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputmaxtags";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let input_tagset = create_simple_tagset(10);
    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(input_tagset.clone()))
        .build()
        .unwrap();

    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 10);
}

#[tokio::test]
async fn test_put_excess_tags() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputmaxtags";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let input_tagset = create_simple_tagset(11);
    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(input_tagset))
        .build()
        .unwrap();

    let result = client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidTag");

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 0);
}

#[tokio::test]
async fn test_delete_obj_tagging() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testdeltags";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let input_tagset = create_simple_tagset(2);
    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(input_tagset))
        .build()
        .unwrap();

    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    client
        .delete_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 0);
}

fn make_random_string(len: usize) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| (b'a' + rng.gen_range(0..26u8)) as char)
        .collect()
}

#[tokio::test]
async fn test_put_max_kvsize_tags() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputmaxkeysize";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let mut tagset = Vec::new();
    for _ in 0..10 {
        tagset.push(
            aws_sdk_s3::types::Tag::builder()
                .key(make_random_string(128))
                .value(make_random_string(256))
                .build()
                .unwrap(),
        );
    }

    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(tagset))
        .build()
        .unwrap();

    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 10);
}

#[tokio::test]
async fn test_put_excess_key_tags() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputexcesskeytags";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let mut tagset = Vec::new();
    for _ in 0..10 {
        tagset.push(
            aws_sdk_s3::types::Tag::builder()
                .key(make_random_string(129))
                .value(make_random_string(256))
                .build()
                .unwrap(),
        );
    }

    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(tagset))
        .build()
        .unwrap();

    let result = client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidTag");

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 0);
}

#[tokio::test]
async fn test_put_excess_val_tags() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputexcessvaltags";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let mut tagset = Vec::new();
    for _ in 0..10 {
        tagset.push(
            aws_sdk_s3::types::Tag::builder()
                .key(make_random_string(128))
                .value(make_random_string(257))
                .build()
                .unwrap(),
        );
    }

    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(tagset))
        .build()
        .unwrap();

    let result = client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidTag");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_put_modify_tags() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputmodifytags";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let tagging1 = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("key").value("val").build().unwrap())
        .tag_set(aws_sdk_s3::types::Tag::builder().key("key2").value("val2").build().unwrap())
        .build()
        .unwrap();

    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging1)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 2);

    let tagging2 = aws_sdk_s3::types::Tagging::builder()
        .tag_set(aws_sdk_s3::types::Tag::builder().key("key3").value("val3").build().unwrap())
        .build()
        .unwrap();

    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging2)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 1);
    assert_eq!(response.tag_set()[0].key(), "key3");
    assert_eq!(response.tag_set()[0].value(), "val3");
}

#[tokio::test]
async fn test_put_obj_with_tags() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "testtagobj1";
    let data = "A".repeat(100);

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from(data.as_bytes().to_vec()))
        .tagging("foo=bar&bar")
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 2);
}

#[tokio::test]
async fn test_set_multipart_tagging() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let tags = "Hello=World&foo=bar";

    let mp = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tags)
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
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();

    let completed_part = aws_sdk_s3::types::CompletedPart::builder()
        .e_tag(part.e_tag().unwrap_or_default())
        .part_number(1)
        .build();
    let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .parts(completed_part)
        .build();

    client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 2);

    let tag_map: std::collections::HashMap<&str, &str> = response
        .tag_set()
        .iter()
        .map(|t| (t.key().as_ref(), t.value().as_ref()))
        .collect();
    assert_eq!(tag_map.get("Hello"), Some(&"World"));
    assert_eq!(tag_map.get("foo"), Some(&"bar"));

    client
        .delete_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 0);
}

#[tokio::test]
async fn test_get_obj_head_tagging() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputtags";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let tagging = aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("tag1")
                .value("val1")
                .build()
                .unwrap(),
        )
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("tag2")
                .value("val2")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    client
        .head_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    let tag_resp = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(tag_resp.tag_set().len(), 2);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_get_tags_acl_public() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputtagsacl";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let resource = make_arn_resource(&format!("{bucket_name}/{key}"));
    let policy = make_json_policy("s3:GetObjectTagging", &resource, None, None, None);
    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let input_tagset = create_simple_tagset(10);
    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(input_tagset.clone()))
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    let response = alt_client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 10);
    for (i, tag) in response.tag_set().iter().enumerate() {
        assert_eq!(tag.key(), i.to_string());
        assert_eq!(tag.value(), i.to_string());
    }
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_put_tags_acl_public() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputtagsacl";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let resource = make_arn_resource(&format!("{bucket_name}/{key}"));
    let policy = make_json_policy("s3:PutObjectTagging", &resource, None, None, None);
    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let input_tagset = create_simple_tagset(10);
    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(input_tagset.clone()))
        .build()
        .unwrap();

    let alt_client = get_alt_client();
    alt_client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 10);
    for (i, tag) in response.tag_set().iter().enumerate() {
        assert_eq!(tag.key(), i.to_string());
        assert_eq!(tag.value(), i.to_string());
    }
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_tags_obj_public() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputtagsacl";
    let bucket_name = create_key_with_random_content(&client, key, None, Some(1024)).await;

    let resource = make_arn_resource(&format!("{bucket_name}/{key}"));
    let policy = make_json_policy("s3:DeleteObjectTagging", &resource, None, None, None);
    client
        .put_bucket_policy()
        .bucket(&bucket_name)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let input_tagset = create_simple_tagset(10);
    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(input_tagset))
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    alt_client
        .delete_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_tagging()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.tag_set().len(), 0);
}

#[tokio::test]
async fn test_put_delete_tags() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let key = "testputmodifytags";
    let bucket = create_key_with_random_content(&client, key, None, None).await;

    let tagset = create_simple_tagset(2);
    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(tagset.clone()))
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let resp = client.get_object_tagging().bucket(&bucket).key(key).send().await.unwrap();
    assert_eq!(resp.tag_set().len(), 2);

    client.delete_object_tagging().bucket(&bucket).key(key).send().await.unwrap();

    let resp = client.get_object_tagging().bucket(&bucket).key(key).send().await.unwrap();
    assert_eq!(resp.tag_set().len(), 0);
}
