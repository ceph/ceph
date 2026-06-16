use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{
    check_configure_versioning_retry, create_objects, get_new_bucket, multipart_upload,
};
use s3_tests_rs::assert_s3_err;

async fn get_body(response: aws_sdk_s3::operation::get_object::GetObjectOutput) -> String {
    let bytes = response.body.collect().await.unwrap().into_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
#[tokio::test]
async fn test_versioning_bucket_create_suspend() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resp = client
        .get_bucket_versioning()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_none());

    check_configure_versioning_retry(&client, &bucket_name, "Suspended", "Suspended").await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;
    check_configure_versioning_retry(&client, &bucket_name, "Suspended", "Suspended").await;
}

async fn create_multiple_versions(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    num_versions: usize,
) -> (Vec<String>, Vec<String>) {
    let mut version_ids = Vec::new();
    let mut contents = Vec::new();

    for i in 0..num_versions {
        let body = format!("content-{i}");
        let response = client
            .put_object()
            .bucket(bucket_name)
            .key(key)
            .body(ByteStream::from(body.as_bytes().to_vec()))
            .send()
            .await
            .unwrap();
        version_ids.push(response.version_id().unwrap_or_default().to_string());
        contents.push(body);
    }
    (version_ids, contents)
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_create_versions() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "testobj";
    let num_versions = 5;
    let (version_ids, contents) = create_multiple_versions(&client, &bucket_name, key, num_versions).await;

    assert_eq!(version_ids.len(), num_versions);
    assert_eq!(contents.len(), num_versions);

    for (i, vid) in version_ids.iter().enumerate() {
        let resp = client
            .get_object()
            .bucket(&bucket_name)
            .key(key)
            .version_id(vid)
            .send()
            .await
            .unwrap();
        let body = get_body(resp).await;
        assert_eq!(body, contents[i]);
    }
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_create_read_remove_head() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "testobj";
    let (mut version_ids, mut contents) =
        create_multiple_versions(&client, &bucket_name, key, 5).await;

    let removed_vid = version_ids.pop().unwrap();
    contents.pop();

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&removed_vid)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    let body = get_body(resp).await;
    assert_eq!(body, *contents.last().unwrap());

    let del_resp = client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert!(del_resp.delete_marker().unwrap_or(false));

    let list_resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(list_resp.versions().len(), version_ids.len());
    assert_eq!(list_resp.delete_markers().len(), 1);
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_stack_delete_markers() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "test1/a";
    create_multiple_versions(&client, &bucket_name, key, 1).await;

    client.delete_object().bucket(&bucket_name).key(key).send().await.unwrap();
    client.delete_object().bucket(&bucket_name).key(key).send().await.unwrap();
    client.delete_object().bucket(&bucket_name).key(key).send().await.unwrap();

    let resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.versions().len(), 1);
    assert_eq!(resp.delete_markers().len(), 3);
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_plain_null_version_removal() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let key = "testobjfoo";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"fooz"))
        .send()
        .await
        .unwrap();

    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id("null")
        .send()
        .await
        .unwrap();

    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchKey");

    let list_resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(list_resp.versions().is_empty());
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_plain_null_version_overwrite() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let key = "testobjfoo";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"fooz"))
        .send()
        .await
        .unwrap();

    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"zzz"))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap_or_default().to_string();

    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(resp).await, "zzz");

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(resp).await, "fooz");

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id("null")
        .send()
        .await
        .unwrap();

    let result = client.get_object().bucket(&bucket_name).key(key).send().await;
    assert_s3_err!(result, 404, "NoSuchKey");
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_plain_null_version_overwrite_suspended() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let key = "testobjbar";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"foooz"))
        .send()
        .await
        .unwrap();

    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;
    check_configure_versioning_retry(&client, &bucket_name, "Suspended", "Suspended").await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b"zzz"))
        .send()
        .await
        .unwrap();

    let resp = client.get_object().bucket(&bucket_name).key(key).send().await.unwrap();
    assert_eq!(get_body(resp).await, "zzz");

    let list_resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(list_resp.versions().len(), 1);

    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .version_id("null")
        .send()
        .await
        .unwrap();

    let result = client.get_object().bucket(&bucket_name).key(key).send().await;
    assert_s3_err!(result, 404, "NoSuchKey");

    let list_resp = client.list_object_versions().bucket(&bucket_name).send().await.unwrap();
    assert!(list_resp.versions().is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_copy_obj_version() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "testobj";
    let num_versions = 3;
    let (version_ids, contents) =
        create_multiple_versions(&client, &bucket_name, key, num_versions).await;

    for i in 0..num_versions {
        let new_key = format!("key_{i}");
        let copy_source = format!("{bucket_name}/{key}?versionId={}", version_ids[i]);
        client
            .copy_object()
            .bucket(&bucket_name)
            .key(&new_key)
            .copy_source(&copy_source)
            .send()
            .await
            .unwrap();
        let resp = client.get_object().bucket(&bucket_name).key(&new_key).send().await.unwrap();
        assert_eq!(get_body(resp).await, contents[i]);
    }

    let another_bucket = get_new_bucket(Some(&client)).await;
    for i in 0..num_versions {
        let new_key = format!("key_{i}");
        let copy_source = format!("{bucket_name}/{key}?versionId={}", version_ids[i]);
        client
            .copy_object()
            .bucket(&another_bucket)
            .key(&new_key)
            .copy_source(&copy_source)
            .send()
            .await
            .unwrap();
        let resp = client.get_object().bucket(&another_bucket).key(&new_key).send().await.unwrap();
        assert_eq!(get_body(resp).await, contents[i]);
    }
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_multi_object_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "key";
    let (version_ids, _contents) =
        create_multiple_versions(&client, &bucket_name, key, 2).await;
    assert_eq!(version_ids.len(), 2);

    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = version_ids
        .iter()
        .map(|v| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(key)
                .version_id(v)
                .build()
                .unwrap()
        })
        .collect();

    let delete = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects.clone()))
        .build()
        .unwrap();
    client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete)
        .send()
        .await
        .unwrap();

    let list_resp = client.list_object_versions().bucket(&bucket_name).send().await.unwrap();
    assert!(list_resp.versions().is_empty());

    let delete = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects))
        .build()
        .unwrap();
    client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete)
        .send()
        .await
        .unwrap();

    let list_resp = client.list_object_versions().bucket(&bucket_name).send().await.unwrap();
    assert!(list_resp.versions().is_empty());
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
#[tokio::test]
async fn test_versioned_object_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "xyz";
    let (version_ids, _contents) =
        create_multiple_versions(&client, &bucket_name, key, 3).await;

    let version_id = &version_ids[1];
    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key(key)
        .version_id(version_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.grants().len(), 1);
    assert_eq!(response.grants()[0].permission().unwrap().as_str(), "FULL_CONTROL");

    client
        .put_object_acl()
        .bucket(&bucket_name)
        .key(key)
        .version_id(version_id)
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key(key)
        .version_id(version_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.grants().len(), 2);

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from_static(b""))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(response.grants().len(), 1);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
#[tokio::test]
async fn test_versioned_object_acl_no_version_specified() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "xyz";
    let (_version_ids, _contents) =
        create_multiple_versions(&client, &bucket_name, key, 3).await;

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    let version_id = response.version_id().unwrap().to_string();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.grants().len(), 1);
    assert_eq!(
        response.grants()[0].permission().unwrap().as_str(),
        "FULL_CONTROL"
    );

    client
        .put_object_acl()
        .bucket(&bucket_name)
        .key(key)
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.grants().len(), 2);
}

// --- delete marker tests ---

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_multi_object_delete_with_marker() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "key";
    let (version_ids, _contents) =
        create_multiple_versions(&client, &bucket_name, key, 2).await;
    assert_eq!(version_ids.len(), 2);

    let mut objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = version_ids
        .iter()
        .map(|v| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(key)
                .version_id(v)
                .build()
                .unwrap()
        })
        .collect();

    // create a delete marker
    let del_resp = client.delete_object().bucket(&bucket_name).key(key).send().await.unwrap();
    assert!(del_resp.delete_marker().unwrap_or(false));
    objects.push(
        aws_sdk_s3::types::ObjectIdentifier::builder()
            .key(key)
            .version_id(del_resp.version_id().unwrap_or_default())
            .build()
            .unwrap(),
    );

    let delete = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects.clone()))
        .build()
        .unwrap();
    client
        .delete_objects()
        .bucket(&bucket_name)
        .delete(delete)
        .send()
        .await
        .unwrap();

    let list_resp = client.list_object_versions().bucket(&bucket_name).send().await.unwrap();
    assert!(list_resp.versions().is_empty());
    assert!(list_resp.delete_markers().is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_multi_object_delete_with_marker_create() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "key";

    let objects = vec![aws_sdk_s3::types::ObjectIdentifier::builder()
        .key(key)
        .build()
        .unwrap()];
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
    assert!(response.deleted()[0].delete_marker().unwrap_or(false));
    let dm_version_id = response.deleted()[0]
        .delete_marker_version_id()
        .unwrap_or_default()
        .to_string();

    let list_resp = client.list_object_versions().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(list_resp.delete_markers().len(), 1);
    assert_eq!(
        list_resp.delete_markers()[0].version_id().unwrap_or_default(),
        dm_version_id
    );
    assert_eq!(list_resp.delete_markers()[0].key().unwrap_or_default(), key);
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_list_marker() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "testobj";
    let key2 = "testobj-1";
    let num_versions = 5;

    let (version_ids, _contents) =
        create_multiple_versions(&client, &bucket_name, key, num_versions).await;
    let (version_ids2, _contents2) =
        create_multiple_versions(&client, &bucket_name, key2, num_versions).await;

    let response = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let mut versions: Vec<_> = response.versions().to_vec();
    versions.reverse();

    // first 5 should be key2 (sorted alphabetically, key2="testobj-1" < key="testobj")
    for i in 0..5 {
        assert_eq!(versions[i].version_id().unwrap_or_default(), version_ids2[i]);
        assert_eq!(versions[i].key().unwrap_or_default(), key2);
    }
    // next 5 should be key
    for i in 0..5 {
        assert_eq!(versions[5 + i].version_id().unwrap_or_default(), version_ids[i]);
        assert_eq!(versions[5 + i].key().unwrap_or_default(), key);
    }
}

// --- version removal helpers ---

async fn check_obj_content(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    version_id: &str,
    expected: &str,
) {
    let resp = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .version_id(version_id)
        .send()
        .await
        .unwrap();
    let body = get_body(resp).await;
    assert_eq!(body, expected);
}

async fn check_obj_versions(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    version_ids: &[String],
    contents: &[String],
) {
    let resp = client
        .list_object_versions()
        .bucket(bucket_name)
        .send()
        .await
        .unwrap();
    let mut versions: Vec<_> = resp.versions().to_vec();
    versions.reverse();
    assert_eq!(versions.len(), version_ids.len());
    for (i, v) in versions.iter().enumerate() {
        assert_eq!(v.version_id().unwrap_or_default(), version_ids[i]);
        assert_eq!(v.key().unwrap_or_default(), key);
        check_obj_content(client, bucket_name, key, &version_ids[i], &contents[i]).await;
    }
}

async fn remove_obj_version(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    version_ids: &mut Vec<String>,
    contents: &mut Vec<String>,
    index: usize,
) {
    let idx = index % version_ids.len();
    let rm_vid = version_ids.remove(idx);
    let rm_content = contents.remove(idx);

    check_obj_content(client, bucket_name, key, &rm_vid, &rm_content).await;
    client
        .delete_object()
        .bucket(bucket_name)
        .key(key)
        .version_id(&rm_vid)
        .send()
        .await
        .unwrap();

    if !version_ids.is_empty() {
        check_obj_versions(client, bucket_name, key, version_ids, contents).await;
    }
}

async fn do_test_create_remove_versions(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    num_versions: usize,
    remove_start_idx: isize,
    idx_inc: isize,
) {
    let (mut version_ids, mut contents) =
        create_multiple_versions(client, bucket_name, key, num_versions).await;

    let mut idx = remove_start_idx;
    for _ in 0..num_versions {
        let actual_idx = if idx < 0 {
            (version_ids.len() as isize + idx) as usize
        } else {
            idx as usize
        };
        remove_obj_version(client, bucket_name, key, &mut version_ids, &mut contents, actual_idx)
            .await;
        idx += idx_inc;
    }
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_create_read_remove() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "testobj";
    let n = 5;

    do_test_create_remove_versions(&client, &bucket_name, key, n, -1, 0).await;
    do_test_create_remove_versions(&client, &bucket_name, key, n, -1, 0).await;
    do_test_create_remove_versions(&client, &bucket_name, key, n, 0, 0).await;
    do_test_create_remove_versions(&client, &bucket_name, key, n, 1, 0).await;
    do_test_create_remove_versions(&client, &bucket_name, key, n, 4, -1).await;
    do_test_create_remove_versions(&client, &bucket_name, key, n, 3, 3).await;
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_create_versions_remove_all() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "testobj";
    let num_versions = 10;
    let (mut version_ids, mut contents) =
        create_multiple_versions(&client, &bucket_name, key, num_versions).await;

    for i in 0..num_versions {
        remove_obj_version(&client, &bucket_name, key, &mut version_ids, &mut contents, i).await;
    }
    assert!(version_ids.is_empty());
    assert!(contents.is_empty());
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_create_versions_remove_special_names() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let keys = ["_testobj", "_", ":"];
    let num_versions = 10;

    for key in &keys {
        let (mut version_ids, mut contents) =
            create_multiple_versions(&client, &bucket_name, key, num_versions).await;

        for i in 0..num_versions {
            remove_obj_version(&client, &bucket_name, key, &mut version_ids, &mut contents, i)
                .await;
        }
        assert!(version_ids.is_empty());
        assert!(contents.is_empty());
    }
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
#[tokio::test]
async fn test_versioning_bucket_atomic_upload_return_version_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();

    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let resp = client
        .put_object()
        .bucket(&bucket_name)
        .key("bar")
        .body(ByteStream::from_static(b""))
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap().to_string();
    assert!(!version_id.is_empty());

    let list_resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    for v in list_resp.versions() {
        assert_eq!(v.version_id().unwrap_or_default(), version_id);
    }

    let bucket2 = get_new_bucket(Some(&client)).await;
    let resp = client
        .put_object()
        .bucket(&bucket2)
        .key("baz")
        .body(ByteStream::from_static(b""))
        .send()
        .await
        .unwrap();
    assert!(resp.version_id().is_none());

    let bucket3 = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket3, "Suspended", "Suspended").await;
    let resp = client
        .put_object()
        .bucket(&bucket3)
        .key("baz")
        .body(ByteStream::from_static(b""))
        .send()
        .await
        .unwrap();
    assert!(resp.version_id().is_none());
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_concurrent_multi_object_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let num_objects = 5;
    let num_versions = 3;
    let key_names: Vec<String> = (0..num_objects).map(|i| format!("key_{i}")).collect();
    let key_refs: Vec<&str> = key_names.iter().map(|s| s.as_str()).collect();

    for _ in 0..num_versions {
        create_objects(&client, &bucket_name, &key_refs).await;
    }

    let resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let versions = resp.versions();
    assert_eq!(versions.len(), num_objects * num_versions);

    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = versions
        .iter()
        .map(|v| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(v.key().unwrap_or_default())
                .version_id(v.version_id().unwrap_or_default())
                .build()
                .unwrap()
        })
        .collect();

    let num_tasks = 5;
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..num_tasks {
        let c = client.clone();
        let bn = bucket_name.clone();
        let objs = objects.clone();
        set.spawn(async move {
            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objs))
                .build()
                .unwrap();
            c.delete_objects()
                .bucket(&bn)
                .delete(delete)
                .send()
                .await
                .unwrap()
        });
    }

    while let Some(result) = set.join_next().await {
        let response = result.unwrap();
        assert_eq!(response.deleted().len(), num_objects * num_versions);
        assert!(response.errors().is_empty());
    }

    let resp = client
        .list_objects()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.contents().is_empty());

    let resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.versions().is_empty());
    assert!(resp.delete_markers().is_empty());
}

async fn check_delete_marker(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected: &str,
) {
    let dm_header = match client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
    {
        Ok(_resp) => "none".to_string(),
        Err(e) => {
            let raw = match &e {
                aws_sdk_s3::error::SdkError::ServiceError(se) => se.raw(),
                other => panic!("Unexpected error type: {other:?}"),
            };
            raw.headers()
                .get("x-amz-delete-marker")
                .unwrap_or("none")
                .to_string()
        }
    };
    assert_eq!(dm_header, expected);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_delete_marker_nonversioned() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "frodo.txt";
    let body = format!("body version {key}");

    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from(body.into_bytes()))
        .send()
        .await
        .unwrap();
    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    check_delete_marker(&client, &bucket_name, key, "false").await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_delete_marker_versioned() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "bilbo.txt";
    let body = format!("body version {key}");

    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from(body.into_bytes()))
        .send()
        .await
        .unwrap();
    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    check_delete_marker(&client, &bucket_name, key, "true").await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_delete_marker_suspended() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "ringo.txt";
    let body = format!("body version {key}");

    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from(body.into_bytes()))
        .send()
        .await
        .unwrap();
    check_configure_versioning_retry(&client, &bucket_name, "Suspended", "Suspended").await;
    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    check_delete_marker(&client, &bucket_name, key, "true").await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: lifecycle not implemented")]
#[tokio::test]
async fn test_delete_marker_expiration() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key = "nugent.ted";
    let body = format!("body version {key}");

    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;
    client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(ByteStream::from(body.into_bytes()))
        .send()
        .await
        .unwrap();
    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    check_delete_marker(&client, &bucket_name, key, "true").await;

    let lifecycle = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
        .rules(
            aws_sdk_s3::types::LifecycleRule::builder()
                .id("dm-1-days")
                .filter(aws_sdk_s3::types::LifecycleRuleFilter::builder().prefix("").build())
                .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
                .expiration(
                    aws_sdk_s3::types::LifecycleExpiration::builder()
                        .expired_object_delete_marker(true)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .rules(
            aws_sdk_s3::types::LifecycleRule::builder()
                .id("noncur-1-days")
                .filter(aws_sdk_s3::types::LifecycleRuleFilter::builder().prefix("").build())
                .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
                .noncurrent_version_expiration(
                    aws_sdk_s3::types::NoncurrentVersionExpiration::builder()
                        .noncurrent_days(1)
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

    let lc_interval = get_config().lc_debug_interval;
    tokio::time::sleep(std::time::Duration::from_secs(6 * lc_interval)).await;

    check_delete_marker(&client, &bucket_name, key, "false").await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
#[tokio::test]
async fn test_versioned_concurrent_object_create_concurrent_remove() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "myobj";
    let num_versions = 5usize;

    for _ in 0..5 {
        let mut set = tokio::task::JoinSet::new();
        for i in 0..num_versions {
            let c = client.clone();
            let bn = bucket_name.clone();
            let k = key.to_string();
            set.spawn(async move {
                c.put_object()
                    .bucket(&bn)
                    .key(&k)
                    .body(ByteStream::from(format!("data {i}").into_bytes()))
                    .send()
                    .await
                    .unwrap();
            });
        }
        while set.join_next().await.is_some() {}

        let resp = client
            .list_object_versions()
            .bucket(&bucket_name)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.versions().len(), num_versions);

        let mut del_set = tokio::task::JoinSet::new();
        for v in resp.versions() {
            let c = client.clone();
            let bn = bucket_name.clone();
            let k = v.key().unwrap_or_default().to_string();
            let vid = v.version_id().unwrap_or_default().to_string();
            del_set.spawn(async move {
                c.delete_object()
                    .bucket(&bn)
                    .key(&k)
                    .version_id(&vid)
                    .send()
                    .await
                    .unwrap();
            });
        }
        while del_set.join_next().await.is_some() {}

        /*
         * Deleting the current version promotes the next from .versions/,
         * so loop until truly empty.
         */
        for _ in 0..10 {
            let resp = client
                .list_object_versions()
                .bucket(&bucket_name)
                .send()
                .await
                .unwrap();
            if resp.versions().is_empty() && resp.delete_markers().is_empty() {
                break;
            }
            for v in resp.versions() {
                client
                    .delete_object()
                    .bucket(&bucket_name)
                    .key(v.key().unwrap_or_default())
                    .version_id(v.version_id().unwrap_or_default())
                    .send()
                    .await
                    .ok();
            }
            for dm in resp.delete_markers() {
                client
                    .delete_object()
                    .bucket(&bucket_name)
                    .key(dm.key().unwrap_or_default())
                    .version_id(dm.version_id().unwrap_or_default())
                    .send()
                    .await
                    .ok();
            }
        }

        let resp = client
            .list_object_versions()
            .bucket(&bucket_name)
            .send()
            .await
            .unwrap();
        assert!(resp.versions().is_empty(), "versions remain after cleanup");
        assert!(resp.delete_markers().is_empty(), "delete markers remain after cleanup");
    }
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
#[tokio::test]
async fn test_versioned_concurrent_object_create_and_remove() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "myobj";
    let num_versions = 3usize;

    let mut all_tasks = tokio::task::JoinSet::new();

    for _ in 0..3 {
        for i in 0..num_versions {
            let c = client.clone();
            let bn = bucket_name.clone();
            let k = key.to_string();
            all_tasks.spawn(async move {
                c.put_object()
                    .bucket(&bn)
                    .key(&k)
                    .body(ByteStream::from(format!("data {i}").into_bytes()))
                    .send()
                    .await
                    .unwrap();
            });
        }

        let c = client.clone();
        let bn = bucket_name.clone();
        all_tasks.spawn(async move {
            let resp = c
                .list_object_versions()
                .bucket(&bn)
                .send()
                .await
                .unwrap();
            for v in resp.versions() {
                c.delete_object()
                    .bucket(&bn)
                    .key(v.key().unwrap_or_default())
                    .version_id(v.version_id().unwrap_or_default())
                    .send()
                    .await
                    .ok();
            }
        });
    }

    while all_tasks.join_next().await.is_some() {}

    /*
     * Final cleanup — loop because deleting the current version promotes
     * the next one from .versions/, creating a new current that wasn't
     * in the listing.
     */
    for _ in 0..10 {
        let resp = client
            .list_object_versions()
            .bucket(&bucket_name)
            .send()
            .await
            .unwrap();
        if resp.versions().is_empty() && resp.delete_markers().is_empty() {
            break;
        }
        for v in resp.versions() {
            client
                .delete_object()
                .bucket(&bucket_name)
                .key(v.key().unwrap_or_default())
                .version_id(v.version_id().unwrap_or_default())
                .send()
                .await
                .ok();
        }
        for dm in resp.delete_markers() {
            client
                .delete_object()
                .bucket(&bucket_name)
                .key(dm.key().unwrap_or_default())
                .version_id(dm.version_id().unwrap_or_default())
                .send()
                .await
                .ok();
        }
    }

    let resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.versions().is_empty(), "versions remain after cleanup");
    assert!(resp.delete_markers().is_empty(), "delete markers remain after cleanup");
}

// --- Suspended versioning helpers ---

fn remove_null_entries(version_ids: &mut Vec<String>, contents: &mut Vec<String>) {
    let mut i = 0;
    while i < version_ids.len() {
        if version_ids[i] == "null" {
            version_ids.remove(i);
            contents.remove(i);
        } else {
            i += 1;
        }
    }
}

async fn delete_suspended_versioning_obj(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    version_ids: &mut Vec<String>,
    contents: &mut Vec<String>,
) {
    client
        .delete_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    remove_null_entries(version_ids, contents);
}

async fn overwrite_suspended_versioning_obj(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    version_ids: &mut Vec<String>,
    contents: &mut Vec<String>,
    content: &str,
) {
    client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(ByteStream::from(content.as_bytes().to_vec()))
        .send()
        .await
        .unwrap();
    remove_null_entries(version_ids, contents);
    contents.push(content.to_string());
    version_ids.push("null".to_string());
}

// --- Suspended versioning tests ---

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_suspend_versions() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "testobj";
    let num_versions = 5;
    let (mut version_ids, mut contents) =
        create_multiple_versions(&client, &bucket_name, key, num_versions).await;

    check_configure_versioning_retry(&client, &bucket_name, "Suspended", "Suspended").await;

    delete_suspended_versioning_obj(&client, &bucket_name, key, &mut version_ids, &mut contents).await;
    delete_suspended_versioning_obj(&client, &bucket_name, key, &mut version_ids, &mut contents).await;

    overwrite_suspended_versioning_obj(&client, &bucket_name, key, &mut version_ids, &mut contents, "null content 1").await;
    overwrite_suspended_versioning_obj(&client, &bucket_name, key, &mut version_ids, &mut contents, "null content 2").await;
    delete_suspended_versioning_obj(&client, &bucket_name, key, &mut version_ids, &mut contents).await;
    overwrite_suspended_versioning_obj(&client, &bucket_name, key, &mut version_ids, &mut contents, "null content 3").await;
    delete_suspended_versioning_obj(&client, &bucket_name, key, &mut version_ids, &mut contents).await;

    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;
    let (new_vids, new_contents) =
        create_multiple_versions(&client, &bucket_name, key, 3).await;
    version_ids.extend(new_vids);
    contents.extend(new_contents);
    let num_versions = version_ids.len();

    for idx in 0..num_versions {
        remove_obj_version(&client, &bucket_name, key, &mut version_ids, &mut contents, idx).await;
    }

    assert!(version_ids.is_empty());
    assert!(contents.is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_suspended_copy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key1 = "testobj1";
    let (mut version_ids, mut contents) =
        create_multiple_versions(&client, &bucket_name, key1, 1).await;

    check_configure_versioning_retry(&client, &bucket_name, "Suspended", "Suspended").await;

    let content = "null content";
    overwrite_suspended_versioning_obj(&client, &bucket_name, key1, &mut version_ids, &mut contents, content).await;

    // copy to another object in same bucket
    let key2 = "testobj2";
    let copy_source = format!("{}/{}", bucket_name, key1);
    client
        .copy_object()
        .bucket(&bucket_name)
        .key(key2)
        .copy_source(&copy_source)
        .send()
        .await
        .unwrap();

    // copy to another non-versioned bucket
    let bucket_name2 = get_new_bucket(Some(&client)).await;
    client
        .copy_object()
        .bucket(&bucket_name2)
        .key(key1)
        .copy_source(&copy_source)
        .send()
        .await
        .unwrap();

    // delete the source object
    client
        .delete_object()
        .bucket(&bucket_name)
        .key(key1)
        .send()
        .await
        .unwrap();

    // get the target object
    let resp = client
        .get_object()
        .bucket(&bucket_name)
        .key(key2)
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(resp).await, content);

    // get the target object from the other bucket
    let resp = client
        .get_object()
        .bucket(&bucket_name2)
        .key(key1)
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(resp).await, content);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_obj_create_overwrite_multipart() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let key = "testobj";
    let num_versions = 3;
    let mut all_contents = Vec::new();

    for _ in 0..num_versions {
        let result = multipart_upload(&client, &bucket_name, key, 5 * 1024 * 1024 + 3 * 1024 * 1024, None, None, None, None).await;
        let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(result.parts))
            .build();
        let resp = client
            .complete_multipart_upload()
            .bucket(&bucket_name)
            .key(key)
            .upload_id(&result.upload_id)
            .multipart_upload(mp)
            .send()
            .await
            .unwrap();
        assert!(resp.e_tag().is_some());
        all_contents.push(result.data);
    }

    let resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let versions = resp.versions();
    assert_eq!(versions.len(), num_versions);

    let mut version_ids: Vec<String> = versions
        .iter()
        .map(|v| v.version_id().unwrap_or_default().to_string())
        .collect();
    version_ids.reverse();

    // verify each version's content
    for (i, vid) in version_ids.iter().enumerate() {
        let resp = client
            .get_object()
            .bucket(&bucket_name)
            .key(key)
            .version_id(vid)
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(body, all_contents[i]);
    }

    // remove all versions
    for vid in &version_ids {
        client
            .delete_object()
            .bucket(&bucket_name)
            .key(key)
            .version_id(vid)
            .send()
            .await
            .unwrap();
    }
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_bucket_multipart_upload_return_version_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();

    // versioning-enabled bucket should return non-empty version-id
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let result = multipart_upload(&client, &bucket_name, "bar", 5 * 1024 * 1024 + 1, None, None, None, None).await;
    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(result.parts))
        .build();
    let resp = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key("bar")
        .upload_id(&result.upload_id)
        .multipart_upload(mp)
        .send()
        .await
        .unwrap();
    let version_id = resp.version_id().unwrap().to_string();
    assert!(!version_id.is_empty());

    let versions_resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    for v in versions_resp.versions() {
        assert_eq!(v.version_id().unwrap_or_default(), version_id);
    }

    // versioning-default bucket should not return version-id
    let bucket_name2 = get_new_bucket(Some(&client)).await;
    let result2 = multipart_upload(&client, &bucket_name2, "baz", 5 * 1024 * 1024 + 1, None, None, None, None).await;
    let mp2 = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(result2.parts))
        .build();
    let resp2 = client
        .complete_multipart_upload()
        .bucket(&bucket_name2)
        .key("baz")
        .upload_id(&result2.upload_id)
        .multipart_upload(mp2)
        .send()
        .await
        .unwrap();
    assert!(resp2.version_id().is_none());

    // versioning-suspended bucket should not return version-id
    let bucket_name3 = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name3, "Suspended", "Suspended").await;
    let result3 = multipart_upload(&client, &bucket_name3, "foo", 5 * 1024 * 1024 + 1, None, None, None, None).await;
    let mp3 = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(result3.parts))
        .build();
    let resp3 = client
        .complete_multipart_upload()
        .bucket(&bucket_name3)
        .key("foo")
        .upload_id(&result3.upload_id)
        .multipart_upload(mp3)
        .send()
        .await
        .unwrap();
    assert!(resp3.version_id().is_none());
}

// Stress tests for bucket cache and versioning concurrency have been
// moved to tests/test_s3/stress.rs — see that module for details.

/*
 * Diagnostic test for special-character keys in versioned buckets.
 * Isolates each key to identify which ones break the version suffix
 * parser in .versions/ filename handling.
 */
#[tokio::test]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
async fn test_versioning_special_key_diagnostics() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let keys = ["_testobj", "_", ":", "key_with_underscores", "mtime-fake"];

    for key in &keys {
        let body1 = format!("{key}-v1");
        let body2 = format!("{key}-v2");

        let r1 = client
            .put_object()
            .bucket(&bucket_name)
            .key(*key)
            .body(ByteStream::from(body1.clone().into_bytes()))
            .send()
            .await
            .unwrap();
        let vid1 = r1.version_id().unwrap_or("").to_string();
        assert!(!vid1.is_empty(), "key {key:?}: PUT v1 missing version_id");

        let r2 = client
            .put_object()
            .bucket(&bucket_name)
            .key(*key)
            .body(ByteStream::from(body2.clone().into_bytes()))
            .send()
            .await
            .unwrap();
        let vid2 = r2.version_id().unwrap_or("").to_string();
        assert!(!vid2.is_empty(), "key {key:?}: PUT v2 missing version_id");
        assert_ne!(vid1, vid2, "key {key:?}: v1 and v2 have same version_id");

        let versions = client
            .list_object_versions()
            .bucket(&bucket_name)
            .send()
            .await
            .unwrap();
        let vers: Vec<_> = versions.versions().iter()
            .filter(|v| v.key().unwrap_or("") == *key)
            .collect();
        assert_eq!(
            vers.len(), 2,
            "key {key:?}: expected 2 versions, got {} (versions: {:?})",
            vers.len(),
            vers.iter().map(|v| v.version_id().unwrap_or("?")).collect::<Vec<_>>()
        );

        let get1 = client
            .get_object()
            .bucket(&bucket_name)
            .key(*key)
            .version_id(&vid1)
            .send()
            .await
            .unwrap();
        let got1 = get_body(get1).await;
        assert_eq!(got1, body1, "key {key:?}: GET v1 content mismatch");

        client
            .delete_object()
            .bucket(&bucket_name)
            .key(*key)
            .version_id(&vid1)
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket_name)
            .key(*key)
            .version_id(&vid2)
            .send()
            .await
            .unwrap();
    }
}
