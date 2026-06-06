use aws_sdk_s3::types::EncodingType;
use s3_tests_rs::client::{get_client, get_unauthenticated_client};
use s3_tests_rs::fixtures::{create_objects_in_new_bucket, get_new_bucket};
use s3_tests_rs::assert_s3_err;

fn get_keys(response: &aws_sdk_s3::operation::list_objects::ListObjectsOutput) -> Vec<String> {
    response
        .contents()
        .iter()
        .map(|o| o.key().unwrap_or_default().to_string())
        .collect()
}

fn get_keys_v2(
    response: &aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output,
) -> Vec<String> {
    response
        .contents()
        .iter()
        .map(|o| o.key().unwrap_or_default().to_string())
        .collect()
}

fn get_prefixes(response: &aws_sdk_s3::operation::list_objects::ListObjectsOutput) -> Vec<String> {
    response
        .common_prefixes()
        .iter()
        .map(|p| p.prefix().unwrap_or_default().to_string())
        .collect()
}

fn get_prefixes_v2(
    response: &aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output,
) -> Vec<String> {
    response
        .common_prefixes()
        .iter()
        .map(|p| p.prefix().unwrap_or_default().to_string())
        .collect()
}

#[tokio::test]
async fn test_bucket_list_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let resp = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.contents().is_empty());
}

#[tokio::test]
async fn test_bucket_list_distinct() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket1 = get_new_bucket(Some(&client)).await;
    let bucket2 = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket1)
        .key("asdf")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"str"))
        .send()
        .await
        .unwrap();

    let resp = client
        .list_objects_v2()
        .bucket(&bucket2)
        .send()
        .await
        .unwrap();
    assert!(resp.contents().is_empty());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
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
    let keys = get_keys(&response);
    assert_eq!(keys.len(), 2);
    assert_eq!(keys, vec!["bar", "baz"]);
    assert!(response.is_truncated().unwrap_or(false));

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .marker("baz")
        .max_keys(2)
        .send()
        .await
        .unwrap();
    let keys = get_keys(&response);
    assert_eq!(keys.len(), 1);
    assert!(!response.is_truncated().unwrap_or(true));
    assert_eq!(keys, vec!["foo"]);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_listv2_many() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo", "bar", "baz"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .max_keys(2)
        .send()
        .await
        .unwrap();
    let keys = get_keys_v2(&response);
    assert_eq!(keys.len(), 2);
    assert_eq!(keys, vec!["bar", "baz"]);
    assert!(response.is_truncated().unwrap_or(false));

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .start_after("baz")
        .max_keys(2)
        .send()
        .await
        .unwrap();
    let keys = get_keys_v2(&response);
    assert_eq!(keys.len(), 1);
    assert!(!response.is_truncated().unwrap_or(true));
    assert_eq!(keys, vec!["foo"]);
}

#[tokio::test]
async fn test_basic_key_count() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    for j in 0..5 {
        client
            .put_object()
            .bucket(&bucket_name)
            .key(&j.to_string())
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b""))
            .send()
            .await
            .unwrap();
    }

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.key_count().unwrap_or(0), 5);
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: file-as-prefix conflict (ENOTDIR)")]
async fn test_bucket_list_delimiter_basic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("/")
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "/");
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["asdf"]);

    let prefixes = get_prefixes(&response);
    assert_eq!(prefixes.len(), 2);
    assert_eq!(prefixes, vec!["foo/", "quux/"]);
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: file-as-prefix conflict (ENOTDIR)")]
async fn test_bucket_listv2_delimiter_basic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("/")
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "/");
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["asdf"]);

    let prefixes = get_prefixes_v2(&response);
    assert_eq!(prefixes.len(), 2);
    assert_eq!(prefixes, vec!["foo/", "quux/"]);
    assert_eq!(
        response.key_count().unwrap_or(0),
        (prefixes.len() + keys.len()) as i32
    );
}

#[tokio::test]
async fn test_bucket_list_delimiter_alt() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "cab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "a");
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["foo"]);

    let prefixes = get_prefixes(&response);
    assert_eq!(prefixes.len(), 2);
    assert_eq!(prefixes, vec!["ba", "ca"]);
}

#[tokio::test]
async fn test_bucket_listv2_delimiter_alt() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "cab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "a");
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["foo"]);

    let prefixes = get_prefixes_v2(&response);
    assert_eq!(prefixes.len(), 2);
    assert_eq!(prefixes, vec!["ba", "ca"]);
}

#[tokio::test]
async fn test_bucket_list_delimiter_percentage() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["b%ar", "b%az", "c%ab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("%")
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "%");
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["foo"]);

    let prefixes = get_prefixes(&response);
    assert_eq!(prefixes.len(), 2);
    assert_eq!(prefixes, vec!["b%", "c%"]);
}

#[tokio::test]
async fn test_bucket_listv2_delimiter_percentage() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["b%ar", "b%az", "c%ab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("%")
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "%");
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["foo"]);

    let prefixes = get_prefixes_v2(&response);
    assert_eq!(prefixes.len(), 2);
    assert_eq!(prefixes, vec!["b%", "c%"]);
}

#[tokio::test]
async fn test_bucket_list_delimiter_whitespace() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["b ar", "b az", "c ab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter(" ")
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), " ");
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["foo"]);

    let prefixes = get_prefixes(&response);
    assert_eq!(prefixes.len(), 2);
    assert_eq!(prefixes, vec!["b ", "c "]);
}

#[tokio::test]
async fn test_bucket_listv2_delimiter_whitespace() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["b ar", "b az", "c ab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter(" ")
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), " ");
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["foo"]);

    let prefixes = get_prefixes_v2(&response);
    assert_eq!(prefixes.len(), 2);
    assert_eq!(prefixes, vec!["b ", "c "]);
}

// --- delimiter dot ---

#[tokio::test]
async fn test_bucket_list_delimiter_dot() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["b.ar", "b.az", "c.ab", "foo"]).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).delimiter(".").send().await.unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), ".");
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["foo"]);
    let prefixes = get_prefixes(&response);
    assert_eq!(prefixes, vec!["b.", "c."]);
}

#[tokio::test]
async fn test_bucket_listv2_delimiter_dot() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["b.ar", "b.az", "c.ab", "foo"]).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).delimiter(".").send().await.unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), ".");
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["foo"]);
    let prefixes = get_prefixes_v2(&response);
    assert_eq!(prefixes, vec!["b.", "c."]);
}

// --- delimiter empty / none / not_exist ---

#[tokio::test]
async fn test_bucket_list_delimiter_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "cab", "foo"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).delimiter("").send().await.unwrap();
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["bar", "baz", "cab", "foo"]);
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_delimiter_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "cab", "foo"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).delimiter("").send().await.unwrap();
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["bar", "baz", "cab", "foo"]);
    assert!(get_prefixes_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_delimiter_none() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "cab", "foo"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["bar", "baz", "cab", "foo"]);
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_delimiter_none() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "cab", "foo"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["bar", "baz", "cab", "foo"]);
    assert!(get_prefixes_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_delimiter_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "cab", "foo"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).delimiter("/").send().await.unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "/");
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["bar", "baz", "cab", "foo"]);
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_delimiter_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "cab", "foo"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).delimiter("/").send().await.unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "/");
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["bar", "baz", "cab", "foo"]);
    assert!(get_prefixes_v2(&response).is_empty());
}

// --- prefix tests ---

#[tokio::test]
async fn test_bucket_list_prefix_basic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).prefix("foo/").send().await.unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "foo/");
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["foo/bar", "foo/baz"]);
}

#[tokio::test]
async fn test_bucket_listv2_prefix_basic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).prefix("foo/").send().await.unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "foo/");
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["foo/bar", "foo/baz"]);
}

#[tokio::test]
async fn test_bucket_list_prefix_alt() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "foo"]).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).prefix("ba").send().await.unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "ba");
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["bar", "baz"]);
}

#[tokio::test]
async fn test_bucket_listv2_prefix_alt() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "foo"]).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).prefix("ba").send().await.unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "ba");
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["bar", "baz"]);
}

#[tokio::test]
async fn test_bucket_list_prefix_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).prefix("").send().await.unwrap();
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["foo/bar", "foo/baz", "quux"]);
}

#[tokio::test]
async fn test_bucket_listv2_prefix_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).prefix("").send().await.unwrap();
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["foo/bar", "foo/baz", "quux"]);
}

#[tokio::test]
async fn test_bucket_list_prefix_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).prefix("d").send().await.unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "d");
    let keys = get_keys(&response);
    assert!(keys.is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_prefix_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).prefix("d").send().await.unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "d");
    let keys = get_keys_v2(&response);
    assert!(keys.is_empty());
}

// --- maxkeys tests ---

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_list_maxkeys_one() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "foo", "quxx"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).max_keys(1).send().await.unwrap();
    assert!(response.is_truncated().unwrap_or(false));
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["bar"]);

    let response = client.list_objects().bucket(&bucket_name).marker("bar").send().await.unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["baz", "foo", "quxx"]);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_listv2_maxkeys_one() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "foo", "quxx"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).max_keys(1).send().await.unwrap();
    assert!(response.is_truncated().unwrap_or(false));
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["bar"]);

    let response = client.list_objects_v2().bucket(&bucket_name).start_after("bar").send().await.unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["baz", "foo", "quxx"]);
}

#[tokio::test]
async fn test_bucket_list_maxkeys_zero() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).max_keys(0).send().await.unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    assert!(get_keys(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_maxkeys_zero() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).max_keys(0).send().await.unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    assert!(get_keys_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_maxkeys_none() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "foo", "quxx"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["bar", "baz", "foo", "quxx"]);
    assert_eq!(response.max_keys().unwrap_or(0), 1000);
}

#[tokio::test]
async fn test_bucket_listv2_maxkeys_none() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "foo", "quxx"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    let keys = get_keys_v2(&response);
    assert_eq!(keys, vec!["bar", "baz", "foo", "quxx"]);
    assert_eq!(response.max_keys().unwrap_or(0), 1000);
}

// --- marker tests ---

#[tokio::test]
async fn test_bucket_list_marker_none() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.marker().unwrap_or_default(), "");
}

#[tokio::test]
async fn test_bucket_list_marker_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "foo", "quxx"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).marker("").send().await.unwrap();
    assert_eq!(response.marker().unwrap_or_default(), "");
    assert!(!response.is_truncated().unwrap_or(true));
    let keys = get_keys(&response);
    assert_eq!(keys, vec!["bar", "baz", "foo", "quxx"]);
}

// --- fetchowner tests ---

#[tokio::test]
async fn test_bucket_listv2_fetchowner_notempty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).fetch_owner(true).send().await.unwrap();
    let objs = response.contents();
    assert!(!objs.is_empty());
    assert!(objs[0].owner().is_some());
}

#[tokio::test]
async fn test_bucket_listv2_fetchowner_defaultempty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
    let objs = response.contents();
    assert!(!objs.is_empty());
    assert!(objs[0].owner().is_none());
}

#[tokio::test]
async fn test_bucket_listv2_fetchowner_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client.list_objects_v2().bucket(&bucket_name).fetch_owner(false).send().await.unwrap();
    let objs = response.contents();
    assert!(!objs.is_empty());
    assert!(objs[0].owner().is_none());
}

// --- continuation token ---

#[tokio::test]
async fn test_bucket_listv2_continuationtoken() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "foo", "quxx"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response1 = client.list_objects_v2().bucket(&bucket_name).max_keys(1).send().await.unwrap();
    let next_token = response1.next_continuation_token().unwrap().to_string();

    let response2 = client.list_objects_v2().bucket(&bucket_name).continuation_token(&next_token).send().await.unwrap();
    assert_eq!(response2.continuation_token().unwrap_or_default(), next_token);
    assert!(!response2.is_truncated().unwrap_or(true));
    let keys = get_keys_v2(&response2);
    assert_eq!(keys, vec!["baz", "foo", "quxx"]);
}

// --- head bucket ---

#[tokio::test]
async fn test_bucket_head() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    client.head_bucket().bucket(&bucket_name).send().await.unwrap();
}

#[tokio::test]
async fn test_bucket_head_notexist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = s3_tests_rs::fixtures::get_new_bucket_name();
    let result = client.head_bucket().bucket(&bucket_name).send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_bucket_notexist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = s3_tests_rs::fixtures::get_new_bucket_name();

    let result = client.list_objects().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_bucketv2_notexist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = s3_tests_rs::fixtures::get_new_bucket_name();

    let result = client.list_objects_v2().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 404, "NoSuchBucket");
}

#[tokio::test]
async fn test_bucket_delete_notexist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = s3_tests_rs::fixtures::get_new_bucket_name();

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

// --- anonymous listing ---

#[tokio::test]
async fn test_bucket_list_objects_anonymous() {
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

    let unauth = get_unauthenticated_client();
    unauth.list_objects().bucket(&bucket_name).send().await.unwrap();
}

#[tokio::test]
async fn test_bucket_listv2_objects_anonymous() {
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

    let unauth = get_unauthenticated_client();
    unauth.list_objects_v2().bucket(&bucket_name).send().await.unwrap();
}

// --- special prefix ---

#[tokio::test]
async fn test_bucket_list_special_prefix() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["_bla/1", "_bla/2", "_bla/3", "_bla/4", "abcd"]).await;
    let client = get_client();

    let response = client.list_objects().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.contents().len(), 5);

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .prefix("_bla/")
        .send()
        .await
        .unwrap();
    assert_eq!(response.contents().len(), 4);
}

// --- good bucket names ---

#[tokio::test]
async fn test_bucket_create_naming_good_starts_alpha() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let name = format!("a{}foo", s3_tests_rs::config::get_config().bucket_prefix);
    client.create_bucket().bucket(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_bucket_create_naming_good_starts_digit() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let name = format!("0{}foo", s3_tests_rs::config::get_config().bucket_prefix);
    client.create_bucket().bucket(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_bucket_create_naming_good_contains_period() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = s3_tests_rs::config::get_config();
    let name = format!("{}aaa.111", cfg.bucket_prefix);
    client.create_bucket().bucket(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_bucket_create_naming_good_contains_hyphen() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = s3_tests_rs::config::get_config();
    let name = format!("{}aaa-111", cfg.bucket_prefix);
    client.create_bucket().bucket(&name).send().await.unwrap();
}

// --- prefix + delimiter combination tests ---

#[tokio::test]
async fn test_bucket_list_prefix_delimiter_basic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("/")
        .prefix("foo/")
        .send()
        .await
        .unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "foo/");
    assert_eq!(response.delimiter().unwrap_or_default(), "/");
    assert_eq!(get_keys(&response), vec!["foo/bar"]);
    assert_eq!(get_prefixes(&response), vec!["foo/baz/"]);
}

#[tokio::test]
async fn test_bucket_listv2_prefix_delimiter_basic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("/")
        .prefix("foo/")
        .send()
        .await
        .unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "foo/");
    assert_eq!(response.delimiter().unwrap_or_default(), "/");
    assert_eq!(get_keys_v2(&response), vec!["foo/bar"]);
    assert_eq!(get_prefixes_v2(&response), vec!["foo/baz/"]);
}

#[tokio::test]
async fn test_bucket_list_prefix_delimiter_alt() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["bar", "bazar", "cab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("a")
        .prefix("ba")
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys(&response), vec!["bar"]);
    assert_eq!(get_prefixes(&response), vec!["baza"]);
}

#[tokio::test]
async fn test_bucket_listv2_prefix_delimiter_alt() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["bar", "bazar", "cab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("a")
        .prefix("ba")
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys_v2(&response), vec!["bar"]);
    assert_eq!(get_prefixes_v2(&response), vec!["baza"]);
}

#[tokio::test]
async fn test_bucket_list_prefix_delimiter_prefix_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["b/a/r", "b/a/c", "b/a/g", "g"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("d")
        .prefix("/")
        .send()
        .await
        .unwrap();
    assert!(get_keys(&response).is_empty());
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_prefix_delimiter_prefix_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["b/a/r", "b/a/c", "b/a/g", "g"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("d")
        .prefix("/")
        .send()
        .await
        .unwrap();
    assert!(get_keys_v2(&response).is_empty());
    assert!(get_prefixes_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_prefix_delimiter_delimiter_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["b/a/c", "b/a/g", "b/a/r", "g"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("z")
        .prefix("b")
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys(&response), vec!["b/a/c", "b/a/g", "b/a/r"]);
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_prefix_delimiter_delimiter_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["b/a/c", "b/a/g", "b/a/r", "g"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("z")
        .prefix("b")
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys_v2(&response), vec!["b/a/c", "b/a/g", "b/a/r"]);
    assert!(get_prefixes_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["b/a/c", "b/a/g", "b/a/r", "g"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("z")
        .prefix("y")
        .send()
        .await
        .unwrap();
    assert!(get_keys(&response).is_empty());
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["b/a/c", "b/a/g", "b/a/r", "g"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("z")
        .prefix("y")
        .send()
        .await
        .unwrap();
    assert!(get_keys_v2(&response).is_empty());
    assert!(get_prefixes_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_delimiter_prefix_ends_with_delimiter() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["asdf/"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("/")
        .prefix("asdf/")
        .max_keys(1000)
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys(&response), vec!["asdf/"]);
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_delimiter_prefix_ends_with_delimiter() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["asdf/"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("/")
        .prefix("asdf/")
        .max_keys(1000)
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys_v2(&response), vec!["asdf/"]);
    assert!(get_prefixes_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_objects_anonymous_fail() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let unauth = get_unauthenticated_client();

    let result = unauth.list_objects().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[tokio::test]
async fn test_bucket_listv2_objects_anonymous_fail() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let unauth = get_unauthenticated_client();

    let result = unauth.list_objects_v2().bucket(&bucket_name).send().await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_list_return_data_versioning() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    s3_tests_rs::fixtures::check_configure_versioning_retry(
        &client, &bucket_name, "Enabled", "Enabled",
    )
    .await;

    let keys = &["bar", "baz", "foo"];
    s3_tests_rs::fixtures::create_objects(&client, &bucket_name, keys).await;

    let mut expected: std::collections::HashMap<String, (String, i64, String)> =
        std::collections::HashMap::new();
    for key in keys {
        let head = client
            .head_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .unwrap();
        expected.insert(
            key.to_string(),
            (
                head.e_tag().unwrap_or_default().to_string(),
                head.content_length().unwrap_or(0),
                head.version_id().unwrap_or_default().to_string(),
            ),
        );
    }

    let response = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    for obj in response.versions() {
        let key = obj.key().unwrap_or_default();
        let (etag, size, vid) = expected.get(key).unwrap();
        assert_eq!(obj.e_tag().unwrap_or_default(), etag);
        assert_eq!(obj.size().unwrap_or(0), *size);
        assert_eq!(obj.version_id().unwrap_or_default(), vid);
    }
}

#[tokio::test]
async fn test_bucket_list_delimiter_unreadable() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "cab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("\x0a")
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys(&response), vec!["bar", "baz", "cab", "foo"]);
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_delimiter_unreadable() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "cab", "foo"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("\x0a")
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys_v2(&response), vec!["bar", "baz", "cab", "foo"]);
    assert!(get_prefixes_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_marker_after_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .marker("zzz")
        .send()
        .await
        .unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    assert!(get_keys(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_startafter_after_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .start_after("zzz")
        .send()
        .await
        .unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    assert!(get_keys_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_prefix_unreadable() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .prefix("\x0a")
        .send()
        .await
        .unwrap();
    assert!(get_keys(&response).is_empty());
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_prefix_unreadable() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["foo/bar", "foo/baz", "quux"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .prefix("\x0a")
        .send()
        .await
        .unwrap();
    assert!(get_keys_v2(&response).is_empty());
    assert!(get_prefixes_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_marker_unreadable() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .marker("\x0a")
        .send()
        .await
        .unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    assert_eq!(get_keys(&response), vec!["bar", "baz", "foo", "quxx"]);
}

#[tokio::test]
async fn test_bucket_listv2_startafter_unreadable() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .start_after("\x0a")
        .send()
        .await
        .unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    assert_eq!(get_keys_v2(&response), vec!["bar", "baz", "foo", "quxx"]);
}

#[tokio::test]
async fn test_bucket_list_marker_not_in_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .marker("blah")
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys(&response), vec!["foo", "quxx"]);
}

#[tokio::test]
async fn test_bucket_listv2_startafter_not_in_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .start_after("blah")
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys_v2(&response), vec!["foo", "quxx"]);
}

#[tokio::test]
async fn test_bucket_list_prefix_none() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["foo/bar", "foo/baz", "quux"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .prefix("")
        .send()
        .await
        .unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "");
    assert_eq!(get_keys(&response), vec!["foo/bar", "foo/baz", "quux"]);
    assert!(get_prefixes(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_listv2_prefix_none() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["foo/bar", "foo/baz", "quux"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .prefix("")
        .send()
        .await
        .unwrap();
    assert_eq!(response.prefix().unwrap_or_default(), "");
    assert_eq!(get_keys_v2(&response), vec!["foo/bar", "foo/baz", "quux"]);
    assert!(get_prefixes_v2(&response).is_empty());
}

#[tokio::test]
async fn test_bucket_list_encoding_basic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("/")
        .encoding_type(aws_sdk_s3::types::EncodingType::Url)
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "/");
    assert_eq!(get_keys(&response), vec!["asdf%2Bb"]);
    assert_eq!(
        get_prefixes(&response),
        vec!["foo%2B1/", "foo/", "quux%20ab/"]
    );
}

#[tokio::test]
async fn test_bucket_listv2_encoding_basic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .delimiter("/")
        .encoding_type(aws_sdk_s3::types::EncodingType::Url)
        .send()
        .await
        .unwrap();
    assert_eq!(response.delimiter().unwrap_or_default(), "/");
    assert_eq!(get_keys_v2(&response), vec!["asdf%2Bb"]);
    assert_eq!(
        get_prefixes_v2(&response),
        vec!["foo%2B1/", "foo/", "quux%20ab/"]
    );
}

#[tokio::test]
async fn test_bucket_listv2_continuationtoken_empty() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "foo", "quxx"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .continuation_token("")
        .send()
        .await
        .unwrap();
    assert!(!response.is_truncated().unwrap_or(true));
    assert_eq!(
        get_keys_v2(&response),
        vec!["bar", "baz", "foo", "quxx"]
    );
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_listv2_both_continuationtoken_startafter() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let key_names = &["bar", "baz", "foo", "quxx"];
    let bucket_name = create_objects_in_new_bucket(key_names).await;
    let client = get_client();

    let response1 = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .start_after("bar")
        .max_keys(1)
        .send()
        .await
        .unwrap();
    let next_token = response1
        .next_continuation_token()
        .unwrap()
        .to_string();

    let response2 = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .start_after("bar")
        .continuation_token(&next_token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response2.continuation_token().unwrap_or_default(),
        next_token
    );
    assert_eq!(response2.start_after().unwrap_or_default(), "bar");
    assert!(!response2.is_truncated().unwrap_or(true));
    assert_eq!(get_keys_v2(&response2), vec!["foo", "quxx"]);
}

async fn validate_bucket_list(
    bucket_name: &str,
    prefix: &str,
    delimiter: &str,
    marker: &str,
    max_keys: i32,
    is_truncated: bool,
    check_objs: &[&str],
    check_prefixes: &[&str],
    next_marker: Option<&str>,
) -> Option<String> {
    let client = get_client();
    let response = client
        .list_objects()
        .bucket(bucket_name)
        .delimiter(delimiter)
        .marker(marker)
        .max_keys(max_keys)
        .prefix(prefix)
        .send()
        .await
        .unwrap();
    assert_eq!(response.is_truncated().unwrap_or(false), is_truncated);
    let actual_marker = response.next_marker().map(|s| s.to_string());
    assert_eq!(actual_marker.as_deref(), next_marker);
    assert_eq!(get_keys(&response), check_objs);
    assert_eq!(get_prefixes(&response), check_prefixes);
    actual_marker
}

async fn validate_bucket_listv2(
    bucket_name: &str,
    prefix: &str,
    delimiter: &str,
    continuation_token: Option<&str>,
    max_keys: i32,
    is_truncated: bool,
    check_objs: &[&str],
    check_prefixes: &[&str],
    last: bool,
) -> Option<String> {
    let client = get_client();
    let mut req = client
        .list_objects_v2()
        .bucket(bucket_name)
        .delimiter(delimiter)
        .max_keys(max_keys)
        .prefix(prefix);
    if let Some(token) = continuation_token {
        req = req.continuation_token(token);
    } else {
        req = req.start_after("");
    }
    let response = req.send().await.unwrap();
    assert_eq!(response.is_truncated().unwrap_or(false), is_truncated);
    if last {
        assert!(response.next_continuation_token().is_none());
    }
    assert_eq!(get_keys_v2(&response), check_objs);
    assert_eq!(get_prefixes_v2(&response), check_prefixes);
    response.next_continuation_token().map(|s| s.to_string())
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_list_delimiter_prefix() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"])
            .await;

    let marker = validate_bucket_list(&bucket_name, "", "/", "", 1, true, &["asdf"], &[], Some("asdf")).await;
    let marker = validate_bucket_list(&bucket_name, "", "/", marker.as_deref().unwrap_or(""), 1, true, &[], &["boo/"], Some("boo/")).await;
    validate_bucket_list(&bucket_name, "", "/", marker.as_deref().unwrap_or(""), 1, false, &[], &["cquux/"], None).await;

    let marker = validate_bucket_list(&bucket_name, "", "/", "", 2, true, &["asdf"], &["boo/"], Some("boo/")).await;
    validate_bucket_list(&bucket_name, "", "/", marker.as_deref().unwrap_or(""), 2, false, &[], &["cquux/"], None).await;

    let marker = validate_bucket_list(&bucket_name, "boo/", "/", "", 1, true, &["boo/bar"], &[], Some("boo/bar")).await;
    validate_bucket_list(&bucket_name, "boo/", "/", marker.as_deref().unwrap_or(""), 1, false, &[], &["boo/baz/"], None).await;

    validate_bucket_list(&bucket_name, "boo/", "/", "", 2, false, &["boo/bar"], &["boo/baz/"], None).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_listv2_delimiter_prefix() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"])
            .await;

    let token = validate_bucket_listv2(&bucket_name, "", "/", None, 1, true, &["asdf"], &[], false).await;
    let token = validate_bucket_listv2(&bucket_name, "", "/", token.as_deref(), 1, true, &[], &["boo/"], false).await;
    validate_bucket_listv2(&bucket_name, "", "/", token.as_deref(), 1, false, &[], &["cquux/"], true).await;

    let token = validate_bucket_listv2(&bucket_name, "", "/", None, 2, true, &["asdf"], &["boo/"], false).await;
    validate_bucket_listv2(&bucket_name, "", "/", token.as_deref(), 2, false, &[], &["cquux/"], true).await;

    let token = validate_bucket_listv2(&bucket_name, "boo/", "/", None, 1, true, &["boo/bar"], &[], false).await;
    validate_bucket_listv2(&bucket_name, "boo/", "/", token.as_deref(), 1, false, &[], &["boo/baz/"], true).await;

    validate_bucket_listv2(&bucket_name, "boo/", "/", None, 2, false, &["boo/bar"], &["boo/baz/"], true).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_list_delimiter_prefix_underscore() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["_obj1_", "_under1/bar", "_under1/baz/xyzzy", "_under2/thud", "_under2/bla"])
            .await;

    let marker = validate_bucket_list(&bucket_name, "", "/", "", 1, true, &["_obj1_"], &[], Some("_obj1_")).await;
    let marker = validate_bucket_list(&bucket_name, "", "/", marker.as_deref().unwrap_or(""), 1, true, &[], &["_under1/"], Some("_under1/")).await;
    validate_bucket_list(&bucket_name, "", "/", marker.as_deref().unwrap_or(""), 1, false, &[], &["_under2/"], None).await;

    let marker = validate_bucket_list(&bucket_name, "", "/", "", 2, true, &["_obj1_"], &["_under1/"], Some("_under1/")).await;
    validate_bucket_list(&bucket_name, "", "/", marker.as_deref().unwrap_or(""), 2, false, &[], &["_under2/"], None).await;

    let marker = validate_bucket_list(&bucket_name, "_under1/", "/", "", 1, true, &["_under1/bar"], &[], Some("_under1/bar")).await;
    validate_bucket_list(&bucket_name, "_under1/", "/", marker.as_deref().unwrap_or(""), 1, false, &[], &["_under1/baz/"], None).await;

    validate_bucket_list(&bucket_name, "_under1/", "/", "", 2, false, &["_under1/bar"], &["_under1/baz/"], None).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_listv2_delimiter_prefix_underscore() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name =
        create_objects_in_new_bucket(&["_obj1_", "_under1/bar", "_under1/baz/xyzzy", "_under2/thud", "_under2/bla"])
            .await;

    let token = validate_bucket_listv2(&bucket_name, "", "/", None, 1, true, &["_obj1_"], &[], false).await;
    let token = validate_bucket_listv2(&bucket_name, "", "/", token.as_deref(), 1, true, &[], &["_under1/"], false).await;
    validate_bucket_listv2(&bucket_name, "", "/", token.as_deref(), 1, false, &[], &["_under2/"], true).await;

    let token = validate_bucket_listv2(&bucket_name, "", "/", None, 2, true, &["_obj1_"], &["_under1/"], false).await;
    validate_bucket_listv2(&bucket_name, "", "/", token.as_deref(), 2, false, &[], &["_under2/"], true).await;

    let token = validate_bucket_listv2(&bucket_name, "_under1/", "/", None, 1, true, &["_under1/bar"], &[], false).await;
    validate_bucket_listv2(&bucket_name, "_under1/", "/", token.as_deref(), 1, false, &[], &["_under1/baz/"], true).await;

    validate_bucket_listv2(&bucket_name, "_under1/", "/", None, 2, false, &["_under1/bar"], &["_under1/baz/"], true).await;
}

#[tokio::test]
async fn test_bucket_list_delimiter_not_skip_special() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let keys = &["0/", "1/", "2/", "3/", "4/", "5/", "6/", "7/", "8/", "9/", "0content"];
    let bucket_name = create_objects_in_new_bucket(keys).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("/")
        .send()
        .await
        .unwrap();

    assert_eq!(get_keys(&response), vec!["0content"]);
    assert_eq!(
        get_prefixes(&response),
        vec!["0/", "1/", "2/", "3/", "4/", "5/", "6/", "7/", "8/", "9/"]
    );
}

fn append_query_param(req: &mut aws_smithy_runtime_api::client::orchestrator::HttpRequest, param: &str) {
    let uri = req.uri().to_string();
    let sep = if uri.contains('?') { "&" } else { "?" };
    let new_uri = format!("{uri}{sep}{param}");
    req.set_uri(new_uri).expect("failed to set URI");
}

#[tokio::test]
async fn test_bucket_list_maxkeys_invalid() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = create_objects_in_new_bucket(&["bar", "baz", "foo", "quxx"]).await;
    let client = get_client();

    let result = client
        .list_objects()
        .bucket(&bucket_name)
        .customize()
        .mutate_request(|req| {
            append_query_param(req, "max-keys=blah");
        })
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidArgument");
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_list_unordered() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let keys_in: Vec<&str> = vec![
        "ado", "bot", "cob", "dog", "emu", "fez", "gnu", "hex",
        "abc/ink", "abc/jet", "abc/kin", "abc/lax", "abc/mux",
        "def/nim", "def/owl", "def/pie", "def/qed", "def/rye",
        "ghi/sew", "ghi/tor", "ghi/uke", "ghi/via", "ghi/wit",
        "xix", "yak", "zoo",
    ];
    let bucket_name = create_objects_in_new_bucket(&keys_in).await;
    let client = get_client();

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .max_keys(1000)
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await
        .unwrap();
    let mut unordered_keys = get_keys(&response);
    assert_eq!(unordered_keys.len(), keys_in.len());
    let mut sorted_in: Vec<String> = keys_in.iter().map(|s| s.to_string()).collect();
    sorted_in.sort();
    unordered_keys.sort();
    assert_eq!(unordered_keys, sorted_in);

    // with prefix
    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .max_keys(1000)
        .prefix("abc/")
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys(&response).len(), 5);

    // incremental retrieval with marker
    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .max_keys(6)
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await
        .unwrap();
    let batch1 = get_keys(&response);
    assert_eq!(batch1.len(), 6);

    let response = client
        .list_objects()
        .bucket(&bucket_name)
        .max_keys(6)
        .marker(batch1.last().unwrap())
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await
        .unwrap();
    let batch2 = get_keys(&response);
    assert_eq!(batch2.len(), 6);

    // no overlap
    let set1: std::collections::HashSet<_> = batch1.iter().collect();
    let set2: std::collections::HashSet<_> = batch2.iter().collect();
    assert_eq!(set1.intersection(&set2).count(), 0);

    // unordered + delimiter → error
    let result = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("/")
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidArgument");
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_listv2_unordered() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let keys_in: Vec<&str> = vec![
        "ado", "bot", "cob", "dog", "emu", "fez", "gnu", "hex",
        "abc/ink", "abc/jet", "abc/kin", "abc/lax", "abc/mux",
        "def/nim", "def/owl", "def/pie", "def/qed", "def/rye",
        "ghi/sew", "ghi/tor", "ghi/uke", "ghi/via", "ghi/wit",
        "xix", "yak", "zoo",
    ];
    let bucket_name = create_objects_in_new_bucket(&keys_in).await;
    let client = get_client();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .max_keys(1000)
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await
        .unwrap();
    let mut unordered_keys = get_keys_v2(&response);
    assert_eq!(unordered_keys.len(), keys_in.len());
    let mut sorted_in: Vec<String> = keys_in.iter().map(|s| s.to_string()).collect();
    sorted_in.sort();
    unordered_keys.sort();
    assert_eq!(unordered_keys, sorted_in);

    // with prefix
    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .max_keys(1000)
        .prefix("abc/")
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await
        .unwrap();
    assert_eq!(get_keys_v2(&response).len(), 5);

    // incremental retrieval
    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .max_keys(6)
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await
        .unwrap();
    let batch1 = get_keys_v2(&response);
    assert_eq!(batch1.len(), 6);

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .max_keys(6)
        .start_after(batch1.last().unwrap())
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await
        .unwrap();
    let batch2 = get_keys_v2(&response);
    assert_eq!(batch2.len(), 6);

    // no overlap
    let set1: std::collections::HashSet<_> = batch1.iter().collect();
    let set2: std::collections::HashSet<_> = batch2.iter().collect();
    assert_eq!(set1.intersection(&set2).count(), 0);

    // unordered + delimiter → error (uses v1 list_objects like the Python test)
    let result = client
        .list_objects()
        .bucket(&bucket_name)
        .delimiter("/")
        .customize()
        .mutate_request(|req| { append_query_param(req, "allow-unordered=true"); })
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidArgument");
}
