use aws_sdk_s3::types::BucketCannedAcl;
use chrono::Utc;
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{get_new_bucket, get_new_bucket_name};
use s3_tests_rs::http::{
    encode_policy, get_endpoint_url, post_object_form, post_object_form_with_filename, sign_policy,
};

fn make_signed_fields(
    policy_doc: &serde_json::Value,
) -> (String, String, String) {
    let cfg = get_config();
    let policy_b64 = encode_policy(policy_doc);
    let signature = sign_policy(&cfg.main_secret_key, policy_b64.as_bytes());
    (cfg.main_access_key.clone(), policy_b64, signature)
}

fn make_expiration(seconds: i64) -> String {
    let expires = Utc::now() + chrono::Duration::seconds(seconds);
    expires.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn make_authenticated_post_fields(
    bucket: &str,
    key_prefix: &str,
    acl: &str,
    content_type: Option<&str>,
    max_size: i64,
    extra_conditions: Option<Vec<serde_json::Value>>,
) -> (Vec<(&'static str, String)>, String) {
    let cfg = get_config();
    let expiration = make_expiration(6000);

    let mut conditions = vec![
        serde_json::json!({"bucket": bucket}),
        serde_json::json!(["starts-with", "$key", key_prefix]),
        serde_json::json!({"acl": acl}),
    ];
    if let Some(ct) = content_type {
        conditions.push(serde_json::json!(["starts-with", "$Content-Type", ct]));
    }
    conditions.push(serde_json::json!(["content-length-range", 0, max_size]));
    if let Some(extra) = extra_conditions {
        conditions.extend(extra);
    }

    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": conditions,
    });
    let policy_b64 = encode_policy(&policy_doc);
    let signature = sign_policy(&cfg.main_secret_key, policy_b64.as_bytes());

    let mut fields: Vec<(&'static str, String)> = vec![
        ("key", format!("{key_prefix}.txt")),
        ("AWSAccessKeyId", cfg.main_access_key.clone()),
        ("acl", acl.to_string()),
        ("signature", signature),
        ("policy", policy_b64),
    ];
    if let Some(ct) = content_type {
        fields.push(("Content-Type", ct.to_string()));
    }

    let url = get_endpoint_url(bucket);
    (fields, url)
}

// --- Anonymous POST ---

#[tokio::test]
async fn test_post_object_anonymous_request() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("acl", "public-read".to_string()),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);

    let resp = client.get_object().bucket(&bucket).key("foo.txt").send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");
}

// --- Authenticated POST ---

#[tokio::test]
async fn test_post_object_authenticated_request() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let (fields, url) =
        make_authenticated_post_fields(&bucket, "foo", "private", Some("text/plain"), 1024, None);
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);

    let resp = client.get_object().bucket(&bucket).key("foo.txt").send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");
}

#[tokio::test]
async fn test_post_object_authenticated_no_content_type() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let (fields, url) =
        make_authenticated_post_fields(&bucket, "foo", "private", None, 1024, None);
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);

    let resp = client.get_object().bucket(&bucket).key("foo.txt").send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");
}

// --- Error cases ---

#[tokio::test]
async fn test_post_object_authenticated_request_bad_access_key() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let cfg = get_config();
    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let policy_b64 = encode_policy(&policy_doc);
    let signature = sign_policy(&cfg.main_secret_key, policy_b64.as_bytes());

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", "foo".to_string()), // bad key
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_post_object_set_success_code() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("acl", "public-read".to_string()),
        ("success_action_status", "201".to_string()),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 201);
    assert!(r.body.contains("<Key>foo.txt</Key>"));
}

#[tokio::test]
async fn test_post_object_set_invalid_success_code() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("acl", "public-read".to_string()),
        ("success_action_status", "404".to_string()),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);
    assert!(r.body.is_empty());
}

#[tokio::test]
async fn test_post_object_expired_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let expiration = make_expiration(-6000); // expired
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let policy_b64 = encode_policy(&policy_doc);
    let signature = sign_policy(&cfg.main_secret_key, policy_b64.as_bytes());

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", cfg.main_access_key.clone()),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_post_object_invalid_signature() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();
    let cfg = get_config();

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let policy_b64 = encode_policy(&policy_doc);

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", cfg.main_access_key.clone()),
        ("acl", "private".to_string()),
        ("signature", "invalid-signature".to_string()),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_post_object_invalid_access_key() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();
    let cfg = get_config();

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let policy_b64 = encode_policy(&policy_doc);
    let signature = sign_policy(&cfg.main_secret_key, policy_b64.as_bytes());

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", "invalid-key".to_string()),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_post_object_missing_policy_condition() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let expiration = make_expiration(6000);
    // missing the "bucket" condition
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let policy_b64 = encode_policy(&policy_doc);
    let signature = sign_policy(&cfg.main_secret_key, policy_b64.as_bytes());

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", cfg.main_access_key.clone()),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_post_object_empty_conditions() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [{}],
    });
    let policy_b64 = encode_policy(&policy_doc);
    let signature = sign_policy(&cfg.main_secret_key, policy_b64.as_bytes());

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", cfg.main_access_key.clone()),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_missing_conditions_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let expiration = make_expiration(6000);
    // no "conditions" key at all
    let policy_doc = serde_json::json!({
        "expiration": expiration,
    });
    let policy_b64 = encode_policy(&policy_doc);
    let signature = sign_policy(&cfg.main_secret_key, policy_b64.as_bytes());

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", cfg.main_access_key.clone()),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_upload_larger_than_chunk() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let (fields, url) = make_authenticated_post_fields(
        &bucket, "foo", "private", Some("text/plain"), 5 * 1024 * 1024, None,
    );
    let body = vec![b'x'; 3 * 1024 * 1024];
    let r = post_object_form(&url, fields, &body).await;
    assert_eq!(r.status, 204);

    let resp = client.get_object().bucket(&bucket).key("foo.txt").send().await.unwrap();
    let got = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(got.len(), body.len());
}

#[tokio::test]
async fn test_post_object_set_key_from_filename() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let (fields, url) = make_authenticated_post_fields(
        &bucket, "foo", "private", Some("text/plain"), 1024, None,
    );
    let mut fields = fields;
    for f in &mut fields {
        if f.0 == "key" {
            f.1 = "${filename}".to_string();
        }
    }
    let r = post_object_form_with_filename(&url, fields, b"bar", "foo.txt").await;
    assert_eq!(r.status, 204);

    let resp = client.get_object().bucket(&bucket).key("foo.txt").send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");
}

#[tokio::test]
async fn test_post_object_ignored_header() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let (mut fields, url) = make_authenticated_post_fields(
        &bucket, "foo", "private", Some("text/plain"), 1024, None,
    );
    fields.push(("x-ignore-foo", "bar".to_string()));
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);
}

#[tokio::test]
async fn test_post_object_case_insensitive_condition_fields() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bUcKeT": bucket},
            ["StArTs-WiTh", "$KeY", "foo"],
            {"AcL": "private"},
            ["StArTs-WiTh", "$CoNtEnT-TyPe", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("kEy", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("aCl", "private".to_string()),
        ("signature", signature),
        ("pOLICy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);
}

#[tokio::test]
async fn test_post_object_escaped_field_values() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", r"\$foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", r"\$foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);

    let resp = client.get_object().bucket(&bucket).key(r"\$foo.txt").send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");
}

#[tokio::test]
async fn test_post_object_success_redirect_action() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let redirect_url = get_endpoint_url(&bucket);
    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["eq", "$success_action_redirect", redirect_url],
            ["content-length-range", 0, 1024],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
        ("success_action_redirect", redirect_url),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 200);

    let resp = client.get_object().bucket(&bucket).key("foo.txt").send().await.unwrap();
    let etag = resp.e_tag().unwrap().trim_matches('"');
    let expected_url = format!(
        "{}?bucket={}&key={}&etag=%22{}%22",
        get_endpoint_url(&bucket),
        bucket,
        "foo.txt",
        etag
    );
    let final_url = r.final_url.as_deref().unwrap_or("");
    assert_eq!(final_url, expected_url);
}

#[tokio::test]
async fn test_post_object_invalid_date_format() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expires = Utc::now() + chrono::Duration::seconds(6000);
    let bad_expiration = expires.to_string();
    let policy_doc = serde_json::json!({
        "expiration": bad_expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", r"\$foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", r"\$foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_no_key_specified() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_missing_signature() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", r"\$foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let (access_key, policy_b64, _signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_user_specified_header() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let extra = vec![
        serde_json::json!(["starts-with", "$x-amz-meta-foo", "bar"]),
    ];
    let (mut fields, url) = make_authenticated_post_fields(
        &bucket, "foo", "private", Some("text/plain"), 1024, Some(extra),
    );
    fields.push(("x-amz-meta-foo", "barclamp".to_string()));
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);

    let resp = client.get_object().bucket(&bucket).key("foo.txt").send().await.unwrap();
    assert_eq!(resp.metadata().unwrap().get("foo").unwrap(), "barclamp");
}

#[tokio::test]
async fn test_post_object_request_missing_policy_specified_field() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let extra = vec![
        serde_json::json!(["starts-with", "$x-amz-meta-foo", "bar"]),
    ];
    let (fields, url) = make_authenticated_post_fields(
        &bucket, "foo", "private", Some("text/plain"), 1024, Some(extra),
    );
    // policy requires x-amz-meta-foo but we don't send it
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_post_object_condition_is_case_sensitive() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    // "CONDITIONS" instead of "conditions"
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "CONDITIONS": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_expires_is_case_sensitive() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    // "EXPIRATION" instead of "expiration"
    let policy_doc = serde_json::json!({
        "EXPIRATION": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_wrong_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let (mut fields, _url) = make_authenticated_post_fields(
        &bucket, "foo", "private", Some("text/plain"), 1024, None,
    );
    for f in &mut fields {
        if f.0 == "key" {
            f.1 = "${filename}".to_string();
        }
    }
    fields.insert(1, ("bucket", bucket.clone()));

    // POST to a different bucket than the one in the policy
    let bad_bucket = get_new_bucket(Some(&client)).await;
    let bad_url = get_endpoint_url(&bad_bucket);
    let r = post_object_form(&bad_url, fields, b"bar").await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_post_object_invalid_request_field_value() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
            ["eq", "$x-amz-meta-foo", ""],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
        ("x-amz-meta-foo", "barclamp".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_post_object_missing_expires_condition() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    // no "expiration" key at all
    let policy_doc = serde_json::json!({
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_upload_size_limit_exceeded() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let (fields, url) = make_authenticated_post_fields(
        &bucket, "foo", "private", Some("text/plain"), 0, None,
    );
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_missing_content_length_argument() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_invalid_content_length_argument() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", -1, 0],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_upload_size_below_minimum() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 512, 1000],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_post_object_upload_size_rgw_chunk_size_bug() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let rgw_max_chunk_size: i64 = 4 * 1024 * 1024; // 4MiB
    let min_size = rgw_max_chunk_size;
    let max_size = rgw_max_chunk_size * 3;
    let test_payload_size = rgw_max_chunk_size + 200;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", min_size, max_size],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let body = vec![b'x'; test_payload_size as usize];
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
    ];
    let r = post_object_form(&url, fields, &body).await;
    assert_eq!(r.status, 204);
}

// --- Tags ---

#[tokio::test]
async fn test_post_object_tags_anonymous_request() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let xml_tagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag>\
        <Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>";

    let url = get_endpoint_url(&bucket);
    let fields = vec![
        ("key", "foo.txt".to_string()),
        ("acl", "public-read".to_string()),
        ("Content-Type", "text/plain".to_string()),
        ("tagging", xml_tagset.to_string()),
    ];
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);

    let resp = client.get_object().bucket(&bucket).key("foo.txt").send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");

    let tag_resp = client.get_object_tagging().bucket(&bucket).key("foo.txt").send().await.unwrap();
    let tag_set = tag_resp.tag_set();
    assert_eq!(tag_set.len(), 2);
    for tag in tag_set {
        let k = tag.key();
        let v = tag.value();
        assert_eq!(k, v);
    }
}

#[tokio::test]
async fn test_post_object_tags_authenticated_request() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let xml_tagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag>\
        <Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>";

    let extra = vec![
        serde_json::json!(["starts-with", "$tagging", ""]),
    ];
    let (mut fields, url) = make_authenticated_post_fields(
        &bucket, "foo", "private", Some("text/plain"), 1024, Some(extra),
    );
    fields.push(("tagging", xml_tagset.to_string()));
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);

    let resp = client.get_object().bucket(&bucket).key("foo.txt").send().await.unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");
}

// --- Checksum ---

#[tokio::test]
async fn test_post_object_upload_checksum() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let megabytes: i64 = 1024 * 1024;
    let max_size = 5 * megabytes;
    let test_payload_size = 2 * megabytes;

    let expiration = make_expiration(6000);
    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", "foo_cksum_test"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, max_size],
        ],
    });
    let (access_key, policy_b64, signature) = make_signed_fields(&policy_doc);
    let url = get_endpoint_url(&bucket);

    let body = vec![b'x'; test_payload_size as usize];

    // good checksum
    let fields = vec![
        ("key", "foo_cksum_test.txt".to_string()),
        ("AWSAccessKeyId", access_key.clone()),
        ("acl", "private".to_string()),
        ("signature", signature.clone()),
        ("policy", policy_b64.clone()),
        ("Content-Type", "text/plain".to_string()),
        ("x-amz-checksum-sha256", "aTL9MeXa9HObn6eP93eygxsJlcwdCwCTysgGAZAgE7w=".to_string()),
    ];
    let r = post_object_form(&url, fields, &body).await;
    assert_eq!(r.status, 204);

    // bad checksum
    let fields = vec![
        ("key", "foo_cksum_test.txt".to_string()),
        ("AWSAccessKeyId", access_key),
        ("acl", "private".to_string()),
        ("signature", signature),
        ("policy", policy_b64),
        ("Content-Type", "text/plain".to_string()),
        ("x-amz-checksum-sha256", "sailorjerry".to_string()),
    ];
    let r = post_object_form(&url, fields, &body).await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn test_encryption_sse_c_post_object_authenticated_request() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let extra = vec![
        serde_json::json!(["starts-with", "$x-amz-server-side-encryption-customer-algorithm", ""]),
        serde_json::json!(["starts-with", "$x-amz-server-side-encryption-customer-key", ""]),
        serde_json::json!(["starts-with", "$x-amz-server-side-encryption-customer-key-md5", ""]),
    ];
    let (mut fields, url) = make_authenticated_post_fields(
        &bucket, "foo", "private", Some("text/plain"), 1024, Some(extra),
    );
    fields.push(("x-amz-server-side-encryption-customer-algorithm", "AES256".to_string()));
    fields.push(("x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=".to_string()));
    fields.push(("x-amz-server-side-encryption-customer-key-md5", "DWygnHRtgiJ77HCm+1rvHw==".to_string()));
    let r = post_object_form(&url, fields, b"bar").await;
    assert_eq!(r.status, 204);

    let resp = client
        .get_object()
        .bucket(&bucket)
        .key("foo.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
        .sse_customer_key_md5("DWygnHRtgiJ77HCm+1rvHw==")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"bar");
}
