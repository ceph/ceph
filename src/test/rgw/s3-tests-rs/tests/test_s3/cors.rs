use std::collections::HashMap;
use std::time::Duration;
use aws_sdk_s3::types::{BucketCannedAcl, CorsConfiguration, CorsRule, ObjectCannedAcl};
use s3_tests_rs::client::{get_client, get_tenant_client};
use s3_tests_rs::fixtures::{get_new_bucket, get_new_bucket_name};
use s3_tests_rs::config::get_config;
use s3_tests_rs::http::{
    cors_request_and_check, get_endpoint_url, get_object_url, presign_get, presign_put,
    presign_get_v2_with_creds, presign_put_v2_with_creds,
    raw_request, raw_request_body,
};
use s3_tests_rs::expect_s3_err;

#[tokio::test]
async fn test_set_cors() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let allowed_methods = vec!["GET".to_string(), "PUT".to_string()];
    let allowed_origins = vec!["*.get".to_string(), "*.put".to_string()];

    let cors_config = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .set_allowed_methods(Some(allowed_methods.clone()))
                .set_allowed_origins(Some(allowed_origins.clone()))
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let result = client.get_bucket_cors().bucket(&bucket_name).send().await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 404);

    client
        .put_bucket_cors()
        .bucket(&bucket_name)
        .cors_configuration(cors_config)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_cors()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let rules = response.cors_rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].allowed_methods(), &allowed_methods);
    assert_eq!(rules[0].allowed_origins(), &allowed_origins);

    client
        .delete_bucket_cors()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let result = client.get_bucket_cors().bucket(&bucket_name).send().await;
    let err = expect_s3_err!(result);
    assert_eq!(err.status, 404);
}

#[tokio::test]
async fn test_cors_origin_wildcard() {
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

    let cors_config = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_methods("GET")
                .allowed_origins("*")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    client
        .put_bucket_cors()
        .bucket(&bucket_name)
        .cors_configuration(cors_config)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_cors()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let rules = response.cors_rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].allowed_origins(), &["*"]);
    assert_eq!(rules[0].allowed_methods(), &["GET"]);
}

#[tokio::test]
async fn test_cors_header_option() {
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

    let cors_config = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_methods("GET")
                .allowed_origins("*")
                .expose_headers("x-amz-meta-header1")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    client
        .put_bucket_cors()
        .bucket(&bucket_name)
        .cors_configuration(cors_config)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_cors()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let rules = response.cors_rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].expose_headers(), &["x-amz-meta-header1"]);
}

#[tokio::test]
async fn test_cors_multi_rules() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let cors_config = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_methods("GET")
                .allowed_origins("*.get")
                .build()
                .unwrap(),
        )
        .cors_rules(
            CorsRule::builder()
                .allowed_methods("PUT")
                .allowed_origins("*.put")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    client
        .put_bucket_cors()
        .bucket(&bucket_name)
        .cors_configuration(cors_config)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_cors()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(response.cors_rules().len(), 2);
}

// --- CORS origin response and presigned tests ---

#[tokio::test]
async fn test_cors_origin_response() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let cors_err = client.get_bucket_cors().bucket(&bucket).send().await;
    assert!(cors_err.is_err());

    let cors_config = CorsConfiguration::builder()
        .cors_rules(CorsRule::builder().allowed_methods("GET").allowed_origins("*suffix").build().unwrap())
        .cors_rules(CorsRule::builder().allowed_methods("GET").allowed_origins("start*end").build().unwrap())
        .cors_rules(CorsRule::builder().allowed_methods("GET").allowed_origins("prefix*").build().unwrap())
        .cors_rules(CorsRule::builder().allowed_methods("PUT").allowed_origins("*.put").build().unwrap())
        .build()
        .unwrap();
    client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = get_endpoint_url(&bucket);
    let obj_url = get_object_url(&bucket, "bar");

    // GET without Origin
    let r = raw_request(reqwest::Method::GET, &url, None).await;
    cors_request_and_check(&r, 200, None, None);

    // GET with matching suffix origin
    let mut h = HashMap::new();
    h.insert("Origin".into(), "foo.suffix".into());
    let r = raw_request(reqwest::Method::GET, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, Some("foo.suffix"), Some("GET"));

    // GET with non-matching origin
    h.insert("Origin".into(), "foo.bar".into());
    let r = raw_request(reqwest::Method::GET, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, None, None);

    // GET with matching start*end
    h.insert("Origin".into(), "startend".into());
    let r = raw_request(reqwest::Method::GET, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, Some("startend"), Some("GET"));

    h.insert("Origin".into(), "start1end".into());
    let r = raw_request(reqwest::Method::GET, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, Some("start1end"), Some("GET"));

    h.insert("Origin".into(), "start12end".into());
    let r = raw_request(reqwest::Method::GET, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, Some("start12end"), Some("GET"));

    h.insert("Origin".into(), "0start12end".into());
    let r = raw_request(reqwest::Method::GET, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, None, None);

    // GET with matching prefix*
    h.insert("Origin".into(), "prefix".into());
    let r = raw_request(reqwest::Method::GET, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, Some("prefix"), Some("GET"));

    h.insert("Origin".into(), "prefix.suffix".into());
    let r = raw_request(reqwest::Method::GET, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, Some("prefix.suffix"), Some("GET"));

    h.insert("Origin".into(), "bla.prefix".into());
    let r = raw_request(reqwest::Method::GET, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, None, None);

    // GET on non-existent object with matching origin
    h.insert("Origin".into(), "foo.suffix".into());
    let r = raw_request(reqwest::Method::GET, &obj_url, Some(&h)).await;
    cors_request_and_check(&r, 404, Some("foo.suffix"), Some("GET"));

    // OPTIONS without required headers
    let r = raw_request(reqwest::Method::OPTIONS, &url, None).await;
    cors_request_and_check(&r, 400, None, None);

    // OPTIONS with matching origin + method
    h.clear();
    h.insert("Origin".into(), "foo.suffix".into());
    h.insert("Access-Control-Request-Method".into(), "GET".into());
    let r = raw_request(reqwest::Method::OPTIONS, &obj_url, Some(&h)).await;
    cors_request_and_check(&r, 200, Some("foo.suffix"), Some("GET"));

    // OPTIONS with non-matching origin
    h.insert("Origin".into(), "foo.bar".into());
    let r = raw_request(reqwest::Method::OPTIONS, &url, Some(&h)).await;
    cors_request_and_check(&r, 403, None, None);

    // OPTIONS with PUT origin
    h.insert("Origin".into(), "foo.put".into());
    h.insert("Access-Control-Request-Method".into(), "GET".into());
    let r = raw_request(reqwest::Method::OPTIONS, &url, Some(&h)).await;
    cors_request_and_check(&r, 403, None, None);

    h.insert("Origin".into(), "foo.put".into());
    h.insert("Access-Control-Request-Method".into(), "PUT".into());
    let r = raw_request(reqwest::Method::OPTIONS, &url, Some(&h)).await;
    cors_request_and_check(&r, 200, Some("foo.put"), Some("PUT"));
}

async fn test_cors_presigned_method(
    client: &aws_sdk_s3::Client,
    method: &str,
    acl: Option<ObjectCannedAcl>,
) {
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let http_method = match method {
        "get_object" => "GET",
        "put_object" => "PUT",
        _ => panic!("invalid method"),
    };

    let url = match method {
        "get_object" => presign_get(client, &bucket, "foo", Duration::from_secs(100000)).await,
        "put_object" => presign_put(client, &bucket, "foo", Duration::from_secs(100000), acl).await,
        _ => unreachable!(),
    };

    // OPTIONS before CORS configured should fail
    let r = raw_request(reqwest::Method::OPTIONS, &url, None).await;
    assert_eq!(r.status, 400);

    let cors_config = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_methods(http_method)
                .allowed_origins("example")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config)
        .send()
        .await
        .unwrap();

    let mut headers = HashMap::new();
    headers.insert("Origin".into(), "example".into());
    headers.insert("Access-Control-Request-Method".into(), http_method.into());
    let r = raw_request(reqwest::Method::OPTIONS, &url, Some(&headers)).await;
    cors_request_and_check(&r, 200, Some("example"), Some(http_method));
}

#[tokio::test]
async fn test_cors_presigned_get_object() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_cors_presigned_method(&get_client(), "get_object", None).await;
}

#[tokio::test]
async fn test_cors_presigned_get_object_tenant() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_cors_presigned_method(&get_tenant_client(), "get_object", None).await;
}

#[tokio::test]
async fn test_cors_presigned_put_object() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_cors_presigned_method(&get_client(), "put_object", None).await;
}

#[tokio::test]
async fn test_cors_presigned_put_object_with_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_cors_presigned_method(&get_client(), "put_object", Some(ObjectCannedAcl::Private)).await;
}

#[tokio::test]
async fn test_cors_presigned_put_object_tenant() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_cors_presigned_method(&get_tenant_client(), "put_object", None).await;
}

#[tokio::test]
async fn test_cors_presigned_put_object_tenant_with_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_cors_presigned_method(&get_tenant_client(), "put_object", Some(ObjectCannedAcl::Private)).await;
}

// --- Presigned URL tests ---

async fn test_presigned_get_not_expired(client: &aws_sdk_s3::Client) {
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let url = presign_get(client, &bucket, "foo", Duration::from_secs(100000)).await;

    // OPTIONS without CORS should fail
    let r = raw_request(reqwest::Method::OPTIONS, &url, None).await;
    assert_eq!(r.status, 400);

    // GET should succeed
    let r = raw_request(reqwest::Method::GET, &url, None).await;
    assert_eq!(r.status, 200);
}

#[tokio::test]
async fn test_object_raw_get_x_amz_expires_not_expired() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_presigned_get_not_expired(&get_client()).await;
}

#[tokio::test]
async fn test_object_raw_get_x_amz_expires_not_expired_tenant() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_presigned_get_not_expired(&get_tenant_client()).await;
}

fn mangle_expires(url: &str, new_expires: &str) -> String {
    let re = regex::Regex::new(r"X-Amz-Expires=\d+").unwrap();
    re.replace(url, &format!("X-Amz-Expires={new_expires}")).to_string()
}

#[tokio::test]
async fn test_object_raw_get_x_amz_expires_out_range_zero() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).acl(BucketCannedAcl::PublicRead).send().await.unwrap();
    client.put_object().bucket(&bucket).key("foo").acl(ObjectCannedAcl::PublicRead).send().await.unwrap();

    let url = presign_get(&client, &bucket, "foo", Duration::from_secs(100)).await;
    let url = mangle_expires(&url, "0");

    let r = raw_request(reqwest::Method::GET, &url, None).await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_object_raw_get_x_amz_expires_out_max_range() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).acl(BucketCannedAcl::PublicRead).send().await.unwrap();
    client.put_object().bucket(&bucket).key("foo").acl(ObjectCannedAcl::PublicRead).send().await.unwrap();

    let url = presign_get(&client, &bucket, "foo", Duration::from_secs(100)).await;
    let url = mangle_expires(&url, "609901");

    let r = raw_request(reqwest::Method::GET, &url, None).await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn test_object_raw_get_x_amz_expires_out_positive_range() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).acl(BucketCannedAcl::PublicRead).send().await.unwrap();
    client.put_object().bucket(&bucket).key("foo").acl(ObjectCannedAcl::PublicRead).send().await.unwrap();

    let url = presign_get(&client, &bucket, "foo", Duration::from_secs(100)).await;
    let url = mangle_expires(&url, "-7");

    let r = raw_request(reqwest::Method::GET, &url, None).await;
    assert_eq!(r.status, 403);
}

async fn test_presigned_put_with_acl(client: &aws_sdk_s3::Client) {
    let bucket = get_new_bucket(Some(client)).await;
    let key = "foo";

    let put_url = presign_put(client, &bucket, key, Duration::from_secs(100000), Some(ObjectCannedAcl::Private)).await;

    let mut headers = HashMap::new();
    headers.insert("x-amz-acl".into(), "private".into());
    let r = raw_request_body(reqwest::Method::PUT, &put_url, Some(&headers), Some(b"hello world".to_vec())).await;
    assert_eq!(r.status, 200);

    let get_url = presign_get(client, &bucket, key, Duration::from_secs(100000)).await;
    let r = raw_request(reqwest::Method::GET, &get_url, None).await;
    assert_eq!(r.status, 200);
}

#[tokio::test]
async fn test_object_presigned_put_object_with_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_presigned_put_with_acl(&get_client()).await;
}

#[tokio::test]
async fn test_object_presigned_put_object_with_acl_tenant() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_presigned_put_with_acl(&get_tenant_client()).await;
}

#[tokio::test]
async fn test_object_raw_put_authenticated_expired() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    client.put_object().bucket(&bucket).key("foo").send().await.unwrap();

    let url = presign_put(&client, &bucket, "foo", Duration::from_secs(100), None).await;
    // mangle the date to make it expired by setting X-Amz-Expires to 0
    // (alternative: set X-Amz-Date to the past, but mangling expires is simpler)
    let url = mangle_expires(&url, "0");

    let mut headers = HashMap::new();
    headers.insert("content-length".into(), "3".into());
    let r = raw_request(reqwest::Method::PUT, &url, Some(&headers)).await;
    assert_eq!(r.status, 403);
}

// --- SigV2 presigned CORS tests ---

async fn test_cors_presigned_method_v2(
    client: &aws_sdk_s3::Client,
    method: &str,
    access_key: &str,
    secret_key: &str,
) {
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let http_method = match method {
        "get_object" => "GET",
        "put_object" => "PUT",
        _ => panic!("invalid method"),
    };

    let url = match method {
        "get_object" => presign_get_v2_with_creds(&bucket, "foo", 100000, access_key, secret_key),
        "put_object" => presign_put_v2_with_creds(&bucket, "foo", 100000, None, access_key, secret_key),
        _ => unreachable!(),
    };

    let r = raw_request(reqwest::Method::OPTIONS, &url, None).await;
    assert_eq!(r.status, 400);

    let cors_config = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_methods(http_method)
                .allowed_origins("example")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config)
        .send()
        .await
        .unwrap();

    let mut headers = HashMap::new();
    headers.insert("Origin".into(), "example".into());
    headers.insert("Access-Control-Request-Method".into(), http_method.into());
    let r = raw_request(reqwest::Method::OPTIONS, &url, Some(&headers)).await;
    cors_request_and_check(&r, 200, Some("example"), Some(http_method));
}

#[tokio::test]
async fn test_cors_presigned_get_object_v2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    test_cors_presigned_method_v2(
        &get_client(), "get_object",
        &cfg.main_access_key, &cfg.main_secret_key,
    ).await;
}

#[tokio::test]
async fn test_cors_presigned_get_object_tenant_v2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    test_cors_presigned_method_v2(
        &get_tenant_client(), "get_object",
        &cfg.tenant_access_key, &cfg.tenant_secret_key,
    ).await;
}

#[tokio::test]
async fn test_cors_presigned_put_object_v2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    test_cors_presigned_method_v2(
        &get_client(), "put_object",
        &cfg.main_access_key, &cfg.main_secret_key,
    ).await;
}

#[tokio::test]
async fn test_cors_presigned_put_object_tenant_v2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    test_cors_presigned_method_v2(
        &get_tenant_client(), "put_object",
        &cfg.tenant_access_key, &cfg.tenant_secret_key,
    ).await;
}
