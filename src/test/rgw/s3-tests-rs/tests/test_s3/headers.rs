use std::collections::HashMap;
use s3_tests_rs::fixtures::{get_new_bucket, get_new_bucket_name};
use s3_tests_rs::http::{sigv2_request, RawResponse};

fn parse_error_code(body: &str) -> Option<String> {
    let start = body.find("<Code>")?;
    let end = body.find("</Code>")?;
    Some(body[start + 6..end].to_string())
}

struct ErrorResult {
    status: u16,
    error_code: String,
}

// --- Object helpers ---

async fn add_header_create_object(
    headers: &HashMap<String, String>,
) -> (String, String) {
    let bucket = get_new_bucket(None).await;
    let key = "foo".to_string();
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, Some("foo"), None,
        Some(headers), None,
    ).await;
    assert!(
        r.status == 200 || r.status == 204,
        "expected success, got {} — {}",
        r.status, r.body
    );
    (bucket, key)
}

async fn add_header_create_bad_object(
    headers: &HashMap<String, String>,
) -> ErrorResult {
    let bucket = get_new_bucket(None).await;
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, Some("foo"), Some(b"bar"),
        Some(headers), None,
    ).await;
    assert!(
        r.status >= 400,
        "expected error, got {} — {}",
        r.status, r.body
    );
    ErrorResult {
        status: r.status,
        error_code: parse_error_code(&r.body).unwrap_or_default(),
    }
}

async fn remove_header_create_object(
    remove: &str,
) -> (String, String) {
    let bucket = get_new_bucket(None).await;
    let key = "foo".to_string();
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, Some("foo"), None,
        None, Some(&[remove]),
    ).await;
    assert!(
        r.status == 200 || r.status == 204,
        "expected success, got {} — {}",
        r.status, r.body
    );
    (bucket, key)
}

async fn remove_header_create_bad_object(
    remove: &str,
) -> ErrorResult {
    let bucket = get_new_bucket(None).await;
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, Some("foo"), Some(b"bar"),
        None, Some(&[remove]),
    ).await;
    assert!(
        r.status >= 400,
        "expected error, got {} — {}",
        r.status, r.body
    );
    ErrorResult {
        status: r.status,
        error_code: parse_error_code(&r.body).unwrap_or_default(),
    }
}

// --- Bucket helpers ---

async fn add_header_create_bucket(
    headers: &HashMap<String, String>,
) -> String {
    let bucket = get_new_bucket_name();
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, None, None,
        Some(headers), None,
    ).await;
    assert!(
        r.status == 200 || r.status == 204,
        "expected success, got {} — {}",
        r.status, r.body
    );
    bucket
}

async fn add_header_create_bad_bucket(
    headers: &HashMap<String, String>,
) -> ErrorResult {
    let bucket = get_new_bucket_name();
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, None, None,
        Some(headers), None,
    ).await;
    assert!(
        r.status >= 400,
        "expected error, got {} — {}",
        r.status, r.body
    );
    ErrorResult {
        status: r.status,
        error_code: parse_error_code(&r.body).unwrap_or_default(),
    }
}

async fn remove_header_create_bucket(
    remove: &str,
) -> String {
    let bucket = get_new_bucket_name();
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, None, None,
        None, Some(&[remove]),
    ).await;
    assert!(
        r.status == 200 || r.status == 204,
        "expected success, got {} — {}",
        r.status, r.body
    );
    bucket
}

async fn remove_header_create_bad_bucket(
    remove: &str,
) -> ErrorResult {
    let bucket = get_new_bucket_name();
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, None, None,
        None, Some(&[remove]),
    ).await;
    assert!(
        r.status >= 400,
        "expected error, got {} — {}",
        r.status, r.body
    );
    ErrorResult {
        status: r.status,
        error_code: parse_error_code(&r.body).unwrap_or_default(),
    }
}

// =====================================================================
// Object tests (auth_aws2)
// =====================================================================

#[tokio::test]
async fn test_object_create_bad_md5_invalid_garbage_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("Content-MD5".to_string(), "AWS HAHAHA".to_string());
    let e = add_header_create_bad_object(&headers).await;
    assert_eq!(e.status, 400);
    assert_eq!(e.error_code, "InvalidDigest");
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[cfg_attr(feature = "fails_on_posix", ignore = "rgw core: sigv2 content-length mismatch not rejected")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "rgw core: sigv2 content-length mismatch not rejected")]
async fn test_object_create_bad_contentlength_mismatch_below_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    // body is "bar" (3 bytes), claim 2
    headers.insert("Content-Length".to_string(), "2".to_string());
    let e = add_header_create_bad_object(&headers).await;
    assert_eq!(e.status, 400);
    assert_eq!(e.error_code, "BadDigest");
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[cfg_attr(feature = "fails_on_posix", ignore = "rgw core: sigv2 wrong error code for bad auth")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "rgw core: sigv2 wrong error code for bad auth")]
async fn test_object_create_bad_authorization_incorrect_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert(
        "Authorization".to_string(),
        "AWS AKIAIGR7ZNNBHC5BKSUB:FWeDfwojDSdS2Ztmpfeubhd9isU=".to_string(),
    );
    let e = add_header_create_bad_object(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "InvalidDigest");
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
async fn test_object_create_bad_authorization_invalid_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("Authorization".to_string(), "AWS HAHAHA".to_string());
    let e = add_header_create_bad_object(&headers).await;
    assert_eq!(e.status, 400);
    assert_eq!(e.error_code, "InvalidArgument");
}

#[tokio::test]
async fn test_object_create_bad_ua_empty_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("User-Agent".to_string(), String::new());
    let (bucket, key) = add_header_create_object(&headers).await;
    // verify the object is accessible by doing another put
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, Some(&key), Some(b"bar"),
        None, None,
    ).await;
    assert!(r.status == 200 || r.status == 204);
}

#[tokio::test]
async fn test_object_create_bad_ua_none_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket, key) = remove_header_create_object("User-Agent").await;
    let r = sigv2_request(
        reqwest::Method::PUT, &bucket, Some(&key), Some(b"bar"),
        None, None,
    ).await;
    assert!(r.status == 200 || r.status == 204);
}

#[tokio::test]
async fn test_object_create_bad_date_invalid_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "Bad Date".to_string());
    let e = add_header_create_bad_object(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "AccessDenied");
}

#[tokio::test]
async fn test_object_create_bad_date_empty_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), String::new());
    let e = add_header_create_bad_object(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "AccessDenied");
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[cfg_attr(feature = "fails_on_posix", ignore = "rgw core: missing sigv2 date not rejected")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "rgw core: missing sigv2 date not rejected")]
async fn test_object_create_bad_date_none_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let e = remove_header_create_bad_object("x-amz-date").await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "AccessDenied");
}

#[tokio::test]
async fn test_object_create_bad_date_before_today_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "Tue, 07 Jul 2010 21:53:04 GMT".to_string());
    let e = add_header_create_bad_object(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "RequestTimeTooSkewed");
}

#[tokio::test]
async fn test_object_create_bad_date_before_epoch_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "Tue, 07 Jul 1950 21:53:04 GMT".to_string());
    let e = add_header_create_bad_object(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "AccessDenied");
}

#[tokio::test]
async fn test_object_create_bad_date_after_end_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "Tue, 07 Jul 9999 21:53:04 GMT".to_string());
    let e = add_header_create_bad_object(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "RequestTimeTooSkewed");
}

// =====================================================================
// Bucket tests (auth_aws2)
// =====================================================================

#[tokio::test]
#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
async fn test_bucket_create_bad_authorization_invalid_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("Authorization".to_string(), "AWS HAHAHA".to_string());
    let e = add_header_create_bad_bucket(&headers).await;
    assert_eq!(e.status, 400);
    assert_eq!(e.error_code, "InvalidArgument");
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
async fn test_bucket_create_bad_ua_empty_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("User-Agent".to_string(), String::new());
    add_header_create_bucket(&headers).await;
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
async fn test_bucket_create_bad_ua_none_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    remove_header_create_bucket("User-Agent").await;
}

#[tokio::test]
async fn test_bucket_create_bad_date_invalid_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "Bad Date".to_string());
    let e = add_header_create_bad_bucket(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "AccessDenied");
}

#[tokio::test]
async fn test_bucket_create_bad_date_empty_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), String::new());
    let e = add_header_create_bad_bucket(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "AccessDenied");
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[cfg_attr(feature = "fails_on_posix", ignore = "rgw core: missing sigv2 date not rejected")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "rgw core: missing sigv2 date not rejected")]
async fn test_bucket_create_bad_date_none_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let e = remove_header_create_bad_bucket("x-amz-date").await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "AccessDenied");
}

#[tokio::test]
async fn test_bucket_create_bad_date_before_today_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "Tue, 07 Jul 2010 21:53:04 GMT".to_string());
    let e = add_header_create_bad_bucket(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "RequestTimeTooSkewed");
}

#[tokio::test]
async fn test_bucket_create_bad_date_after_today_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "Tue, 07 Jul 2030 21:53:04 GMT".to_string());
    let e = add_header_create_bad_bucket(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "RequestTimeTooSkewed");
}

#[tokio::test]
async fn test_bucket_create_bad_date_before_epoch_aws2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "Tue, 07 Jul 1950 21:53:04 GMT".to_string());
    let e = add_header_create_bad_bucket(&headers).await;
    assert_eq!(e.status, 403);
    assert_eq!(e.error_code, "AccessDenied");
}
