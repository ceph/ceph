use std::io::Write;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumAlgorithm;
use s3_tests_rs::client::{get_client, get_client_checksum_when_required};
use s3_tests_rs::fixtures::get_new_bucket;
use s3_tests_rs::http::RequestHeaderCapture;

async fn put_and_capture(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    disable_payload_signing: bool,
) -> (String, usize) {
    let capture = RequestHeaderCapture::new();
    let mut op = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .customize()
        .interceptor(capture.clone());
    if disable_payload_signing {
        op = op.disable_payload_signing();
    }
    op.send().await.unwrap();
    let sha256_header = capture
        .header("x-amz-content-sha256")
        .expect("x-amz-content-sha256 header missing");
    (sha256_header, body.len())
}

async fn put_with_checksum_and_capture(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    algorithm: ChecksumAlgorithm,
    disable_payload_signing: bool,
) -> String {
    let capture = RequestHeaderCapture::new();
    let mut op = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .checksum_algorithm(algorithm)
        .customize()
        .interceptor(capture.clone());
    if disable_payload_signing {
        op = op.disable_payload_signing();
    }
    op.send().await.unwrap();
    capture
        .header("x-amz-content-sha256")
        .expect("x-amz-content-sha256 header missing")
}

async fn get_and_verify(client: &aws_sdk_s3::Client, bucket: &str, key: &str, expected: &[u8]) {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().to_vec();
    assert_eq!(body, expected, "round-trip body mismatch");
}

fn make_body(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

async fn put_streaming_and_capture(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    data: &[u8],
    algorithm: Option<ChecksumAlgorithm>,
    disable_payload_signing: bool,
) -> String {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(data).unwrap();
    let body = ByteStream::read_from()
        .path(file.path())
        .buffer_size(1024)
        .build()
        .await
        .unwrap();

    let capture = RequestHeaderCapture::new();
    let mut builder = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body);
    if let Some(alg) = algorithm {
        builder = builder.checksum_algorithm(alg);
    }
    let mut op = builder.customize().interceptor(capture.clone());
    if disable_payload_signing {
        op = op.disable_payload_signing();
    }
    op.send().await.unwrap();
    // file stays alive until here
    drop(file);
    capture
        .header("x-amz-content-sha256")
        .expect("x-amz-content-sha256 header missing")
}

// =====================================================================
// Mode 1: Signed payload (default client, checksums WhenSupported)
//   Over HTTP the Rust SDK computes an inline SHA256 hash rather than
//   using STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER. This is valid —
//   the server accepts both forms.
// =====================================================================

#[tokio::test]
async fn test_put_object_signing_default() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = b"the quick brown fox jumps over the lazy dog".to_vec();

    let (sha256, _) = put_and_capture(&client, &bucket, "foo", body.clone(), false).await;
    assert!(
        !sha256.contains("UNSIGNED"),
        "expected signed payload mode, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", &body).await;
}

#[tokio::test]
async fn test_put_object_signing_default_large() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = make_body(8 * 1024 * 1024);

    let (sha256, _) = put_and_capture(&client, &bucket, "large", body.clone(), false).await;
    assert!(
        !sha256.contains("UNSIGNED"),
        "expected signed payload mode, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "large", &body).await;
}

// =====================================================================
// Mode 2: STREAMING-AWS4-HMAC-SHA256-PAYLOAD (or inline SHA256)
//   checksum_when_required client + payload signing ON
// =====================================================================

#[tokio::test]
async fn test_put_object_signing_streaming_payload() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = b"small body no checksum".to_vec();

    let (sha256, _) = put_and_capture(&client, &bucket, "foo", body.clone(), false).await;
    // With checksums disabled and small body, SDK may compute inline SHA256
    // hash or use streaming signed payload
    assert!(
        !sha256.contains("UNSIGNED"),
        "expected signed mode, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", &body).await;
}

#[tokio::test]
async fn test_put_object_signing_streaming_payload_large() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = make_body(8 * 1024 * 1024);

    let (sha256, _) = put_and_capture(&client, &bucket, "large", body.clone(), false).await;
    assert!(
        !sha256.contains("UNSIGNED"),
        "expected signed mode, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "large", &body).await;
}

// =====================================================================
// Mode 3: Unsigned payload (default client + disable_payload_signing)
//   Over HTTP with a Vec<u8> body, the Rust SDK sends UNSIGNED-PAYLOAD
//   rather than STREAMING-UNSIGNED-PAYLOAD-TRAILER. The TRAILER variant
//   requires a truly streaming body or explicit checksum algorithm.
// =====================================================================

#[tokio::test]
async fn test_put_object_signing_unsigned_default_checksums() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = b"unsigned with default checksum config".to_vec();

    let (sha256, _) = put_and_capture(&client, &bucket, "foo", body.clone(), true).await;
    assert!(
        sha256.contains("UNSIGNED"),
        "expected unsigned mode, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", &body).await;
}

#[tokio::test]
async fn test_put_object_signing_unsigned_default_checksums_large() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = make_body(8 * 1024 * 1024);

    let (sha256, _) = put_and_capture(&client, &bucket, "large", body.clone(), true).await;
    assert!(
        sha256.contains("UNSIGNED"),
        "expected unsigned mode, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "large", &body).await;
}

// =====================================================================
// Mode 4: UNSIGNED-PAYLOAD
//   checksum_when_required client + disable_payload_signing
// =====================================================================

#[tokio::test]
async fn test_put_object_signing_unsigned_payload() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = b"unsigned no checksum".to_vec();

    let (sha256, _) = put_and_capture(&client, &bucket, "foo", body.clone(), true).await;
    assert_eq!(
        sha256, "UNSIGNED-PAYLOAD",
        "expected UNSIGNED-PAYLOAD, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", &body).await;
}

#[tokio::test]
async fn test_put_object_signing_unsigned_payload_large() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = make_body(8 * 1024 * 1024);

    let (sha256, _) = put_and_capture(&client, &bucket, "large", body.clone(), true).await;
    assert_eq!(
        sha256, "UNSIGNED-PAYLOAD",
        "expected UNSIGNED-PAYLOAD, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "large", &body).await;
}

// =====================================================================
// Explicit checksum algorithm tests
//   Over HTTP with known-length Vec<u8> bodies, the Rust SDK sends the
//   checksum as a header (not a trailer) and uses inline SHA256 or
//   UNSIGNED-PAYLOAD. The STREAMING-*-TRAILER modes require streaming
//   bodies with unknown content length.
// =====================================================================

#[tokio::test]
async fn test_put_object_checksum_crc32_signed() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = b"crc32 signed payload".to_vec();

    let sha256 = put_with_checksum_and_capture(
        &client, &bucket, "foo", body.clone(),
        ChecksumAlgorithm::Crc32, false,
    ).await;
    assert!(
        !sha256.contains("UNSIGNED"),
        "expected signed mode with checksum, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", &body).await;
}

#[tokio::test]
async fn test_put_object_checksum_crc32_unsigned() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = b"crc32 unsigned payload".to_vec();

    let sha256 = put_with_checksum_and_capture(
        &client, &bucket, "foo", body.clone(),
        ChecksumAlgorithm::Crc32, true,
    ).await;
    assert!(
        sha256.contains("UNSIGNED"),
        "expected unsigned mode with checksum, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", &body).await;
}

#[tokio::test]
async fn test_put_object_checksum_sha256_signed() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = b"sha256 checksum signed".to_vec();

    let sha256 = put_with_checksum_and_capture(
        &client, &bucket, "foo", body.clone(),
        ChecksumAlgorithm::Sha256, false,
    ).await;
    assert!(
        !sha256.contains("UNSIGNED"),
        "expected signed mode with checksum, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", &body).await;
}

#[tokio::test]
async fn test_put_object_checksum_sha1_unsigned() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = b"sha1 unsigned payload".to_vec();

    let sha256 = put_with_checksum_and_capture(
        &client, &bucket, "foo", body.clone(),
        ChecksumAlgorithm::Sha1, true,
    ).await;
    assert!(
        sha256.contains("UNSIGNED"),
        "expected unsigned mode with checksum, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", &body).await;
}

#[tokio::test]
async fn test_put_object_checksum_crc32c_unsigned() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let body = b"crc32c unsigned payload".to_vec();

    let sha256 = put_with_checksum_and_capture(
        &client, &bucket, "foo", body.clone(),
        ChecksumAlgorithm::Crc32C, true,
    ).await;
    assert!(
        sha256.contains("UNSIGNED"),
        "expected unsigned mode with checksum, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", &body).await;
}

// =====================================================================
// Streaming body tests — file-backed ByteStream via read_from().path()
//
// With the default client (checksums WhenSupported), file-backed bodies
// produce STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER regardless of
// disable_payload_signing — the checksum interceptor overrides the
// payload signing setting.
//
// With WhenRequired + no explicit checksum, the SDK falls back to
// UNSIGNED-PAYLOAD (with disable) or inline SHA256 (without).
// =====================================================================

#[tokio::test]
async fn test_put_object_streaming_signed_payload_trailer() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let data = b"streaming signed payload with trailer checksum";

    let sha256 = put_streaming_and_capture(
        &client, &bucket, "foo", data, None, false,
    ).await;
    assert_eq!(
        sha256, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        "expected STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", data).await;
}

#[tokio::test]
async fn test_put_object_streaming_disable_payload_signing() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let data = b"streaming disable_payload_signing with default checksums";

    let sha256 = put_streaming_and_capture(
        &client, &bucket, "foo", data, None, true,
    ).await;
    // Checksum interceptor overrides disable_payload_signing — still uses
    // STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER with WhenSupported
    assert_eq!(
        sha256, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        "checksum interceptor should override payload signing, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", data).await;
}

#[tokio::test]
async fn test_put_object_streaming_unsigned_payload() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let data = b"streaming unsigned no checksum";

    let sha256 = put_streaming_and_capture(
        &client, &bucket, "foo", data, None, true,
    ).await;
    assert!(
        sha256.contains("UNSIGNED"),
        "expected unsigned mode, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", data).await;
}

#[tokio::test]
async fn test_put_object_streaming_signed_no_checksum() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client_checksum_when_required();
    let bucket = get_new_bucket(Some(&client)).await;
    let data = b"streaming signed no checksum";

    let sha256 = put_streaming_and_capture(
        &client, &bucket, "foo", data, None, false,
    ).await;
    // Over HTTP with a streaming body and no checksum, the SDK sends
    // UNSIGNED-PAYLOAD even without disable_payload_signing — the SDK
    // skips hashing streaming bodies when no checksum is needed.
    assert!(
        sha256 == "UNSIGNED-PAYLOAD"
            || sha256.starts_with("STREAMING-AWS4-HMAC-SHA256"),
        "expected UNSIGNED-PAYLOAD or STREAMING signed, got: {sha256}"
    );

    get_and_verify(&client, &bucket, "foo", data).await;
}
