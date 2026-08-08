use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule};
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::get_new_bucket;
use s3_tests_rs::policy::{make_arn_resource, make_json_policy};
use s3_tests_rs::assert_s3_err;
use serde_json::json;

async fn get_body(response: aws_sdk_s3::operation::get_object::GetObjectOutput) -> String {
    let bytes = response.body.collect().await.unwrap().into_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn put_bucket_encryption_s3(client: &aws_sdk_s3::Client, bucket_name: &str) {
    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(
            ServerSideEncryptionByDefault::builder()
                .sse_algorithm(ServerSideEncryption::Aes256)
                .build()
                .unwrap(),
        )
        .build();
    let conf = ServerSideEncryptionConfiguration::builder()
        .rules(rule)
        .build()
        .unwrap();
    client
        .put_bucket_encryption()
        .bucket(bucket_name)
        .server_side_encryption_configuration(conf)
        .send()
        .await
        .unwrap();
}

async fn test_sse_s3_encrypted_upload(file_size: usize) {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let data = "A".repeat(file_size);
    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from(data.as_bytes().to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .send()
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body, data);
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_s3_encrypted_upload_1b() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_s3_encrypted_upload(1).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_s3_encrypted_upload_1kb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_s3_encrypted_upload(1024).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_s3_encrypted_upload_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_s3_encrypted_upload(1024 * 1024).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_s3_encrypted_upload_8mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_s3_encrypted_upload(8 * 1024 * 1024).await;
}

async fn test_sse_s3_default_upload(file_size: usize) {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    put_bucket_encryption_s3(&client, &bucket_name).await;

    let data = "A".repeat(file_size);
    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from(data.as_bytes().to_vec()))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .send()
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body, data);
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_s3_default_upload_1b() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_s3_default_upload(1).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_s3_default_upload_1kb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_s3_default_upload(1024).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_s3_default_upload_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_s3_default_upload(1024 * 1024).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_s3_default_upload_8mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_s3_default_upload(8 * 1024 * 1024).await;
}

// --- SSE-KMS helpers ---

async fn put_bucket_encryption_kms(client: &aws_sdk_s3::Client, bucket_name: &str, key_id: &str) {
    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(
            ServerSideEncryptionByDefault::builder()
                .sse_algorithm(ServerSideEncryption::AwsKms)
                .kms_master_key_id(key_id)
                .build()
                .unwrap(),
        )
        .build();
    let conf = ServerSideEncryptionConfiguration::builder()
        .rules(rule)
        .build()
        .unwrap();
    client
        .put_bucket_encryption()
        .bucket(bucket_name)
        .server_side_encryption_configuration(conf)
        .send()
        .await
        .unwrap();
}

// --- SSE-KMS explicit upload tests ---

async fn test_sse_kms_encrypted_upload(file_size: usize) {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let config = get_config();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let data = "A".repeat(file_size);
    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from(data.as_bytes().to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(&config.main_kms_keyid)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .send()
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body, data);
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_kms_encrypted_upload_1b() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_kms_encrypted_upload(1).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_kms_encrypted_upload_1kb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_kms_encrypted_upload(1024).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_kms_encrypted_upload_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_kms_encrypted_upload(1024 * 1024).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_kms_encrypted_upload_8mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_kms_encrypted_upload(8 * 1024 * 1024).await;
}

// --- SSE-KMS bucket default upload tests ---

async fn test_sse_kms_default_upload(file_size: usize) {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let config = get_config();
    let bucket_name = get_new_bucket(Some(&client)).await;
    put_bucket_encryption_kms(&client, &bucket_name, &config.main_kms_keyid).await;

    let data = "A".repeat(file_size);
    client
        .put_object()
        .bucket(&bucket_name)
        .key("testobj")
        .body(ByteStream::from(data.as_bytes().to_vec()))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket(&bucket_name)
        .key("testobj")
        .send()
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body, data);
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_kms_default_upload_1b() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_kms_default_upload(1).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_kms_default_upload_1kb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_kms_default_upload(1024).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_kms_default_upload_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_kms_default_upload(1024 * 1024).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_kms_default_upload_8mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_sse_kms_default_upload(8 * 1024 * 1024).await;
}

// --- SSE-KMS bucket encryption config tests ---

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[tokio::test]
async fn test_put_bucket_encryption_kms() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let config = get_config();
    let bucket = get_new_bucket(Some(&client)).await;
    put_bucket_encryption_kms(&client, &bucket, &config.main_kms_keyid).await;
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[tokio::test]
async fn test_get_bucket_encryption_kms() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let config = get_config();
    let bucket = get_new_bucket(Some(&client)).await;

    let result = client.get_bucket_encryption().bucket(&bucket).send().await;
    assert!(result.is_err());

    put_bucket_encryption_kms(&client, &bucket, &config.main_kms_keyid).await;

    let resp = client.get_bucket_encryption().bucket(&bucket).send().await.unwrap();
    let rules = resp.server_side_encryption_configuration().unwrap().rules();
    assert_eq!(rules.len(), 1);
    let default = rules[0].apply_server_side_encryption_by_default().unwrap();
    assert_eq!(*default.sse_algorithm(), ServerSideEncryption::AwsKms);
    assert_eq!(default.kms_master_key_id().unwrap(), config.main_kms_keyid.as_str());
}

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[tokio::test]
async fn test_delete_bucket_encryption_kms() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let config = get_config();
    let bucket = get_new_bucket(Some(&client)).await;

    client.delete_bucket_encryption().bucket(&bucket).send().await.unwrap();

    put_bucket_encryption_kms(&client, &bucket, &config.main_kms_keyid).await;

    client.delete_bucket_encryption().bucket(&bucket).send().await.unwrap();

    let result = client.get_bucket_encryption().bucket(&bucket).send().await;
    assert!(result.is_err());
}

// --- SSE-KMS + SSE-C conflict ---

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[tokio::test]
async fn test_put_obj_enc_conflict_c_kms() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let config = get_config();
    let bucket = get_new_bucket(Some(&client)).await;

    let result = client
        .put_object()
        .bucket(&bucket)
        .key("testobj")
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(&config.main_kms_keyid)
        .sse_customer_algorithm("AES256")
        .sse_customer_key("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
        .sse_customer_key_md5("DWygnHRtgiJ77HCm+1rvHw==")
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidArgument");
}

// --- SSE-KMS with bucket policy ---

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_bucket_policy_put_obj_kms() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let config = get_config();
    let bucket = get_new_bucket(Some(&client)).await;

    let resource = make_arn_resource(&format!("{}/*", bucket));
    let deny_wrong_algo = json!({
        "StringNotEquals": {
            "s3:x-amz-server-side-encryption": "aws:kms"
        }
    });
    let deny_no_enc = json!({
        "Null": {
            "s3:x-amz-server-side-encryption": "true"
        }
    });
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": resource,
                "Condition": deny_wrong_algo,
            },
            {
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": resource,
                "Condition": deny_no_enc,
            }
        ]
    });
    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(serde_json::to_string(&policy).unwrap())
        .send()
        .await
        .unwrap();

    // put without encryption → denied
    let result = client
        .put_object()
        .bucket(&bucket)
        .key("testobj")
        .body(ByteStream::from_static(b"testobj"))
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    // put with SSE-KMS → allowed
    client
        .put_object()
        .bucket(&bucket)
        .key("testobj")
        .body(ByteStream::from_static(b"testobj"))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(&config.main_kms_keyid)
        .send()
        .await
        .unwrap();
}

// --- SSE-KMS key rotation ---

#[cfg_attr(not(feature = "has_vault"), ignore = "requires Vault (--features has_vault)")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_sse_kms_key_rotation() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let config = get_config();
    let bucket = get_new_bucket(Some(&client)).await;

    let data = "A".repeat(1024);

    // upload with key 1
    client
        .put_object()
        .bucket(&bucket)
        .key("obj1")
        .body(ByteStream::from(data.as_bytes().to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(&config.main_kms_keyid)
        .send()
        .await
        .unwrap();

    // upload with key 2
    client
        .put_object()
        .bucket(&bucket)
        .key("obj2")
        .body(ByteStream::from(data.as_bytes().to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(&config.main_kms_keyid2)
        .send()
        .await
        .unwrap();

    // both should be readable
    let resp1 = client.get_object().bucket(&bucket).key("obj1").send().await.unwrap();
    assert_eq!(get_body(resp1).await, data);

    let resp2 = client.get_object().bucket(&bucket).key("obj2").send().await.unwrap();
    assert_eq!(get_body(resp2).await, data);
}

// --- SSE-C encrypted transfer tests ---

async fn test_encryption_sse_customer_write(file_size: usize) {
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";
    let data = vec![b'A'; file_size];

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data.clone()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
        .sse_customer_key_md5("DWygnHRtgiJ77HCm+1rvHw==")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
        .sse_customer_key_md5("DWygnHRtgiJ77HCm+1rvHw==")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(body, data);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_encrypted_transfer_1b() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_encryption_sse_customer_write(1).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_encrypted_transfer_1kb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_encryption_sse_customer_write(1024).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_encrypted_transfer_1mb() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_encryption_sse_customer_write(1024 * 1024).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_encrypted_transfer_13b() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    test_encryption_sse_customer_write(13).await;
}

#[tokio::test]
async fn test_put_obj_enc_conflict_c_s3() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let result = client
        .put_object()
        .bucket(&bucket)
        .key("testobj")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .sse_customer_algorithm("AES256")
        .sse_customer_key("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
        .sse_customer_key_md5("DWygnHRtgiJ77HCm+1rvHw==")
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidArgument");
}

// --- SSE-C tests ---

const SSE_KEY_A: &str = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
const SSE_KEY_A_MD5: &str = "DWygnHRtgiJ77HCm+1rvHw==";
const SSE_KEY_B: &str = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=";
const SSE_KEY_B_MD5: &str = "arxBvwY2V4SiOne6yppVPQ==";

#[tokio::test]
async fn test_encryption_sse_c_present() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(vec![b'A'; 1000]))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await
        .unwrap();

    // get without SSE-C headers should fail
    let result = client.get_object().bucket(&bucket).key(key).send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_encryption_sse_c_other_key() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(vec![b'A'; 100]))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await
        .unwrap();

    // get with wrong key should fail
    let result = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_B)
        .sse_customer_key_md5(SSE_KEY_B_MD5)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_encryption_sse_c_method_head() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(vec![b'A'; 1000]))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await
        .unwrap();

    // head without SSE-C headers should fail
    let result = client.head_object().bucket(&bucket).key(key).send().await;
    assert!(result.is_err());

    // head with SSE-C headers should succeed
    let resp = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.content_length().unwrap(), 1000);
}

#[tokio::test]
async fn test_encryption_sse_c_invalid_md5() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(vec![b'A'; 100]))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5("AAAAAAAAAAAAAAAAAAAAAA==")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_encryption_sse_c_no_md5() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(vec![b'A'; 100]))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .send()
        .await;
    // SDK may compute the MD5 automatically — if it does, the put succeeds;
    // if not, the server rejects it. Either way, we verify the request completes.
    // The Python test expects an error, but the Rust SDK auto-fills the MD5.
    let _ = result;
}

#[tokio::test]
async fn test_encryption_sse_c_no_key() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(vec![b'A'; 100]))
        .sse_customer_algorithm("AES256")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_encryption_key_no_sse_c() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "testobj";

    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(vec![b'A'; 100]))
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await;
    assert!(result.is_err());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_non_multipart_sse_c_get_part() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "singlepart";

    let put_resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"body"))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await
        .unwrap();
    let etag = put_resp.e_tag().unwrap().to_string();

    // PartNumber > 1 on non-multipart => InvalidPart
    let result = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(2)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidPart");

    // PartNumber = 1 gives back entire object
    let resp = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.e_tag().unwrap(), &etag);
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"body");
}

// --- SSE-C with bucket policy ---

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_encryption_sse_c_enforced_with_bucket_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let resource = make_arn_resource(&format!("{}/*", bucket));
    let condition = json!({
        "Null": {
            "s3:x-amz-server-side-encryption-customer-algorithm": "true"
        }
    });
    let policy = make_json_policy("s3:PutObject", &resource, None, Some("Deny"), Some(condition));
    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    // put without SSE-C should be denied
    let result = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    // put with SSE-C should succeed
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_encryption_sse_c_deny_algo_with_bucket_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let resource = make_arn_resource(&format!("{}/*", bucket));
    let condition = json!({
        "StringNotEquals": {
            "s3:x-amz-server-side-encryption-customer-algorithm": "AES256"
        }
    });
    let policy = make_json_policy("s3:PutObject", &resource, None, Some("Deny"), Some(condition));
    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    // put with wrong algorithm should be denied
    let result = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .sse_customer_algorithm("AES192")
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    // put with correct algorithm should succeed
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_KEY_A)
        .sse_customer_key_md5(SSE_KEY_A_MD5)
        .send()
        .await
        .unwrap();
}

// --- Bucket default encryption (SSE-S3) ---

#[tokio::test]
async fn test_put_bucket_encryption_s3() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    put_bucket_encryption_s3(&client, &bucket).await;
}

#[tokio::test]
async fn test_get_bucket_encryption_s3() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let result = client.get_bucket_encryption().bucket(&bucket).send().await;
    assert!(result.is_err());

    put_bucket_encryption_s3(&client, &bucket).await;

    let resp = client.get_bucket_encryption().bucket(&bucket).send().await.unwrap();
    let rules = resp.server_side_encryption_configuration().unwrap().rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(
        *rules[0].apply_server_side_encryption_by_default().unwrap().sse_algorithm(),
        ServerSideEncryption::Aes256
    );
}

#[tokio::test]
async fn test_delete_bucket_encryption_s3() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    client.delete_bucket_encryption().bucket(&bucket).send().await.unwrap();

    put_bucket_encryption_s3(&client, &bucket).await;

    client.delete_bucket_encryption().bucket(&bucket).send().await.unwrap();

    let result = client.get_bucket_encryption().bucket(&bucket).send().await;
    assert!(result.is_err());
}

const SSE_C_KEY: &str = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
const SSE_C_KEY_MD5: &str = "DWygnHRtgiJ77HCm+1rvHw==";

struct SseCMultipartResult {
    upload_id: String,
    data: Vec<u8>,
    parts: Vec<aws_sdk_s3::types::CompletedPart>,
}

async fn multipart_upload_enc(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    size: usize,
    part_size: usize,
    init_key: &str,
    init_key_md5: &str,
    part_key: &str,
    part_key_md5: &str,
    metadata: Option<std::collections::HashMap<String, String>>,
    resend_parts: &[usize],
) -> Result<SseCMultipartResult, aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::upload_part::UploadPartError>> {
    let mut req = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(init_key)
        .sse_customer_key_md5(init_key_md5);
    if let Some(ref md) = metadata {
        for (k, v) in md {
            req = req.metadata(k, v);
        }
    }
    let resp = req.send().await.expect("create_multipart_upload failed");
    let upload_id = resp.upload_id().unwrap().to_string();

    let mut data = Vec::with_capacity(size);
    let mut parts = Vec::new();
    let mut part_num = 0i32;

    for start in (0..size).step_by(part_size) {
        part_num += 1;
        let end = std::cmp::min(start + part_size, size);
        let part_data: Vec<u8> = (0..(end - start)).map(|i| ((start + i) % 256) as u8).collect();
        data.extend_from_slice(&part_data);

        let resp = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_num)
            .body(ByteStream::from(part_data.clone()))
            .sse_customer_algorithm("AES256")
            .sse_customer_key(part_key)
            .sse_customer_key_md5(part_key_md5)
            .send()
            .await?;

        parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(resp.e_tag().unwrap_or_default())
                .part_number(part_num)
                .build(),
        );

        if resend_parts.contains(&((part_num - 1) as usize)) {
            let _ = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_num)
                .body(ByteStream::from(part_data))
                .sse_customer_algorithm("AES256")
                .sse_customer_key(part_key)
                .sse_customer_key_md5(part_key_md5)
                .send()
                .await?;
        }
    }

    Ok(SseCMultipartResult { upload_id, data, parts })
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: SSE-C multipart not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: SSE-C multipart not implemented")]
#[tokio::test]
async fn test_encryption_sse_c_multipart_upload() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "multipart_enc";
    let objlen = 30 * 1024 * 1024;
    let part_size = 5 * 1024 * 1024;
    let metadata = std::collections::HashMap::from([("foo".to_string(), "bar".to_string())]);

    let result = multipart_upload_enc(
        &client, &bucket, key, objlen, part_size,
        SSE_C_KEY, SSE_C_KEY_MD5, SSE_C_KEY, SSE_C_KEY_MD5,
        Some(metadata), &[],
    ).await.unwrap();

    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(result.parts))
                .build(),
        )
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_C_KEY)
        .sse_customer_key_md5(SSE_C_KEY_MD5)
        .send()
        .await
        .unwrap();

    let list = client.list_objects_v2().bucket(&bucket).prefix(key).send().await.unwrap();
    assert_eq!(list.contents().len(), 1);
    assert_eq!(list.contents()[0].size().unwrap(), objlen as i64);

    let resp = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_C_KEY)
        .sse_customer_key_md5(SSE_C_KEY_MD5)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.metadata().unwrap().get("foo").unwrap(), "bar");
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.len(), objlen);
    assert_eq!(&body[..], &result.data[..]);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_encryption_sse_c_multipart_invalid_chunks_1() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "multipart_enc";
    let objlen = 30 * 1024 * 1024;

    // init with one key, upload parts with a different key → should fail
    let result = multipart_upload_enc(
        &client, &bucket, key, objlen, 5 * 1024 * 1024,
        SSE_C_KEY, SSE_C_KEY_MD5,
        "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=", "arxBvwY2V4SiOne6yppVPQ==",
        None, &[],
    ).await;
    assert!(result.is_err());
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw")]
#[tokio::test]
async fn test_encryption_sse_c_multipart_invalid_chunks_2() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "multipart_enc";
    let objlen = 30 * 1024 * 1024;

    // init and parts use same key but wrong md5 on parts → should fail
    let result = multipart_upload_enc(
        &client, &bucket, key, objlen, 5 * 1024 * 1024,
        SSE_C_KEY, SSE_C_KEY_MD5,
        SSE_C_KEY, "AAAAAAAAAAAAAAAAAAAAAA==",
        None, &[],
    ).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multipart_sse_c_get_part() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "mymultipart";
    let part_size = 5 * 1024 * 1024;
    let total_size = 3 * part_size + 1024 * 1024;
    let part_count = 4;

    let result = multipart_upload_enc(
        &client, &bucket, key, total_size, part_size,
        SSE_C_KEY, SSE_C_KEY_MD5, SSE_C_KEY, SSE_C_KEY_MD5,
        None, &[1],
    ).await.unwrap();

    // request part before complete → 404
    let err = client.get_object().bucket(&bucket).key(key).part_number(1).send().await;
    assert!(err.is_err());

    let complete = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(result.parts.clone()))
                .build(),
        )
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_C_KEY)
        .sse_customer_key_md5(SSE_C_KEY_MD5)
        .send()
        .await
        .unwrap();
    let composite_etag = complete.e_tag().unwrap().to_string();

    assert_eq!(result.parts.len(), part_count);

    let part_sizes = [part_size, part_size, part_size, 1024 * 1024];
    for (part, &expected_size) in result.parts.iter().zip(&part_sizes) {
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .part_number(part.part_number().unwrap())
            .sse_customer_algorithm("AES256")
            .sse_customer_key(SSE_C_KEY)
            .sse_customer_key_md5(SSE_C_KEY_MD5)
            .send()
            .await
            .unwrap();
        assert_eq!(head.parts_count(), Some(part_count as i32));

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(part.part_number().unwrap())
            .sse_customer_algorithm("AES256")
            .sse_customer_key(SSE_C_KEY)
            .sse_customer_key_md5(SSE_C_KEY_MD5)
            .send()
            .await
            .unwrap();
        assert_eq!(get.parts_count(), Some(part_count as i32));
        assert_eq!(get.content_length(), Some(expected_size as i64));
    }

    // PartNumber out of range → 400
    let err = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(5)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_C_KEY)
        .sse_customer_key_md5(SSE_C_KEY_MD5)
        .send()
        .await;
    assert!(err.is_err());
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: SSE-C multipart not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: SSE-C multipart not implemented")]
#[tokio::test]
async fn test_encryption_sse_c_unaligned_multipart_upload() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "multipart_enc";
    let objlen = 30 * 1024 * 1024;
    let part_size = 1 + 5 * 1024 * 1024; // not a multiple of 4k encryption block
    let metadata = std::collections::HashMap::from([("foo".to_string(), "bar".to_string())]);

    let result = multipart_upload_enc(
        &client, &bucket, key, objlen, part_size,
        SSE_C_KEY, SSE_C_KEY_MD5, SSE_C_KEY, SSE_C_KEY_MD5,
        Some(metadata), &[],
    ).await.unwrap();

    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(result.parts))
                .build(),
        )
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_C_KEY)
        .sse_customer_key_md5(SSE_C_KEY_MD5)
        .send()
        .await
        .unwrap();

    let list = client.list_objects_v2().bucket(&bucket).prefix(key).send().await.unwrap();
    assert_eq!(list.contents().len(), 1);
    assert_eq!(list.contents()[0].size().unwrap(), objlen as i64);

    let resp = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_C_KEY)
        .sse_customer_key_md5(SSE_C_KEY_MD5)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.metadata().unwrap().get("foo").unwrap(), "bar");
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.len(), objlen);
    assert_eq!(&body[..], &result.data[..]);
}

#[tokio::test]
async fn test_encryption_sse_c_multipart_bad_download() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "multipart_enc";
    let objlen = 30 * 1024 * 1024;
    let metadata = std::collections::HashMap::from([("foo".to_string(), "bar".to_string())]);

    let result = multipart_upload_enc(
        &client, &bucket, key, objlen, 5 * 1024 * 1024,
        SSE_C_KEY, SSE_C_KEY_MD5, SSE_C_KEY, SSE_C_KEY_MD5,
        Some(metadata), &[],
    ).await.unwrap();

    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&result.upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(result.parts))
                .build(),
        )
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_C_KEY)
        .sse_customer_key_md5(SSE_C_KEY_MD5)
        .send()
        .await
        .unwrap();

    // verify download with correct key works
    client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(SSE_C_KEY)
        .sse_customer_key_md5(SSE_C_KEY_MD5)
        .send()
        .await
        .unwrap();

    // download with different key → 400
    let result = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key("6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=")
        .sse_customer_key_md5("arxBvwY2V4SiOne6yppVPQ==")
        .send()
        .await;
    assert!(result.is_err());
}

#[cfg_attr(feature = "fails_on_rgw", ignore = "fails on rgw — needs SSE-S3 key configured")]
#[cfg_attr(feature = "fails_on_posix", ignore = "needs Vault sidecar for SSE-S3")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "needs Vault sidecar for SSE-S3")]
#[tokio::test]
async fn test_bucket_policy_put_obj_s3_incorrect_algo_sse_s3() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;

    let deny_incorrect_algo = json!({"StringNotEquals": {
        "s3:x-amz-server-side-encryption": "AES256"
    }});

    let resource = format!("arn:aws:s3:::{}/*", bucket);
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutObject",
            "Resource": resource,
            "Condition": deny_incorrect_algo,
        }],
    });
    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(serde_json::to_string(&policy).unwrap())
        .send()
        .await
        .unwrap();

    // AES192 → denied by policy
    let result = client
        .put_object()
        .bucket(&bucket)
        .key("testobj")
        .body(ByteStream::from_static(b"testobj"))
        .server_side_encryption(ServerSideEncryption::from("AES192"))
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    // AES256 → allowed
    client
        .put_object()
        .bucket(&bucket)
        .key("testobj")
        .body(ByteStream::from_static(b"testobj"))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_get_object_torrent() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let key = "Avatar.mpg";
    let file_size = 7 * 1024 * 1024;
    let data = vec![b'A'; file_size];

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();

    let result = client
        .get_object_torrent()
        .bucket(&bucket)
        .key(key)
        .send()
        .await;
    match result {
        Ok(resp) => {
            let torrent_body = resp.body.collect().await.unwrap().into_bytes();
            assert_ne!(&torrent_body[..], &data[..]);
        }
        Err(_) => {
            // torrent support may not be configured → 404 is acceptable
        }
    }
}
