use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::{
    build_s3_client_with_session_token, get_iam_root_client, get_sts_client,
};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::get_new_bucket_name;

#[tokio::test]
async fn test_assume_role() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_root_client();
    let sts_client = get_sts_client();
    let cfg = get_config();

    let role_name = format!("{}AssumeTestRole", cfg.iam_name_prefix);

    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "sts:AssumeRole"
        }]
    })
    .to_string();

    let create_resp = iam_client
        .create_role()
        .role_name(&role_name)
        .assume_role_policy_document(&trust_policy)
        .path(&cfg.iam_path_prefix)
        .send()
        .await
        .unwrap();
    let role_arn = create_resp.role().unwrap().arn().to_string();

    let s3_full_access = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "*"
        }]
    })
    .to_string();

    iam_client
        .put_role_policy()
        .role_name(&role_name)
        .policy_name("S3FullAccess")
        .policy_document(&s3_full_access)
        .send()
        .await
        .unwrap();

    let assume_resp = sts_client
        .assume_role()
        .role_arn(&role_arn)
        .role_session_name("test-session")
        .send()
        .await
        .unwrap();

    let creds = assume_resp.credentials().unwrap();
    let temp_access_key = creds.access_key_id();
    let temp_secret_key = creds.secret_access_key();
    let temp_session_token = creds.session_token();

    let s3_client =
        build_s3_client_with_session_token(temp_access_key, temp_secret_key, temp_session_token);

    let bucket = get_new_bucket_name();
    s3_client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();

    s3_client
        .put_object()
        .bucket(&bucket)
        .key("sts-test-object")
        .body(ByteStream::from_static(b"hello from assumed role"))
        .send()
        .await
        .unwrap();

    let get_resp = s3_client
        .get_object()
        .bucket(&bucket)
        .key("sts-test-object")
        .send()
        .await
        .unwrap();
    let body = get_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"hello from assumed role");

    s3_client
        .delete_object()
        .bucket(&bucket)
        .key("sts-test-object")
        .send()
        .await
        .unwrap();

    s3_client
        .delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();

    iam_client
        .delete_role_policy()
        .role_name(&role_name)
        .policy_name("S3FullAccess")
        .send()
        .await
        .unwrap();

    iam_client
        .delete_role()
        .role_name(&role_name)
        .send()
        .await
        .unwrap();
}
