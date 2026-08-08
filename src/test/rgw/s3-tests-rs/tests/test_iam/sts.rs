use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::{
    build_iam_client_with_session_token, build_s3_client, build_s3_client_with_session_token,
    build_sts_client, get_iam_alt_root_client, get_iam_alt_root_s3client,
    get_iam_root_client, get_iam_root_s3client, get_sts_client,
};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{get_new_bucket, get_new_bucket_name};

fn require_webidentity(cfg: &s3_tests_rs::config::S3TestConfig) -> (String, String) {
    let token = cfg.webidentity_token.as_ref()
        .expect("webidentity not configured — run keycloak-vstart.sh first");
    let realm = cfg.webidentity_realm.as_ref()
        .expect("webidentity realm not configured");
    (token.clone(), realm.clone())
}

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

#[tokio::test]
async fn test_account_role_policy_allow() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let user_name = format!("{}RPUser", cfg.iam_name_prefix);
    let role_name = format!("{}RPRole", cfg.iam_name_prefix);

    let user = iam.create_user().user_name(&user_name).path(path).send().await.unwrap();
    let user_arn = user.user().unwrap().arn().to_string();

    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                        "Principal": {"AWS": user_arn}}]
    }).to_string();

    let role = iam.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    let key = iam.create_access_key().user_name(&user_name).send().await.unwrap()
        .access_key().unwrap().clone();
    let sts = build_sts_client(key.access_key_id(), key.secret_access_key());

    let assume = sts.assume_role().role_arn(&role_arn)
        .role_session_name("test").send().await.unwrap();
    let creds = assume.credentials().unwrap();
    let s3 = build_s3_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    /* denied without role policy */
    let err = s3.list_buckets().send().await;
    assert!(err.is_err());

    /* add role policy */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"}]
    }).to_string();
    iam.put_role_policy().role_name(&role_name).policy_name("AllowList")
        .policy_document(&policy).send().await.unwrap();

    /* now allowed */
    let resp = s3.list_buckets().send().await;
    assert!(resp.is_ok(), "should be allowed after role policy");

    /* clean up */
    iam.delete_role_policy().role_name(&role_name).policy_name("AllowList").send().await.unwrap();
    iam.delete_access_key().user_name(&user_name).access_key_id(key.access_key_id()).send().await.unwrap();
    iam.delete_user().user_name(&user_name).send().await.unwrap();
    iam.delete_role().role_name(&role_name).send().await.unwrap();
}

#[tokio::test]
async fn test_cross_account_assume_role() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_root = get_iam_root_client();
    let iam_alt = get_iam_alt_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let user_name = format!("{}XAUser", cfg.iam_name_prefix);
    let role_name = format!("{}XARole", cfg.iam_name_prefix);

    /* create user in alt account */
    let user = iam_alt.create_user().user_name(&user_name).path(path).send().await.unwrap();
    let user_arn = user.user().unwrap().arn().to_string();
    let key = iam_alt.create_access_key().user_name(&user_name).send().await.unwrap()
        .access_key().unwrap().clone();

    /* create role in main account trusting the alt user */
    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                        "Principal": {"AWS": user_arn}}]
    }).to_string();
    let role = iam_root.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    /* alt user assumes main account role */
    let sts = build_sts_client(key.access_key_id(), key.secret_access_key());
    let assume = sts.assume_role().role_arn(&role_arn)
        .role_session_name("xacct").send().await.unwrap();
    let creds = assume.credentials().unwrap();

    /* create bucket with main account root */
    let roots3 = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&roots3)).await;

    let s3 = build_s3_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    /* denied without role policy */
    let err = s3.list_objects_v2().bucket(&bucket).send().await;
    assert!(err.is_err());

    /* add role policy allowing s3:ListBucket */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "*"}]
    }).to_string();
    iam_root.put_role_policy().role_name(&role_name).policy_name("AllowList")
        .policy_document(&policy).send().await.unwrap();

    /* now allowed (same-account bucket) */
    let resp = s3.list_objects_v2().bucket(&bucket).send().await;
    assert!(resp.is_ok(), "should be allowed with role policy on same-account bucket");

    /* clean up */
    roots3.delete_bucket().bucket(&bucket).send().await.unwrap();
    iam_root.delete_role_policy().role_name(&role_name).policy_name("AllowList").send().await.unwrap();
    iam_root.delete_role().role_name(&role_name).send().await.unwrap();
    iam_alt.delete_access_key().user_name(&user_name).access_key_id(key.access_key_id()).send().await.unwrap();
    iam_alt.delete_user().user_name(&user_name).send().await.unwrap();
}

#[tokio::test]
async fn test_cross_account_role_create_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_root = get_iam_root_client();
    let iam_alt = get_iam_alt_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let user_name = format!("{}XCBUser", cfg.iam_name_prefix);
    let role_name = format!("{}XCBRole", cfg.iam_name_prefix);
    let bucket_name = get_new_bucket_name();

    let user = iam_alt.create_user().user_name(&user_name).path(path).send().await.unwrap();
    let user_arn = user.user().unwrap().arn().to_string();
    let key = iam_alt.create_access_key().user_name(&user_name).send().await.unwrap()
        .access_key().unwrap().clone();

    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                        "Principal": {"AWS": user_arn}}]
    }).to_string();
    let role = iam_root.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    let sts = build_sts_client(key.access_key_id(), key.secret_access_key());
    let assume = sts.assume_role().role_arn(&role_arn)
        .role_session_name("xacct-cb").send().await.unwrap();
    let creds = assume.credentials().unwrap();

    let s3 = build_s3_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    /* denied without role policy */
    let err = s3.create_bucket().bucket(&bucket_name).send().await;
    assert!(err.is_err());

    /* add role policy allowing CreateBucket */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": ["s3:CreateBucket", "s3:PutBucketAcl"], "Resource": "*"}]
    }).to_string();
    iam_root.put_role_policy().role_name(&role_name).policy_name("AllowCreate")
        .policy_document(&policy).send().await.unwrap();

    /* now allowed */
    s3.create_bucket().bucket(&bucket_name).send().await.unwrap();

    /* clean up */
    let roots3 = get_iam_root_s3client();
    roots3.delete_bucket().bucket(&bucket_name).send().await.unwrap();
    iam_root.delete_role_policy().role_name(&role_name).policy_name("AllowCreate").send().await.unwrap();
    iam_root.delete_role().role_name(&role_name).send().await.unwrap();
    iam_alt.delete_access_key().user_name(&user_name).access_key_id(key.access_key_id()).send().await.unwrap();
    iam_alt.delete_user().user_name(&user_name).send().await.unwrap();
}

#[tokio::test]
async fn test_cross_account_role_get_role() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_root = get_iam_root_client();
    let iam_alt = get_iam_alt_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let user_name = format!("{}XGRUser", cfg.iam_name_prefix);
    let role_name = format!("{}XGRRole", cfg.iam_name_prefix);

    let user = iam_alt.create_user().user_name(&user_name).path(path).send().await.unwrap();
    let user_arn = user.user().unwrap().arn().to_string();
    let key = iam_alt.create_access_key().user_name(&user_name).send().await.unwrap()
        .access_key().unwrap().clone();

    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                        "Principal": {"AWS": user_arn}}]
    }).to_string();
    let role = iam_root.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    let sts = build_sts_client(key.access_key_id(), key.secret_access_key());
    let assume = sts.assume_role().role_arn(&role_arn)
        .role_session_name("xacct-gr").send().await.unwrap();
    let creds = assume.credentials().unwrap();

    let session_iam = build_iam_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    /* denied without role policy */
    let err = session_iam.get_role().role_name(&role_name).send().await;
    assert!(err.is_err());

    /* add role policy allowing GetRole */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "iam:GetRole", "Resource": "*"}]
    }).to_string();
    iam_root.put_role_policy().role_name(&role_name).policy_name("AllowGetRole")
        .policy_document(&policy).send().await.unwrap();

    /* now allowed */
    let resp = session_iam.get_role().role_name(&role_name).send().await;
    assert!(resp.is_ok(), "should be allowed with role policy for GetRole");

    /* clean up */
    iam_root.delete_role_policy().role_name(&role_name).policy_name("AllowGetRole").send().await.unwrap();
    iam_root.delete_role().role_name(&role_name).send().await.unwrap();
    iam_alt.delete_access_key().user_name(&user_name).access_key_id(key.access_key_id()).send().await.unwrap();
    iam_alt.delete_user().user_name(&user_name).send().await.unwrap();
}

#[tokio::test]
async fn test_assume_role_with_web_identity() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam = get_iam_root_client();
    let cfg = get_config();
    let token = match cfg.webidentity_token.as_ref() {
        Some(t) => t.clone(),
        None => { eprintln!("SKIP: webidentity not configured"); return; }
    };
    let realm = cfg.webidentity_realm.as_deref().unwrap_or("demorealm");
    let path = &cfg.iam_path_prefix;
    let aud = cfg.webidentity_aud.as_deref().unwrap_or("account");

    let role_name = format!("{}WebIdRole", cfg.iam_name_prefix);
    let thumbprint = cfg.webidentity_thumbprint.as_deref().unwrap_or("");
    let oidc_url = format!("http://localhost:8080/realms/{}", realm);

    /* create OIDC provider in RGW */
    let oidc_resp = iam.create_open_id_connect_provider()
        .url(&oidc_url)
        .client_id_list(aud)
        .thumbprint_list(thumbprint)
        .send()
        .await
        .unwrap();
    let oidc_arn = oidc_resp.open_id_connect_provider_arn().unwrap().to_string();

    /* create role trusting the OIDC provider */
    let oidc_app_id_key = format!("localhost:8080/realms/{}:app_id", realm);
    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Federated": [&oidc_arn]},
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {"StringEquals": {
                &oidc_app_id_key: aud
            }}
        }]
    }).to_string();

    let role = iam.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    let role_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*"}
    }).to_string();
    iam.put_role_policy().role_name(&role_name).policy_name("S3Full")
        .policy_document(&role_policy).send().await.unwrap();

    /* assume role with web identity token */
    let sts = get_sts_client();
    let resp = sts.assume_role_with_web_identity()
        .role_arn(&role_arn)
        .role_session_name("webid-test")
        .web_identity_token(&token)
        .send()
        .await
        .unwrap();

    let creds = resp.credentials().unwrap();
    let s3 = build_s3_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    let bucket = get_new_bucket_name();
    s3.create_bucket().bucket(&bucket).send().await.unwrap();
    s3.delete_bucket().bucket(&bucket).send().await.unwrap();

    /* clean up */
    iam.delete_role_policy().role_name(&role_name).policy_name("S3Full").send().await.unwrap();
    iam.delete_role().role_name(&role_name).send().await.unwrap();
    iam.delete_open_id_connect_provider()
        .open_id_connect_provider_arn(&oidc_arn).send().await.unwrap();
}
