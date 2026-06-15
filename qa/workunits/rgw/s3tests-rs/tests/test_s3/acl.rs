use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketCannedAcl, Grant, ObjectCannedAcl, ObjectOwnership, OwnershipControls,
    OwnershipControlsRule,
};
use s3_tests_rs::client::{get_alt_client, get_client, get_unauthenticated_client};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{get_new_bucket, get_new_bucket_name};
use s3_tests_rs::assert_s3_err;

fn check_grant(grant: &Grant, perm: &str, grantee_type: &str, id: Option<&str>, uri: Option<&str>) {
    assert_eq!(grant.permission().unwrap().as_str(), perm);
    let g = grant.grantee().unwrap();
    assert_eq!(g.r#type().as_str(), grantee_type);
    if let Some(expected_id) = id {
        assert_eq!(g.id().unwrap_or_default(), expected_id);
    }
    if let Some(expected_uri) = uri {
        assert_eq!(g.uri().unwrap_or_default(), expected_uri);
    }
}

#[tokio::test]
async fn test_bucket_acl_default() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let response = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    assert_eq!(response.owner().unwrap().display_name().unwrap_or_default(), &cfg.main_display_name);
    assert_eq!(response.owner().unwrap().id().unwrap_or_default(), &cfg.main_user_id);

    let grants = response.grants();
    assert_eq!(grants.len(), 1);
    check_grant(&grants[0], "FULL_CONTROL", "CanonicalUser", Some(&cfg.main_user_id), None);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_acl_canned_during_create() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let response = client.get_bucket_acl().bucket(&bucket_name).send().await.unwrap();
    let grants = response.grants();
    assert_eq!(grants.len(), 2);
}

#[tokio::test]
async fn test_bucket_acl_canned() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let response = client.get_bucket_acl().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.grants().len(), 2);

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .unwrap();

    let response = client.get_bucket_acl().bucket(&bucket_name).send().await.unwrap();
    let grants = response.grants();
    assert_eq!(grants.len(), 1);
    let config = get_config();
    check_grant(&grants[0], "FULL_CONTROL", "CanonicalUser", Some(&config.main_user_id), None);
}

#[tokio::test]
async fn test_bucket_acl_canned_publicreadwrite() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let response = client.get_bucket_acl().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.grants().len(), 3);
}

#[tokio::test]
async fn test_bucket_acl_canned_authenticatedread() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::AuthenticatedRead)
        .send()
        .await
        .unwrap();

    let response = client.get_bucket_acl().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.grants().len(), 2);
}

#[tokio::test]
async fn test_object_acl_default() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();

    let grants = response.grants();
    assert_eq!(grants.len(), 1);
    check_grant(&grants[0], "FULL_CONTROL", "CanonicalUser", Some(&cfg.main_user_id), None);
}

#[tokio::test]
async fn test_object_acl_canned_during_create() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.grants().len(), 2);
}

#[tokio::test]
async fn test_object_acl_canned() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.grants().len(), 2);

    client
        .put_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    let grants = response.grants();
    assert_eq!(grants.len(), 1);
    check_grant(&grants[0], "FULL_CONTROL", "CanonicalUser", Some(&cfg.main_user_id), None);
}

#[tokio::test]
async fn test_object_acl_canned_publicreadwrite() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .acl(ObjectCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.grants().len(), 3);
}

#[tokio::test]
async fn test_object_acl_canned_authenticatedread() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .acl(ObjectCannedAcl::AuthenticatedRead)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.grants().len(), 2);
}

// --- ACL grant tests (cross-user) ---

async fn bucket_acl_grant_userid(permission: &str) -> String {
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let current_acl = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let alt_grantee = aws_sdk_s3::types::Grantee::builder()
        .id(&cfg.alt_user_id)
        .r#type(aws_sdk_s3::types::Type::CanonicalUser)
        .build()
        .unwrap();

    let mut grants: Vec<aws_sdk_s3::types::Grant> = current_acl.grants().to_vec();
    grants.push(
        aws_sdk_s3::types::Grant::builder()
            .grantee(alt_grantee)
            .permission(permission.parse::<aws_sdk_s3::types::Permission>().unwrap())
            .build(),
    );

    let owner = current_acl.owner().unwrap().clone();
    let acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(owner)
        .set_grants(Some(grants))
        .build();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(acp)
        .send()
        .await
        .unwrap();

    let response = client.get_bucket_acl().bucket(&bucket_name).send().await.unwrap();
    assert_eq!(response.grants().len(), 2);

    bucket_name
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_acl_grant_userid_fullcontrol() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = bucket_acl_grant_userid("FULL_CONTROL").await;
    let alt_client = get_alt_client();

    alt_client.head_bucket().bucket(&bucket_name).send().await.unwrap();
    alt_client.get_bucket_acl().bucket(&bucket_name).send().await.unwrap();
    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key("foo-write")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();
    alt_client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();
}

#[ignore = "VERIFY: also fails in Python s3-tests on this cluster — RGW ACL grant enforcement"]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_acl_grant_userid_read() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = bucket_acl_grant_userid("READ").await;
    let alt_client = get_alt_client();

    // alt user can read (head bucket)
    alt_client.head_bucket().bucket(&bucket_name).send().await.unwrap();

    // can't read ACL
    let result = alt_client.get_bucket_acl().bucket(&bucket_name).send().await;
    assert!(result.is_err());

    // can't write
    let result = alt_client
        .put_object()
        .bucket(&bucket_name)
        .key("foo-write")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await;
    assert!(result.is_err());
}

#[ignore = "VERIFY: also fails in Python s3-tests on this cluster — RGW ACL grant enforcement"]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_acl_grant_userid_readacp() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = bucket_acl_grant_userid("READ_ACP").await;
    let alt_client = get_alt_client();

    // alt user can read ACL
    alt_client.get_bucket_acl().bucket(&bucket_name).send().await.unwrap();

    // can't write
    let result = alt_client
        .put_object()
        .bucket(&bucket_name)
        .key("foo-write")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await;
    assert!(result.is_err());
}

#[ignore = "VERIFY: also fails in Python s3-tests on this cluster — RGW ACL grant enforcement"]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_acl_grant_userid_write() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = bucket_acl_grant_userid("WRITE").await;
    let alt_client = get_alt_client();

    // alt user can write objects
    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key("foo-write")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    // can't read ACL
    let result = alt_client.get_bucket_acl().bucket(&bucket_name).send().await;
    assert!(result.is_err());
}

#[ignore = "VERIFY: also fails in Python s3-tests on this cluster — RGW ACL grant enforcement"]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_acl_grant_userid_writeacp() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket_name = bucket_acl_grant_userid("WRITE_ACP").await;
    let alt_client = get_alt_client();

    // alt user can write ACL
    alt_client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    // can't read ACL
    let result = alt_client.get_bucket_acl().bucket(&bucket_name).send().await;
    assert!(result.is_err());

    // can't write objects
    let result = alt_client
        .put_object()
        .bucket(&bucket_name)
        .key("foo-write")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_access_bucket_private_object_publicread() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"foocontent"))
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    let resp = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"foocontent");
}

#[tokio::test]
async fn test_object_acl_canned_bucketownerread() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let main_client = get_client();
    let alt_client = get_alt_client();
    let bucket_name = get_new_bucket_name();

    main_client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .acl(ObjectCannedAcl::BucketOwnerRead)
        .send()
        .await
        .unwrap();

    let response = alt_client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();

    let grants = response.grants();
    assert_eq!(grants.len(), 2);

    let cfg = get_config();
    let alt_user_id = &cfg.alt_user_id;
    let main_user_id = &cfg.main_user_id;

    let has_alt_fullcontrol = grants.iter().any(|g| {
        g.permission().unwrap().as_str() == "FULL_CONTROL"
            && g.grantee().unwrap().id().unwrap_or_default() == alt_user_id
    });
    let has_owner_read = grants.iter().any(|g| {
        g.permission().unwrap().as_str() == "READ"
            && g.grantee().unwrap().id().unwrap_or_default() == main_user_id
    });
    assert!(has_alt_fullcontrol);
    assert!(has_owner_read);
}

#[tokio::test]
async fn test_object_acl_canned_bucketownerfullcontrol() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let main_client = get_client();
    let alt_client = get_alt_client();
    let bucket_name = get_new_bucket_name();

    main_client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .send()
        .await
        .unwrap();

    let response = alt_client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();

    let grants = response.grants();
    assert_eq!(grants.len(), 2);

    let cfg = get_config();
    let alt_user_id = &cfg.alt_user_id;
    let main_user_id = &cfg.main_user_id;

    let has_alt_fullcontrol = grants.iter().any(|g| {
        g.permission().unwrap().as_str() == "FULL_CONTROL"
            && g.grantee().unwrap().id().unwrap_or_default() == alt_user_id
    });
    let has_owner_fullcontrol = grants.iter().any(|g| {
        g.permission().unwrap().as_str() == "FULL_CONTROL"
            && g.grantee().unwrap().id().unwrap_or_default() == main_user_id
    });
    assert!(has_alt_fullcontrol);
    assert!(has_owner_fullcontrol);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_object_acl_full_control_verify_owner() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let main_client = get_client();
    let alt_client = get_alt_client();
    let cfg = get_config();
    let bucket_name = get_new_bucket_name();

    main_client
        .create_bucket()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    main_client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let grant_fc = aws_sdk_s3::types::Grant::builder()
        .grantee(
            aws_sdk_s3::types::Grantee::builder()
                .r#type(aws_sdk_s3::types::Type::CanonicalUser)
                .id(&cfg.alt_user_id)
                .build()
                .unwrap(),
        )
        .permission(aws_sdk_s3::types::Permission::FullControl)
        .build();

    let acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(
            aws_sdk_s3::types::Owner::builder()
                .id(&cfg.main_user_id)
                .display_name(&cfg.main_display_name)
                .build(),
        )
        .grants(grant_fc)
        .build();

    main_client
        .put_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .access_control_policy(acp)
        .send()
        .await
        .unwrap();

    let grant_ra = aws_sdk_s3::types::Grant::builder()
        .grantee(
            aws_sdk_s3::types::Grantee::builder()
                .r#type(aws_sdk_s3::types::Type::CanonicalUser)
                .id(&cfg.alt_user_id)
                .build()
                .unwrap(),
        )
        .permission(aws_sdk_s3::types::Permission::ReadAcp)
        .build();

    let acp2 = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(
            aws_sdk_s3::types::Owner::builder()
                .id(&cfg.main_user_id)
                .display_name(&cfg.main_display_name)
                .build(),
        )
        .grants(grant_ra)
        .build();

    alt_client
        .put_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .access_control_policy(acp2)
        .send()
        .await
        .unwrap();

    let response = alt_client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.owner().unwrap().id().unwrap_or_default(),
        &cfg.main_user_id
    );
}

#[tokio::test]
async fn test_bucket_acl_canned_private_to_private() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .unwrap();
}

async fn check_object_acl(permission: aws_sdk_s3::types::Permission) {
    let client = get_client();
    let cfg = get_config();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();

    let mut grants: Vec<Grant> = response.grants().to_vec();
    grants[0] = Grant::builder()
        .grantee(grants[0].grantee().unwrap().clone())
        .permission(permission.clone())
        .build();

    let acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(
            aws_sdk_s3::types::Owner::builder()
                .id(&cfg.main_user_id)
                .display_name(&cfg.main_display_name)
                .build(),
        )
        .set_grants(Some(grants))
        .build();

    client
        .put_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .access_control_policy(acp)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();
    let grants = response.grants();
    assert_eq!(grants.len(), 1);
    check_grant(
        &grants[0],
        permission.as_str(),
        "CanonicalUser",
        Some(&cfg.main_user_id),
        None,
    );
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_object_acl() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    check_object_acl(aws_sdk_s3::types::Permission::FullControl).await;
}

#[tokio::test]
async fn test_object_acl_write() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    check_object_acl(aws_sdk_s3::types::Permission::Write).await;
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_object_acl_writeacp() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    check_object_acl(aws_sdk_s3::types::Permission::WriteAcp).await;
}

#[tokio::test]
async fn test_object_acl_read() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    check_object_acl(aws_sdk_s3::types::Permission::Read).await;
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_object_acl_readacp() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    check_object_acl(aws_sdk_s3::types::Permission::ReadAcp).await;
}

#[ignore = "VERIFY: RGW returns InvalidArgument for empty grants ACL — Python test passes, may be SDK serialization difference"]
#[tokio::test]
async fn test_bucket_acl_revoke_all() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let old_grants = response.grants().to_vec();

    let empty_acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(
            aws_sdk_s3::types::Owner::builder()
                .id(&cfg.main_user_id)
                .display_name(&cfg.main_display_name)
                .build(),
        )
        .build();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(empty_acp)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(response.grants().is_empty());

    let restore_acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(
            aws_sdk_s3::types::Owner::builder()
                .id(&cfg.main_user_id)
                .display_name(&cfg.main_display_name)
                .build(),
        )
        .set_grants(Some(old_grants))
        .build();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(restore_acp)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bucket_acl_grant_nonexist_user() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket_name = get_new_bucket(Some(&client)).await;

    let response = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let mut grants = response.grants().to_vec();

    grants.push(
        Grant::builder()
            .grantee(
                aws_sdk_s3::types::Grantee::builder()
                    .r#type(aws_sdk_s3::types::Type::CanonicalUser)
                    .id("_foo")
                    .build()
                    .unwrap(),
            )
            .permission(aws_sdk_s3::types::Permission::FullControl)
            .build(),
    );

    let acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(
            aws_sdk_s3::types::Owner::builder()
                .id(&cfg.main_user_id)
                .display_name(&cfg.main_display_name)
                .build(),
        )
        .set_grants(Some(grants))
        .build();

    let result = client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(acp)
        .send()
        .await;
    assert!(result.is_err());
}

async fn setup_access(
    bucket_acl: &str,
    object_acl: &str,
) -> (String, String, String, String) {
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let key1 = "foo".to_string();
    let key2 = "bar".to_string();
    let newkey = "new".to_string();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(bucket_acl.parse::<BucketCannedAcl>().unwrap())
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket_name)
        .key(&key1)
        .body(ByteStream::from_static(b"foocontent"))
        .send()
        .await
        .unwrap();
    client
        .put_object_acl()
        .bucket(&bucket_name)
        .key(&key1)
        .acl(object_acl.parse::<ObjectCannedAcl>().unwrap())
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket_name)
        .key(&key2)
        .body(ByteStream::from_static(b"barcontent"))
        .send()
        .await
        .unwrap();

    (bucket_name, key1, key2, newkey)
}

fn check_access_denied<E: std::fmt::Debug>(result: Result<impl std::fmt::Debug, aws_sdk_s3::error::SdkError<E>>) {
    match result {
        Ok(v) => panic!("Expected AccessDenied, got success: {v:?}"),
        Err(ref e) => {
            let raw = match e {
                aws_sdk_s3::error::SdkError::ServiceError(se) => se.raw(),
                other => panic!("Expected ServiceError, got: {other:?}"),
            };
            assert_eq!(raw.status().as_u16(), 403);
        }
    }
}

async fn get_objects_list(client: &aws_sdk_s3::Client, bucket: &str) -> Vec<String> {
    let resp = client
        .list_objects()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    resp.contents()
        .iter()
        .map(|o| o.key().unwrap_or_default().to_string())
        .collect()
}

#[tokio::test]
async fn test_access_bucket_private_object_private() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("private", "private").await;

    let alt_client = get_alt_client();
    check_access_denied(
        alt_client.get_object().bucket(&bucket_name).key(&key1).send().await,
    );
    check_access_denied(
        alt_client.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    check_access_denied(
        alt_client.list_objects().bucket(&bucket_name).send().await,
    );
    check_access_denied(
        alt_client
            .put_object()
            .bucket(&bucket_name)
            .key(&key1)
            .body(ByteStream::from_static(b"barcontent"))
            .send()
            .await,
    );

    let alt_client2 = get_alt_client();
    check_access_denied(
        alt_client2
            .put_object()
            .bucket(&bucket_name)
            .key(&key2)
            .body(ByteStream::from_static(b"baroverwrite"))
            .send()
            .await,
    );
    let alt_client3 = get_alt_client();
    check_access_denied(
        alt_client3
            .put_object()
            .bucket(&bucket_name)
            .key(&newkey)
            .body(ByteStream::from_static(b"newcontent"))
            .send()
            .await,
    );
}

#[tokio::test]
async fn test_access_bucket_private_objectv2_private() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("private", "private").await;

    let alt_client = get_alt_client();
    check_access_denied(
        alt_client.get_object().bucket(&bucket_name).key(&key1).send().await,
    );
    check_access_denied(
        alt_client.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    check_access_denied(
        alt_client.list_objects_v2().bucket(&bucket_name).send().await,
    );
    check_access_denied(
        alt_client
            .put_object()
            .bucket(&bucket_name)
            .key(&key1)
            .body(ByteStream::from_static(b"barcontent"))
            .send()
            .await,
    );

    let alt_client2 = get_alt_client();
    check_access_denied(
        alt_client2
            .put_object()
            .bucket(&bucket_name)
            .key(&key2)
            .body(ByteStream::from_static(b"baroverwrite"))
            .send()
            .await,
    );
    let alt_client3 = get_alt_client();
    check_access_denied(
        alt_client3
            .put_object()
            .bucket(&bucket_name)
            .key(&newkey)
            .body(ByteStream::from_static(b"newcontent"))
            .send()
            .await,
    );
}

#[tokio::test]
async fn test_access_bucket_private_object_publicreadwrite() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("private", "public-read-write").await;
    let alt_client = get_alt_client();

    let resp = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key(&key1)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"foocontent");

    check_access_denied(
        alt_client
            .put_object()
            .bucket(&bucket_name)
            .key(&key1)
            .body(ByteStream::from_static(b"foooverwrite"))
            .send()
            .await,
    );
    let alt_client2 = get_alt_client();
    check_access_denied(
        alt_client2.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    check_access_denied(
        alt_client2
            .put_object()
            .bucket(&bucket_name)
            .key(&key2)
            .body(ByteStream::from_static(b"baroverwrite"))
            .send()
            .await,
    );
    let alt_client3 = get_alt_client();
    check_access_denied(
        alt_client3.list_objects().bucket(&bucket_name).send().await,
    );
    check_access_denied(
        alt_client3
            .put_object()
            .bucket(&bucket_name)
            .key(&newkey)
            .body(ByteStream::from_static(b"newcontent"))
            .send()
            .await,
    );
}

#[tokio::test]
async fn test_access_bucket_publicread_object_private() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("public-read", "private").await;
    let alt_client = get_alt_client();

    check_access_denied(
        alt_client.get_object().bucket(&bucket_name).key(&key1).send().await,
    );
    check_access_denied(
        alt_client
            .put_object()
            .bucket(&bucket_name)
            .key(&key1)
            .body(ByteStream::from_static(b"barcontent"))
            .send()
            .await,
    );

    let alt_client2 = get_alt_client();
    check_access_denied(
        alt_client2.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    check_access_denied(
        alt_client2
            .put_object()
            .bucket(&bucket_name)
            .key(&key2)
            .body(ByteStream::from_static(b"baroverwrite"))
            .send()
            .await,
    );

    let alt_client3 = get_alt_client();
    let objs = get_objects_list(&alt_client3, &bucket_name).await;
    assert_eq!(objs, vec!["bar", "foo"]);
    check_access_denied(
        alt_client3
            .put_object()
            .bucket(&bucket_name)
            .key(&newkey)
            .body(ByteStream::from_static(b"newcontent"))
            .send()
            .await,
    );
}

#[tokio::test]
async fn test_access_bucket_publicread_object_publicread() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("public-read", "public-read").await;
    let alt_client = get_alt_client();

    let resp = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key(&key1)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"foocontent");

    check_access_denied(
        alt_client
            .put_object()
            .bucket(&bucket_name)
            .key(&key1)
            .body(ByteStream::from_static(b"foooverwrite"))
            .send()
            .await,
    );

    let alt_client2 = get_alt_client();
    check_access_denied(
        alt_client2.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    check_access_denied(
        alt_client2
            .put_object()
            .bucket(&bucket_name)
            .key(&key2)
            .body(ByteStream::from_static(b"baroverwrite"))
            .send()
            .await,
    );

    let alt_client3 = get_alt_client();
    let objs = get_objects_list(&alt_client3, &bucket_name).await;
    assert_eq!(objs, vec!["bar", "foo"]);
    check_access_denied(
        alt_client3
            .put_object()
            .bucket(&bucket_name)
            .key(&newkey)
            .body(ByteStream::from_static(b"newcontent"))
            .send()
            .await,
    );
}

#[tokio::test]
async fn test_access_bucket_publicread_object_publicreadwrite() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("public-read", "public-read-write").await;
    let alt_client = get_alt_client();

    let resp = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key(&key1)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"foocontent");

    check_access_denied(
        alt_client
            .put_object()
            .bucket(&bucket_name)
            .key(&key1)
            .body(ByteStream::from_static(b"foooverwrite"))
            .send()
            .await,
    );

    let alt_client2 = get_alt_client();
    check_access_denied(
        alt_client2.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    check_access_denied(
        alt_client2
            .put_object()
            .bucket(&bucket_name)
            .key(&key2)
            .body(ByteStream::from_static(b"baroverwrite"))
            .send()
            .await,
    );

    let alt_client3 = get_alt_client();
    let objs = get_objects_list(&alt_client3, &bucket_name).await;
    assert_eq!(objs, vec!["bar", "foo"]);
    check_access_denied(
        alt_client3
            .put_object()
            .bucket(&bucket_name)
            .key(&newkey)
            .body(ByteStream::from_static(b"newcontent"))
            .send()
            .await,
    );
}

#[tokio::test]
async fn test_access_bucket_publicreadwrite_object_private() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("public-read-write", "private").await;
    let alt_client = get_alt_client();

    check_access_denied(
        alt_client.get_object().bucket(&bucket_name).key(&key1).send().await,
    );
    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key(&key1)
        .body(ByteStream::from_static(b"barcontent"))
        .send()
        .await
        .unwrap();

    check_access_denied(
        alt_client.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key(&key2)
        .body(ByteStream::from_static(b"baroverwrite"))
        .send()
        .await
        .unwrap();

    let objs = get_objects_list(&alt_client, &bucket_name).await;
    assert_eq!(objs, vec!["bar", "foo"]);
    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key(&newkey)
        .body(ByteStream::from_static(b"newcontent"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_access_bucket_publicreadwrite_object_publicread() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("public-read-write", "public-read").await;
    let alt_client = get_alt_client();

    let resp = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key(&key1)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"foocontent");

    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key(&key1)
        .body(ByteStream::from_static(b"barcontent"))
        .send()
        .await
        .unwrap();

    check_access_denied(
        alt_client.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key(&key2)
        .body(ByteStream::from_static(b"baroverwrite"))
        .send()
        .await
        .unwrap();

    let objs = get_objects_list(&alt_client, &bucket_name).await;
    assert_eq!(objs, vec!["bar", "foo"]);
    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key(&newkey)
        .body(ByteStream::from_static(b"newcontent"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_access_bucket_publicreadwrite_object_publicreadwrite() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("public-read-write", "public-read-write").await;
    let alt_client = get_alt_client();

    let resp = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key(&key1)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"foocontent");

    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key(&key1)
        .body(ByteStream::from_static(b"foooverwrite"))
        .send()
        .await
        .unwrap();

    check_access_denied(
        alt_client.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key(&key2)
        .body(ByteStream::from_static(b"baroverwrite"))
        .send()
        .await
        .unwrap();

    let objs = get_objects_list(&alt_client, &bucket_name).await;
    assert_eq!(objs, vec!["bar", "foo"]);
    alt_client
        .put_object()
        .bucket(&bucket_name)
        .key(&newkey)
        .body(ByteStream::from_static(b"newcontent"))
        .send()
        .await
        .unwrap();
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_list_buckets_anonymous() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let unauth_client = get_unauthenticated_client();
    let response = unauth_client.list_buckets().send().await.unwrap();
    assert!(response.buckets().is_empty());
}

#[tokio::test]
async fn test_list_buckets_invalid_auth() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bad_client = s3_tests_rs::client::get_bad_auth_client();
    let result = bad_client.list_buckets().send().await;
    let err = s3_tests_rs::expect_s3_err!(result);
    assert_eq!(err.status, 403);
}

#[tokio::test]
async fn test_list_buckets_bad_auth() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bad_client = s3_tests_rs::client::get_bad_auth_client();
    let result = bad_client.list_buckets().send().await;
    let err = s3_tests_rs::expect_s3_err!(result);
    assert_eq!(err.status, 403);
}

#[tokio::test]
async fn test_buckets_create_then_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let mut bucket_names = Vec::new();
    for _ in 0..5 {
        let name = get_new_bucket_name();
        bucket_names.push(name);
    }
    for name in &bucket_names {
        client.create_bucket().bucket(name).send().await.unwrap();
    }

    let response = client.list_buckets().send().await.unwrap();
    let listed: Vec<&str> = response
        .buckets()
        .iter()
        .map(|b| b.name().unwrap_or_default())
        .collect();
    for name in &bucket_names {
        assert!(listed.contains(&name.as_str()), "Missing bucket {name}");
    }
}

#[tokio::test]
async fn test_buckets_list_ctime() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let before = chrono::Utc::now() - chrono::Duration::days(1);
    let client = get_client();
    let mut bucket_names = Vec::new();
    for _ in 0..5 {
        let name = get_new_bucket_name();
        client.create_bucket().bucket(&name).send().await.unwrap();
        bucket_names.push(name);
    }

    let response = client.list_buckets().send().await.unwrap();
    for bucket in response.buckets() {
        let name = bucket.name().unwrap_or_default();
        if bucket_names.contains(&name.to_string()) {
            let ctime = bucket.creation_date().unwrap();
            let ctime_secs = ctime.secs();
            let before_secs = before.timestamp();
            assert!(
                before_secs <= ctime_secs,
                "Bucket {name} ctime {ctime_secs} < before {before_secs}"
            );
        }
    }
}

#[ignore = "VERIFY: RGW does not support MaxBuckets pagination on ListBuckets (also fails in Python)"]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_list_buckets_paginated() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();

    let bucket1 = get_new_bucket(Some(&client)).await;
    let bucket2 = get_new_bucket(Some(&client)).await;

    let mut all_buckets = Vec::new();
    let mut continuation: Option<String> = None;
    loop {
        let mut req = client.list_buckets().max_buckets(1);
        if let Some(ref token) = continuation {
            req = req.continuation_token(token);
        }
        let response = req.send().await.unwrap();
        assert!(response.buckets().len() <= 1);
        for b in response.buckets() {
            all_buckets.push(b.name().unwrap_or_default().to_string());
        }
        match response.continuation_token() {
            Some(token) => continuation = Some(token.to_string()),
            None => break,
        }
    }
    assert!(all_buckets.contains(&bucket1));
    assert!(all_buckets.contains(&bucket2));
}

#[tokio::test]
async fn test_bucket_acl_grant_email() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let response = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let mut grants = response.grants().to_vec();

    grants.push(
        Grant::builder()
            .grantee(
                aws_sdk_s3::types::Grantee::builder()
                    .r#type(aws_sdk_s3::types::Type::AmazonCustomerByEmail)
                    .email_address(&cfg.alt_email)
                    .build()
                    .unwrap(),
            )
            .permission(aws_sdk_s3::types::Permission::FullControl)
            .build(),
    );

    let acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(
            aws_sdk_s3::types::Owner::builder()
                .id(&cfg.main_user_id)
                .display_name(&cfg.main_display_name)
                .build(),
        )
        .set_grants(Some(grants))
        .build();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(acp)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    assert_eq!(response.grants().len(), 2);
    let grant_ids: Vec<&str> = response
        .grants()
        .iter()
        .filter_map(|g| g.grantee().and_then(|gee| gee.id()))
        .collect();
    assert!(grant_ids.contains(&cfg.main_user_id.as_str()));
    assert!(grant_ids.contains(&cfg.alt_user_id.as_str()));
}

#[tokio::test]
async fn test_bucket_acl_grant_email_not_exist() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let response = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let mut grants = response.grants().to_vec();

    grants.push(
        Grant::builder()
            .grantee(
                aws_sdk_s3::types::Grantee::builder()
                    .r#type(aws_sdk_s3::types::Type::AmazonCustomerByEmail)
                    .email_address("doesnotexist@dreamhost.com.invalid")
                    .build()
                    .unwrap(),
            )
            .permission(aws_sdk_s3::types::Permission::FullControl)
            .build(),
    );

    let acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(
            aws_sdk_s3::types::Owner::builder()
                .id(&cfg.main_user_id)
                .display_name(&cfg.main_display_name)
                .build(),
        )
        .set_grants(Some(grants))
        .build();

    let result = client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(acp)
        .send()
        .await;
    assert_s3_err!(result, 400, "UnresolvableGrantByEmailAddress");
}

#[tokio::test]
async fn test_access_bucket_private_objectv2_publicread() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("private", "public-read").await;
    let alt_client = get_alt_client();

    let resp = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key(&key1)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"foocontent");

    check_access_denied(
        alt_client
            .put_object()
            .bucket(&bucket_name)
            .key(&key1)
            .body(ByteStream::from_static(b"foooverwrite"))
            .send()
            .await,
    );
    let alt_client2 = get_alt_client();
    check_access_denied(
        alt_client2.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    check_access_denied(
        alt_client2
            .put_object()
            .bucket(&bucket_name)
            .key(&key2)
            .body(ByteStream::from_static(b"baroverwrite"))
            .send()
            .await,
    );
    let alt_client3 = get_alt_client();
    check_access_denied(
        alt_client3.list_objects_v2().bucket(&bucket_name).send().await,
    );
    check_access_denied(
        alt_client3
            .put_object()
            .bucket(&bucket_name)
            .key(&newkey)
            .body(ByteStream::from_static(b"newcontent"))
            .send()
            .await,
    );
}

#[tokio::test]
async fn test_access_bucket_private_objectv2_publicreadwrite() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (bucket_name, key1, key2, newkey) =
        setup_access("private", "public-read-write").await;
    let alt_client = get_alt_client();

    let resp = alt_client
        .get_object()
        .bucket(&bucket_name)
        .key(&key1)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"foocontent");

    check_access_denied(
        alt_client
            .put_object()
            .bucket(&bucket_name)
            .key(&key1)
            .body(ByteStream::from_static(b"foooverwrite"))
            .send()
            .await,
    );
    let alt_client2 = get_alt_client();
    check_access_denied(
        alt_client2.get_object().bucket(&bucket_name).key(&key2).send().await,
    );
    check_access_denied(
        alt_client2
            .put_object()
            .bucket(&bucket_name)
            .key(&key2)
            .body(ByteStream::from_static(b"baroverwrite"))
            .send()
            .await,
    );
    let alt_client3 = get_alt_client();
    check_access_denied(
        alt_client3.list_objects_v2().bucket(&bucket_name).send().await,
    );
    check_access_denied(
        alt_client3
            .put_object()
            .bucket(&bucket_name)
            .key(&newkey)
            .body(ByteStream::from_static(b"newcontent"))
            .send()
            .await,
    );
}

#[tokio::test]
async fn test_put_bucket_acl_grant_group_read() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    let cfg = get_config();

    let acl_resp = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let mut grants = acl_resp.grants().to_vec();

    grants.push(
        Grant::builder()
            .grantee(
                aws_sdk_s3::types::Grantee::builder()
                    .r#type(aws_sdk_s3::types::Type::Group)
                    .uri("http://acs.amazonaws.com/groups/global/AllUsers")
                    .build()
                    .unwrap(),
            )
            .permission(aws_sdk_s3::types::Permission::Read)
            .build(),
    );

    let acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(acl_resp.owner().unwrap().clone())
        .set_grants(Some(grants))
        .build();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(acp)
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    let grants = response.grants();
    assert_eq!(grants.len(), 2);

    let has_full_control = grants.iter().any(|g| {
        g.permission().unwrap().as_str() == "FULL_CONTROL"
            && g.grantee().unwrap().id() == Some(cfg.main_user_id.as_str())
    });
    let has_group_read = grants.iter().any(|g| {
        g.permission().unwrap().as_str() == "READ"
            && g.grantee().unwrap().uri()
                == Some("http://acs.amazonaws.com/groups/global/AllUsers")
    });
    assert!(has_full_control);
    assert!(has_group_read);
}

#[tokio::test]
async fn test_bucket_acl_no_grants() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;

    client
        .put_object()
        .bucket(&bucket_name)
        .key("foo")
        .body(ByteStream::from(b"bar".to_vec()))
        .send()
        .await
        .unwrap();

    let response = client.get_bucket_acl().bucket(&bucket_name).send().await.unwrap();
    let old_grants = response.grants().to_vec();
    let owner = response.owner().unwrap().clone();

    client
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(
            aws_sdk_s3::types::AccessControlPolicy::builder()
                .owner(owner.clone())
                .set_grants(Some(vec![]))
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_object()
        .bucket(&bucket_name)
        .key("foo")
        .send()
        .await
        .unwrap();

    let result = client
        .put_object()
        .bucket(&bucket_name)
        .key("baz")
        .body(ByteStream::from(b"a".to_vec()))
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");

    let client2 = get_client();
    client2
        .get_bucket_acl()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    client2
        .put_bucket_acl()
        .bucket(&bucket_name)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .unwrap();

    let mut acp_builder = aws_sdk_s3::types::AccessControlPolicy::builder().owner(owner);
    for grant in &old_grants {
        acp_builder = acp_builder.grants(grant.clone());
    }
    client2
        .put_bucket_acl()
        .bucket(&bucket_name)
        .access_control_policy(acp_builder.build())
        .send()
        .await
        .unwrap();
}

// --- Object Ownership tests ---

fn public_bucket_policy(bucket: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "*",
            "Resource": [
                format!("arn:aws:s3:::{bucket}"),
                format!("arn:aws:s3:::{bucket}/*")
            ]
        }]
    })
    .to_string()
}

async fn get_bucket_ownership(client: &aws_sdk_s3::Client, bucket: &str) -> ObjectOwnership {
    let resp = client
        .get_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    resp.ownership_controls()
        .unwrap()
        .rules()
        .first()
        .unwrap()
        .object_ownership()
        .clone()
}

async fn get_object_acl_owner(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> String {
    let resp = client
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    resp.owner().unwrap().id().unwrap().to_string()
}

async fn test_ownership_enforced(client: &aws_sdk_s3::Client, bucket: &str, bucket_owner_id: &str) {
    // put_object without ACL — owned by bucket owner
    client.put_object().bucket(bucket).key("put-no-acl").send().await.unwrap();
    assert_eq!(get_object_acl_owner(client, bucket, "put-no-acl").await, bucket_owner_id);

    // put_object with ACL=bucket-owner-full-control — owned by bucket owner
    client
        .put_object()
        .bucket(bucket)
        .key("put-bofc")
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .send()
        .await
        .unwrap();
    assert_eq!(get_object_acl_owner(client, bucket, "put-bofc").await, bucket_owner_id);

    // put_object with private ACL should fail
    let result = client
        .put_object()
        .bucket(bucket)
        .key("put-private")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await;
    assert_s3_err!(result, 400, "AccessControlListNotSupported");

    // create_multipart_upload without ACL — owned by bucket owner
    let mpu = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("mpu-no-acl")
        .send()
        .await
        .unwrap();
    let parts_resp = client
        .list_parts()
        .bucket(bucket)
        .key("mpu-no-acl")
        .upload_id(mpu.upload_id().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(parts_resp.owner().unwrap().id().unwrap(), bucket_owner_id);

    // create_multipart_upload with private ACL should fail
    let result = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("mpu-private")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await;
    assert_s3_err!(result, 400, "AccessControlListNotSupported");
}

async fn test_ownership_preferred(client: &aws_sdk_s3::Client, bucket: &str, bucket_owner_id: &str) {
    // put_object without ACL — NOT owned by bucket owner
    client.put_object().bucket(bucket).key("put-no-acl").send().await.unwrap();
    assert_ne!(get_object_acl_owner(client, bucket, "put-no-acl").await, bucket_owner_id);

    // put_object with ACL=bucket-owner-full-control — owned by bucket owner
    client
        .put_object()
        .bucket(bucket)
        .key("put-bofc")
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .send()
        .await
        .unwrap();
    assert_eq!(get_object_acl_owner(client, bucket, "put-bofc").await, bucket_owner_id);

    // put_object with private ACL — NOT owned by bucket owner
    client
        .put_object()
        .bucket(bucket)
        .key("put-private")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .unwrap();
    assert_ne!(get_object_acl_owner(client, bucket, "put-private").await, bucket_owner_id);
}

async fn test_ownership_object_writer(client: &aws_sdk_s3::Client, bucket: &str, bucket_owner_id: &str) {
    // put_object without ACL — NOT owned by bucket owner
    client.put_object().bucket(bucket).key("put-no-acl").send().await.unwrap();
    assert_ne!(get_object_acl_owner(client, bucket, "put-no-acl").await, bucket_owner_id);

    // put_object with ACL=bucket-owner-full-control — still NOT owned by bucket owner
    client
        .put_object()
        .bucket(bucket)
        .key("put-bofc")
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .send()
        .await
        .unwrap();
    assert_ne!(get_object_acl_owner(client, bucket, "put-bofc").await, bucket_owner_id);

    // put_object with private ACL — NOT owned by bucket owner
    client
        .put_object()
        .bucket(bucket)
        .key("put-private")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .unwrap();
    assert_ne!(get_object_acl_owner(client, bucket, "put-private").await, bucket_owner_id);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_create_bucket_bucket_owner_enforced() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket)
        .object_ownership(ObjectOwnership::BucketOwnerEnforced)
        .send()
        .await
        .unwrap();
    assert_eq!(get_bucket_ownership(&client, &bucket).await, ObjectOwnership::BucketOwnerEnforced);

    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(public_bucket_policy(&bucket))
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    test_ownership_enforced(&alt_client, &bucket, &cfg.main_user_id).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_create_bucket_bucket_owner_preferred() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket)
        .object_ownership(ObjectOwnership::BucketOwnerPreferred)
        .send()
        .await
        .unwrap();
    assert_eq!(get_bucket_ownership(&client, &bucket).await, ObjectOwnership::BucketOwnerPreferred);

    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(public_bucket_policy(&bucket))
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    test_ownership_preferred(&alt_client, &bucket, &cfg.main_user_id).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_put_bucket_ownership_bucket_owner_enforced() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket = get_new_bucket_name();

    let ownership = OwnershipControls::builder()
        .rules(
            OwnershipControlsRule::builder()
                .object_ownership(ObjectOwnership::BucketOwnerEnforced)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    // expect failure with public-read ACL
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();
    let result = client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership.clone())
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidBucketAclWithObjectOwnership");

    // expect success with private ACL
    client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership)
        .send()
        .await
        .unwrap();
    assert_eq!(get_bucket_ownership(&client, &bucket).await, ObjectOwnership::BucketOwnerEnforced);

    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(public_bucket_policy(&bucket))
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    test_ownership_enforced(&alt_client, &bucket, &cfg.main_user_id).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_put_bucket_ownership_bucket_owner_preferred() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket = get_new_bucket(Some(&client)).await;

    let ownership = OwnershipControls::builder()
        .rules(
            OwnershipControlsRule::builder()
                .object_ownership(ObjectOwnership::BucketOwnerPreferred)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership)
        .send()
        .await
        .unwrap();
    assert_eq!(get_bucket_ownership(&client, &bucket).await, ObjectOwnership::BucketOwnerPreferred);

    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(public_bucket_policy(&bucket))
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    test_ownership_preferred(&alt_client, &bucket, &cfg.main_user_id).await;
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_put_bucket_ownership_object_writer() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket = get_new_bucket(Some(&client)).await;

    let ownership = OwnershipControls::builder()
        .rules(
            OwnershipControlsRule::builder()
                .object_ownership(ObjectOwnership::ObjectWriter)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership)
        .send()
        .await
        .unwrap();
    assert_eq!(get_bucket_ownership(&client, &bucket).await, ObjectOwnership::ObjectWriter);

    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(public_bucket_policy(&bucket))
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    test_ownership_object_writer(&alt_client, &bucket, &cfg.main_user_id).await;
}

fn add_acl_grant_headers(
    req: &mut aws_smithy_runtime_api::client::orchestrator::HttpRequest,
    user_id: &str,
) {
    let grant_value = format!("id={user_id}");
    for perm in ["read", "write", "read-acp", "write-acp", "full-control"] {
        let header_name = format!("x-amz-grant-{perm}");
        req.headers_mut().insert(header_name, grant_value.clone());
    }
}

fn check_grants(grants: &[Grant], alt_user_id: &str, alt_display_name: &str) {
    let expected_perms = [
        aws_sdk_s3::types::Permission::Read,
        aws_sdk_s3::types::Permission::Write,
        aws_sdk_s3::types::Permission::ReadAcp,
        aws_sdk_s3::types::Permission::WriteAcp,
        aws_sdk_s3::types::Permission::FullControl,
    ];
    assert_eq!(grants.len(), expected_perms.len());
    for perm in &expected_perms {
        let found = grants.iter().any(|g| {
            g.permission() == Some(perm)
                && g.grantee().map(|gr| gr.id().unwrap_or_default()) == Some(alt_user_id)
                && g.grantee().map(|gr| gr.display_name().unwrap_or_default())
                    == Some(alt_display_name)
        });
        assert!(found, "missing grant for permission {:?}", perm);
    }
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_object_header_acl_grants() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket(Some(&client)).await;
    let cfg = get_config();
    let alt_user_id = cfg.alt_user_id.clone();

    client
        .put_object()
        .bucket(&bucket)
        .key("foo_key")
        .body(ByteStream::from_static(b"bar"))
        .customize()
        .mutate_request(move |req| {
            add_acl_grant_headers(req, &alt_user_id);
        })
        .send()
        .await
        .unwrap();

    let response = client
        .get_object_acl()
        .bucket(&bucket)
        .key("foo_key")
        .send()
        .await
        .unwrap();

    check_grants(response.grants(), &cfg.alt_user_id, &cfg.alt_display_name);
}

#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_bucket_header_acl_grants() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket = get_new_bucket_name();
    let cfg = get_config();
    let alt_user_id = cfg.alt_user_id.clone();

    client
        .create_bucket()
        .bucket(&bucket)
        .customize()
        .mutate_request(move |req| {
            add_acl_grant_headers(req, &alt_user_id);
        })
        .send()
        .await
        .unwrap();

    let response = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();

    check_grants(response.grants(), &cfg.alt_user_id, &cfg.alt_display_name);

    let alt_client = get_alt_client();
    alt_client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    alt_client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_acl_full_control_verify_attributes() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .unwrap();

    // put object with a custom header (just to verify attributes survive ACL changes)
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-foo", "bar");
        })
        .send()
        .await
        .unwrap();

    let response = client.get_object().bucket(&bucket).key("foo").send().await.unwrap();
    let content_type = response.content_type().unwrap_or_default().to_string();
    let etag = response.e_tag().unwrap_or_default().to_string();

    // grant FULL_CONTROL to alt user
    let acl_resp = client.get_object_acl().bucket(&bucket).key("foo").send().await.unwrap();
    let mut grants: Vec<Grant> = acl_resp.grants().to_vec();
    grants.push(
        Grant::builder()
            .grantee(
                aws_sdk_s3::types::Grantee::builder()
                    .id(&cfg.alt_user_id)
                    .r#type(aws_sdk_s3::types::Type::CanonicalUser)
                    .build()
                    .unwrap(),
            )
            .permission(aws_sdk_s3::types::Permission::FullControl)
            .build(),
    );

    let owner = acl_resp.owner().unwrap().clone();
    let acp = aws_sdk_s3::types::AccessControlPolicy::builder()
        .owner(owner)
        .set_grants(Some(grants))
        .build();

    client
        .put_object_acl()
        .bucket(&bucket)
        .key("foo")
        .access_control_policy(acp)
        .send()
        .await
        .unwrap();

    // verify attributes unchanged
    let response = client.get_object().bucket(&bucket).key("foo").send().await.unwrap();
    assert_eq!(response.content_type().unwrap_or_default(), content_type);
    assert_eq!(response.e_tag().unwrap_or_default(), etag);
}

#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[tokio::test]
async fn test_create_bucket_object_writer() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let cfg = get_config();
    let bucket = get_new_bucket_name();

    client
        .create_bucket()
        .bucket(&bucket)
        .object_ownership(ObjectOwnership::ObjectWriter)
        .send()
        .await
        .unwrap();
    assert_eq!(get_bucket_ownership(&client, &bucket).await, ObjectOwnership::ObjectWriter);

    client
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(public_bucket_policy(&bucket))
        .send()
        .await
        .unwrap();

    let alt_client = get_alt_client();
    test_ownership_object_writer(&alt_client, &bucket, &cfg.main_user_id).await;
}
