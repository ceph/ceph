use s3_tests_rs::client::{build_s3_client, get_iam_root_client};
use s3_tests_rs::config::get_config;

#[tokio::test]
async fn test_account_group_create() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}G1", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    /* list should be empty initially */
    let resp = client.list_groups().path_prefix(path).send().await.unwrap();
    assert_eq!(resp.groups().len(), 0);

    let resp = client
        .create_group()
        .group_name(&name)
        .path(path)
        .send()
        .await
        .unwrap();
    let group = resp.group().unwrap();
    assert_eq!(group.group_name(), name);
    assert_eq!(group.path(), *path);
    assert!(!group.group_id().is_empty());
    assert!(group.arn().starts_with("arn:aws:iam:"));
    assert!(group.arn().ends_with(&format!(":group{}{}", path, name)));

    /* duplicate must fail */
    let err = client.create_group().group_name(&name).send().await;
    assert!(err.is_err());

    /* get_group returns the same group */
    let resp = client.get_group().group_name(&name).send().await.unwrap();
    assert_eq!(resp.group().unwrap().group_id(), group.group_id());

    /* list shows the group */
    let resp = client.list_groups().path_prefix(path).send().await.unwrap();
    assert_eq!(resp.groups().len(), 1);

    client.delete_group().group_name(&name).send().await.unwrap();

    /* gone after delete */
    let err = client.get_group().group_name(&name).send().await;
    assert!(err.is_err());

    let resp = client.list_groups().path_prefix(path).send().await.unwrap();
    assert_eq!(resp.groups().len(), 0);
}

#[tokio::test]
async fn test_account_group_case_insensitive_name() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name_upper = format!("{}G1", cfg.iam_name_prefix);
    let name_lower = format!("{}g1", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    let resp = client
        .create_group()
        .group_name(&name_upper)
        .path(path)
        .send()
        .await
        .unwrap();
    let created = resp.group().unwrap();

    /* case-insensitive duplicate must fail */
    let err = client.create_group().group_name(&name_lower).send().await;
    assert!(err.is_err());

    /* get by lowercase returns same group */
    let resp = client.get_group().group_name(&name_lower).send().await.unwrap();
    assert_eq!(resp.group().unwrap().group_id(), created.group_id());

    /* delete by lowercase */
    client.delete_group().group_name(&name_lower).send().await.unwrap();

    let err = client.delete_group().group_name(&name_upper).send().await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_account_group_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;

    let resp = client.list_groups().path_prefix(path).send().await.unwrap();
    assert_eq!(resp.groups().len(), 0);

    let name1 = format!("{}aa", cfg.iam_name_prefix);
    let name2 = format!("{}Ab", cfg.iam_name_prefix);
    let name3 = format!("{}ac", cfg.iam_name_prefix);
    let name4 = format!("{}Ad", cfg.iam_name_prefix);

    client.create_group().group_name(&name4).path(&format!("{}w/", path)).send().await.unwrap();
    client.create_group().group_name(&name3).path(&format!("{}x/", path)).send().await.unwrap();
    client.create_group().group_name(&name2).path(&format!("{}y/", path)).send().await.unwrap();
    client.create_group().group_name(&name1).path(&format!("{}z/", path)).send().await.unwrap();

    let resp = client.list_groups().path_prefix(path).send().await.unwrap();
    let names: Vec<&str> = resp.groups().iter().map(|g| g.group_name()).collect();
    assert_eq!(names, vec![&name1, &name2, &name3, &name4]);

    for n in [&name1, &name2, &name3, &name4] {
        client.delete_group().group_name(n).send().await.unwrap();
    }
}

#[tokio::test]
async fn test_account_group_update() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let name = format!("{}GUpd", cfg.iam_name_prefix);
    let username = format!("{}UUpd", cfg.iam_name_prefix);

    let resp = client.create_group().group_name(&name).path(path).send().await.unwrap();
    let group_id = resp.group().unwrap().group_id().to_string();

    client.create_user().user_name(&username).path(path).send().await.unwrap();
    client.add_user_to_group().group_name(&name).user_name(&username).send().await.unwrap();

    let resp = client.list_groups_for_user().user_name(&username).send().await.unwrap();
    assert_eq!(resp.groups().len(), 1);
    assert_eq!(resp.groups()[0].group_name(), name);
    assert_eq!(resp.groups()[0].group_id(), group_id);

    let new_path = format!("{}new/", path);
    let new_name = format!("{}NG1", cfg.iam_name_prefix);
    client
        .update_group()
        .group_name(&name)
        .new_group_name(&new_name)
        .new_path(&new_path)
        .send()
        .await
        .unwrap();

    let resp = client.get_group().group_name(&new_name).send().await.unwrap();
    assert_eq!(resp.group().unwrap().path(), new_path);
    assert_eq!(resp.group().unwrap().group_name(), new_name);
    assert_eq!(resp.group().unwrap().group_id(), group_id);
    assert_eq!(resp.users().len(), 1);
    assert_eq!(resp.users()[0].user_name(), username);

    let resp = client.list_groups_for_user().user_name(&username).send().await.unwrap();
    assert_eq!(resp.groups().len(), 1);
    assert_eq!(resp.groups()[0].group_name(), new_name);

    /* clean up */
    client.remove_user_from_group().group_name(&new_name).user_name(&username).send().await.unwrap();
    client.delete_group().group_name(&new_name).send().await.unwrap();
    client.delete_user().user_name(&username).send().await.unwrap();
}

#[tokio::test]
async fn test_account_inline_group_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}GP", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;
    let policy_name = "List";

    let policy1 = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Deny", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::test"}]
    }).to_string();
    let policy2 = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::test"}]
    }).to_string();

    /* fail on nonexistent group */
    assert!(client.get_group_policy().group_name(&name).policy_name(policy_name).send().await.is_err());
    assert!(client.delete_group_policy().group_name(&name).policy_name(policy_name).send().await.is_err());
    assert!(client.put_group_policy().group_name(&name).policy_name(policy_name).policy_document(&policy1).send().await.is_err());

    client.create_group().group_name(&name).path(path).send().await.unwrap();

    /* fail on nonexistent policy */
    assert!(client.get_group_policy().group_name(&name).policy_name(policy_name).send().await.is_err());
    assert!(client.delete_group_policy().group_name(&name).policy_name(policy_name).send().await.is_err());

    /* put, get, list */
    client.put_group_policy().group_name(&name).policy_name(policy_name).policy_document(&policy1).send().await.unwrap();

    let resp = client.list_group_policies().group_name(&name).send().await.unwrap();
    assert_eq!(resp.policy_names(), &[policy_name]);

    /* overwrite */
    client.put_group_policy().group_name(&name).policy_name(policy_name).policy_document(&policy2).send().await.unwrap();
    let resp = client.list_group_policies().group_name(&name).send().await.unwrap();
    assert_eq!(resp.policy_names(), &[policy_name]);

    /* delete group should fail while policy attached */
    let err = client.delete_group().group_name(&name).send().await;
    assert!(err.is_err());

    /* delete policy then delete group */
    client.delete_group_policy().group_name(&name).policy_name(policy_name).send().await.unwrap();
    assert!(client.get_group_policy().group_name(&name).policy_name(policy_name).send().await.is_err());

    let resp = client.list_group_policies().group_name(&name).send().await.unwrap();
    assert!(resp.policy_names().is_empty());

    client.delete_group().group_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_managed_group_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}GMP", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;
    let policy1 = "arn:aws:iam::aws:policy/AmazonS3FullAccess";
    let policy2 = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess";

    /* fail on nonexistent group */
    assert!(client.attach_group_policy().group_name(&name).policy_arn(policy1).send().await.is_err());
    assert!(client.detach_group_policy().group_name(&name).policy_arn(policy1).send().await.is_err());
    assert!(client.list_attached_group_policies().group_name(&name).send().await.is_err());

    client.create_group().group_name(&name).path(path).send().await.unwrap();

    /* detach unattached fails */
    assert!(client.detach_group_policy().group_name(&name).policy_arn(policy1).send().await.is_err());

    /* attach + idempotent re-attach */
    client.attach_group_policy().group_name(&name).policy_arn(policy1).send().await.unwrap();
    client.attach_group_policy().group_name(&name).policy_arn(policy1).send().await.unwrap();

    let resp = client.list_attached_group_policies().group_name(&name).send().await.unwrap();
    assert_eq!(resp.attached_policies().len(), 1);
    assert_eq!(resp.attached_policies()[0].policy_name(), Some("AmazonS3FullAccess"));

    /* attach second */
    client.attach_group_policy().group_name(&name).policy_arn(policy2).send().await.unwrap();

    let resp = client.list_attached_group_policies().group_name(&name).send().await.unwrap();
    assert_eq!(resp.attached_policies().len(), 2);

    /* detach second */
    client.detach_group_policy().group_name(&name).policy_arn(policy2).send().await.unwrap();
    assert!(client.detach_group_policy().group_name(&name).policy_arn(policy2).send().await.is_err());

    let resp = client.list_attached_group_policies().group_name(&name).send().await.unwrap();
    assert_eq!(resp.attached_policies().len(), 1);

    /* delete group should fail while policy attached */
    let err = client.delete_group().group_name(&name).send().await;
    assert!(err.is_err());

    /* clean up */
    client.detach_group_policy().group_name(&name).policy_arn(policy1).send().await.unwrap();
    client.delete_group().group_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_inline_group_policy_allow() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let username = format!("{}GUsr", cfg.iam_name_prefix);
    let groupname = format!("{}GGrp", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    client.create_user().user_name(&username).path(path).send().await.unwrap();
    let key = client.create_access_key().user_name(&username).send().await.unwrap()
        .access_key().unwrap().clone();
    let user_s3 = build_s3_client(key.access_key_id(), key.secret_access_key());

    client.create_group().group_name(&groupname).path(path).send().await.unwrap();
    client.add_user_to_group().group_name(&groupname).user_name(&username).send().await.unwrap();

    /* user should be denied */
    let err = user_s3.list_buckets().send().await;
    assert!(err.is_err());

    /* add group policy */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
    }).to_string();
    client.put_group_policy().group_name(&groupname).policy_name("AllowStar").policy_document(&policy).send().await.unwrap();

    /* now allowed */
    let resp = user_s3.list_buckets().send().await;
    assert!(resp.is_ok(), "user should be allowed via group policy");

    /* clean up */
    client.delete_group_policy().group_name(&groupname).policy_name("AllowStar").send().await.unwrap();
    client.remove_user_from_group().group_name(&groupname).user_name(&username).send().await.unwrap();
    client.delete_group().group_name(&groupname).send().await.unwrap();
    client.delete_access_key().user_name(&username).access_key_id(key.access_key_id()).send().await.unwrap();
    client.delete_user().user_name(&username).send().await.unwrap();
}

#[tokio::test]
async fn test_account_managed_group_policy_allow() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let username = format!("{}MGUsr", cfg.iam_name_prefix);
    let groupname = format!("{}MGGrp", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    client.create_user().user_name(&username).path(path).send().await.unwrap();
    let key = client.create_access_key().user_name(&username).send().await.unwrap()
        .access_key().unwrap().clone();
    let user_s3 = build_s3_client(key.access_key_id(), key.secret_access_key());

    client.create_group().group_name(&groupname).path(path).send().await.unwrap();
    client.add_user_to_group().group_name(&groupname).user_name(&username).send().await.unwrap();

    /* user should be denied */
    let err = user_s3.list_buckets().send().await;
    assert!(err.is_err());

    /* attach managed policy */
    let policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess";
    client.attach_group_policy().group_name(&groupname).policy_arn(policy_arn).send().await.unwrap();

    /* now allowed */
    let resp = user_s3.list_buckets().send().await;
    assert!(resp.is_ok(), "user should be allowed via managed group policy");

    /* clean up */
    client.detach_group_policy().group_name(&groupname).policy_arn(policy_arn).send().await.unwrap();
    client.remove_user_from_group().group_name(&groupname).user_name(&username).send().await.unwrap();
    client.delete_group().group_name(&groupname).send().await.unwrap();
    client.delete_access_key().user_name(&username).access_key_id(key.access_key_id()).send().await.unwrap();
    client.delete_user().user_name(&username).send().await.unwrap();
}
