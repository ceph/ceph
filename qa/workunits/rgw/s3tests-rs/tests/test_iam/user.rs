use aws_sdk_iam::types::StatusType;
use s3_tests_rs::client::get_iam_root_client;
use s3_tests_rs::config::get_config;

#[tokio::test]
async fn test_account_user_create() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name1 = format!("{}U1", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    let resp = client
        .create_user()
        .user_name(&name1)
        .path(path)
        .send()
        .await
        .unwrap();
    let user = resp.user().unwrap();
    assert_eq!(user.user_name(), name1);
    assert_eq!(user.path(), *path);
    assert!(!user.user_id().is_empty());
    assert!(user.arn().starts_with("arn:aws:iam:"));
    assert!(user.arn().ends_with(&format!(":user{}{}", path, name1)));

    /* duplicate name must fail */
    let path2 = format!("{}foo/", path);
    let err = client
        .create_user()
        .user_name(&name1)
        .path(&path2)
        .send()
        .await;
    assert!(err.is_err(), "duplicate create_user should fail");

    /* second user with different name succeeds */
    let name2 = format!("{}U2", cfg.iam_name_prefix);
    let resp2 = client
        .create_user()
        .user_name(&name2)
        .path(&path2)
        .send()
        .await
        .unwrap();
    let user2 = resp2.user().unwrap();
    assert_eq!(user2.user_name(), name2);
    assert_eq!(user2.path(), path2);

    client.delete_user().user_name(&name2).send().await.unwrap();
    client.delete_user().user_name(&name1).send().await.unwrap();
}

#[tokio::test]
async fn test_account_user_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}Del", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    /* delete nonexistent must fail */
    let err = client
        .delete_user()
        .user_name(&name)
        .send()
        .await;
    assert!(err.is_err());

    let resp = client
        .create_user()
        .user_name(&name)
        .path(path)
        .send()
        .await
        .unwrap();
    let uid = resp.user().unwrap().user_id().to_string();

    client.delete_user().user_name(&name).send().await.unwrap();

    /* re-create should produce a new user_id */
    let resp2 = client
        .create_user()
        .user_name(&name)
        .path(path)
        .send()
        .await
        .unwrap();
    let uid2 = resp2.user().unwrap().user_id().to_string();
    assert_ne!(uid, uid2);

    client.delete_user().user_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_user_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;

    let resp = client
        .list_users()
        .path_prefix(path)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.users().len(), 0);
    assert!(!resp.is_truncated());

    let name1 = format!("{}aa", cfg.iam_name_prefix);
    let name2 = format!("{}Ab", cfg.iam_name_prefix);
    let name3 = format!("{}ac", cfg.iam_name_prefix);
    let name4 = format!("{}Ad", cfg.iam_name_prefix);

    client.create_user().user_name(&name4).path(&format!("{}w/", path)).send().await.unwrap();
    client.create_user().user_name(&name3).path(&format!("{}x/", path)).send().await.unwrap();
    client.create_user().user_name(&name2).path(&format!("{}y/", path)).send().await.unwrap();
    client.create_user().user_name(&name1).path(&format!("{}z/", path)).send().await.unwrap();

    /* list should return all four in case-insensitive sorted order */
    let resp = client
        .list_users()
        .path_prefix(path)
        .send()
        .await
        .unwrap();
    let names: Vec<&str> = resp.users().iter().map(|u| u.user_name()).collect();
    assert_eq!(names, vec![&name1, &name2, &name3, &name4]);

    for n in [&name1, &name2, &name3, &name4] {
        client.delete_user().user_name(n).send().await.unwrap();
    }
}

#[tokio::test]
async fn test_account_user_update_name() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let name1 = format!("{}a", cfg.iam_name_prefix);
    let new_name1 = format!("{}z", cfg.iam_name_prefix);

    /* update nonexistent must fail */
    let err = client
        .update_user()
        .user_name(&name1)
        .new_user_name(&new_name1)
        .send()
        .await;
    assert!(err.is_err());

    client.create_user().user_name(&name1).path(path).send().await.unwrap();
    let resp = client.get_user().user_name(&name1).send().await.unwrap();
    let uid = resp.user().unwrap().user_id().to_string();

    client
        .update_user()
        .user_name(&name1)
        .new_user_name(&new_name1)
        .send()
        .await
        .unwrap();

    /* old name gone */
    let err = client.get_user().user_name(&name1).send().await;
    assert!(err.is_err());

    /* new name works, same user_id */
    let resp = client.get_user().user_name(&new_name1).send().await.unwrap();
    assert_eq!(resp.user().unwrap().user_name(), new_name1);
    assert_eq!(resp.user().unwrap().user_id(), uid);

    client.delete_user().user_name(&new_name1).send().await.unwrap();
}

#[tokio::test]
async fn test_account_user_update_path() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let name = format!("{}a", cfg.iam_name_prefix);

    client.create_user().user_name(&name).path(path).send().await.unwrap();
    let resp = client.get_user().user_name(&name).send().await.unwrap();
    let uid = resp.user().unwrap().user_id().to_string();
    assert_eq!(resp.user().unwrap().path(), *path);

    let new_path = format!("{}z/", path);
    client
        .update_user()
        .user_name(&name)
        .new_path(&new_path)
        .send()
        .await
        .unwrap();

    let resp = client.get_user().user_name(&name).send().await.unwrap();
    assert_eq!(resp.user().unwrap().path(), new_path);
    assert_eq!(resp.user().unwrap().user_id(), uid);

    client.delete_user().user_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_user_access_key_create() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}AK", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    /* create key for nonexistent user must fail */
    let err = client.create_access_key().user_name(&name).send().await;
    assert!(err.is_err());

    client.create_user().user_name(&name).path(path).send().await.unwrap();

    let resp = client
        .create_access_key()
        .user_name(&name)
        .send()
        .await
        .unwrap();
    let key = resp.access_key().unwrap();
    assert_eq!(key.user_name(), name);
    assert!(!key.access_key_id().is_empty());
    assert!(!key.secret_access_key().is_empty());
    assert_eq!(key.status(), &StatusType::Active);

    /* clean up */
    client
        .delete_access_key()
        .user_name(&name)
        .access_key_id(key.access_key_id())
        .send()
        .await
        .unwrap();
    client.delete_user().user_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_user_access_key_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}AKDel", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    /* delete key for nonexistent user must fail */
    let err = client
        .delete_access_key()
        .user_name("nosuchuser")
        .access_key_id("abcdefghijklmnopqrstu")
        .send()
        .await;
    assert!(err.is_err());

    client.create_user().user_name(&name).path(path).send().await.unwrap();

    /* delete nonexistent key must fail */
    let err = client
        .delete_access_key()
        .user_name(&name)
        .access_key_id("abcdefghijklmnopqrstu")
        .send()
        .await;
    assert!(err.is_err());

    let resp = client.create_access_key().user_name(&name).send().await.unwrap();
    let keyid = resp.access_key().unwrap().access_key_id().to_string();

    client
        .delete_access_key()
        .user_name(&name)
        .access_key_id(&keyid)
        .send()
        .await
        .unwrap();

    /* double-delete must fail */
    let err = client
        .delete_access_key()
        .user_name(&name)
        .access_key_id(&keyid)
        .send()
        .await;
    assert!(err.is_err());

    /* list should be empty */
    let resp = client.list_access_keys().user_name(&name).send().await.unwrap();
    assert_eq!(resp.access_key_metadata().len(), 0);

    client.delete_user().user_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_user_access_key_update() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}AKUpd", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    /* update key for nonexistent user must fail */
    let err = client
        .update_access_key()
        .user_name("nosuchuser")
        .access_key_id("abcdefghijklmnopqrstu")
        .status(StatusType::Active)
        .send()
        .await;
    assert!(err.is_err());

    client.create_user().user_name(&name).path(path).send().await.unwrap();

    let resp = client.create_access_key().user_name(&name).send().await.unwrap();
    let keyid = resp.access_key().unwrap().access_key_id().to_string();

    /* update nonexistent key must fail */
    let err = client
        .update_access_key()
        .user_name(&name)
        .access_key_id("abcdefghijklmnopqrstu")
        .status(StatusType::Active)
        .send()
        .await;
    assert!(err.is_err());

    client
        .update_access_key()
        .user_name(&name)
        .access_key_id(&keyid)
        .status(StatusType::Inactive)
        .send()
        .await
        .unwrap();

    let resp = client.list_access_keys().user_name(&name).send().await.unwrap();
    let keys = resp.access_key_metadata();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].access_key_id(), Some(keyid.as_str()));
    assert_eq!(keys[0].status(), Some(&StatusType::Inactive));

    /* clean up */
    client
        .delete_access_key()
        .user_name(&name)
        .access_key_id(&keyid)
        .send()
        .await
        .unwrap();
    client.delete_user().user_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_user_access_key_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}AKList", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    /* list keys for nonexistent user must fail */
    let err = client.list_access_keys().user_name("nosuchuser").send().await;
    assert!(err.is_err());

    client.create_user().user_name(&name).path(path).send().await.unwrap();

    let resp = client.list_access_keys().user_name(&name).send().await.unwrap();
    assert_eq!(resp.access_key_metadata().len(), 0);

    let id1 = client
        .create_access_key()
        .user_name(&name)
        .send()
        .await
        .unwrap()
        .access_key()
        .unwrap()
        .access_key_id()
        .to_string();

    let resp = client.list_access_keys().user_name(&name).send().await.unwrap();
    assert_eq!(resp.access_key_metadata().len(), 1);
    assert_eq!(resp.access_key_metadata()[0].access_key_id(), Some(id1.as_str()));

    let id2 = client
        .create_access_key()
        .user_name(&name)
        .send()
        .await
        .unwrap()
        .access_key()
        .unwrap()
        .access_key_id()
        .to_string();

    let resp = client.list_access_keys().user_name(&name).send().await.unwrap();
    let mut ids: Vec<&str> = resp
        .access_key_metadata()
        .iter()
        .map(|k| k.access_key_id().unwrap_or(""))
        .collect();
    ids.sort();
    let mut expected = vec![id1.as_str(), id2.as_str()];
    expected.sort();
    assert_eq!(ids, expected);

    /* clean up */
    client.delete_access_key().user_name(&name).access_key_id(&id1).send().await.unwrap();
    client.delete_access_key().user_name(&name).access_key_id(&id2).send().await.unwrap();
    client.delete_user().user_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_user_case_insensitive_name() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name_upper = format!("{}U1", cfg.iam_name_prefix);
    let name_lower = format!("{}u1", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    let resp = client
        .create_user()
        .user_name(&name_upper)
        .path(path)
        .send()
        .await
        .unwrap();
    let created = resp.user().unwrap();

    /* case-insensitive duplicate must fail */
    let err = client
        .create_user()
        .user_name(&name_lower)
        .send()
        .await;
    assert!(err.is_err(), "case-insensitive duplicate should fail");

    /* get by lowercase should return the uppercase user */
    let resp = client
        .get_user()
        .user_name(&name_lower)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.user().unwrap().user_id(), created.user_id());

    /* delete by lowercase should work */
    client
        .delete_user()
        .user_name(&name_lower)
        .send()
        .await
        .unwrap();

    let err = client.get_user().user_name(&name_lower).send().await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_account_user_list_path_prefix() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;

    let resp = client.list_users().path_prefix(path).send().await.unwrap();
    assert_eq!(resp.users().len(), 0);

    let name1 = format!("{}a", cfg.iam_name_prefix);
    let name2 = format!("{}b", cfg.iam_name_prefix);
    let name3 = format!("{}c", cfg.iam_name_prefix);
    let name4 = format!("{}d", cfg.iam_name_prefix);

    client.create_user().user_name(&name1).path(path).send().await.unwrap();
    client.create_user().user_name(&name2).path(path).send().await.unwrap();
    client.create_user().user_name(&name3).path(&format!("{}a/", path)).send().await.unwrap();
    client.create_user().user_name(&name4).path(&format!("{}a/x/", path)).send().await.unwrap();

    let list_names = |resp: &aws_sdk_iam::operation::list_users::ListUsersOutput| -> Vec<String> {
        resp.users().iter().map(|u| u.user_name().to_string()).collect()
    };

    let resp = client.list_users().path_prefix(path).send().await.unwrap();
    assert_eq!(list_names(&resp), vec![name1.clone(), name2.clone(), name3.clone(), name4.clone()]);

    let resp = client.list_users().path_prefix(&format!("{}a", path)).send().await.unwrap();
    assert_eq!(list_names(&resp), vec![name3.clone(), name4.clone()]);

    let resp = client.list_users().path_prefix(&format!("{}a/x", path)).send().await.unwrap();
    assert_eq!(list_names(&resp), vec![name4.clone()]);

    let resp = client.list_users().path_prefix(&format!("{}a/x/d", path)).send().await.unwrap();
    assert!(list_names(&resp).is_empty());

    for n in [&name1, &name2, &name3, &name4] {
        client.delete_user().user_name(n).send().await.unwrap();
    }
}

#[tokio::test]
async fn test_account_current_user_access_key_create() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();

    /* omit UserName to operate on the current (root) user */
    let resp = client.create_access_key().send().await.unwrap();
    let key = resp.access_key().unwrap();
    let keyid = key.access_key_id().to_string();
    assert!(!keyid.is_empty());
    assert!(!key.secret_access_key().is_empty());
    assert_eq!(key.status(), &StatusType::Active);

    /* clean up */
    client.delete_access_key().access_key_id(&keyid).send().await.unwrap();
}

#[tokio::test]
async fn test_account_current_user_access_key_update() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();

    /* nonexistent key must fail */
    let err = client
        .update_access_key()
        .access_key_id("abcdefghijklmnopqrstu")
        .status(StatusType::Active)
        .send()
        .await;
    assert!(err.is_err());

    let resp = client.create_access_key().send().await.unwrap();
    let keyid = resp.access_key().unwrap().access_key_id().to_string();

    client
        .update_access_key()
        .access_key_id(&keyid)
        .status(StatusType::Inactive)
        .send()
        .await
        .unwrap();

    /* verify status changed via list */
    let resp = client.list_access_keys().send().await.unwrap();
    let found = resp
        .access_key_metadata()
        .iter()
        .find(|k| k.access_key_id() == Some(keyid.as_str()));
    assert!(found.is_some());
    assert_eq!(found.unwrap().status(), Some(&StatusType::Inactive));

    /* clean up */
    client.delete_access_key().access_key_id(&keyid).send().await.unwrap();
}

#[tokio::test]
async fn test_account_current_user_access_key_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();

    /* nonexistent key must fail */
    let err = client
        .delete_access_key()
        .access_key_id("abcdefghijklmnopqrstu")
        .send()
        .await;
    assert!(err.is_err());

    let resp = client.create_access_key().send().await.unwrap();
    let keyid = resp.access_key().unwrap().access_key_id().to_string();

    client.delete_access_key().access_key_id(&keyid).send().await.unwrap();

    /* double-delete must fail */
    let err = client.delete_access_key().access_key_id(&keyid).send().await;
    assert!(err.is_err());

    /* verify it's gone from list */
    let resp = client.list_access_keys().send().await.unwrap();
    let found = resp
        .access_key_metadata()
        .iter()
        .any(|k| k.access_key_id() == Some(keyid.as_str()));
    assert!(!found);
}
