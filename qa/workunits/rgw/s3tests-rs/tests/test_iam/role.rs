use s3_tests_rs::client::get_iam_root_client;
use s3_tests_rs::config::get_config;

fn assume_role_policy() -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "sts:AssumeRole"
        }]
    })
    .to_string()
}

#[tokio::test]
async fn test_account_role_create_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let role_name = format!("{}TestRole", cfg.iam_name_prefix);

    let create_resp = client
        .create_role()
        .role_name(&role_name)
        .assume_role_policy_document(assume_role_policy())
        .path(&cfg.iam_path_prefix)
        .description("integration test role")
        .send()
        .await
        .unwrap();

    let role = create_resp.role().unwrap();
    assert_eq!(role.role_name(), role_name);
    assert_eq!(role.path(), cfg.iam_path_prefix);

    client
        .delete_role()
        .role_name(&role_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_account_role_get() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let role_name = format!("{}GetRole", cfg.iam_name_prefix);

    client
        .create_role()
        .role_name(&role_name)
        .assume_role_policy_document(assume_role_policy())
        .path(&cfg.iam_path_prefix)
        .description("get test role")
        .send()
        .await
        .unwrap();

    let get_resp = client
        .get_role()
        .role_name(&role_name)
        .send()
        .await
        .unwrap();
    let got = get_resp.role().unwrap();
    assert_eq!(got.role_name(), role_name);
    assert_eq!(got.description().unwrap_or(""), "get test role");

    client
        .delete_role()
        .role_name(&role_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_account_role_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let role1 = format!("{}ListRole1", cfg.iam_name_prefix);
    let role2 = format!("{}ListRole2", cfg.iam_name_prefix);

    client
        .create_role()
        .role_name(&role1)
        .assume_role_policy_document(assume_role_policy())
        .path(&cfg.iam_path_prefix)
        .send()
        .await
        .unwrap();

    client
        .create_role()
        .role_name(&role2)
        .assume_role_policy_document(assume_role_policy())
        .path(&cfg.iam_path_prefix)
        .send()
        .await
        .unwrap();

    let list_resp = client
        .list_roles()
        .path_prefix(&cfg.iam_path_prefix)
        .send()
        .await
        .unwrap();

    let names: Vec<&str> = list_resp
        .roles()
        .iter()
        .map(|r| r.role_name())
        .collect();
    assert!(names.contains(&role1.as_str()));
    assert!(names.contains(&role2.as_str()));

    client.delete_role().role_name(&role1).send().await.unwrap();
    client.delete_role().role_name(&role2).send().await.unwrap();
}

#[tokio::test]
async fn test_account_role_case_insensitive_name() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name_upper = format!("{}R1", cfg.iam_name_prefix);
    let name_lower = format!("{}r1", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    let resp = client
        .create_role()
        .role_name(&name_upper)
        .path(path)
        .assume_role_policy_document(assume_role_policy())
        .send()
        .await
        .unwrap();
    let rid = resp.role().unwrap().role_id().to_string();

    /* case-insensitive duplicate must fail */
    let err = client
        .create_role()
        .role_name(&name_lower)
        .assume_role_policy_document(assume_role_policy())
        .send()
        .await;
    assert!(err.is_err());

    /* get by lowercase returns same role */
    let resp = client.get_role().role_name(&name_lower).send().await.unwrap();
    assert_eq!(resp.role().unwrap().role_id(), rid);

    /* delete by lowercase */
    client.delete_role().role_name(&name_lower).send().await.unwrap();

    let err = client.get_role().role_name(&name_lower).send().await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_account_role_delete() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}RDel", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    /* delete nonexistent must fail */
    let err = client.delete_role().role_name(&name).send().await;
    assert!(err.is_err());

    let resp = client
        .create_role()
        .role_name(&name)
        .path(path)
        .assume_role_policy_document(assume_role_policy())
        .send()
        .await
        .unwrap();
    let rid = resp.role().unwrap().role_id().to_string();

    client.delete_role().role_name(&name).send().await.unwrap();

    /* re-create produces new role_id */
    let resp = client
        .create_role()
        .role_name(&name)
        .path(path)
        .assume_role_policy_document(assume_role_policy())
        .send()
        .await
        .unwrap();
    assert_ne!(resp.role().unwrap().role_id(), rid);

    client.delete_role().role_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_role_list_sorted() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;

    let resp = client.list_roles().path_prefix(path).send().await.unwrap();
    assert_eq!(resp.roles().len(), 0);

    let name1 = format!("{}aa", cfg.iam_name_prefix);
    let name2 = format!("{}Ab", cfg.iam_name_prefix);
    let name3 = format!("{}ac", cfg.iam_name_prefix);
    let name4 = format!("{}Ad", cfg.iam_name_prefix);

    for (n, p) in [(&name4, "w/"), (&name3, "x/"), (&name2, "y/"), (&name1, "z/")] {
        client.create_role().role_name(n).path(&format!("{}{}", path, p))
            .assume_role_policy_document(assume_role_policy()).send().await.unwrap();
    }

    let resp = client.list_roles().path_prefix(path).send().await.unwrap();
    let names: Vec<&str> = resp.roles().iter().map(|r| r.role_name()).collect();
    assert_eq!(names, vec![&name1, &name2, &name3, &name4]);

    for n in [&name1, &name2, &name3, &name4] {
        client.delete_role().role_name(n).send().await.unwrap();
    }
}

#[tokio::test]
async fn test_account_role_list_path_prefix() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;

    let name1 = format!("{}a", cfg.iam_name_prefix);
    let name2 = format!("{}b", cfg.iam_name_prefix);
    let name3 = format!("{}c", cfg.iam_name_prefix);
    let name4 = format!("{}d", cfg.iam_name_prefix);

    client.create_role().role_name(&name1).path(path)
        .assume_role_policy_document(assume_role_policy()).send().await.unwrap();
    client.create_role().role_name(&name2).path(path)
        .assume_role_policy_document(assume_role_policy()).send().await.unwrap();
    client.create_role().role_name(&name3).path(&format!("{}a/", path))
        .assume_role_policy_document(assume_role_policy()).send().await.unwrap();
    client.create_role().role_name(&name4).path(&format!("{}a/x/", path))
        .assume_role_policy_document(assume_role_policy()).send().await.unwrap();

    let list = |prefix: &str| {
        let c = &client;
        let p = prefix.to_string();
        async move {
            let resp = c.list_roles().path_prefix(&p).send().await.unwrap();
            resp.roles().iter().map(|r| r.role_name().to_string()).collect::<Vec<_>>()
        }
    };

    assert_eq!(list(path).await, vec![name1.clone(), name2.clone(), name3.clone(), name4.clone()]);
    assert_eq!(list(&format!("{}a", path)).await, vec![name3.clone(), name4.clone()]);
    assert_eq!(list(&format!("{}a/x", path)).await, vec![name4.clone()]);
    assert!(list(&format!("{}a/x/d", path)).await.is_empty());

    for n in [&name1, &name2, &name3, &name4] {
        client.delete_role().role_name(n).send().await.unwrap();
    }
}

#[tokio::test]
async fn test_account_role_update() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}RUpd", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;

    /* update nonexistent must fail */
    let err = client.update_role().role_name(&name).send().await;
    assert!(err.is_err());

    client.create_role().role_name(&name).path(path)
        .assume_role_policy_document(assume_role_policy()).send().await.unwrap();

    let resp = client.get_role().role_name(&name).send().await.unwrap();
    let rid = resp.role().unwrap().role_id().to_string();

    let desc = "my role description";
    client.update_role().role_name(&name).description(desc)
        .max_session_duration(43200).send().await.unwrap();

    let resp = client.get_role().role_name(&name).send().await.unwrap();
    let role = resp.role().unwrap();
    assert_eq!(role.role_id(), rid);
    assert_eq!(role.description().unwrap_or(""), desc);
    assert_eq!(role.max_session_duration().unwrap_or(0), 43200);

    client.delete_role().role_name(&name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_role_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let role_name = format!("{}RP", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;
    let policy_name = "MyPolicy";
    let policy2_name = "AnotherPolicy";

    let role_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
    }).to_string();

    /* fail on nonexistent role */
    assert!(client.get_role_policy().role_name(&role_name).policy_name(policy_name).send().await.is_err());
    assert!(client.delete_role_policy().role_name(&role_name).policy_name(policy_name).send().await.is_err());
    assert!(client.put_role_policy().role_name(&role_name).policy_name(policy_name).policy_document(&role_policy).send().await.is_err());

    client.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(assume_role_policy()).send().await.unwrap();

    /* fail on nonexistent policy */
    assert!(client.get_role_policy().role_name(&role_name).policy_name(policy_name).send().await.is_err());
    assert!(client.delete_role_policy().role_name(&role_name).policy_name(policy_name).send().await.is_err());

    /* put + get + list */
    client.put_role_policy().role_name(&role_name).policy_name(policy_name)
        .policy_document(&role_policy).send().await.unwrap();

    let resp = client.list_role_policies().role_name(&role_name).send().await.unwrap();
    assert_eq!(resp.policy_names(), &[policy_name]);

    /* put second policy */
    client.put_role_policy().role_name(&role_name).policy_name(policy2_name)
        .policy_document(&role_policy).send().await.unwrap();

    let resp = client.list_role_policies().role_name(&role_name).send().await.unwrap();
    let names = resp.policy_names();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&policy2_name.to_string()));
    assert!(names.contains(&policy_name.to_string()));

    /* delete both */
    client.delete_role_policy().role_name(&role_name).policy_name(policy_name).send().await.unwrap();
    client.delete_role_policy().role_name(&role_name).policy_name(policy2_name).send().await.unwrap();

    assert!(client.get_role_policy().role_name(&role_name).policy_name(policy_name).send().await.is_err());

    client.delete_role().role_name(&role_name).send().await.unwrap();
}

#[tokio::test]
async fn test_account_role_policy_managed() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}RMP", cfg.iam_name_prefix);
    let path = &cfg.iam_path_prefix;
    let policy1 = "arn:aws:iam::aws:policy/AmazonS3FullAccess";
    let policy2 = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess";

    /* fail on nonexistent role */
    assert!(client.attach_role_policy().role_name(&name).policy_arn(policy1).send().await.is_err());
    assert!(client.detach_role_policy().role_name(&name).policy_arn(policy1).send().await.is_err());
    assert!(client.list_attached_role_policies().role_name(&name).send().await.is_err());

    client.create_role().role_name(&name).path(path)
        .assume_role_policy_document(assume_role_policy()).send().await.unwrap();

    /* detach unattached fails */
    assert!(client.detach_role_policy().role_name(&name).policy_arn(policy1).send().await.is_err());

    /* attach + idempotent re-attach */
    client.attach_role_policy().role_name(&name).policy_arn(policy1).send().await.unwrap();
    client.attach_role_policy().role_name(&name).policy_arn(policy1).send().await.unwrap();

    let resp = client.list_attached_role_policies().role_name(&name).send().await.unwrap();
    assert_eq!(resp.attached_policies().len(), 1);
    assert_eq!(resp.attached_policies()[0].policy_name(), Some("AmazonS3FullAccess"));

    /* attach second */
    client.attach_role_policy().role_name(&name).policy_arn(policy2).send().await.unwrap();

    let resp = client.list_attached_role_policies().role_name(&name).send().await.unwrap();
    assert_eq!(resp.attached_policies().len(), 2);

    /* detach second */
    client.detach_role_policy().role_name(&name).policy_arn(policy2).send().await.unwrap();
    assert!(client.detach_role_policy().role_name(&name).policy_arn(policy2).send().await.is_err());

    let resp = client.list_attached_role_policies().role_name(&name).send().await.unwrap();
    assert_eq!(resp.attached_policies().len(), 1);

    /* delete role should fail while policy attached */
    let err = client.delete_role().role_name(&name).send().await;
    assert!(err.is_err());

    /* clean up */
    client.detach_role_policy().role_name(&name).policy_arn(policy1).send().await.unwrap();
    client.delete_role().role_name(&name).send().await.unwrap();
}
