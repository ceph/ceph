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
