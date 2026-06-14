use s3_tests_rs::client::get_iam_root_client;
use s3_tests_rs::config::get_config;

#[tokio::test]
async fn test_account_user_smoke() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let name = format!("{}Smoke", cfg.iam_name_prefix);

    let resp = client
        .create_user()
        .user_name(&name)
        .path(&cfg.iam_path_prefix)
        .send()
        .await;
    eprintln!("create_user: {:?}", resp.as_ref().map(|r| r.user().unwrap().user_name()));
    let user = resp.unwrap().user().unwrap().clone();
    assert_eq!(user.user_name(), name);

    let del = client
        .delete_user()
        .user_name(&name)
        .send()
        .await;
    eprintln!("delete_user: {:?}", del.as_ref().map(|_| "ok"));
    del.unwrap();
}
