use s3_tests_rs::client::get_iam_root_client;
use s3_tests_rs::config::get_config;

#[tokio::test]
async fn test_account_oidc_provider() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let url_host = format!("{}example.com", &cfg.iam_path_prefix[1..]);
    let url = format!("http://{}", url_host);

    let create_resp = client
        .create_open_id_connect_provider()
        .url(&url)
        .client_id_list("my-application-id")
        .thumbprint_list("3768084dfb3d2b68b7897bf5f565da8efEXAMPLE")
        .send()
        .await
        .unwrap();
    let arn = create_resp
        .open_id_connect_provider_arn()
        .unwrap()
        .to_string();
    assert!(arn.ends_with(&format!(":oidc-provider/{}", url_host)));

    let list_resp = client
        .list_open_id_connect_providers()
        .send()
        .await
        .unwrap();
    let arns: Vec<&str> = list_resp
        .open_id_connect_provider_list()
        .iter()
        .filter_map(|p| p.arn.as_deref())
        .collect();
    assert!(arns.contains(&arn.as_str()));

    let get_resp = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.url().unwrap(), url);
    assert_eq!(get_resp.client_id_list(), &["my-application-id"]);
    assert_eq!(
        get_resp.thumbprint_list(),
        &["3768084dfb3d2b68b7897bf5f565da8efEXAMPLE"]
    );

    client
        .delete_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();

    let list_resp = client
        .list_open_id_connect_providers()
        .send()
        .await
        .unwrap();
    let arns: Vec<&str> = list_resp
        .open_id_connect_provider_list()
        .iter()
        .filter_map(|p| p.arn.as_deref())
        .collect();
    assert!(!arns.contains(&arn.as_str()));
}

#[tokio::test]
async fn test_verify_add_new_client_id_to_oidc() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let url_host = format!("{}example.com", &cfg.iam_path_prefix[1..]);
    let url = format!("http://{}", url_host);

    let create_resp = client
        .create_open_id_connect_provider()
        .url(&url)
        .client_id_list("app-jee-jsp")
        .thumbprint_list("3768084dfb3d2b68b7897bf5f565da8efEXAMPLE")
        .send()
        .await
        .unwrap();
    let arn = create_resp
        .open_id_connect_provider_arn()
        .unwrap()
        .to_string();

    let get_resp = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.client_id_list().len(), 1);
    assert_eq!(get_resp.client_id_list()[0], "app-jee-jsp");

    client
        .add_client_id_to_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .client_id("app-profile-jsp")
        .send()
        .await
        .unwrap();

    let get_resp = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.client_id_list().len(), 2);

    client
        .delete_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_verify_add_existing_client_id_to_oidc() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let url_host = format!("{}example.com", &cfg.iam_path_prefix[1..]);
    let url = format!("http://{}", url_host);

    let create_resp = client
        .create_open_id_connect_provider()
        .url(&url)
        .client_id_list("app-jee-jsp")
        .client_id_list("app-profile-jsp")
        .thumbprint_list("3768084dfb3d2b68b7897bf5f565da8efEXAMPLE")
        .send()
        .await
        .unwrap();
    let arn = create_resp
        .open_id_connect_provider_arn()
        .unwrap()
        .to_string();

    let get_resp = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.client_id_list().len(), 2);

    client
        .add_client_id_to_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .client_id("app-profile-jsp")
        .send()
        .await
        .unwrap();

    let get_resp = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.client_id_list().len(), 2);

    client
        .delete_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_verify_remove_client_id_from_oidc() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let url_host = format!("{}example.com", &cfg.iam_path_prefix[1..]);
    let url = format!("http://{}", url_host);

    let create_resp = client
        .create_open_id_connect_provider()
        .url(&url)
        .client_id_list("app-jee-jsp")
        .client_id_list("app-profile-jsp")
        .thumbprint_list("3768084dfb3d2b68b7897bf5f565da8efEXAMPLE")
        .send()
        .await
        .unwrap();
    let arn = create_resp
        .open_id_connect_provider_arn()
        .unwrap()
        .to_string();

    let get_resp = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.client_id_list().len(), 2);

    client
        .remove_client_id_from_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .client_id("app-profile-jsp")
        .send()
        .await
        .unwrap();

    let get_resp = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.client_id_list().len(), 1);
    assert_eq!(get_resp.client_id_list()[0], "app-jee-jsp");

    client
        .delete_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_verify_update_thumbprintlist_of_oidc() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_iam_root_client();
    let cfg = get_config();
    let url_host = format!("{}example.com", &cfg.iam_path_prefix[1..]);
    let url = format!("http://{}", url_host);

    let create_resp = client
        .create_open_id_connect_provider()
        .url(&url)
        .client_id_list("app-jee-jsp")
        .thumbprint_list("3768084dfb3d2b68b7897bf5f565da8efEXAMPLE")
        .send()
        .await
        .unwrap();
    let arn = create_resp
        .open_id_connect_provider_arn()
        .unwrap()
        .to_string();

    let get_resp = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_resp.thumbprint_list(),
        &["3768084dfb3d2b68b7897bf5f565da8efEXAMPLE"]
    );

    client
        .update_open_id_connect_provider_thumbprint()
        .open_id_connect_provider_arn(&arn)
        .thumbprint_list("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .thumbprint_list("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        .send()
        .await
        .unwrap();

    let get_resp = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.thumbprint_list().len(), 2);
    assert!(get_resp
        .thumbprint_list()
        .contains(&"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()));
    assert!(get_resp
        .thumbprint_list()
        .contains(&"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()));

    client
        .delete_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
}
