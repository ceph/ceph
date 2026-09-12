pub use s3_tests_rs::admin::admin_request;

pub async fn set_user_quota(uid: &str, max_size: i64, max_objects: i64, enabled: bool) {
    let query = format!(
        "quota&uid={uid}&quota-type=user&max-size={max_size}&max-objects={max_objects}&enabled={enabled}"
    );
    let resp = admin_request(reqwest::Method::PUT, "/admin/user", &query, None).await;
    assert_eq!(resp.status, 200, "set_user_quota failed: {}", resp.body);
}

pub async fn get_user_quota(uid: &str) -> (i64, i64, bool) {
    let query = format!("quota&uid={uid}&quota-type=user");
    let resp = admin_request(reqwest::Method::GET, "/admin/user", &query, None).await;
    assert_eq!(resp.status, 200, "get_user_quota failed: {}", resp.body);
    let v: serde_json::Value = serde_json::from_str(&resp.body)
        .expect("failed to parse quota JSON");
    let max_size = v["max_size"].as_i64().unwrap_or(-1);
    let max_objects = v["max_objects"].as_i64().unwrap_or(-1);
    let enabled = v["enabled"].as_bool().unwrap_or(false);
    (max_size, max_objects, enabled)
}

pub async fn disable_user_quota(uid: &str) {
    set_user_quota(uid, -1, -1, false).await;
}
