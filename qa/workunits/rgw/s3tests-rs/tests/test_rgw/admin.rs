use std::collections::HashMap;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4::SigningParams;
use aws_smithy_runtime_api::client::identity::Identity;

use s3_tests_rs::config::get_config;
use s3_tests_rs::http::RawResponse;

fn build_reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client")
}

pub async fn admin_request(
    method: reqwest::Method,
    path: &str,
    query: &str,
    body: Option<&[u8]>,
) -> RawResponse {
    let cfg = get_config();
    let access_key = &cfg.iam_access_key;
    let secret_key = &cfg.iam_secret_key;

    let proto = if cfg.default_is_secure { "https" } else { "http" };
    let host = format!("{}:{}", cfg.default_host, cfg.default_port);
    let url = if query.is_empty() {
        format!("{proto}://{host}{path}")
    } else {
        format!("{proto}://{host}{path}?{query}")
    };

    let body_bytes = body.unwrap_or(&[]);

    let credentials = Credentials::new(access_key, secret_key, None, None, "s3-tests-rs");
    let identity: Identity = credentials.into();
    let mut settings = SigningSettings::default();
    settings.payload_checksum_kind =
        aws_sigv4::http_request::PayloadChecksumKind::XAmzSha256;
    let signing_params = SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .expect("signing params");

    let signable_body = if body_bytes.is_empty() {
        SignableBody::empty()
    } else {
        SignableBody::Bytes(body_bytes)
    };

    let headers_vec = vec![("host", host.as_str())];
    let signable_request = SignableRequest::new(
        method.as_str(),
        &url,
        headers_vec.into_iter(),
        signable_body,
    )
    .expect("signable request");

    let (instructions, _signature) = sign(signable_request, &signing_params.into())
        .expect("signing")
        .into_parts();

    let http_client = build_reqwest_client();
    let mut req = http_client.request(method, &url);
    req = req.header("host", &host);
    for (name, value) in instructions.headers() {
        req = req.header(name, value);
    }
    if !body_bytes.is_empty() {
        req = req.body(body_bytes.to_vec());
    }

    let resp = req.send().await.expect("admin request failed");
    let status = resp.status().as_u16();
    let mut resp_headers = HashMap::new();
    for (k, v) in resp.headers() {
        resp_headers.insert(
            k.as_str().to_lowercase(),
            v.to_str().unwrap_or_default().to_string(),
        );
    }
    let resp_body = resp.text().await.unwrap_or_default();
    RawResponse {
        status,
        headers: resp_headers,
        body: resp_body,
        final_url: None,
    }
}

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
