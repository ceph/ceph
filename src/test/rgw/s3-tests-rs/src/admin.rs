use std::collections::HashMap;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4::SigningParams;
use aws_smithy_runtime_api::client::identity::Identity;

use crate::config::get_config;
use crate::http::RawResponse;

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

pub async fn driver_hint(hint: &str, params: &[(&str, &str)]) -> RawResponse {
    let mut query_parts = vec![format!("hint={hint}")];
    for (k, v) in params {
        query_parts.push(format!("{k}={v}"));
    }
    let query = query_parts.join("&");
    admin_request(reqwest::Method::DELETE, "/admin/driver/hint", &query, None).await
}

/// `PUT /admin/nsfs/adopt?bucket=<name>` -- mark an existing bucket as
/// carrying the nsfs extensions.
///
/// A function point rather than a hint:  it is what an operator runs once
/// against a tree that came from NooBaa, so it has its own resource and
/// its own capability instead of riding the dev-gated hint endpoint.
/// `None` when the deployment does not offer it.
pub async fn nsfs_adopt(bucket: &str) -> Option<HashMap<String, String>> {
    let resp = admin_request(
        reqwest::Method::PUT, "/admin/nsfs/adopt",
        &format!("bucket={bucket}"), None).await;
    if resp.status != 200 {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(&resp.body).ok()?;
    let map = v.as_object()?;
    let mut out = HashMap::new();
    for (k, val) in map {
        let s = match val {
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        out.insert(k.clone(), s);
    }
    Some(out)
}

/// The `results` a hint reported, or `None` if it did not report any.
///
/// A hint can both act and answer -- whether the buffered copy is armed,
/// how many bytes it has moved -- and the answer is what lets a test
/// assert rather than assume.  `None` covers every way the answer can be
/// missing: a non-200, a driver that does not implement the hint, a body
/// that is not a hint document.  It is never the same as an empty map,
/// which is a hint that ran and had nothing to say.
pub async fn driver_hint_results(
    hint: &str,
    params: &[(&str, &str)],
) -> Option<HashMap<String, String>> {
    let resp = driver_hint(hint, params).await;
    if resp.status != 200 {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(&resp.body).ok()?;
    let map = v.get("results")?.as_object()?;
    let mut out = HashMap::new();
    for (k, val) in map {
        let s = match val {
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        out.insert(k.clone(), s);
    }
    Some(out)
}
