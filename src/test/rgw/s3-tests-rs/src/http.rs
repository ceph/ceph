use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use base64::Engine;
use hmac::{Hmac, Mac};
use sha1::Sha1;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4::SigningParams;
use aws_smithy_runtime_api::client::identity::Identity;

use std::fmt;
use std::sync::{Arc, Mutex};

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::{
    AfterDeserializationInterceptorContextRef, BeforeDeserializationInterceptorContextRef,
    BeforeTransmitInterceptorContextRef,
};
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;

use chrono::Utc;

use crate::config::get_config;

pub fn get_endpoint_url(bucket: &str) -> String {
    let cfg = get_config();
    let proto = if cfg.default_is_secure { "https" } else { "http" };
    if cfg.force_path_style {
        format!("{proto}://{}:{}/{bucket}", cfg.default_host, cfg.default_port)
    } else {
        format!("{proto}://{bucket}.{}:{}", cfg.default_host, cfg.default_port)
    }
}

pub fn get_object_url(bucket: &str, key: &str) -> String {
    format!("{}/{key}", get_endpoint_url(bucket))
}

fn build_reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client")
}

pub struct RawResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
    pub final_url: Option<String>,
}

impl RawResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_lowercase()).map(|s| s.as_str())
    }
}

pub async fn raw_request(
    method: reqwest::Method,
    url: &str,
    headers: Option<&HashMap<String, String>>,
) -> RawResponse {
    raw_request_body(method, url, headers, None).await
}

pub async fn raw_request_body(
    method: reqwest::Method,
    url: &str,
    headers: Option<&HashMap<String, String>>,
    body: Option<Vec<u8>>,
) -> RawResponse {
    let client = build_reqwest_client();
    let mut req = client.request(method, url);
    if let Some(hdrs) = headers {
        for (k, v) in hdrs {
            req = req.header(k.as_str(), v.as_str());
        }
    }
    if let Some(b) = body {
        req = req.body(b);
    }
    let resp = req.send().await.expect("raw HTTP request failed");
    let status = resp.status().as_u16();
    let mut resp_headers = HashMap::new();
    for (k, v) in resp.headers() {
        resp_headers.insert(
            k.as_str().to_lowercase(),
            v.to_str().unwrap_or_default().to_string(),
        );
    }
    let body = resp.text().await.unwrap_or_default();
    RawResponse {
        status,
        headers: resp_headers,
        body,
        final_url: None,
    }
}

pub async fn signed_request(
    method: reqwest::Method,
    bucket: &str,
    query: &str,
    body: Option<&[u8]>,
) -> RawResponse {
    signed_request_with_creds(method, bucket, query, body, None).await
}

pub async fn signed_request_with_creds(
    method: reqwest::Method,
    bucket: &str,
    query: &str,
    body: Option<&[u8]>,
    creds: Option<(&str, &str)>,
) -> RawResponse {
    let cfg = get_config();
    let (access_key, secret_key) = creds.unwrap_or((&cfg.main_access_key, &cfg.main_secret_key));

    let url = if query.is_empty() {
        get_endpoint_url(bucket)
    } else {
        format!("{}?{query}", get_endpoint_url(bucket))
    };

    let body_bytes = body.unwrap_or(&[]);
    let host = if cfg.force_path_style {
        format!("{}:{}", cfg.default_host, cfg.default_port)
    } else {
        format!("{bucket}.{}:{}", cfg.default_host, cfg.default_port)
    };

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

    let resp = req.send().await.expect("signed request failed");
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

/// Captures HTTP response headers and body from an SDK call.
///
/// Attach to any SDK operation via `.customize().interceptor(capture.clone())`.
/// After `.send().await`, read the captured data:
///
/// ```ignore
/// let capture = ResponseHeaderCapture::new();
/// let resp = client.get_bucket_logging()
///     .bucket(&bucket)
///     .customize()
///     .interceptor(capture.clone())
///     .send().await?;
/// let last_modified = capture.header("last-modified");
/// let raw_body = capture.body();
/// ```
#[derive(Clone)]
pub struct ResponseHeaderCapture {
    headers: Arc<Mutex<HashMap<String, String>>>,
    body: Arc<Mutex<String>>,
    status: Arc<Mutex<u16>>,
}

impl ResponseHeaderCapture {
    pub fn new() -> Self {
        Self {
            headers: Arc::new(Mutex::new(HashMap::new())),
            body: Arc::new(Mutex::new(String::new())),
            status: Arc::new(Mutex::new(0)),
        }
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .lock()
            .unwrap()
            .get(&name.to_lowercase())
            .cloned()
    }

    pub fn status(&self) -> u16 {
        *self.status.lock().unwrap()
    }

    pub fn body(&self) -> String {
        self.body.lock().unwrap().clone()
    }
}

impl fmt::Debug for ResponseHeaderCapture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseHeaderCapture").finish()
    }
}

impl Intercept for ResponseHeaderCapture {
    fn name(&self) -> &'static str {
        "ResponseHeaderCapture"
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        *self.status.lock().unwrap() = response.status().as_u16();
        let mut hdrs = self.headers.lock().unwrap();
        hdrs.clear();
        for (k, v) in response.headers().iter() {
            hdrs.insert(k.to_lowercase(), v.to_string());
        }
        Ok(())
    }

    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        if let Some(body_bytes) = response.body().bytes() {
            *self.body.lock().unwrap() =
                String::from_utf8_lossy(body_bytes).to_string();
        }
        Ok(())
    }
}

/// Captures outgoing HTTP request headers from an SDK call.
///
/// Attach to any SDK operation via `.customize().interceptor(capture.clone())`.
/// After `.send().await`, read the captured headers:
///
/// ```ignore
/// let capture = RequestHeaderCapture::new();
/// client.put_object()
///     .bucket(&bucket).key("foo").body(body)
///     .customize()
///     .interceptor(capture.clone())
///     .send().await?;
/// let sha256 = capture.header("x-amz-content-sha256");
/// ```
#[derive(Clone)]
pub struct RequestHeaderCapture {
    headers: Arc<Mutex<HashMap<String, String>>>,
}

impl RequestHeaderCapture {
    pub fn new() -> Self {
        Self {
            headers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .lock()
            .unwrap()
            .get(&name.to_lowercase())
            .cloned()
    }
}

impl fmt::Debug for RequestHeaderCapture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestHeaderCapture").finish()
    }
}

impl Intercept for RequestHeaderCapture {
    fn name(&self) -> &'static str {
        "RequestHeaderCapture"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request();
        let mut hdrs = self.headers.lock().unwrap();
        hdrs.clear();
        for (k, v) in request.headers().iter() {
            hdrs.insert(k.to_lowercase(), v.to_string());
        }
        Ok(())
    }
}

pub fn cors_request_and_check(
    resp: &RawResponse,
    expect_status: u16,
    expect_allow_origin: Option<&str>,
    expect_allow_methods: Option<&str>,
) {
    assert_eq!(
        resp.status, expect_status,
        "expected HTTP {expect_status}, got {}",
        resp.status
    );
    match expect_allow_origin {
        Some(origin) => {
            assert_eq!(
                resp.header("access-control-allow-origin"),
                Some(origin),
                "expected Access-Control-Allow-Origin: {origin}"
            );
        }
        None => {
            assert!(
                resp.header("access-control-allow-origin").is_none(),
                "expected no Access-Control-Allow-Origin header"
            );
        }
    }
    match expect_allow_methods {
        Some(methods) => {
            assert_eq!(
                resp.header("access-control-allow-methods"),
                Some(methods),
                "expected Access-Control-Allow-Methods: {methods}"
            );
        }
        None => {
            assert!(
                resp.header("access-control-allow-methods").is_none(),
                "expected no Access-Control-Allow-Methods header"
            );
        }
    }
}

pub async fn presign_get(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expires_in: Duration,
) -> String {
    let presigning_config = aws_sdk_s3::presigning::PresigningConfig::builder()
        .expires_in(expires_in)
        .build()
        .unwrap();
    client
        .get_object()
        .bucket(bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .unwrap()
        .uri()
        .to_string()
}

pub async fn presign_put(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expires_in: Duration,
    acl: Option<aws_sdk_s3::types::ObjectCannedAcl>,
) -> String {
    let presigning_config = aws_sdk_s3::presigning::PresigningConfig::builder()
        .expires_in(expires_in)
        .build()
        .unwrap();
    let mut req = client.put_object().bucket(bucket).key(key);
    if let Some(a) = acl {
        req = req.acl(a);
    }
    req.presigned(presigning_config)
        .await
        .unwrap()
        .uri()
        .to_string()
}

// --- SigV2 signing helpers ---

type HmacSha1 = Hmac<Sha1>;
const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

fn sigv2_sign(secret_key: &str, string_to_sign: &str) -> String {
    let mut mac = HmacSha1::new_from_slice(secret_key.as_bytes()).expect("HMAC key");
    mac.update(string_to_sign.as_bytes());
    B64.encode(mac.finalize().into_bytes())
}

fn sigv2_canonicalize_headers(headers: &HashMap<String, String>) -> String {
    let mut amz: Vec<(String, &str)> = headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-amz-"))
        .map(|(k, v)| (k.to_lowercase(), v.as_str()))
        .collect();
    amz.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = String::new();
    for (k, v) in &amz {
        out.push_str(k);
        out.push(':');
        out.push_str(v);
        out.push('\n');
    }
    out
}

fn sigv2_canonicalize_resource(bucket: &str, key: Option<&str>, sub_resource: Option<&str>) -> String {
    let mut res = format!("/{bucket}");
    if let Some(k) = key {
        res.push('/');
        res.push_str(k);
    }
    if let Some(sr) = sub_resource {
        res.push('?');
        res.push_str(sr);
    }
    res
}

pub fn presign_get_v2(bucket: &str, key: &str, expires_secs: u64) -> String {
    let cfg = get_config();
    presign_v2(
        reqwest::Method::GET, bucket, key, expires_secs, None,
        &cfg.main_access_key, &cfg.main_secret_key,
    )
}

pub fn presign_put_v2(
    bucket: &str,
    key: &str,
    expires_secs: u64,
    acl: Option<&str>,
) -> String {
    let cfg = get_config();
    presign_v2(
        reqwest::Method::PUT, bucket, key, expires_secs, acl,
        &cfg.main_access_key, &cfg.main_secret_key,
    )
}

pub fn presign_get_v2_with_creds(
    bucket: &str,
    key: &str,
    expires_secs: u64,
    access_key: &str,
    secret_key: &str,
) -> String {
    presign_v2(reqwest::Method::GET, bucket, key, expires_secs, None, access_key, secret_key)
}

pub fn presign_put_v2_with_creds(
    bucket: &str,
    key: &str,
    expires_secs: u64,
    acl: Option<&str>,
    access_key: &str,
    secret_key: &str,
) -> String {
    presign_v2(reqwest::Method::PUT, bucket, key, expires_secs, acl, access_key, secret_key)
}

fn presign_v2(
    method: reqwest::Method,
    bucket: &str,
    key: &str,
    expires_secs: u64,
    acl: Option<&str>,
    access_key: &str,
    secret_key: &str,
) -> String {
    let expires = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + expires_secs;

    let mut amz_headers = HashMap::new();
    if let Some(a) = acl {
        amz_headers.insert("x-amz-acl".to_string(), a.to_string());
    }
    let canon_headers = sigv2_canonicalize_headers(&amz_headers);
    let canon_resource = sigv2_canonicalize_resource(bucket, Some(key), None);

    let string_to_sign = format!(
        "{}\n\n\n{}\n{}{}",
        method.as_str(),
        expires,
        canon_headers,
        canon_resource
    );
    let signature = sigv2_sign(secret_key, &string_to_sign);

    let base_url = get_object_url(bucket, key);
    let mut url = reqwest::Url::parse(&base_url).expect("valid base URL");
    url.query_pairs_mut()
        .append_pair("AWSAccessKeyId", access_key)
        .append_pair("Expires", &expires.to_string())
        .append_pair("Signature", &signature);
    if let Some(a) = acl {
        url.query_pairs_mut().append_pair("x-amz-acl", a);
    }
    url.to_string()
}

pub async fn sigv2_request(
    method: reqwest::Method,
    bucket: &str,
    key: Option<&str>,
    body: Option<&[u8]>,
    add_headers: Option<&HashMap<String, String>>,
    remove_headers: Option<&[&str]>,
) -> RawResponse {
    sigv2_request_with_creds(method, bucket, key, body, add_headers, remove_headers, None).await
}

pub async fn sigv2_request_with_creds(
    method: reqwest::Method,
    bucket: &str,
    key: Option<&str>,
    body: Option<&[u8]>,
    add_headers: Option<&HashMap<String, String>>,
    remove_headers: Option<&[&str]>,
    creds: Option<(&str, &str)>,
) -> RawResponse {
    let cfg = get_config();
    let (access_key, secret_key) = creds.unwrap_or((&cfg.main_access_key, &cfg.main_secret_key));

    let url = match key {
        Some(k) => get_object_url(bucket, k),
        None => get_endpoint_url(bucket),
    };

    let body_bytes = body.unwrap_or(&[]);
    let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    let canon_resource = sigv2_canonicalize_resource(bucket, key, None);

    // Content-MD5 and Content-Type are part of the SigV2 StringToSign, so
    // if add_headers supplies them they must be included in signing. Other
    // headers (Authorization, User-Agent, x-amz-date) are applied strictly
    // AFTER signing to produce intentionally broken requests.
    let content_md5 = add_headers
        .and_then(|h| h.iter().find(|(k, _)| k.eq_ignore_ascii_case("Content-MD5")).map(|(_, v)| v.as_str()))
        .unwrap_or("");
    let content_type = add_headers
        .and_then(|h| h.iter().find(|(k, _)| k.eq_ignore_ascii_case("Content-Type")).map(|(_, v)| v.as_str()))
        .unwrap_or("");

    let string_to_sign = format!(
        "{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        content_md5,
        content_type,
        date,
        canon_resource
    );
    let signature = sigv2_sign(secret_key, &string_to_sign);

    let mut final_headers: HashMap<String, String> = HashMap::new();
    final_headers.insert("Date".to_string(), date);
    final_headers.insert(
        "Authorization".to_string(),
        format!("AWS {}:{}", access_key, signature),
    );

    // Post-signing: inject extra headers (replacing existing ones if any)
    if let Some(extra) = add_headers {
        for (k, v) in extra {
            final_headers.insert(k.clone(), v.clone());
        }
    }

    // Post-signing: remove headers
    if let Some(to_remove) = remove_headers {
        for h in to_remove {
            final_headers.retain(|k, _| !k.eq_ignore_ascii_case(h));
        }
    }

    let http_client = build_reqwest_client();
    let mut req = http_client.request(method, &url);
    for (k, v) in &final_headers {
        req = req.header(k.as_str(), v.as_str());
    }
    if body_bytes.is_empty() {
        req = req.header("content-length", "0");
    } else {
        req = req.body(body_bytes.to_vec());
    }

    let resp = req.send().await.expect("sigv2 request failed");
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

// --- POST Object (browser upload) helpers ---

pub fn sign_policy(secret_key: &str, policy_b64: &[u8]) -> String {
    let mut mac =
        HmacSha1::new_from_slice(secret_key.as_bytes()).expect("HMAC key");
    mac.update(policy_b64);
    B64.encode(mac.finalize().into_bytes())
}

pub fn encode_policy(policy_json: &serde_json::Value) -> String {
    let json_str = serde_json::to_string(policy_json).unwrap();
    B64.encode(json_str.as_bytes())
}

pub async fn post_object_form(
    url: &str,
    fields: Vec<(&str, String)>,
    file_body: &[u8],
) -> RawResponse {
    post_object_form_with_filename(url, fields, file_body, "file").await
}

pub async fn post_object_form_with_filename(
    url: &str,
    fields: Vec<(&str, String)>,
    file_body: &[u8],
    filename: &str,
) -> RawResponse {
    let client = build_reqwest_client();
    let mut form = reqwest::multipart::Form::new();
    for (k, v) in fields {
        form = form.text(k.to_string(), v);
    }
    form = form.part(
        "file",
        reqwest::multipart::Part::bytes(file_body.to_vec()).file_name(filename.to_string()),
    );
    let resp = client
        .post(url)
        .multipart(form)
        .send()
        .await
        .expect("POST object request failed");
    let status = resp.status().as_u16();
    let final_url = Some(resp.url().to_string());
    let mut resp_headers = HashMap::new();
    for (k, v) in resp.headers() {
        resp_headers.insert(
            k.as_str().to_lowercase(),
            v.to_str().unwrap_or_default().to_string(),
        );
    }
    let body = resp.text().await.unwrap_or_default();
    RawResponse {
        status,
        headers: resp_headers,
        body,
        final_url,
    }
}
