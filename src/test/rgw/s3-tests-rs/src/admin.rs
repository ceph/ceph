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
    admin_request_as(&cfg.iam_access_key, &cfg.iam_secret_key,
                     method, path, query, body).await
}

/// The same request signed as somebody else.
///
/// An endpoint gated on a capability needs a credential that lacks
/// it, or the gate is never observed to close.
pub async fn admin_request_as(
    access_key: &str,
    secret_key: &str,
    method: reqwest::Method,
    path: &str,
    query: &str,
    body: Option<&[u8]>,
) -> RawResponse {
    let cfg = get_config();

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

/// A JSON object as strings, one level of nesting flattened with dotted
/// keys.
///
/// The profile endpoints answer with scalars and one nested object --
/// `converted`, which an upgrade fills in -- and a test wants
/// `converted.objects` rather than a blob of JSON to re-parse.  Deeper
/// nesting stringifies, which nothing here produces.
fn flatten_json(body: &str) -> Option<HashMap<String, String>> {
    fn scalar(v: &serde_json::Value) -> String {
        match v {
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        }
    }
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let map = v.as_object()?;
    let mut out = HashMap::new();
    for (k, val) in map {
        match val {
            serde_json::Value::Object(inner) => {
                for (ik, iv) in inner {
                    out.insert(format!("{k}.{ik}"), scalar(iv));
                }
            }
            other => {
                out.insert(k.clone(), scalar(other));
            }
        }
    }
    Some(out)
}

/// `PUT /admin/nsfs/profile?bucket=<name>&profile=<name>` -- move a
/// bucket to a profile.
///
/// A function point rather than a hint:  it is what an operator runs once
/// against a tree that came from NooBaa, so it has its own resource and
/// its own capability instead of riding the dev-gated hint endpoint.
/// `None` when the deployment does not offer it.
///
/// Leaving `base` rewrites the tree, so the answer carries
/// `converting` and a `converted` count.  `converting` true means the
/// rewrite stopped partway and this call may be issued again to
/// resume;  the profile itself changed either way, which is why the
/// request still succeeded.
pub async fn nsfs_set_profile(bucket: &str, profile: &str)
    -> Option<HashMap<String, String>> {
    let resp = admin_request(
        reqwest::Method::PUT, "/admin/nsfs/profile",
        &format!("bucket={bucket}&profile={profile}"), None).await;
    if resp.status != 200 {
        return None;
    }
    flatten_json(&resp.body)
}

/// Which profile a bucket is in, without changing it.
pub async fn nsfs_get_profile(bucket: &str) -> Option<HashMap<String, String>> {
    let resp = admin_request(
        reqwest::Method::GET, "/admin/nsfs/profile",
        &format!("bucket={bucket}"), None).await;
    if resp.status != 200 {
        return None;
    }
    flatten_json(&resp.body)
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

/// Identity records as a test found them, to put back when it is done.
///
/// `rgw-vstart.sh --with-impersonation` binds the suite's main and alt
/// users for the whole run, and tests across the suite read across
/// identities on the strength of it.  A test that rewrites or removes one
/// of those records takes this first and restores it after, rather than
/// deleting, so the run's binding survives it.
pub struct SavedIdentities(Vec<(String, Option<serde_json::Value>)>);

impl SavedIdentities {
    pub async fn take(keys: &[&str]) -> Self {
        let mut found = Vec::new();
        for key in keys {
            let r = nsfs_get_identity(key).await;
            let rec = if r.status == 200 {
                serde_json::from_str::<serde_json::Value>(&r.body).ok()
            } else {
                None
            };
            found.push((key.to_string(), rec));
        }
        SavedIdentities(found)
    }

    /// Each record as it was:  absent stays absent;  present is
    /// rewritten fresh with its uid, gid and groups, so a field a test
    /// added does not survive.
    pub async fn restore(&self) {
        for (key, rec) in &self.0 {
            let _ = nsfs_delete_identity(key).await;
            let Some(rec) = rec else {
                continue;
            };
            let (Some(uid), Some(gid)) = (rec["uid"].as_u64(), rec["gid"].as_u64())
            else {
                continue;
            };
            let (uid, gid) = (uid.to_string(), gid.to_string());
            let groups = rec["groups"].as_array().map(|g| {
                g.iter().filter_map(|v| v.as_u64()).map(|v| v.to_string())
                    .collect::<Vec<_>>().join(",")
            });
            let mut params: Vec<(&str, &str)> =
                vec![("identity", key.as_str()), ("uid", &uid), ("gid", &gid)];
            if let Some(g) = groups.as_deref() {
                params.push(("groups", g));
            }
            let r = nsfs_put_identity(&params).await;
            assert_eq!(r.status, 200, "restoring {key}'s record failed: {}",
                       r.body);
        }
    }
}

/// The filesystem's verdict on a request, where impersonation adds one.
///
/// With impersonation on, nsfs serves each request as its POSIX identity,
/// so the filesystem decides an object's content as well as S3:  a read
/// S3 permits is still refused to an identity the file's mode excludes,
/// and a requester with no POSIX record is refused outright.  A test
/// asserting S3's authorization asks for this first, and asserts the
/// refusal where the filesystem refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsVerdict {
    /// Not impersonating, or nothing could be asked:  S3 alone decides.
    NotImpersonating,
    Permitted,
    Refused,
    /// The requester has no POSIX record, and is refused before any file.
    NoIdentity,
}

impl FsVerdict {
    /// Refused by the filesystem, whatever S3 permits.
    pub fn refuses(self) -> bool {
        matches!(self, FsVerdict::Refused | FsVerdict::NoIdentity)
    }
}

/// Ask the gateway, through its `access` driver hint, whether `user`
/// served as its POSIX identity may read (`mode` `r`) or write (`w`)
/// `key` in `bucket`.  The gateway asks the kernel as that identity, so
/// the suite does not re-implement the check.
///
/// Best effort, as the capability query is:  no answer -- not nsfs, no
/// admin access, a driver without the hint -- is `NotImpersonating`, so a
/// test keeps its S3 assertions where nothing says otherwise.  Ask after
/// the object exists.  `user` is the request's user, `tenant$id` for a
/// tenant's, empty for anonymous.
pub async fn fs_access(bucket: &str, key: &str, user: &str, mode: &str)
                       -> FsVerdict {
    let (b, k, u) = (q(bucket), q(key), q(user));
    let r = driver_hint_results("access", &[("bucket", &b), ("key", &k),
                                            ("user", &u), ("mode", mode)])
        .await;
    match r.as_ref().and_then(|m| m.get("verdict")).map(String::as_str) {
        Some("permitted") => FsVerdict::Permitted,
        Some("refused") => FsVerdict::Refused,
        Some("no-identity") => FsVerdict::NoIdentity,
        _ => FsVerdict::NotImpersonating,
    }
}

/// Percent-encode a query-string value.
///
/// An owner key carries `$` between tenant and user.  SigV4 signs the
/// canonical -- encoded -- query, so a raw `$` on the wire signs one
/// string and sends another.
pub fn q(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9'
                | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `PUT /admin/nsfs/identity` -- write one identity record.
///
/// The parameters are passed through rather than taken as a struct.
/// The verb is a full replace, so which fields a caller omits is the
/// thing under test, and a struct with defaults would hide it.
pub async fn nsfs_put_identity(params: &[(&str, &str)]) -> RawResponse {
    let query = params.iter()
        .map(|(k, v)| format!("{}={}", k, q(v)))
        .collect::<Vec<_>>()
        .join("&");
    admin_request(reqwest::Method::PUT, "/admin/nsfs/identity",
                  &query, None).await
}

/// `GET /admin/nsfs/identity?identity=<owner>` -- read one record.
pub async fn nsfs_get_identity(key: &str) -> RawResponse {
    admin_request(reqwest::Method::GET, "/admin/nsfs/identity",
                  &format!("identity={}", q(key)), None).await
}

/// `GET /admin/nsfs/identity` with no key -- every record.
pub async fn nsfs_list_identities() -> RawResponse {
    admin_request(reqwest::Method::GET, "/admin/nsfs/identity",
                  "", None).await
}

/// `DELETE /admin/nsfs/identity?identity=<owner>`.
pub async fn nsfs_delete_identity(key: &str) -> RawResponse {
    admin_request(reqwest::Method::DELETE, "/admin/nsfs/identity",
                  &format!("identity={}", q(key)), None).await
}

/// The response body as JSON, or `None` if it is not an object.
///
/// Not `flatten_json`:  an identity renders its unset fields by
/// omitting them and its group list as an array, and flattening
/// would make an absent key and an empty list read alike.
pub fn as_json(resp: &RawResponse) -> Option<serde_json::Value> {
    serde_json::from_str(&resp.body).ok()
}

/// `GET /admin/nsfs/credentials` -- what a request would be served as.
///
/// With no key it resolves the *caller*, which is the only form that
/// exercises the key an authenticated request actually produces.
pub async fn nsfs_credentials() -> RawResponse {
    admin_request(reqwest::Method::GET, "/admin/nsfs/credentials",
                  "", None).await
}

/// The same, signed as somebody else.
pub async fn nsfs_credentials_as(access_key: &str, secret_key: &str)
    -> RawResponse {
    admin_request_as(access_key, secret_key, reqwest::Method::GET,
                     "/admin/nsfs/credentials", "", None).await
}

/// `GET /admin/nsfs/credentials?identity=<owner>` -- somebody else's.
pub async fn nsfs_credentials_for(key: &str) -> RawResponse {
    admin_request(reqwest::Method::GET, "/admin/nsfs/credentials",
                  &format!("identity={}", q(key)), None).await
}

/// `PUT /admin/user?caps` -- grant capabilities to a user.
///
/// Admin Ops endpoints always require a non-default capability, so a
/// test identity that needs to call one has to be given it.  The
/// alternative would be opening the endpoint, which is not on offer.
///
/// Tests running at once grant to the same user, and the user record is
/// written under optimistic concurrency:  the loser of a race gets
/// `-ECANCELED`, a 500.  Granting is idempotent, so it is retried.
pub async fn grant_user_caps(uid: &str, caps: &str) -> RawResponse {
    let query = format!("caps&uid={}&user-caps={}", q(uid), q(caps));
    let mut r = admin_request(reqwest::Method::PUT, "/admin/user", &query, None).await;
    for _ in 0..10 {
        if r.status != 500 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        r = admin_request(reqwest::Method::PUT, "/admin/user", &query, None).await;
    }
    r
}
