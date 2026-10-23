//! What the deployment under test implements, read from it rather than
//! declared by whoever invoked the suite.
//!
//! `GET /admin/features` reports properties of the backend: whether a
//! rename moves the name, whether copy_file_range shares extents, which
//! FSStrategy was selected.  A test whose correct expectation differs
//! between nsfs-on-XFS and nsfs-on-GPFS can ask, instead of carrying a
//! claim that is right on one of them.
//!
//! This is **additive to the `fails_on_*` gates, not a replacement for
//! them**.  Those are declared by the caller and need nothing from the
//! endpoint, which is the only thing that works against a black box --
//! AWS itself, or a vendor's gateway.  This suite is run against AWS.
//! So every failure here is silent and means *unknown*:
//!
//!   * no such resource -- a backend that does not report, or an older one
//!   * 403 -- the credentials lack `info=read`
//!   * a 200 whose body is not a capability document.  Against AWS,
//!     `/admin/features` is a path-style request for a bucket named
//!     `admin`, and somebody owns that name.  A status code is not
//!     evidence; the shape is.
//!
//! And **absent means unknown, never false**.  A caller that cannot tell
//! those apart will assert the opposite of the truth against any backend
//! that does not report, which is why every accessor returns `Option`.

use std::collections::HashMap;

use tokio::sync::OnceCell;

use crate::admin::admin_request;

#[derive(Debug, Clone, Default)]
pub struct Features {
    pub backend: Option<String>,
    values: HashMap<String, String>,
}

impl Features {
    /// `None` when the feature was not reported -- which is not `false`.
    pub fn get_bool(&self, name: &str) -> Option<bool> {
        match self.values.get(name).map(String::as_str) {
            Some("true") => Some(true),
            Some("false") => Some(false),
            _ => None,
        }
    }

    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }
}

static FEATURES: OnceCell<Option<Features>> = OnceCell::const_new();

fn parse(body: &str) -> Option<Features> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    /* the shape is the evidence, not the status code */
    let map = v.get("features")?.as_object()?;

    let mut values = HashMap::new();
    for (k, val) in map {
        let s = match val {
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        values.insert(k.clone(), s);
    }
    Some(Features {
        backend: v.get("backend").and_then(|b| b.as_str()).map(String::from),
        values,
    })
}

async fn load() -> Option<Features> {
    let resp = admin_request(reqwest::Method::GET, "/admin/features", "", None).await;
    if resp.status != 200 {
        eprintln!(
            "fs features: unavailable (HTTP {});  expectations fall back to \
             the declared gates",
            resp.status
        );
        return None;
    }
    match parse(&resp.body) {
        Some(f) => {
            eprintln!(
                "fs features: backend={} {:?}",
                f.backend.as_deref().unwrap_or("?"),
                f.values
            );
            Some(f)
        }
        None => {
            eprintln!(
                "fs features: 200 but not a capability document;  treating \
                 as unknown"
            );
            None
        }
    }
}

/// The deployment's reported features, or `None` if it did not report.
/// Fetched once per process.
pub async fn features() -> Option<&'static Features> {
    FEATURES.get_or_init(load).await.as_ref()
}

/// Convenience for the common case.  `None` means unknown.
pub async fn feature_bool(name: &str) -> Option<bool> {
    features().await.and_then(|f| f.get_bool(name))
}

/// Which access model the gateway runs: what it does when the
/// filesystem denies an operation S3 authorised.
///
/// `"posix-strict"` -- both must allow, which is NooBaa's model.
/// `"s3-metadata"` -- S3 decides for an object's tags and ACL, never
/// for its bytes.
///
/// **Asked of the gateway first, and that is the point.**  A
/// deployment cannot be wrong about its own configuration, while a
/// declaration passed to the suite drifts the moment somebody changes
/// the gateway and not the invocation.  No request reveals the model
/// on its own: every model answers alike wherever S3 and POSIX agree,
/// which is nearly always.
///
/// `S3TEST_ACCESS_MODEL` is the fallback and only that -- for an
/// endpoint that reports no features at all, which is AWS and any
/// vendor gateway. It is consulted when the map is silent, never over
/// it.
///
/// `None` means unknown, as everywhere in this module, and a test
/// that cannot tell must assert nothing about the model rather than
/// guess one.
pub async fn access_model() -> Option<String> {
    if let Some(m) = features().await.and_then(|f| f.get_str("access_model")) {
        return Some(m.to_string());
    }
    std::env::var("S3TEST_ACCESS_MODEL").ok().filter(|v| !v.is_empty())
}

/// Whether ETags are unique-per-write tokens rather than content
/// MD5s -- `rgw_non_md5_etag`.
///
/// The gateway's report where it makes one, since it is what does the
/// hashing;  s3tests.conf's `non_md5_etag` otherwise, which is every
/// endpoint but nsfs and defaults to the digest regime.  So an S3
/// endpoint this suite knows nothing about is tested for MD5 ETags, and
/// a gateway that says it is in the other regime is believed.
pub async fn non_md5_etag() -> bool {
    match feature_bool("non_md5_etag").await {
        Some(v) => v,
        None => crate::config::get_config().non_md5_etag,
    }
}

/// How many seconds a lifecycle day lasts on this gateway, as it
/// reports it.
///
/// `Some(Some(n))`:  lifecycle runs and `n` seconds stand for a day.
/// `Some(None)`:  the gateway reports lifecycle off, or running on real
/// days, so a test waiting for an expiry cannot see one.  `None`:  not
/// reported, as on every backend but nsfs, and the caller keeps
/// s3tests.conf's `lc_debug_interval`.
pub async fn lifecycle_day_secs() -> Option<Option<u64>> {
    let f = features().await?;
    let threads = f.get_bool("lc_threads")?;
    let interval: i64 = f.get_str("lc_debug_interval")?.parse().ok()?;
    Some((threads && interval > 0).then_some(interval as u64))
}

/// Whether an S3 grant reaches another identity's object *bytes* on a
/// backend where the filesystem would deny it.
///
/// The data half of the question [`cross_account_metadata`] asks, and
/// a separate one:  `s3-metadata` answers yes to that and no to this.
/// `None` is unknown; `Some(false)` is a backend that reported a model
/// which does not do this.
///
/// Pass it to `unless_posix_governs_data!` rather than reading it deep
/// inside a helper, so a test says at its call site which model it is
/// asserting against.
///
/// The model applies only under impersonation:  without it every
/// request runs as the gateway, which the filesystem denies nothing, so
/// S3 is the only authority and the answer is yes whatever the model.
pub async fn cross_account_data() -> Option<bool> {
    if feature_bool("impersonate").await == Some(false) {
        return Some(true);
    }
    match access_model().await.as_deref() {
        Some("s3-native") => Some(true),
        Some("s3-metadata") | Some("posix-strict") => Some(false),
        /* a model this suite has not heard of: unknown, not false */
        Some(_) => None,
        None => None,
    }
}

/// Whether an S3 grant reaches another identity's object *metadata* --
/// its tags and ACL -- on a backend where the filesystem would deny it.
///
/// The question nearly every affected test actually asks.  Both
/// `s3-metadata` and `s3-native` do this, S3 policy governing it;
/// `posix-strict` does not, as NooBaa does not.  `None` is unknown;
/// `Some(false)` is a backend that reported a model which does not do
/// this.
///
/// Yes without impersonation, whatever the model, as for
/// [`cross_account_data`].
pub async fn cross_account_metadata() -> Option<bool> {
    if feature_bool("impersonate").await == Some(false) {
        return Some(true);
    }
    match access_model().await.as_deref() {
        Some("s3-metadata") | Some("s3-native") => Some(true),
        Some("posix-strict") => Some(false),
        /* a model this suite has not heard of: unknown, not false */
        Some(_) => None,
        None => None,
    }
}
