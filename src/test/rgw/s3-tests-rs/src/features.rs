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
