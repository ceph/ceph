//! The imported identities:  the account's user is its root, and the
//! account's IAM user runs as the account.
//!
//! Observe mode only, against an adopted RGW.  The capture conf names
//! three identities:  `[s3 main]` is the NooBaa account, `[s3 alt]` the
//! IAM user created in it through NooBaa's IAM API, with the key NooBaa
//! minted for it, and `[iam]` an Admin Ops key on the gateway
//! (`adoption/standup.rs`, `write_capture_conf`).
//!
//! The import writes a row for the account and one for each IAM user,
//! the user's own copy of its account's tuple as NooBaa holds it (I9,
//! DECISIONS 2026-10-27).  Which one the user's requests run as is
//! `rgw_nsfs_user_identity_override`:  off, the account's, the user's
//! row kept and not consulted;  on, the user's own.  The test reads the
//! gateway's setting and expects accordingly, so it passes under either
//! when the gateway is right.  `GET /admin/nsfs/credentials` says which
//! row a request runs as.
//!
//! **The row, not the uid, is the discrimination.**  `config` gives the
//! account the running uid and NooBaa copies it into the IAM user, and
//! nothing in the stand-up changes the account after, so both rows hold
//! the same ids.  The control is the IAM user's own row, asked for by
//! name:  it must exist, and hold its account's ids.

use s3_tests_rs::admin::{admin_request, admin_request_as};
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;

use crate::generate::observing;

/// A top-level string field of an Admin Ops reply.
fn field(body: &str, name: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get(name).and_then(|f| f.as_str()).map(str::to_string))
        .unwrap_or_default()
}

/// The RGW user an access key belongs to.
async fn user_by_key(key: &str) -> (u16, String) {
    let r = admin_request(reqwest::Method::GET, "/admin/user",
                          &format!("access-key={key}&format=json"), None).await;
    (r.status, r.body)
}

#[tokio::test]
async fn corpus_imported_iam_user_runs_as_its_account() {
    if !observing() {
        eprintln!("generate mode;  the identities are checked on an adopted gateway");
        return;
    }
    let cfg = get_config();

    /* who the IAM user became */
    let (status, iam_user) = user_by_key(&cfg.alt_access_key).await;
    assert_eq!(status, 200,
        "the gateway does not know the IAM user's key:  the import did not \
         bring it across.\n{iam_user}");
    let user = field(&iam_user, "user_id");
    let account = field(&iam_user, "account_id");
    assert!(!account.is_empty(), "the IAM user {user} was imported outside any account");

    /* The account's own user is its root;  without that it is an
     * ordinary user with no policy, and RGW denies it everything the
     * account owns.  The IAM user, by the same lookup, is not. */
    let (status, root) = user_by_key(&cfg.main_access_key).await;
    assert_eq!(status, 200, "the gateway does not know the account's key:\n{root}");
    assert_eq!(field(&root, "type"), "root",
        "the account's user is not its root:\n{root}");
    assert_ne!(field(&iam_user, "type"), "root",
        "the IAM user was made its account's root:\n{iam_user}");

    /* the control:  a row of its own, its copy of the account's ids */
    let creds = |who: String| async move {
        admin_request(reqwest::Method::GET, "/admin/nsfs/credentials",
                      &format!("identity={who}&format=json"), None).await
    };
    let own = creds(user.clone()).await;
    let acct = creds(account.clone()).await;
    assert_eq!((field(&own.body, "outcome"), field(&own.body, "row")),
               ("resolved".to_string(), user.clone()),
        "the import gave the IAM user no row of its own:\n{}", own.body);
    let ids = |body: &str| serde_json::from_str::<serde_json::Value>(body).ok()
        .map(|v| (v["uid"].clone(), v["gid"].clone()));
    assert_eq!(ids(&own.body), ids(&acct.body),
        "the IAM user's row is not a copy of its account's:\n  user {}\n  account {}",
        own.body, acct.body);

    /* and what its requests run as, which the gateway's override
     * decides */
    let overriding = features().await
        .and_then(|f| f.get_bool("user_identity_override"))
        .unwrap_or(false);
    eprintln!("user identity override {}", if overriding { "on" } else { "off" });
    let expected = if overriding { user.clone() } else { account.clone() };
    let ran = admin_request_as(&cfg.alt_access_key, &cfg.alt_secret_key,
                               reqwest::Method::GET, "/admin/nsfs/credentials",
                               "format=json", None).await;
    assert_eq!(ran.status, 200, "the IAM user's own request failed:\n{}", ran.body);
    assert_eq!((field(&ran.body, "outcome"), field(&ran.body, "row")),
               ("resolved".to_string(), expected.clone()),
        "with the user identity override {} the IAM user should run as {}'s \
         row:\n{}", if overriding { "on" } else { "off" }, expected, ran.body);
}
