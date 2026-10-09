//! The POSIX identity an owner is served as.
//!
//! The record says which uid and gid the gateway acts as for an
//! owner, and which supplementary groups go with them.  Nothing
//! inside the gateway can invent one -- the values come from a
//! directory or from an operator -- so the whole field set is managed
//! over Admin Ops, including the fields no request path reads yet.
//! A field that cannot be written and read back is a field whose
//! first real consumer finds its bugs.
//!
//! PUT is a full replace.  An absent parameter means the field is
//! unset, not that it keeps its previous value.  That is what lets
//! `groups` travel in all three of its states -- absent is unset,
//! empty is an explicit "this identity has none", a list is the list
//! -- and the difference between the first two is what the directory
//! arm will turn on.

use s3_tests_rs::admin::{
    admin_request_as, as_json, nsfs_credentials, nsfs_credentials_as,
    nsfs_credentials_for, nsfs_delete_identity, nsfs_get_identity,
    grant_user_caps, nsfs_list_identities, nsfs_put_identity, q,
};
use s3_tests_rs::client::get_iam_root_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::TestGuard;

/// `None` from the accessor means unknown, so skip rather than assert.
async fn nsfs_only() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs"),
        None => false,
    }
}

/// Every field, so a round trip proves the record and not the subset
/// impersonation will consume.
fn full_record(key: &str) -> Vec<(&'static str, String)> {
    vec![
        ("identity", key.to_string()),
        ("uid", "1001".to_string()),
        ("gid", "2002".to_string()),
        ("groups", "10,20,30".to_string()),
        ("new_buckets_path", "/gpfs/rgw1/nsfs/newbuckets".to_string()),
        ("custom_bucket_path_allowed_list",
         "/gpfs/rgw1/nsfs/allowed".to_string()),
        ("fs_backend", "GPFS".to_string()),
        ("noobaa_id", "6a2bdeabf5c8e167f92cb079".to_string()),
    ]
}

fn as_params<'a>(v: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    v.iter().map(|(k, val)| (*k, val.as_str())).collect()
}

/// Leave no row behind:  the table is gateway-wide, not per-bucket,
/// so a test that does not clean up interferes with the list case.
async fn scrub(key: &str) {
    let _ = nsfs_delete_identity(key).await;
}

#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_identity_every_field_round_trips() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let key = "identity-round-trip";
    scrub(key).await;

    /* the control:  absent before it is written, so a leftover row
     * cannot make the assertions below pass */
    let before = nsfs_get_identity(key).await;
    assert_ne!(before.status, 200,
        "a row for {key} already existed;  the round trip would prove nothing");

    let rec = full_record(key);
    let resp = nsfs_put_identity(&as_params(&rec)).await;
    assert_eq!(resp.status, 200, "put failed: {}", resp.body);

    let resp = nsfs_get_identity(key).await;
    assert_eq!(resp.status, 200, "get failed: {}", resp.body);
    let j = as_json(&resp).expect("the response is not JSON");

    assert_eq!(j["identity"], key);
    assert_eq!(j["uid"], 1001);
    assert_eq!(j["gid"], 2002);
    assert_eq!(j["groups"], serde_json::json!([10, 20, 30]));
    assert_eq!(j["new_buckets_path"], "/gpfs/rgw1/nsfs/newbuckets");
    assert_eq!(j["custom_bucket_path_allowed_list"],
               "/gpfs/rgw1/nsfs/allowed");
    assert_eq!(j["fs_backend"], "GPFS");
    assert_eq!(j["noobaa_id"], "6a2bdeabf5c8e167f92cb079");
    assert!(j.get("distinguished_name").is_none(),
        "an unset field was rendered anyway");

    scrub(key).await;
}

/// Each field changes on its own.  A full replace resends the rest,
/// so what this proves is that the one that moved is the one that was
/// meant to and the others came back unchanged.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_identity_fields_update_independently() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let key = "identity-independent";
    scrub(key).await;

    let base = full_record(key);
    assert_eq!(nsfs_put_identity(&as_params(&base)).await.status, 200);

    let changes: &[(&str, &str, serde_json::Value)] = &[
        ("uid", "1234", serde_json::json!(1234)),
        ("gid", "5678", serde_json::json!(5678)),
        ("groups", "7", serde_json::json!([7])),
        ("new_buckets_path", "/gpfs/rgw1/nsfs/other",
         serde_json::json!("/gpfs/rgw1/nsfs/other")),
        ("custom_bucket_path_allowed_list", "/gpfs/rgw1/nsfs/elsewhere",
         serde_json::json!("/gpfs/rgw1/nsfs/elsewhere")),
        ("fs_backend", "CEPH_FS", serde_json::json!("CEPH_FS")),
        ("noobaa_id", "0000000000000000deadbeef",
         serde_json::json!("0000000000000000deadbeef")),
    ];

    for (field, value, want) in changes {
        let mut rec = full_record(key);
        for entry in rec.iter_mut() {
            if entry.0 == *field {
                entry.1 = value.to_string();
            }
        }
        let resp = nsfs_put_identity(&as_params(&rec)).await;
        assert_eq!(resp.status, 200, "put of {field} failed: {}", resp.body);

        let j = as_json(&nsfs_get_identity(key).await)
            .expect("the response is not JSON");
        assert_eq!(&j[*field], want, "{field} did not take");

        /* and nothing else moved */
        for (other, orig) in full_record(key) {
            if other == *field || other == "identity" {
                continue;
            }
            let got = &j[other];
            let expected: serde_json::Value = if other == "groups" {
                serde_json::json!([10, 20, 30])
            } else if other == "uid" || other == "gid" {
                serde_json::json!(orig.parse::<u64>().unwrap())
            } else {
                serde_json::json!(orig)
            };
            assert_eq!(got, &expected,
                "changing {field} disturbed {other}");
        }
    }

    scrub(key).await;
}

/// Unset, explicitly empty, and populated are three answers, and the
/// wire has to carry all three.  Omitting the parameter is unset;
/// `groups=` is the empty list;  a value is the list.  Collapse the
/// first two and a read-modify-write silently converts "nobody has
/// said" into "this identity has none", which are different
/// instructions to the group loader.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_identity_groups_distinguish_unset_from_empty() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let key = "identity-groups";
    scrub(key).await;

    /* populated first, so the two erasing cases below have something
     * to erase and cannot pass by never having written anything */
    assert_eq!(nsfs_put_identity(&[
        ("identity", key), ("uid", "1"), ("gid", "1"),
        ("groups", "10,20,30")]).await.status, 200);
    let j = as_json(&nsfs_get_identity(key).await).unwrap();
    assert_eq!(j["groups"], serde_json::json!([10, 20, 30]));

    /* present and empty:  an explicit empty list */
    assert_eq!(nsfs_put_identity(&[
        ("identity", key), ("uid", "1"), ("gid", "1"),
        ("groups", "")]).await.status, 200);
    let j = as_json(&nsfs_get_identity(key).await).unwrap();
    assert_eq!(j["groups"], serde_json::json!([]),
        "an explicit empty list did not survive");

    /* absent:  unset, which renders as no key at all */
    assert_eq!(nsfs_put_identity(&[
        ("identity", key), ("uid", "1"), ("gid", "1")]).await.status, 200);
    let j = as_json(&nsfs_get_identity(key).await).unwrap();
    assert!(j.get("groups").is_none(),
        "an omitted group list came back as something: {j}");

    scrub(key).await;
}

/// The directory arm:  a name and no ids.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_identity_directory_backed() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let key = "identity-directory";
    scrub(key).await;

    let resp = nsfs_put_identity(&[
        ("identity", key),
        ("distinguished_name", "someone"),
        ("new_buckets_path", "/gpfs/rgw1/nsfs/someone")]).await;
    assert_eq!(resp.status, 200, "put failed: {}", resp.body);

    let j = as_json(&nsfs_get_identity(key).await).unwrap();
    assert_eq!(j["distinguished_name"], "someone");
    assert!(j.get("uid").is_none(), "a directory-backed record carried a uid");
    assert!(j.get("gid").is_none(), "a directory-backed record carried a gid");

    scrub(key).await;
}

/// The rejections, each with the accepting control first.  A refusal
/// that cannot be contrasted with an acceptance proves only that the
/// endpoint says no to something.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_identity_invalid_records_are_refused() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let key = "identity-refused";
    scrub(key).await;

    /* the controls */
    assert_eq!(nsfs_put_identity(&[
        ("identity", key), ("uid", "5"), ("gid", "5")]).await.status, 200,
        "the local arm alone should be accepted");
    assert_eq!(nsfs_put_identity(&[
        ("identity", key), ("distinguished_name", "someone")]).await.status,
        200, "the directory arm alone should be accepted");
    scrub(key).await;

    let bad: &[(&str, Vec<(&str, &str)>)] = &[
        ("both identity arms", vec![
            ("identity", key), ("uid", "5"), ("gid", "5"),
            ("distinguished_name", "someone")]),
        ("a uid with no gid", vec![("identity", key), ("uid", "5")]),
        ("a gid with no uid", vec![("identity", key), ("gid", "5")]),
        ("a malformed group list", vec![
            ("identity", key), ("uid", "5"), ("gid", "5"),
            ("groups", "1,,2")]),
        ("a non-numeric group", vec![
            ("identity", key), ("uid", "5"), ("gid", "5"),
            ("groups", "1,x")]),
        ("a non-numeric uid", vec![
            ("identity", key), ("uid", "root"), ("gid", "5")]),
        ("the invalid uid", vec![
            ("identity", key), ("uid", "4294967295"), ("gid", "5")]),
        ("no key at all", vec![("uid", "5"), ("gid", "5")]),
        /* Keys no owner can ever render to.  `$alice` parses to an
         * empty tenant and renders back as `alice`, so a row under
         * that key could never be found;  `alice$` round-trips but
         * is a user with no id.  Both would otherwise be accepted
         * with a 200 that told an operator nothing was wrong. */
        ("a leading tenant separator", vec![
            ("identity", "$alice"), ("uid", "5"), ("gid", "5")]),
        ("an empty tenant and namespace", vec![
            ("identity", "$$alice"), ("uid", "5"), ("gid", "5")]),
        ("an empty user id", vec![
            ("identity", "alice$"), ("uid", "5"), ("gid", "5")]),
    ];

    for (what, params) in bad {
        let resp = nsfs_put_identity(params).await;
        /* 400, not merely "not 200":  a refusal for the wrong
         * reason -- a 403 from the capability gate, a 500 from a
         * crash in the parser -- would otherwise read as success */
        assert_eq!(resp.status, 400,
            "{what} was not refused as malformed: {} {}",
            resp.status, resp.body);
        let wrote = params.iter()
            .find(|(k, _)| *k == "identity")
            .map(|(_, v)| *v)
            .unwrap_or(key);
        assert_eq!(nsfs_get_identity(wrote).await.status, 404,
            "{what} was refused but left a row behind");
    }
}

/// Whether the key names anything is reported, not enforced.
///
/// A record may legitimately be written before the account it names,
/// so an unresolved key is not an error -- but an operator who has
/// mistyped one deserves to see it at the moment they make it.  Both
/// polarities, because a flag that is always false says nothing.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_identity_reports_whether_the_key_resolves() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let cfg = get_config();
    let known = cfg.main_user_id.clone();
    let unknown = "identity-nobody-by-that-name";
    scrub(&known).await;
    scrub(unknown).await;

    let resp = nsfs_put_identity(&[
        ("identity", &known), ("uid", "12"), ("gid", "12")]).await;
    assert_eq!(resp.status, 200, "put failed: {}", resp.body);
    let j = as_json(&resp).unwrap();
    assert_eq!(j["resolves"], true,
        "{known} is a real user and the put did not say so");
    let j = as_json(&nsfs_get_identity(&known).await).unwrap();
    assert_eq!(j["resolves"], true, "the get disagreed with the put");

    let resp = nsfs_put_identity(&[
        ("identity", unknown), ("uid", "13"), ("gid", "13")]).await;
    assert_eq!(resp.status, 200,
        "an unresolved key is not an error: {}", resp.body);
    let j = as_json(&resp).unwrap();
    assert_eq!(j["resolves"], false,
        "a key naming nothing was reported as resolving");
    /* and it was still written */
    let j = as_json(&nsfs_get_identity(unknown).await)
        .expect("the row should exist regardless");
    assert_eq!(j["uid"], 13);

    scrub(&known).await;
    scrub(unknown).await;
}

/// An owner key carries a tenant separator, and the endpoint has to
/// round-trip it rather than truncate at the `$`.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_identity_tenant_qualified_key() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let key = "atenant$auser";
    scrub(key).await;

    assert_eq!(nsfs_put_identity(&[
        ("identity", key), ("uid", "3"), ("gid", "3")]).await.status, 200);
    let j = as_json(&nsfs_get_identity(key).await).unwrap();
    assert_eq!(j["identity"], key, "the tenant separator did not survive");

    scrub(key).await;
}

/// List and delete.  Removing what is not there succeeds, because
/// the caller asked for the row to be gone and it is -- an importer
/// re-run should not have to distinguish the cases.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_identity_list_and_delete() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let (a, b) = ("identity-list-a", "identity-list-b");
    scrub(a).await;
    scrub(b).await;

    for key in [a, b] {
        assert_eq!(nsfs_put_identity(&[
            ("identity", key), ("uid", "1"), ("gid", "1")]).await.status, 200);
    }

    let resp = nsfs_list_identities().await;
    assert_eq!(resp.status, 200, "list failed: {}", resp.body);
    let j = as_json(&resp).expect("the response is not JSON");
    let keys: Vec<String> = j["identities"].as_array()
        .expect("identities is not an array")
        .iter()
        .filter_map(|e| e["identity"].as_str().map(|s| s.to_string()))
        .collect();
    assert!(keys.iter().any(|k| k == a), "{a} missing from the list");
    assert!(keys.iter().any(|k| k == b), "{b} missing from the list");

    assert_eq!(nsfs_delete_identity(a).await.status, 200);
    assert_eq!(nsfs_get_identity(a).await.status, 404);
    assert_eq!(nsfs_delete_identity(a).await.status, 200,
        "deleting an absent identity should succeed");

    let j = as_json(&nsfs_list_identities().await).unwrap();
    let keys: Vec<String> = j["identities"].as_array().unwrap().iter()
        .filter_map(|e| e["identity"].as_str().map(|s| s.to_string()))
        .collect();
    assert!(!keys.iter().any(|k| k == a), "{a} survived its delete");
    assert!(keys.iter().any(|k| k == b), "{b} was deleted too");

    scrub(b).await;
}

/// The capability gate, both polarities.
///
/// Every verb is gated -- READ on the getter, WRITE on the writers --
/// and a record carrying uid and gid is a grant of filesystem
/// identity, so an ordinary S3 credential must not be able to read
/// one or mint one.  The control is the same request signed by the
/// admin credential, which must succeed;  without it a gate that
/// refused everyone would pass.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_identity_requires_the_nsfs_capability() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let cfg = get_config();
    let key = "identity-caps";
    scrub(key).await;

    /* the control:  the admin credential can do all three */
    assert_eq!(nsfs_put_identity(&[
        ("identity", key), ("uid", "8"), ("gid", "8")]).await.status, 200);
    assert_eq!(nsfs_get_identity(key).await.status, 200);

    let unprivileged = |method: reqwest::Method, query: String| {
        /* the alt user, which carries no capabilities at all.  The
         * main user has `nsfs: *` from the gateway's own bootstrap,
         * so it is the wrong control -- it would prove the gate open
         * for everyone. */
        let ak = cfg.alt_access_key.clone();
        let sk = cfg.alt_secret_key.clone();
        async move {
            admin_request_as(&ak, &sk, method, "/admin/nsfs/identity",
                             &query, None).await
        }
    };

    let r = unprivileged(reqwest::Method::GET,
                         format!("identity={}", q(key))).await;
    assert_eq!(r.status, 403,
        "an ordinary credential read an identity record: {}", r.body);

    let r = unprivileged(reqwest::Method::GET, String::new()).await;
    assert_eq!(r.status, 403,
        "an ordinary credential listed the identity table: {}", r.body);

    let r = unprivileged(reqwest::Method::PUT,
                         format!("identity={}&uid=0&gid=0", q(key))).await;
    assert_eq!(r.status, 403,
        "an ordinary credential wrote an identity record: {}", r.body);

    let r = unprivileged(reqwest::Method::DELETE,
                         format!("identity={}", q(key))).await;
    assert_eq!(r.status, 403,
        "an ordinary credential deleted an identity record: {}", r.body);

    /* the refusals changed nothing:  the record is as the control
     * left it, not uid 0 and not gone */
    let j = as_json(&nsfs_get_identity(key).await)
        .expect("the record should still be readable by the admin");
    assert_eq!(j["uid"], 8, "a refused write took effect anyway");

    scrub(key).await;
}

// ---------------------------------------------------------------
// Resolution, through real authentication.
//
// Everything above writes and reads rows.  These drive the other
// end:  what a *request* is resolved to.  The key a request
// produces is the part most likely to be wrong, and a unit test
// that builds an applier by hand assumes the answer it is checking.
// ---------------------------------------------------------------

/// The caller's own credentials, all three outcomes.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_credentials_resolve_the_caller() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    /* the caller is whoever admin_request() signs as -- the iam
     * user, not `main`.  Getting this wrong the first time is what
     * the endpoint's `identity` field is for. */
    let cfg = get_config();
    let me = cfg.iam_user_id.clone();
    scrub(&me).await;

    /* no row:  no impersonation, and a 200 saying so rather than an
     * error -- the diagnostic has to work in the broken case */
    let resp = nsfs_credentials().await;
    assert_eq!(resp.status, 200, "credentials failed: {}", resp.body);
    let j = as_json(&resp).expect("not JSON");
    assert_eq!(j["outcome"], "none");
    assert_eq!(j["identity"], me,
        "the endpoint resolved under a key that is not the caller");
    assert!(j.get("uid").is_none(), "an unresolved answer carried a uid");

    /* a row:  the credentials come back */
    assert_eq!(nsfs_put_identity(&[
        ("identity", &me), ("uid", "1001"), ("gid", "2002"),
        ("groups", "10,20,30")]).await.status, 200);
    let j = as_json(&nsfs_credentials().await).unwrap();
    assert_eq!(j["outcome"], "resolved");
    assert_eq!(j["uid"], 1001);
    assert_eq!(j["gid"], 2002);
    assert_eq!(j["groups"], serde_json::json!([10, 20, 30]));

    /* an absent group list clears rather than inherits -- the same
     * property the unit test asserts, now through a real request */
    assert_eq!(nsfs_put_identity(&[
        ("identity", &me), ("uid", "1001"), ("gid", "2002")]).await.status,
        200);
    let j = as_json(&nsfs_credentials().await).unwrap();
    assert_eq!(j["groups"], serde_json::json!([]),
        "an absent list resolved to something");

    /* a directory-backed row:  asked for, cannot be supplied, and
     * must not fall back to serving the request unimpersonated */
    assert_eq!(nsfs_put_identity(&[
        ("identity", &me),
        ("distinguished_name", "uid=someone,dc=example,dc=com")])
        .await.status, 200);
    let j = as_json(&nsfs_credentials().await).unwrap();
    assert_eq!(j["outcome"], "refused");
    assert!(j.get("uid").is_none());

    scrub(&me).await;
}

/// Asking about somebody else is the same gate, and reaches rows
/// that are not the caller's.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_credentials_for_another_identity() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let cfg = get_config();
    let other = "credentials-somebody-else";
    scrub(other).await;

    assert_eq!(nsfs_put_identity(&[
        ("identity", other), ("uid", "31"), ("gid", "32")]).await.status, 200);

    let j = as_json(&nsfs_credentials_for(other).await).unwrap();
    assert_eq!(j["identity"], other);
    assert_eq!(j["outcome"], "resolved");
    assert_eq!(j["uid"], 31);

    /* and it is genuinely not the caller's own answer */
    let j = as_json(&nsfs_credentials().await).unwrap();
    assert_ne!(j["identity"], other);

    /* a key no owner renders to is malformed, not merely unfound */
    assert_eq!(nsfs_credentials_for("$alice").await.status, 400);

    /* the gate holds here too */
    let r = nsfs_credentials_as(&cfg.alt_access_key,
                                &cfg.alt_secret_key).await;
    assert_eq!(r.status, 403,
        "an uncapped credential read an identity: {}", r.body);

    scrub(other).await;
}

/// The key choice, made falsifiable.
///
/// Every other test here runs as a standalone user, for whom
/// `s->owner` and `s->user` are the same value — so they pass
/// whichever key the driver uses, and prove nothing about the
/// choice.  This one runs as a **member of an account**, where the
/// two differ: `get_aclowner()` reports the account, `s->user`
/// reports the member.
///
/// A row is written under both, with different uids.  If the driver
/// ever keys on the ACL owner, the member is served as 9999 and this
/// fails.  That is the whole point of it.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_credentials_of_an_account_member() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let cfg = get_config();
    let root = get_iam_root_client();
    let account = cfg.iam_root_user_id.clone();      /* "RGW1111..." */
    let name = format!("{}nsfscred", cfg.iam_name_prefix);

    /* leave nothing from a previous run */
    let _ = root.delete_access_key().user_name(&name)
        .access_key_id("x").send().await;
    let _ = root.delete_user().user_name(&name).send().await;

    let created = root.create_user()
        .user_name(&name)
        .path(&cfg.iam_path_prefix)
        .send().await.expect("create_user");
    /* IAM returns the member's rgw_user directly -- a generated
     * UUID, unique across accounts.  This is the key. */
    let member = created.user().unwrap().user_id().to_string();
    assert_ne!(member, account, "the member's id is its account's");

    let ak = root.create_access_key().user_name(&name).send().await
        .expect("create_access_key");
    let k = ak.access_key().unwrap();
    let (mak, msk) = (k.access_key_id().to_string(),
                      k.secret_access_key().to_string());

    /* Admin Ops always needs a cap, so the member is given one
     * rather than the endpoint being opened. */
    let r = grant_user_caps(&member, "nsfs=read").await;
    assert_eq!(r.status, 200, "granting nsfs to the member failed: {}",
               r.body);

    scrub(&member).await;
    scrub(&account).await;

    /* two rows, deliberately different */
    assert_eq!(nsfs_put_identity(&[
        ("identity", &member), ("uid", "4242"), ("gid", "4242"),
        ("groups", "77")]).await.status, 200);
    assert_eq!(nsfs_put_identity(&[
        ("identity", &account), ("uid", "9999"), ("gid", "9999")])
        .await.status, 200);

    /* the control:  the account's own row resolves to 9999, so
     * "not 9999" below is a real discrimination and not an artifact
     * of that row being missing */
    let j = as_json(&nsfs_credentials_for(&account).await).unwrap();
    assert_eq!(j["outcome"], "resolved");
    assert_eq!(j["uid"], 9999);

    /* and now as the member itself, through real authentication */
    let resp = nsfs_credentials_as(&mak, &msk).await;
    assert_eq!(resp.status, 200, "member credentials failed: {}", resp.body);
    let j = as_json(&resp).expect("not JSON");

    assert_eq!(j["identity"], member,
        "the request resolved under {} rather than the member",
        j["identity"]);
    assert_ne!(j["identity"], serde_json::json!(account),
        "the member collapsed onto its account");
    assert_eq!(j["uid"], 4242, "the member was served as the wrong row");
    assert_ne!(j["uid"], serde_json::json!(9999),
        "the member was served as its account");
    assert_eq!(j["groups"], serde_json::json!([77]));

    /* clean up */
    scrub(&member).await;
    scrub(&account).await;
    let _ = root.delete_access_key().user_name(&name)
        .access_key_id(&mak).send().await;
    let _ = root.delete_user().user_name(&name).send().await;
}
