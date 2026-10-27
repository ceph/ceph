//! A role given a filesystem identity of its own (I9):
//! `rgw_nsfs_role_identity_override`, NooBaa's documented future -- *"the
//! role inherits filesystem identity from that account unless overridden
//! by the role entity"*.
//!
//! A role session is carried as the role (`rgw::sal::role_session_ns`), and
//! the gateway serves it as the role's own identity row under the override,
//! when it has one, and as its account's otherwise.  The override is
//! daemon-global, so this runs alone, in profile `role-identity`, after
//! `baseline`;  the `identity-override` driver hint turns it on and off for
//! the test and puts the gateway's own setting back after.
//!
//! What the session ran as is what the `ran-as` hint reports for the role
//! principal, at the attribute write and the delete:  the identity the
//! driver chose, which is the subject;  whether that identity may then
//! write is the access model's, and is not asserted.  The account is `rgwcopya`'s uid and the role
//! `rgwcopyb`'s, `rgw-vstart.sh`'s users for I9, so no other test's rows
//! share them.

use std::future::Future;
use std::panic::{resume_unwind, AssertUnwindSafe};

use futures_util::FutureExt;
use s3_tests_rs::admin::{admin_request, as_json, driver_hint_results, nsfs_put_identity, q};
use s3_tests_rs::client::{build_s3_client, build_s3_client_with_session_token,
                          build_sts_client};
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{PrivateAccount, TestGuard};

const UID_ACCOUNT: &str = "70109";
const UID_ROLE: &str = "70110";

async fn impersonating_nsfs() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs")
            && f.get_bool("impersonate").unwrap_or(false),
        None => false,
    }
}

/// `body` with the role override set to `setting`, and back to the
/// gateway's own after, panic included.
async fn with_role_override<F, T>(setting: &str, body: F) -> Option<T>
where
    F: Future<Output = T>,
{
    let r = driver_hint_results("identity-override", &[("role", setting)]).await?;
    assert_eq!(r.get("role_identity_override").map(String::as_str),
               Some(if setting == "on" { "true" } else { "false" }),
               "the hint did not set the role override: {r:?}");
    let out = AssertUnwindSafe(body).catch_unwind().await;
    let _ = driver_hint_results("identity-override", &[("role", "default")]).await;
    match out {
        Ok(v) => Some(v),
        Err(panic) => resume_unwind(panic),
    }
}

/// The identity the gateway chose for `principal` at the attribute write
/// and the delete of one object, made by `owner` and acted on by `session`.
///
/// The choice is what is asserted, not the outcome:  `ran-as` records the
/// identity before anything touches the filesystem, and whether that
/// identity may then write is the access model's -- a role given an
/// identity of its own is refused in its account's bucket under
/// `posix-strict`, and performed under `s3-native`.  So the object is the
/// owner's, and the session's tag and delete are not required to succeed.
async fn ran_as(principal: &str, owner: &aws_sdk_s3::Client,
                session: &aws_sdk_s3::Client, bucket: &str, key: &str)
    -> (Option<String>, Option<String>)
{
    owner.put_object().bucket(bucket).key(key).body(b"role".to_vec().into())
        .send().await.expect("the bucket's owner could not write");
    let sel = driver_hint_results("ran-as", &[("select", principal)]).await
        .expect("selecting ran-as failed;  is rgw_driver_debug_apis on?");
    assert_eq!(sel.get("selecting").map(String::as_str), Some(principal));
    let _ = session.put_object_tagging().bucket(bucket).key(key)
        .tagging(aws_sdk_s3::types::Tagging::builder()
            .tag_set(aws_sdk_s3::types::Tag::builder()
                .key("i9").value("role").build().unwrap())
            .build().unwrap())
        .send().await;
    let _ = session.delete_object().bucket(bucket).key(key).send().await;
    let seen = driver_hint_results("ran-as", &[]).await.expect("reading ran-as");
    let _ = owner.delete_object().bucket(bucket).key(key).send().await;
    (seen.get("ran_as_attrs").cloned(), seen.get("ran_as_delete").cloned())
}

/// What `GET /admin/nsfs/credentials?role=` says a session on the role is
/// served as:  the principal, the row, and the uid.
async fn served(role_id: &str) -> (String, String, Option<String>) {
    let r = admin_request(reqwest::Method::GET, "/admin/nsfs/credentials",
                          &format!("role={}&format=json", q(role_id)), None).await;
    assert_eq!(r.status, 200, "the role's credentials: {}", r.body);
    let j = as_json(&r).expect("not JSON");
    (j["identity"].as_str().unwrap_or_default().to_string(),
     j["row"].as_str().unwrap_or_default().to_string(),
     j["uid"].as_u64().map(|u| u.to_string()))
}

/// Under the role override a session on a role given an identity runs as
/// that identity;  without the override, and for a role given none, as
/// its account's.  Each case is the others' control.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_role_session_runs_as_the_identity_it_was_given() {
    let _guard = TestGuard::setup();
    if !impersonating_nsfs().await {
        eprintln!("not nsfs under impersonation;  nothing to check");
        return;
    }
    let cfg = get_config();
    let Some(acct) = PrivateAccount::create("roleov").await else {
        eprintln!("could not make this test's own account");
        return;
    };
    let root = &acct.root;

    /* a user to assume the role as, and to make the bucket */
    let uname = format!("{}roleovuser", cfg.iam_name_prefix);
    root.create_user().user_name(&uname).path(&cfg.iam_path_prefix)
        .send().await.expect("create_user");
    let ak = root.create_access_key().user_name(&uname).send().await
        .expect("create_access_key");
    let k = ak.access_key().unwrap();
    let (uak, usk) = (k.access_key_id().to_string(), k.secret_access_key().to_string());
    let all = r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"*","Resource":"*"}}"#;
    root.put_user_policy().user_name(&uname).policy_name("RoleOvAll")
        .policy_document(all).send().await.expect("put_user_policy");

    /* the role, which the account's users may assume */
    let rname = format!("{}RoleOv", cfg.iam_name_prefix);
    let trust = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"AWS": "*"},
                       "Action": "sts:AssumeRole"}]
    }).to_string();
    let role = root.create_role().role_name(&rname)
        .assume_role_policy_document(&trust).send().await.expect("create_role");
    let role = role.role().unwrap();
    let (role_id, role_arn) = (role.role_id().to_string(), role.arn().to_string());
    root.put_role_policy().role_name(&rname).policy_name("RoleOvAll")
        .policy_document(all).send().await.expect("put_role_policy");
    /* the role principal, as `authenticated_user(s)` renders it for an
     * account with no tenant */
    let principal = format!("$role${role_id}");

    /* the account's row, and the role's */
    for (p, uid) in [(("identity", acct.id.as_str()), UID_ACCOUNT),
                     (("role", role_id.as_str()), UID_ROLE)] {
        let r = nsfs_put_identity(&[p, ("uid", uid), ("gid", uid)]).await;
        assert_eq!(r.status, 200, "binding {}: {}", p.1, r.body);
    }

    let bucket = format!("{}roleov", cfg.bucket_prefix);
    let owner_s3 = build_s3_client(&uak, &usk);
    let _ = owner_s3.delete_bucket().bucket(&bucket).send().await;
    owner_s3.create_bucket().bucket(&bucket).send().await.expect("create_bucket");

    let sts = build_sts_client(&uak, &usk);
    let creds = sts.assume_role().role_arn(&role_arn).role_session_name("i9")
        .send().await.expect("assume_role");
    let c = creds.credentials().unwrap();
    let session = build_s3_client_with_session_token(
        c.access_key_id(), c.secret_access_key(), c.session_token());

    let (off, off_served) = with_role_override("off", async {
        (ran_as(&principal, &owner_s3, &session, &bucket, "off").await,
         served(&role_id).await)
    }).await.expect("no identity-override hint");
    let (on, on_served) = with_role_override("on", async {
        (ran_as(&principal, &owner_s3, &session, &bucket, "on").await,
         served(&role_id).await)
    }).await.expect("no identity-override hint");
    /* a role given no identity follows its account, under the override too */
    let _ = admin_request(reqwest::Method::DELETE, "/admin/nsfs/identity",
                          &format!("role={}", q(&role_id)), None).await;
    let (none, none_served) = with_role_override("on", async {
        (ran_as(&principal, &owner_s3, &session, &bucket, "none").await,
         served(&role_id).await)
    }).await.expect("no identity-override hint");
    /* a role that does not exist is served as nothing */
    let ghost = admin_request(reqwest::Method::GET, "/admin/nsfs/credentials",
                              "role=no-such-role-id&format=json", None).await;

    let _ = owner_s3.delete_bucket().bucket(&bucket).send().await;
    let _ = root.delete_role_policy().role_name(&rname).policy_name("RoleOvAll").send().await;
    let _ = root.delete_role().role_name(&rname).send().await;
    let _ = root.delete_user_policy().user_name(&uname).policy_name("RoleOvAll").send().await;
    let _ = root.delete_access_key().user_name(&uname).access_key_id(&uak).send().await;
    let _ = root.delete_user().user_name(&uname).send().await;

    /* The delete records its choice before anything touches the
     * filesystem, so it is the one asserted in every case.  The tag write
     * is reached only where the access model lets the session read the
     * object first:  always as the account, whose object it is, and as
     * the role's own identity only under `s3-native`.  Where it is
     * reached it must agree. */
    for ((attrs, del), uid, what) in [
        (&off, UID_ACCOUNT, "without the role override the session did not run as its account"),
        (&on, UID_ROLE, "under the role override the session did not run as the role's identity"),
        (&none, UID_ACCOUNT, "a role given no identity did not follow its account under the override"),
    ] {
        assert_eq!(del.as_deref(), Some(uid), "{what} (the delete)");
        if let Some(a) = attrs {
            assert_eq!(a, uid, "{what} (the tag write)");
        }
    }
    assert_eq!(off.0.as_deref(), Some(UID_ACCOUNT),
        "the tag write, as the account, whose object it is, was not reached");

    /* the operator's view agrees with what the session ran as, and says
     * which row it came from */
    let role_row = format!("role:{role_id}");
    for ((who, row, uid), (want_row, want_uid), what) in [
        (off_served, (acct.id.as_str(), UID_ACCOUNT), "without the role override"),
        (on_served, (role_row.as_str(), UID_ROLE), "under the role override"),
        (none_served, (acct.id.as_str(), UID_ACCOUNT), "a role given no identity"),
    ] {
        assert_eq!(who, principal, "{what}:  credentials?role= named {who}");
        assert_eq!(row, want_row, "{what}:  credentials?role= resolved from {row}");
        assert_eq!(uid.as_deref(), Some(want_uid), "{what}:  credentials?role= uid");
    }
    assert_eq!(ghost.status, 200, "{}", ghost.body);
    assert_eq!(as_json(&ghost).unwrap()["outcome"], "none",
        "a role that does not exist was served as something");

}
