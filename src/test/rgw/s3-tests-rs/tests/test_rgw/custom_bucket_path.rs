//! NooBaa's custom bucket path (DECISIONS.md 2026-10-14):  a CreateBucket
//! carrying the custom-path header, with a path the owner's allowlist
//! admits, creates the bucket there.  The header's name is
//! `rgw_nsfs_custom_bucket_path_header`, by default NooBaa's own,
//! `x-noobaa-custom-bucket-path`, which is what is sent here.
//!
//! Where the bucket lands is the unit suite's to check, which can look;
//! what is checked here is that the header reaches the gateway's
//! decision through a real request:  denied with no allowlist, admitted
//! with one, denied outside it.

use s3_tests_rs::admin::{as_json, driver_hint, driver_hint_results,
                         nsfs_delete_identity, nsfs_put_identity};
use s3_tests_rs::fixtures::PrivateUser;
use s3_tests_rs::features::{feature_bool, features};
use s3_tests_rs::hints::with_hint;
use s3_tests_rs::fixtures::{get_new_bucket_name, TestGuard};

const HEADER: &str = "x-noobaa-custom-bucket-path";
/* the uid this module's user is bound to under impersonation */
const UID_CUSTOMPATH: &str = "70101";
/* I9's fixture users, `rgw-vstart.sh`'s `rgwcopya` and `rgwcopyb`:  no
 * other test's rows carry their uids, so the override test's rows cannot
 * disagree with another's on a uid a file names */
const UID_COPY_A: &str = "70109";
const UID_COPY_B: &str = "70110";

/// `None` from the accessor means unknown, so skip rather than assert.
async fn nsfs_only() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs"),
        None => false,
    }
}

/// The default `new_buckets_path` for an owner created in RGW, from
/// the adoption hint, which reports it as `base_path`.
///
/// Not a root buckets live under:  a bucket is found through the
/// record that places it, and the record carries its path
/// (archive/NSFS_BUCKET_PLACEMENT_PLAN.md §7).  What this test wants of it is
/// only that it is a path the gateway can create in, so the allowlist
/// has something real to admit.
async fn default_new_buckets_path() -> String {
    let r = driver_hint("adoption", &[]).await;
    assert_eq!(r.status, 200, "the adoption hint: {}", r.body);
    let v = as_json(&r).expect("the adoption hint's reply is not JSON");
    v["results"]["base_path"].as_str()
        .expect("the adoption hint reports no base_path")
        .trim_end_matches('/')
        .to_string()
}

/// A CreateBucket with the header set to `path`;  the HTTP status.
async fn create_at(client: &aws_sdk_s3::Client, bucket: &str, path: &str)
    -> u16 {
    let path = path.to_string();
    match client.create_bucket().bucket(bucket)
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert(HEADER, path.clone());
        })
        .send().await
    {
        Ok(_) => 200,
        Err(e) => e.raw_response().map_or(0, |r| r.status().as_u16()),
    }
}

#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_custom_bucket_path_follows_the_allowlist() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let impersonating = feature_bool("impersonate").await == Some(true);
    /* A user of this module's own.  This test deletes the identity
     * row of whoever it names, to show that a request with no
     * allowlist is denied -- and 590 tests authenticate as the main
     * user, for whom a missing row is a correct 403. */
    let Some(user) = PrivateUser::create("custompath").await else {
        eprintln!("no IAM root credentials;  nothing to check");
        return;
    };
    let me = user.id.clone();
    let base = default_new_buckets_path().await;
    /* under the base path, which the allowlist names;  the raw-prefix
     * match NooBaa makes is the unit suite's to pin */
    let bucket = get_new_bucket_name();
    let at = format!("{base}/custom-{bucket}");

    /* no allowlist:  denied, which shows the header was read */
    nsfs_delete_identity(&me).await;
    assert_eq!(create_at(&user.client, &bucket, &at).await, 403,
               "a custom path was admitted with no allowlist");

    /* An allowlist admitting it, and no uid.  Under impersonation nsfs
     * checks the owner's own access to the path before it makes the
     * directory, and an owner whose credentials do not resolve is
     * denied rather than checked as the gateway (`RGW_NSFS_VADE_MECUM.md` §12, "No authenticated user means refuse").  The
     * injected case restores the old reading, which admitted it.
     * Without impersonation nothing is checked as anybody, so it is
     * admitted. */
    let r = nsfs_put_identity(&[("identity", me.as_str()),
                                ("custom_bucket_path_allowed_list", base.as_str())]).await;
    assert_eq!(r.status, 200, "setting the allowlist: {}", r.body);
    let unresolved = create_at(&user.client, &bucket, &at).await;
    if unresolved == 200 {
        let _ = user.client.delete_bucket().bucket(&bucket).send().await;
    }
    let injected = if impersonating {
        let st = with_hint("inject-unresolved-as-gateway",
                           &[("enable", "true"), ("user", &me)],
                           create_at(&user.client, &bucket, &at)).await;
        if st == 200 {
            let _ = user.client.delete_bucket().bucket(&bucket).send().await;
        }
        Some(st)
    } else {
        None
    };

    /* An owner with access to the path:  a location made for this
     * user by the `unplaced-directory` hint, inside the gateway's base
     * path, so the test assumes nothing about the gateway host's
     * filesystem.  Under impersonation the user is bound to a uid and
     * the location is that uid's;  without it, the gateway's. */
    let loc = get_new_bucket_name();
    let mut row = vec![("identity", me.as_str())];
    let mut make = vec![("op", "create"), ("name", loc.as_str()),
                        ("mode", "0770")];
    if impersonating {
        row.extend([("uid", UID_CUSTOMPATH), ("gid", UID_CUSTOMPATH)]);
        make.push(("as", me.as_str()));
    }
    let r = nsfs_put_identity(&row).await;
    assert_eq!(r.status, 200, "binding the user: {}", r.body);
    let made = driver_hint_results("unplaced-directory", &make).await
        .expect("could not make the user a location");
    let path = made.get("path").cloned().expect("the location's path");
    row.push(("custom_bucket_path_allowed_list", path.as_str()));
    let r = nsfs_put_identity(&row).await;
    assert_eq!(r.status, 200, "setting the allowlist: {}", r.body);

    let admitted_name = get_new_bucket_name();
    let admitted = create_at(&user.client, &admitted_name,
                             &format!("{path}/custom-{admitted_name}")).await;
    if admitted == 200 {
        let _ = user.client.delete_bucket().bucket(&admitted_name)
            .send().await;
    }

    /* and one path it does not admit */
    let other = get_new_bucket_name();
    let outside =
        create_at(&user.client, &other, "/no/such/allowed/prefix").await;

    let mut remove = vec![("op", "remove"), ("name", loc.as_str())];
    if impersonating {
        remove.push(("as", me.as_str()));
    }
    let _ = driver_hint_results("unplaced-directory", &remove).await;

    if impersonating {
        assert_eq!(unresolved, 403,
                   "an owner whose credentials do not resolve was admitted \
                    to a custom path;  its access was checked as the gateway");
        assert_eq!(injected, Some(200),
                   "inject-unresolved-as-gateway did not admit the owner;  \
                    the arm does not reproduce the defect");
    } else {
        assert_eq!(unresolved, 200,
                   "a custom path the allowlist admits was denied");
    }
    assert_eq!(admitted, 200,
               "a custom path the allowlist admits, which its owner may \
                write, was denied");
    assert_eq!(outside, 403, "a custom path outside the allowlist was admitted");
}

/// I9:  an account user's own allowlist admits a custom path under
/// `rgw_nsfs_user_identity_override`, as NooBaa checks the requesting
/// record's;  without it the account's does.  The user's row admits one
/// location and the account's another, so each setting admits exactly
/// one of the two.  The test reads the gateway's setting and expects
/// accordingly, so it passes under either when the gateway is right.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_an_account_users_own_allowlist_follows_the_override() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let impersonating = feature_bool("impersonate").await == Some(true);
    let overriding = feature_bool("user_identity_override").await == Some(true);
    eprintln!("user identity override {}", if overriding { "on" } else { "off" });
    let cfg = s3_tests_rs::config::get_config();
    let Some(acct) = s3_tests_rs::fixtures::PrivateAccount::create("upath").await else {
        eprintln!("could not make this test's own account");
        return;
    };
    let root = &acct.root;
    let name = format!("{}upathuser", cfg.iam_name_prefix);
    let _ = root.delete_user_policy().user_name(&name).policy_name("UpathAll")
        .send().await;
    if let Ok(keys) = root.list_access_keys().user_name(&name).send().await {
        for k in keys.access_key_metadata() {
            if let Some(id) = k.access_key_id() {
                let _ = root.delete_access_key().user_name(&name)
                    .access_key_id(id).send().await;
            }
        }
    }
    let _ = root.delete_user().user_name(&name).send().await;
    let created = root.create_user().user_name(&name).path(&cfg.iam_path_prefix)
        .send().await.expect("create_user");
    let me = created.user().unwrap().user_id().to_string();
    let ak = root.create_access_key().user_name(&name).send().await
        .expect("create_access_key");
    let k = ak.access_key().unwrap();
    let client = s3_tests_rs::client::build_s3_client(k.access_key_id(),
                                                      k.secret_access_key());
    root.put_user_policy().user_name(&name).policy_name("UpathAll")
        .policy_document(r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"*","Resource":"*"}}"#)
        .send().await.expect("put_user_policy");

    /* a row each, the user's and the account's, with ids under
     * impersonation so each can be checked for access to its location */
    let bind = |who: &str, uid: &'static str| {
        let who = who.to_string();
        async move {
            let mut p = vec![("identity", who.as_str())];
            if impersonating {
                p.extend([("uid", uid), ("gid", uid)]);
            }
            let r = nsfs_put_identity(&p).await;
            assert_eq!(r.status, 200, "binding {who}: {}", r.body);
        }
    };
    bind(&acct.id, UID_COPY_B).await;
    bind(&me, UID_COPY_A).await;

    /* a location each, made as each by the `unplaced-directory` hint */
    let make = |owner: String| async move {
        let loc = get_new_bucket_name();
        let mut p = vec![("op", "create".to_string()), ("name", loc.clone()),
                         ("mode", "0770".to_string())];
        if impersonating {
            p.push(("as", owner));
        }
        let pr: Vec<(&str, &str)> = p.iter().map(|(a, b)| (*a, b.as_str())).collect();
        let made = driver_hint_results("unplaced-directory", &pr).await
            .expect("could not make a location");
        (loc, made.get("path").cloned().expect("the location's path"))
    };
    let (uloc, upath) = make(me.clone()).await;
    let (aloc, apath) = make(acct.id.clone()).await;
    for (who, uid, path) in [(acct.id.as_str(), UID_COPY_B, apath.as_str()),
                             (me.as_str(), UID_COPY_A, upath.as_str())] {
        let mut p = vec![("identity", who), ("custom_bucket_path_allowed_list", path)];
        if impersonating {
            p.extend([("uid", uid), ("gid", uid)]);
        }
        let r = nsfs_put_identity(&p).await;
        assert_eq!(r.status, 200, "setting {who}'s allowlist: {}", r.body);
    }

    let try_at = |path: String| {
        let client = client.clone();
        async move {
            let b = get_new_bucket_name();
            let st = create_at(&client, &b, &format!("{path}/custom-{b}")).await;
            if st == 200 {
                let _ = client.delete_bucket().bucket(&b).send().await;
            }
            st
        }
    };
    let at_user = try_at(upath.clone()).await;
    let at_account = try_at(apath.clone()).await;

    for (loc, owner) in [(uloc, me.clone()), (aloc, acct.id.clone())] {
        let mut p = vec![("op", "remove".to_string()), ("name", loc)];
        if impersonating {
            p.push(("as", owner));
        }
        let pr: Vec<(&str, &str)> = p.iter().map(|(a, b)| (*a, b.as_str())).collect();
        let _ = driver_hint_results("unplaced-directory", &pr).await;
    }
    nsfs_delete_identity(&me).await;
    nsfs_delete_identity(&acct.id).await;
    let _ = root.delete_user_policy().user_name(&name).policy_name("UpathAll")
        .send().await;
    let _ = root.delete_access_key().user_name(&name)
        .access_key_id(k.access_key_id()).send().await;
    let _ = root.delete_user().user_name(&name).send().await;

    if overriding {
        assert_eq!(at_user, 200, "under the override the user's own allowlist \
                                  did not admit its path");
        assert_eq!(at_account, 403, "under the override the account's \
                                     allowlist still admitted");
    } else {
        assert_eq!(at_user, 403, "without the override the user's own allowlist \
                                  admitted a path");
        assert_eq!(at_account, 200, "without the override the account's \
                                     allowlist did not admit its path");
    }
}
