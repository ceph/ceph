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
/// (NSFS_BUCKET_PLACEMENT_PLAN.md 7).  What this test wants of it is
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
     * denied rather than checked as the gateway (n-plan §6.1).  The
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
