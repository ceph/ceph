//! Supplementary groups through nsswitch:  `rgw_nsfs_dynamic_supplemental_groups`,
//! NooBaa's `NSFS_ENABLE_DYNAMIC_SUPPLEMENTAL_GROUPS` (I8).
//!
//! With the switch on, an identity row with no group list of its own runs
//! with the user's Unix groups as nsswitch returns them.  The switch is
//! daemon-global -- it changes what every list-less row means -- so these
//! run alone, in profile `dynamic-groups`, after `baseline`;  the
//! `dynamic-groups` driver hint turns it on for each test and back to the
//! gateway's own setting after.
//!
//! The fixture is `rgw-vstart.sh --with-impersonation`'s:  `grouped.txt` in
//! `impersonate-fixture` at `0040 root:rgwfix`, so only membership of
//! `rgwfix` (70010) can grant its read;  `rgwalice` (70101) is in
//! `rgwfix` in `/etc/group`, `rgwbob` (70102) is not.

use std::future::Future;
use std::panic::{resume_unwind, AssertUnwindSafe};

use futures_util::FutureExt;
use s3_tests_rs::access_model::{Access, Class};
use s3_tests_rs::admin::{driver_hint_results, nsfs_put_identity};
use s3_tests_rs::client::get_client;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{PrivateUser, TestGuard};
use s3_tests_rs::policy::{make_arn_resource, make_json_policy};

const UID_ALICE: &str = "70101";
const UID_BOB: &str = "70102";
const BUCKET: &str = "impersonate-fixture";

async fn impersonating_nsfs() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs")
            && f.get_bool("impersonate").unwrap_or(false),
        None => false,
    }
}

/// Whether `user` is in `group` in this host's nsswitch, which is the
/// gateway's:  the suite runs beside the vstart it tests.
fn member(user: &str, group: &str) -> bool {
    std::process::Command::new("id").args(["-nG", user]).output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout)
             .split_whitespace().any(|g| g == group))
        .unwrap_or(false)
}

/// The switch on for `body`, and back to the gateway's own setting after,
/// panic included, as `hints::with_hint` does for an injection hint.
async fn with_dynamic_groups<F, T>(body: F) -> Option<T>
where
    F: Future<Output = T>,
{
    let on = driver_hint_results("dynamic-groups", &[("set", "on")]).await?;
    assert_eq!(on.get("dynamic_groups").map(String::as_str), Some("true"),
               "the hint did not turn the switch on: {on:?}");
    let out = AssertUnwindSafe(body).catch_unwind().await;
    let _ = driver_hint_results("dynamic-groups", &[("set", "default")]).await;
    match out {
        Ok(v) => Some(v),
        Err(panic) => resume_unwind(panic),
    }
}

/// Users of this test's own, granted the fixture bucket's reads by the
/// main user in one policy, as
/// `impersonation::test_impersonation_honours_the_group_vector` grants
/// its one reader.
async fn readers(tags: &[&str]) -> Option<Vec<PrivateUser>> {
    let mut us = Vec::new();
    for t in tags {
        us.push(PrivateUser::create(t).await?);
    }
    let principals: Vec<String> = us.iter()
        .map(|u| format!("arn:aws:iam:::user/{}", u.id)).collect();
    let doc = make_json_policy(
        "s3:GetObject", &make_arn_resource(&format!("{BUCKET}/*")),
        Some(serde_json::json!({"AWS": principals})), None, None);
    get_client().put_bucket_policy().bucket(BUCKET).policy(&doc).send().await
        .expect("could not grant the readers s3:GetObject");
    Some(us)
}

/// Test 4, and the explicit empty list's sharp edge end to end.
///
/// With the switch on, a list-less row for `rgwalice` reads `grouped.txt`
/// through `rgwfix`, which comes from `/etc/group` and from nowhere in the
/// row.  The controls:  the same row with an explicit **empty** list --
/// no groups, not "ask nsswitch" -- is denied, and so is a list-less row
/// for `rgwbob`, whom `/etc/group` leaves out.  The permission check is
/// the subject, so `expect` asserts the gateway's answer at either access
/// model.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_dynamic_groups_grant_through_unix_groups() {
    let _guard = TestGuard::setup();
    if !impersonating_nsfs().await {
        eprintln!("not nsfs under impersonation;  nothing to check");
        return;
    }
    assert!(member("rgwalice", "rgwfix") && !member("rgwbob", "rgwfix"),
        "the fixture's memberships are not as rgw-vstart.sh makes them;  \
         restart it with --with-impersonation, which puts them back");
    let Some(us) = readers(&["dyngrpa", "dyngrpb"]).await else {
        eprintln!("could not make this test's own users");
        return;
    };
    let (alice_u, bob_u) = (&us[0], &us[1]);
    assert!(alice_u.bind(UID_ALICE, UID_ALICE, None).await, "binding alice failed");
    assert!(bob_u.bind(UID_BOB, UID_BOB, None).await, "binding bob failed");

    let ran = with_dynamic_groups(async {
        let gated = Access::ask(Class::Data, BUCKET, "grouped.txt",
                                &alice_u.id, "r").await;
        assert!(!gated.denied(),
            "a list-less row did not take rgwfix from /etc/group");
        gated.expect(alice_u.client.get_object().bucket(BUCKET)
            .key("grouped.txt").send().await);

        let gated = Access::ask(Class::Data, BUCKET, "grouped.txt",
                                &bob_u.id, "r").await;
        assert!(gated.denied(),
            "control:  rgwbob, outside rgwfix, was granted the read");
        gated.expect(bob_u.client.get_object().bucket(BUCKET)
            .key("grouped.txt").send().await);

        /* the explicit empty list wins over nsswitch */
        let r = nsfs_put_identity(&[
            ("identity", alice_u.id.as_str()), ("uid", UID_ALICE),
            ("gid", UID_ALICE), ("groups", "")]).await;
        assert_eq!(r.status, 200, "rebinding alice with an empty list: {}", r.body);
        let gated = Access::ask(Class::Data, BUCKET, "grouped.txt",
                                &alice_u.id, "r").await;
        assert!(gated.denied(),
            "an explicit empty list asked nsswitch, and granted the read");
        gated.expect(alice_u.client.get_object().bucket(BUCKET)
            .key("grouped.txt").send().await);
    }).await;
    if ran.is_none() {
        eprintln!("no dynamic-groups hint;  nothing to check");
    }
    let _ = get_client().delete_bucket_policy().bucket(BUCKET).send().await;
}

/// Test 6, end to end:  the gateway follows `/etc/group` as it stands.
///
/// Whatever `rgwalice`'s membership of `rgwfix` is when this runs, a
/// list-less row for her reads `grouped.txt` if and only if she is in it.
/// `validate.rs` runs it twice:  once as the fixture has it, then after
/// `gpasswd -d rgwalice rgwfix`, putting the membership back after;  so
/// the two runs are each other's control.  The uid's entry is cleared
/// first, as a sweep would find the change within a TTL;  the sweep's own
/// bound is a unit test.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_dynamic_groups_follow_the_membership() {
    let _guard = TestGuard::setup();
    if !impersonating_nsfs().await {
        eprintln!("not nsfs under impersonation;  nothing to check");
        return;
    }
    let in_group = member("rgwalice", "rgwfix");
    eprintln!("rgwalice is {}in rgwfix", if in_group { "" } else { "not " });
    let Some(us) = readers(&["dyngrpm"]).await else {
        eprintln!("could not make this test's own user");
        return;
    };
    let alice_u = &us[0];
    assert!(alice_u.bind(UID_ALICE, UID_ALICE, None).await, "binding alice failed");

    let ran = with_dynamic_groups(async {
        let _ = driver_hint_results("nsswitch-cache",
                                    &[("forget_uid", UID_ALICE)]).await;
        let gated = Access::ask(Class::Data, BUCKET, "grouped.txt",
                                &alice_u.id, "r").await;
        assert_eq!(gated.denied(), !in_group,
            "the gateway's groups for rgwalice do not follow /etc/group:  \
             she is {}in rgwfix", if in_group { "" } else { "not " });
        gated.expect(alice_u.client.get_object().bucket(BUCKET)
            .key("grouped.txt").send().await);
    }).await;
    if ran.is_none() {
        eprintln!("no dynamic-groups hint;  nothing to check");
    }
    let _ = get_client().delete_bucket_policy().bucket(BUCKET).send().await;
}
