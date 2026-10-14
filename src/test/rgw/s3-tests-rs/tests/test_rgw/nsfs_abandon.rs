//! `/admin/nsfs/abandoned` -- NooBaa records an operator has decided will
//! not be carried further, and why (DECISIONS.md 2026-10-13).
//!
//! Under its own cap, `nsfs-adoption`:  abandoning lets adoption complete,
//! and a bucket abandoned before its import is not served once it has, so
//! the shared `nsfs` cap must not open it.  A gateway with no NooBaa store
//! -- the ordinary suite setup -- has nothing to abandon, so what is
//! checked here is the gate and the refusals;  the abandon and un-abandon
//! cycle is the unit suite's, which has a store.

use s3_tests_rs::admin::{admin_request, admin_request_as, grant_user_caps, q};
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::TestGuard;

const PATH: &str = "/admin/nsfs/abandoned";

/// `None` from the accessor means unknown, so skip rather than assert.
async fn nsfs_only() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs"),
        None => false,
    }
}

/// The admin credential, granted the cap.  Not part of vstart's
/// bootstrap, so that holding `nsfs=*` is never mistaken for holding this.
async fn grant_admin() {
    let cfg = get_config();
    let r = grant_user_caps(&cfg.iam_user_id, "nsfs-adoption=*").await;
    assert_eq!(r.status, 200, "granting nsfs-adoption failed: {}", r.body);
}

/// Every verb is gated on `nsfs-adoption`, and `nsfs=*` does not carry
/// it.  The main test user holds `nsfs=*` from the gateway's bootstrap
/// and is refused;  the alt user holds nothing and is refused;  the
/// control is the admin credential once granted the cap, which must get
/// through, or a gate refusing everyone would pass.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_abandoned_requires_the_nsfs_adoption_capability() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let cfg = get_config();
    grant_admin().await;

    /* the control */
    let r = admin_request(reqwest::Method::GET, PATH, "", None).await;
    assert_eq!(r.status, 200, "the admin, granted the cap, was refused: {}",
               r.body);

    for (who, ak, sk) in [
        ("the main user, holding nsfs=*", &cfg.main_access_key, &cfg.main_secret_key),
        ("the alt user, holding nothing", &cfg.alt_access_key, &cfg.alt_secret_key),
    ] {
        let r = admin_request_as(ak, sk, reqwest::Method::GET, PATH, "", None)
            .await;
        assert_eq!(r.status, 403, "{who} listed abandoned records: {}", r.body);
        let r = admin_request_as(ak, sk, reqwest::Method::PUT, PATH,
                                 &format!("id={}&reason={}", q("x"), q("y")),
                                 None).await;
        assert_eq!(r.status, 403, "{who} abandoned a record: {}", r.body);
        let r = admin_request_as(ak, sk, reqwest::Method::DELETE, PATH,
                                 &format!("id={}", q("x")), None).await;
        assert_eq!(r.status, 403, "{who} took back an abandonment: {}", r.body);
    }
}

/// The refusals a caller holding the cap can reach without a store:  no
/// reason is 400, an `_id` nothing holds is 404, and taking back what
/// was never abandoned is 404.  The listing is an array.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_abandoned_refuses_what_it_should() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    grant_admin().await;

    let r = admin_request(reqwest::Method::PUT, PATH,
                          &format!("id={}", q("6560e1f1c0ffee0000000bad")),
                          None).await;
    assert_eq!(r.status, 400, "abandoning with no reason: {}", r.body);

    let r = admin_request(reqwest::Method::PUT, PATH,
                          &format!("id={}&reason={}",
                                   q("6560e1f1c0ffee0000000bad"),
                                   q("nothing holds this")),
                          None).await;
    assert_eq!(r.status, 404, "abandoning an _id nothing holds: {}", r.body);

    let r = admin_request(reqwest::Method::DELETE, PATH,
                          &format!("id={}", q("6560e1f1c0ffee0000000bad")),
                          None).await;
    assert_eq!(r.status, 404, "taking back what was never abandoned: {}",
               r.body);

    let r = admin_request(reqwest::Method::GET, PATH, "", None).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let v: serde_json::Value = serde_json::from_str(&r.body)
        .unwrap_or_else(|e| panic!("the listing is not JSON ({e}): {}", r.body));
    assert!(v.is_array(), "the listing is not an array: {}", r.body);
}
