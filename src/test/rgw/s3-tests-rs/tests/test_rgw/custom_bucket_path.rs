//! NooBaa's custom bucket path (DECISIONS.md 2026-10-14):  a CreateBucket
//! carrying the custom-path header, with a path the owner's allowlist
//! admits, creates the bucket there.  The header's name is
//! `rgw_nsfs_custom_bucket_path_header`, by default NooBaa's own,
//! `x-noobaa-custom-bucket-path`, which is what is sent here.
//!
//! Where the bucket lands is the unit suite's to check, which can look;
//! what is checked here is that the header reaches the gateway's
//! decision through a real request:  refused with no allowlist, admitted
//! with one, refused outside it.

use s3_tests_rs::admin::{as_json, driver_hint, nsfs_delete_identity, nsfs_put_identity};
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket_name, TestGuard};

const HEADER: &str = "x-noobaa-custom-bucket-path";

/// `None` from the accessor means unknown, so skip rather than assert.
async fn nsfs_only() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs"),
        None => false,
    }
}

/// The gateway's base path, from the adoption hint, which reports it.
async fn base_path() -> String {
    let r = driver_hint("adoption", &[]).await;
    assert_eq!(r.status, 200, "the adoption hint: {}", r.body);
    let v = as_json(&r).expect("the adoption hint's reply is not JSON");
    v["results"]["base_path"].as_str()
        .expect("the adoption hint reports no base_path")
        .trim_end_matches('/')
        .to_string()
}

/// A CreateBucket with the header set to `path`;  the HTTP status.
async fn create_at(bucket: &str, path: &str) -> u16 {
    let client = get_client();
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
    let cfg = get_config();
    let me = cfg.main_user_id.clone();
    let base = base_path().await;
    /* a sibling of the base path, which the base path admits as a
     * string prefix, as NooBaa matches */
    let bucket = get_new_bucket_name();
    let at = format!("{base}-custom-{bucket}");

    /* no allowlist:  refused, which shows the header was read */
    nsfs_delete_identity(&me).await;
    assert_eq!(create_at(&bucket, &at).await, 403,
               "a custom path was admitted with no allowlist");

    /* an allowlist admitting it */
    let r = nsfs_put_identity(&[("identity", me.as_str()),
                                ("custom_bucket_path_allowed_list", base.as_str())]).await;
    assert_eq!(r.status, 200, "setting the allowlist: {}", r.body);
    let created = create_at(&bucket, &at).await;

    /* and one path it does not admit */
    let other = get_new_bucket_name();
    let outside = create_at(&other, "/no/such/allowed/prefix").await;

    let client = get_client();
    if created == 200 {
        let _ = client.delete_bucket().bucket(&bucket).send().await;
    }
    nsfs_delete_identity(&me).await;

    assert_eq!(created, 200, "a custom path the allowlist admits was refused");
    assert_eq!(outside, 403, "a custom path outside the allowlist was admitted");
}
