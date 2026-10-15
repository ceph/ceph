//! `/admin/nsfs/bucket` -- a directory that is already there, placed as
//! a bucket:  NooBaa's `manage_nsfs bucket add --path` (DECISIONS.md
//! 2026-10-14).
//!
//! Under `nsfs-adoption`, for now.  The directory is made by the gateway,
//! through the `unplaced-directory` driver hint, under its base path,
//! where an operator would most likely make one:  a directory there is
//! not a bucket until it is placed.  The gateway owns it, as it owns the
//! base path, and the test needs no access to the gateway's filesystem.

use s3_tests_rs::admin::{admin_request, admin_request_as, as_json, driver_hint,
                         grant_user_caps, nsfs_delete_identity, nsfs_put_identity,
                         q};
use s3_tests_rs::client::get_iam_root_s3client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket_name, TestGuard};
use s3_tests_rs::http::RawResponse;

const PATH: &str = "/admin/nsfs/bucket";

/// `None` from the accessor means unknown, so skip rather than assert.
async fn nsfs_only() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs"),
        None => false,
    }
}

/// Is the gateway acting as each identity on the filesystem?  Asked
/// of the gateway rather than inferred.
async fn impersonating() -> bool {
    match features().await {
        Some(f) => f.get_bool("impersonate").unwrap_or(false),
        None => false,
    }
}

/// The owner is an account, `[iam root]`'s, as every bucket is in
/// NooBaa's model:  a bucket belongs to an account, never to a user
/// (`bucketspace_fs.js` create_bucket).  Placed buckets are
/// BucketOwnerEnforced, so the account root reads them with no policy.
fn owner() -> String {
    get_config().iam_root_user_id.clone()
}

/// Under impersonation every request runs as its POSIX identity, and an
/// identity with none is refused.  The account is given the fixture's
/// `rgwalice` (70101), as the impersonation tests give the main user,
/// from `rgw-vstart.sh --with-impersonation`;  and taken away
/// afterwards.  The nextest group `serial-nsfs-identity` keeps the tests
/// that write identity rows apart.
const UID_OWNER: &str = "70101";

async fn bind_owner() {
    if !impersonating().await {
        return;
    }
    let r = nsfs_put_identity(&[("identity", owner().as_str()),
                                ("uid", UID_OWNER), ("gid", UID_OWNER)]).await;
    assert_eq!(r.status, 200, "binding the owner failed: {}", r.body);
}

async fn unbind_owner() {
    if impersonating().await {
        let _ = nsfs_delete_identity(&owner()).await;
    }
}

async fn grant_admin() {
    let cfg = get_config();
    let r = grant_user_caps(&cfg.iam_user_id, "nsfs-adoption=*").await;
    assert_eq!(r.status, 200, "granting nsfs-adoption failed: {}", r.body);
}

async fn unplaced(params: &[(&str, &str)]) -> RawResponse {
    driver_hint("unplaced-directory", params).await
}

/// `<base>/<name>`, made by the gateway at `mode`, with one file in it;
/// its path.
async fn make_unplaced(name: &str, mode: &str) -> String {
    let content = q("here first");
    let r = unplaced(&[("op", "create"), ("name", name), ("mode", mode),
                       ("file", "already.txt"), ("content", &content)]).await;
    assert_eq!(r.status, 200, "making the directory {name}: {}", r.body);
    let v = as_json(&r).expect("the hint's reply is not JSON");
    v["results"]["path"].as_str()
        .expect("the hint reports no path")
        .to_string()
}

fn place_query(name: &str, path: &str, owner: &str) -> String {
    format!("name={}&path={}&owner={}", q(name), q(path), q(owner))
}

async fn place(name: &str, path: &str, owner: &str) -> RawResponse {
    admin_request(reqwest::Method::PUT, PATH, &place_query(name, path, owner),
                  None).await
}

/// The main user holds `nsfs=*` and is refused;  the control is the
/// admin credential granted `nsfs-adoption`, which must get through to
/// the endpoint's own refusal of a missing argument.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_place_bucket_requires_the_nsfs_adoption_capability() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let cfg = get_config();
    grant_admin().await;

    let r = admin_request(reqwest::Method::PUT, PATH, "", None).await;
    assert_eq!(r.status, 400, "the admin, granted the cap: {}", r.body);

    let r = admin_request_as(&cfg.main_access_key, &cfg.main_secret_key,
                             reqwest::Method::PUT, PATH,
                             &place_query("x-y-z", "/no/such/placement/path",
                                         &cfg.main_user_id),
                             None).await;
    assert_eq!(r.status, 403, "the main user, holding nsfs=*, placed a bucket: {}",
               r.body);
}

/// A directory with a file in it becomes a bucket the owning account can
/// read through S3;  the bucket's name is then taken;  deleting the bucket
/// leaves the directory.  Under impersonation, a directory the owner
/// cannot write is refused first.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_place_bucket_serves_a_directory_that_is_there() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    grant_admin().await;
    let me = owner();
    let me = me.as_str();

    bind_owner().await;
    let bucket = get_new_bucket_name();
    let dir = make_unplaced(&bucket, "0755").await;

    if impersonating().await {
        /* the gateway made it, so the owner cannot write it:  refused,
         * then admitted once it is opened, as an operator would */
        let r = place(&bucket, &dir, me).await;
        assert_eq!(r.status, 403, "placed where the owner cannot write: {}",
                   r.body);
        let r = unplaced(&[("op", "mode"), ("name", &bucket), ("mode", "0777")])
            .await;
        assert_eq!(r.status, 200, "opening {dir}: {}", r.body);
    }
    let r = place(&bucket, &dir, me).await;
    assert_eq!(r.status, 200, "placing {dir}: {}", r.body);
    let v = as_json(&r).expect("the reply is not JSON");
    assert_eq!(v["name"], bucket.as_str());
    assert_eq!(v["path"], dir.as_str());

    let client = get_iam_root_s3client();
    let listed = client.list_objects_v2().bucket(&bucket).send().await
        .expect("listing the placed bucket");
    let keys: Vec<_> = listed.contents().iter()
        .filter_map(|o| o.key().map(str::to_string)).collect();
    assert_eq!(keys, vec!["already.txt".to_string()]);
    let got = client.get_object().bucket(&bucket).key("already.txt").send().await
        .expect("reading the file through S3");
    let body = got.body.collect().await.expect("the body").into_bytes();
    assert_eq!(&body[..], b"here first");

    /* taken, while it is placed */
    let r = place(&bucket, &dir, me).await;
    assert_eq!(r.status, 409, "placed twice: {}", r.body);

    /* emptied, as S3 refuses a non-empty bucket, and deleted */
    client.delete_object().bucket(&bucket).key("already.txt").send().await
        .expect("deleting the file through S3");
    client.delete_bucket().bucket(&bucket).send().await
        .expect("deleting the placed bucket");

    /* the directory is still there to remove, and carries nothing */
    let r = unplaced(&[("op", "remove"), ("name", &bucket)]).await;
    unbind_owner().await;
    assert_eq!(r.status, 200,
               "deleting the bucket did not leave its directory as it was: {}",
               r.body);
}

/// The refusals:  a path that does not exist and an unknown owner are
/// 404;  a path that is not canonical, an owner who is a user in no
/// account, and a bad bucket name are 400.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_place_bucket_refusals() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    grant_admin().await;
    let me = owner();
    let me = me.as_str();

    bind_owner().await;
    let bucket = get_new_bucket_name();
    /* open to the owner, so the access check is not what refuses */
    let dir = make_unplaced(&bucket, "0777").await;

    let r = place(&bucket, &format!("{dir}-absent"), me).await;
    assert_eq!(r.status, 404, "a path that does not exist: {}", r.body);
    let r = place(&bucket, &format!("{dir}/../{bucket}"), me).await;
    assert_eq!(r.status, 400, "a path that is not canonical: {}", r.body);
    let r = place(&bucket, &dir, "no-such-owner-here").await;
    assert_eq!(r.status, 404, "an unknown owner: {}", r.body);
    let r = place(&bucket, &dir, &get_config().main_user_id).await;
    assert_eq!(r.status, 400, "an owner who is a user in no account: {}",
               r.body);
    let r = place("ab", &dir, me).await;
    assert_eq!(r.status, 400, "an invalid bucket name: {}", r.body);

    /* the control:  the same directory, owner and name are accepted */
    let r = place(&bucket, &dir, me).await;
    assert_eq!(r.status, 200, "the control was refused: {}", r.body);

    let client = get_iam_root_s3client();
    client.delete_object().bucket(&bucket).key("already.txt").send().await
        .expect("deleting the file through S3");
    client.delete_bucket().bucket(&bucket).send().await
        .expect("deleting the placed bucket");
    let r = unplaced(&[("op", "remove"), ("name", &bucket)]).await;
    unbind_owner().await;
    assert_eq!(r.status, 200, "removing {dir}: {}", r.body);
}
