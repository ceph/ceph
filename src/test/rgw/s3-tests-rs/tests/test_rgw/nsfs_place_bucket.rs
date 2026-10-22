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
                         grant_user_caps, q};
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

/* Under impersonation the account is served as the row
 * `rgw-vstart.sh --with-impersonation` provisions for it, `rgwacct1`
 * (70107), for the whole run.  Nothing here changes that row:  every
 * IAM test running as the account at the same time would be served as
 * whatever this module set it to, or denied while it was absent. */

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

/// The main user holds `nsfs=*` and is denied;  the control is the
/// admin credential granted `nsfs-adoption`, which must get through to
/// the endpoint's own rejection of a missing argument.
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
/// leaves the directory.  Under impersonation, placement on a directory
/// the owner cannot write is denied first.
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

    let bucket = get_new_bucket_name();
    let dir = make_unplaced(&bucket, "0755").await;

    if impersonating().await {
        /* the gateway made it, so the owner cannot write it:  denied,
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

    /* emptied, as S3 rejects deleting a non-empty bucket, and deleted */
    client.delete_object().bucket(&bucket).key("already.txt").send().await
        .expect("deleting the file through S3");
    client.delete_bucket().bucket(&bucket).send().await
        .expect("deleting the placed bucket");

    /* the directory is still there to remove, and carries nothing */
    let r = unplaced(&[("op", "remove"), ("name", &bucket)]).await;
    assert_eq!(r.status, 200,
               "deleting the bucket did not leave its directory as it was: {}",
               r.body);
}

/// The rejections:  a path that does not exist and an unknown owner are
/// 404;  a path that is not canonical, an owner who is a user in no
/// account, and a bad bucket name are 400.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_place_bucket_rejections() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    grant_admin().await;
    let me = owner();
    let me = me.as_str();

    let bucket = get_new_bucket_name();
    /* open to the owner, so the access check does not deny it */
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
    assert_eq!(r.status, 200, "the control was rejected: {}", r.body);

    let client = get_iam_root_s3client();
    client.delete_object().bucket(&bucket).key("already.txt").send().await
        .expect("deleting the file through S3");
    client.delete_bucket().bucket(&bucket).send().await
        .expect("deleting the placed bucket");
    let r = unplaced(&[("op", "remove"), ("name", &bucket)]).await;
    assert_eq!(r.status, 200, "removing {dir}: {}", r.body);
}

/// One step's outcome:  "ok", or the error's code.
fn outcome<T, E>(r: Result<T, E>) -> String
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::fmt::Display,
{
    match r {
        Ok(_) => "ok".to_string(),
        Err(e) => e.code().map(str::to_string).unwrap_or_else(|| e.to_string()),
    }
}

/// What an account does with its own bucket, step by step, each step
/// recorded rather than asserted, so that one failure does not hide the
/// rest.
async fn owners_sequence(client: &aws_sdk_s3::Client, bucket: &str)
                         -> Vec<(&'static str, String)> {
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, CompletedMultipartUpload,
                            CompletedPart, Tag, Tagging, VersioningConfiguration};

    let tagging = |k: &str| Tagging::builder()
        .tag_set(Tag::builder().key(k).value("v").build().expect("a tag"))
        .build().expect("a tag set");
    let mut steps = Vec::new();

    steps.push(("put k1", outcome(client.put_object().bucket(bucket).key("k1")
        .body(ByteStream::from_static(b"one")).send().await)));
    steps.push(("get k1", outcome(client.get_object().bucket(bucket).key("k1")
        .send().await)));
    steps.push(("put a/b/k2 (nested)", outcome(client.put_object().bucket(bucket)
        .key("a/b/k2").body(ByteStream::from_static(b"two")).send().await)));
    steps.push(("list", outcome(client.list_objects_v2().bucket(bucket)
        .send().await)));
    steps.push(("put object tagging", outcome(client.put_object_tagging()
        .bucket(bucket).key("k1").tagging(tagging("t")).send().await)));
    steps.push(("put bucket tagging", outcome(client.put_bucket_tagging()
        .bucket(bucket).tagging(tagging("b")).send().await)));

    let mpu = async {
        let up = client.create_multipart_upload().bucket(bucket).key("mp")
            .send().await.map_err(|e| format!("create {}", outcome::<(), _>(Err(e))))?;
        let id = up.upload_id().unwrap_or_default().to_string();
        let mut parts = Vec::new();
        for (n, body) in [(1, vec![b'x'; 5 * 1024 * 1024]), (2, vec![b'y'; 100])] {
            let p = client.upload_part().bucket(bucket).key("mp").upload_id(&id)
                .part_number(n).body(ByteStream::from(body)).send().await
                .map_err(|e| format!("part {n} {}", outcome::<(), _>(Err(e))))?;
            parts.push(CompletedPart::builder().part_number(n)
                .e_tag(p.e_tag().unwrap_or_default()).build());
        }
        client.list_parts().bucket(bucket).key("mp").upload_id(&id).send().await
            .map_err(|e| format!("list parts {}", outcome::<(), _>(Err(e))))?;
        client.complete_multipart_upload().bucket(bucket).key("mp").upload_id(&id)
            .multipart_upload(CompletedMultipartUpload::builder()
                .set_parts(Some(parts)).build())
            .send().await
            .map_err(|e| format!("complete {}", outcome::<(), _>(Err(e))))?;
        Ok::<(), String>(())
    };
    steps.push(("multipart (2 parts, list, complete)",
                mpu.await.err().unwrap_or_else(|| "ok".to_string())));

    steps.push(("enable versioning", outcome(client.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(VersioningConfiguration::builder()
            .status(BucketVersioningStatus::Enabled).build())
        .send().await)));
    steps.push(("put k1 again (versioned)", outcome(client.put_object()
        .bucket(bucket).key("k1").body(ByteStream::from_static(b"one, again"))
        .send().await)));
    steps.push(("list versions", outcome(client.list_object_versions()
        .bucket(bucket).send().await)));
    steps.push(("delete a/b/k2", outcome(client.delete_object().bucket(bucket)
        .key("a/b/k2").send().await)));
    steps
}

/// A bucket on a directory its account owns, as a NooBaa bucket's is
/// its account's, `0770` (`bucketspace_fs.js` create_bucket):  the
/// account does everything with it that it does with a bucket the
/// gateway made.  Under impersonation only, where the directory's owner
/// is an identity and not the gateway;  the gateway must be unprivileged
/// for the answer to mean anything, as root's capabilities override the
/// directory's mode.
///
/// The account is served as `rgwalice` (70101), and the directory is
/// made as it by the `unplaced-directory` hint's `as`, in a sticky
/// directory the gateway makes for it under the base path.
///
/// **It fails today, which is the point of it.**  Run 2026-10-16 on an
/// unprivileged impersonating gateway, both columns fail, for
/// different reasons.
///
/// *The owned column fails at the first step.*  Placement fails with
/// EACCES -- `writing user.nsfs.extensions on bucket ...: (13) Permission
/// denied`, after which the bucket is rolled back -- so the account
/// never reaches one S3 operation on its own directory, and every
/// later step reads `NoSuchBucket`.  Marking a directory the gateway
/// does not own needs `CAP_DAC_OVERRIDE` and `CAP_FOWNER` raised
/// around that write:  raise site 1 of the (n) plan, built in its
/// phase E.
///
/// *The control column fails one step*, on a bucket the gateway made:
/// overwriting an object in a versioned bucket answers 500 (`ERROR:
/// version demote failed for k1 sr=2`).  `demote_current_version()`
/// hard-links the current object into `.versions/` as the gateway,
/// and with `fs.protected_hardlinks=1` a process may not link a file
/// it neither owns nor can write -- so the demote fails as soon as
/// the object is an identity's.  `safe_link()` takes an identity and
/// none of its four call sites passes one;  that is phase D.
///
/// **Run again 2026-10-22, after raise site 1 (ceph `b06489fb743`):**
/// the control column is clean -- phase D's demote carries the identity
/// -- and the owned column passes every step but one:  placement and
/// every bucket-state write succeed.  Multipart fails at create,
/// `could not create bucket .multipart_...: (13) Permission denied`:
/// staging is made by the gateway, which cannot write a bucket directory
/// the account owns at 0770.  Who owns staging is raise site 4's open
/// question.
///
/// Ignored until site 4 lands;  run it with `--run-ignored all`, and
/// every step prints its outcome beside the control's, so the table
/// says how much of (n) has arrived.
#[ignore = "multipart fails on the account's directory until (n) raise site 4 decides who owns staging"]
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[cfg_attr(feature = "fails_on_rados", ignore = "fails on rados")]
#[cfg_attr(feature = "fails_on_dbstore", ignore = "fails on dbstore")]
#[cfg_attr(feature = "fails_on_posix", ignore = "fails on posix")]
#[tokio::test]
async fn test_place_bucket_serves_an_account_owned_directory() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await || !impersonating().await {
        eprintln!("not an impersonating nsfs gateway;  nothing to check");
        return;
    }
    grant_admin().await;
    let me = owner();
    let me = me.as_str();

    let client = get_iam_root_s3client();
    let control = get_new_bucket_name();
    client.create_bucket().bucket(&control).send().await
        .expect("creating the control bucket");

    let owned = get_new_bucket_name();
    let r = unplaced(&[("op", "create"), ("name", &owned), ("mode", "0770"),
                       ("as", me)]).await;
    assert_eq!(r.status, 200, "making {owned} as the account: {}", r.body);
    let dir = as_json(&r).expect("the hint's reply is not JSON")["results"]["path"]
        .as_str().expect("the hint reports no path").to_string();
    /* Placement is the first step and is recorded, not asserted.  It
     * is what fails today, and asserting it here would hide every
     * step below it and skip the removal of a directory this process
     * cannot remove afterwards. */
    let placed = place(&owned, &dir, me).await;
    let placement = if placed.status == 200 {
        "ok".to_string()
    } else {
        format!("{} {}", placed.status, placed.body)
    };

    let rc = owners_sequence(&client, &control).await;
    let ro = owners_sequence(&client, &owned).await;

    eprintln!("{:40} {:24} owned (account 0770)", "step", "control (gateway)");
    eprintln!("{:40} {:24} {placement}", "place the bucket",
              "n/a, the gateway made it");
    for ((n, a), (_, b)) in rc.iter().zip(ro.iter()) {
        eprintln!("{n:40} {a:24} {b}");
    }

    let _ = s3_tests_rs::cleanup::nuke_bucket(&client, &control).await;
    let _ = s3_tests_rs::cleanup::nuke_bucket(&client, &owned).await;
    let removed = unplaced(&[("op", "remove"), ("name", &owned), ("as", me)]).await;

    let failures = |v: &[(&str, String)]| v.iter().filter(|(_, o)| o != "ok")
        .map(|(n, o)| format!("{n}: {o}")).collect::<Vec<_>>();
    assert!(failures(&rc).is_empty(), "the control failed: {:?}", failures(&rc));
    assert_eq!(placed.status, 200, "placing {dir}: {}", placed.body);
    assert!(failures(&ro).is_empty(),
            "the account's steps failed on its own directory: {:?}",
            failures(&ro));
    assert_eq!(removed.status, 200, "removing {dir}: {}", removed.body);
}
