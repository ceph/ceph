//! Serving a request as the POSIX identity that made it.
//!
//! These need a gateway started with `rgw_nsfs_impersonate` on and
//! the capability present — `rgw-vstart.sh --with-impersonation` —
//! so they skip when the backend reports impersonation off rather
//! than failing on every run that did not ask for it.
//!
//! The fixture ids come from that same option:  group `rgwfix`
//! (70010), `rgwalice` (70101) in it, `rgwbob` (70102) not.

use s3_tests_rs::admin::{nsfs_delete_identity, nsfs_put_identity};
use s3_tests_rs::client::{get_alt_client, get_client};
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};
use s3_tests_rs::policy::{make_arn_resource, make_json_policy};

const GID_SHARED: &str = "70010";
const UID_ALICE: &str = "70101";
const UID_BOB: &str = "70102";

async fn nsfs_only() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs"),
        None => false,
    }
}

/// Map the two S3 users onto the fixture's POSIX identities.
///
/// `alice` carries the gating group, `bob` carries none — which is
/// the difference the group test turns on, and is also the
/// `Credentials::groups` empty-vector case from the resolver.
async fn bind_identities(alice_groups: Option<&str>) {
    let cfg = get_config();
    let mut a: Vec<(&str, &str)> = vec![
        ("identity", &cfg.main_user_id),
        ("uid", UID_ALICE),
        ("gid", UID_ALICE),
    ];
    if let Some(g) = alice_groups {
        a.push(("groups", g));
    }
    let r = nsfs_put_identity(&a).await;
    assert_eq!(r.status, 200, "binding the main user failed: {}", r.body);

    let r = nsfs_put_identity(&[
        ("identity", &cfg.alt_user_id),
        ("uid", UID_BOB),
        ("gid", UID_BOB),
    ]).await;
    assert_eq!(r.status, 200, "binding the alt user failed: {}", r.body);
}

async fn unbind_identities() {
    let cfg = get_config();
    let _ = nsfs_delete_identity(&cfg.main_user_id).await;
    let _ = nsfs_delete_identity(&cfg.alt_user_id).await;
}

/// Is the gateway actually impersonating?
///
/// Asked of the gateway rather than inferred.  The credential
/// endpoint resolves a stored record whether or not the switch is
/// on, so a successful resolve there says nothing about it.
async fn impersonating() -> bool {
    match features().await {
        Some(f) => f.get_bool("impersonate").unwrap_or(false),
        None => false,
    }
}

/// Identity isolation:  what one identity writes, another cannot
/// read — because the *filesystem* says so, not because S3 does.
///
/// nsfs creates owner-only, so under impersonation a bucket made by
/// alice is `alice:alice 0700` and its objects `alice:alice 0600`.
/// bob is then refused by the kernel with nothing seeded.
///
/// The bucket policy is the whole reason this test means anything.
/// Without it bob is refused by RGW's own authorization before any
/// file is opened, and the test would pass identically with
/// impersonation off — proving nothing.  Granting bob `s3:GetObject`
/// removes the S3 answer, so a denial can only come from POSIX.
///
/// It also proves impersonation covers *creation*:  if only reads
/// carried a personality, the object would be written owned by the
/// gateway and alice's own read-back would be what failed.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_impersonation_isolates_identities() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    if !impersonating().await {
        eprintln!("impersonation is not enabled;  start with \
                   --with-impersonation");
        return;
    }
    let cfg = get_config();
    bind_identities(None).await;

    let alice = get_client();
    let bob = get_alt_client();
    let bucket = get_new_bucket(Some(&alice)).await;
    let key = "alices-object";

    let put = alice.put_object()
        .bucket(&bucket).key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"alice wrote this"))
        .send().await;
    assert!(put.is_ok(), "alice could not write her own object: {put:?}");

    /* let bob through at the S3 layer, so only the filesystem can
     * still say no */
    let resource = make_arn_resource(&format!("{bucket}/*"));
    let doc = make_json_policy(
        "s3:GetObject", &resource,
        Some(serde_json::json!({"AWS": [format!("arn:aws:iam:::user/{}",
                                                cfg.alt_user_id)]})),
        None, None);
    let pol = alice.put_bucket_policy()
        .bucket(&bucket).policy(&doc).send().await;
    assert!(pol.is_ok(), "could not grant bob s3:GetObject: {pol:?}");

    /* the control:  alice reads back what she wrote.  Without this a
     * denial for bob would be indistinguishable from the whole path
     * being broken. */
    let mine = alice.get_object().bucket(&bucket).key(key).send().await;
    assert!(mine.is_ok(), "alice could not read her own object: {mine:?}");

    let theirs = bob.get_object().bucket(&bucket).key(key).send().await;
    assert!(theirs.is_err(),
        "bob read an object owned by alice although S3 was the only thing \
         permitting it;  the filesystem identity is not in effect");

    unbind_identities().await;
}

/// The group vector, which is the case worth the fixture.
///
/// `grouped.txt` is `0040 root:rgwfix` — owner and other bits empty
/// on purpose, so a supplementary group is the only thing that can
/// grant a read.  alice with 70010 in her list gets in;  alice
/// without it does not, and that second leg is the same identity,
/// so nothing but the group vector differs between them.
///
/// It is also the clear-versus-inherit case:  with no list the
/// resolver yields an empty vector that must be *installed*.  If it
/// instead left the gateway's own groups in place, the second leg
/// would pass when it should fail.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_impersonation_honours_the_group_vector() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    if !impersonating().await {
        eprintln!("impersonation is not enabled;  start with \
                   --with-impersonation");
        return;
    }

    let alice = get_client();
    let bucket = "impersonate-fixture";

    /* with the gating group */
    bind_identities(Some(GID_SHARED)).await;
    let open = alice.get_object().bucket(bucket).key("open.txt")
        .send().await;
    assert!(open.is_ok(),
        "the control object was unreadable;  the fixture or the gateway \
         is wrong, not the group vector: {open:?}");
    let gated = alice.get_object().bucket(bucket).key("grouped.txt")
        .send().await;
    assert!(gated.is_ok(),
        "holding gid {GID_SHARED} did not grant the read: {gated:?}");

    /* same identity, no group */
    bind_identities(None).await;
    let open = alice.get_object().bucket(bucket).key("open.txt")
        .send().await;
    assert!(open.is_ok(),
        "the control object became unreadable without the group; \
         something other than the group vector changed: {open:?}");
    let gated = alice.get_object().bucket(bucket).key("grouped.txt")
        .send().await;
    assert!(gated.is_err(),
        "the object was read without the group in the vector — an absent \
         list left the gateway's own groups in place rather than clearing");

    unbind_identities().await;
}

/// An identity with no record is refused, not served as the gateway.
///
/// Section 4 of the design makes this an invariant rather than a
/// policy:  a valid account always has a POSIX record, so one
/// without is a provisioning fault.  The alternative — falling back
/// to the gateway's own credentials — would give an unprovisioned
/// user the daemon's filesystem reach while every provisioned one
/// was confined, which is the escalation the whole design refuses.
///
/// Both polarities, and in this order:  the write must succeed with
/// a record, so that its failure without one is attributable to the
/// record and not to anything else about the request.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_impersonation_refuses_an_unprovisioned_identity() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    if !impersonating().await {
        eprintln!("impersonation is not enabled;  start with \
                   --with-impersonation");
        return;
    }
    let cfg = get_config();
    let alice = get_client();

    bind_identities(None).await;
    let bucket = get_new_bucket(Some(&alice)).await;

    /* the control */
    let ok = alice.put_object()
        .bucket(&bucket).key("provisioned")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send().await;
    assert!(ok.is_ok(), "a provisioned identity could not write: {ok:?}");

    /* take the record away and try the same thing */
    let _ = nsfs_delete_identity(&cfg.main_user_id).await;
    let denied = alice.put_object()
        .bucket(&bucket).key("unprovisioned")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send().await;
    assert!(denied.is_err(),
        "an identity with no POSIX record was served;  the request ran as \
         the gateway rather than being refused");

    /* and reads too, not only writes */
    let read = alice.get_object().bucket(&bucket).key("provisioned")
        .send().await;
    assert!(read.is_err(),
        "an identity with no POSIX record read an object");

    unbind_identities().await;
}
