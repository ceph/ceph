//! Serving a request as the POSIX identity that made it.
//!
//! These need a gateway started with `rgw_nsfs_impersonate` on and
//! the capability present — `rgw-vstart.sh --with-impersonation` —
//! so they skip when the backend reports impersonation off rather
//! than failing on every run that did not ask for it.
//!
//! The fixture ids come from that same option:  group `rgwfix`
//! (70010), `rgwalice` (70101) in it, `rgwbob` (70102) not.

use s3_tests_rs::admin::{nsfs_delete_identity, nsfs_put_identity,
                         SavedIdentities};
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
    remember_identities().await;
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

/// What the two users were bound to before this test touched them,
/// put back by `unbind_identities()`.  Each test is its own process, so
/// once per test.
static FOUND: tokio::sync::OnceCell<SavedIdentities> =
    tokio::sync::OnceCell::const_new();

async fn remember_identities() {
    FOUND.get_or_init(|| async {
        let cfg = get_config();
        SavedIdentities::take(&[&cfg.main_user_id, &cfg.alt_user_id]).await
    }).await;
}

/// Put both users back as `remember_identities()` found them.
async fn unbind_identities() {
    if let Some(found) = FOUND.get() {
        found.restore().await;
    }
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

/// A nested key:  its directories are the gateway's, its object the
/// identity's.
///
/// The directories a key needs are made by the gateway, `1777` under
/// impersonation (NSFS_IMPERSONATION.md 8a), so any identity can create
/// in them.  Only the object belongs to whoever wrote it.  alice's PUT
/// makes `a/b/`;  bob then writes beside her in a directory made during
/// her request, which only the gateway's mode permits.  Each is then
/// refused the other's object by the filesystem, S3 having granted both
/// reads -- the isolation test's argument, one level down.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_impersonation_owns_a_nested_key() {
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
    let hers = "a/b/alices-object";
    let his = "a/b/bobs-object";

    let put = alice.put_object()
        .bucket(&bucket).key(hers)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"alice wrote this"))
        .send().await;
    assert!(put.is_ok(), "alice could not write a nested key: {put:?}");

    /* let bob through at the S3 layer, so only the filesystem can
     * still say no */
    let resource = make_arn_resource(&format!("{bucket}/*"));
    let doc = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": [format!("arn:aws:iam:::user/{}",
                                          cfg.alt_user_id)]},
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": resource,
        }],
    }).to_string();
    let pol = alice.put_bucket_policy()
        .bucket(&bucket).policy(&doc).send().await;
    assert!(pol.is_ok(), "could not grant bob read and write: {pol:?}");

    let put = bob.put_object()
        .bucket(&bucket).key(his)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"bob wrote this"))
        .send().await;
    assert!(put.is_ok(),
        "bob could not write in a directory made during alice's request;  \
         the key's directories are not the gateway's: {put:?}");

    /* the controls:  each reads back what they wrote */
    let mine = alice.get_object().bucket(&bucket).key(hers).send().await;
    assert!(mine.is_ok(), "alice could not read her own nested object: {mine:?}");
    let mine = bob.get_object().bucket(&bucket).key(his).send().await;
    assert!(mine.is_ok(), "bob could not read his own nested object: {mine:?}");

    let theirs = bob.get_object().bucket(&bucket).key(hers).send().await;
    assert!(theirs.is_err(),
        "bob read alice's nested object although S3 was the only thing \
         permitting it;  it is not owned by her");
    let theirs = alice.get_object().bucket(&bucket).key(his).send().await;
    assert!(theirs.is_err(),
        "alice read bob's nested object;  it is not owned by him");

    unbind_identities().await;
}

/// A completed multipart upload belongs to the identity that uploaded it.
///
/// Completion assembles the parts into a new file, or links in a data
/// file the parts were written to, and publishes it.  Unless that runs
/// as the uploader the object is the gateway's, and the uploader is then
/// refused its own object.  The isolation test's argument again:  bob is
/// granted the read by bucket policy, so his refusal is the filesystem's.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_impersonation_owns_a_completed_upload() {
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
    let key = "alices-upload";

    /* three equal parts and a short one, so a layout that can link the
     * parts' data file in as the object does */
    let part = 64 * 1024;
    let up = s3_tests_rs::fixtures::multipart_upload(
        &alice, &bucket, key, 3 * part + 100, Some(part), None, None, None)
        .await;
    let done = alice.complete_multipart_upload()
        .bucket(&bucket).key(key).upload_id(&up.upload_id)
        .multipart_upload(aws_sdk_s3::types::CompletedMultipartUpload::builder()
                          .set_parts(Some(up.parts.clone())).build())
        .send().await;
    assert!(done.is_ok(), "alice could not complete her upload: {done:?}");

    let resource = make_arn_resource(&format!("{bucket}/*"));
    let doc = make_json_policy(
        "s3:GetObject", &resource,
        Some(serde_json::json!({"AWS": [format!("arn:aws:iam:::user/{}",
                                                cfg.alt_user_id)]})),
        None, None);
    let pol = alice.put_bucket_policy()
        .bucket(&bucket).policy(&doc).send().await;
    assert!(pol.is_ok(), "could not grant bob s3:GetObject: {pol:?}");

    /* the control, and the case that failed:  alice reads her own
     * completed object, all of it */
    let mine = alice.get_object().bucket(&bucket).key(key).send().await;
    assert!(mine.is_ok(),
        "alice could not read the object her upload completed;  it was \
         made as the gateway: {mine:?}");
    let body = mine.unwrap().body.collect().await
        .expect("reading the body").into_bytes();
    assert_eq!(&body[..], &up.data[..], "the completed object's bytes differ");

    let theirs = bob.get_object().bucket(&bucket).key(key).send().await;
    assert!(theirs.is_err(),
        "bob read alice's completed upload although S3 was the only thing \
         permitting it;  it is not owned by her");

    unbind_identities().await;
}

/// A copy is the requester's:  it reads the source as them, and it
/// belongs to them.
///
/// The read half is the one with teeth.  bob is granted `s3:GetObject`
/// on alice's object by bucket policy and refused it by the filesystem,
/// as in the isolation test.  A copy that read the source as the gateway
/// would hand him the same bytes as an object of his own;  so his copy
/// of it must be refused too.  The ownership half:  alice's copy of her
/// own object is hers, so she reads it and bob, granted it by policy, is
/// refused.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_impersonation_copies_as_the_requester() {
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
    let bobs_bucket = get_new_bucket(Some(&bob)).await;
    let key = "alices-object";

    let put = alice.put_object()
        .bucket(&bucket).key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"alice wrote this"))
        .send().await;
    assert!(put.is_ok(), "alice could not write her own object: {put:?}");

    let resource = make_arn_resource(&format!("{bucket}/*"));
    let doc = make_json_policy(
        "s3:GetObject", &resource,
        Some(serde_json::json!({"AWS": [format!("arn:aws:iam:::user/{}",
                                                cfg.alt_user_id)]})),
        None, None);
    let pol = alice.put_bucket_policy()
        .bucket(&bucket).policy(&doc).send().await;
    assert!(pol.is_ok(), "could not grant bob s3:GetObject: {pol:?}");

    /* the premise:  the filesystem refuses bob the read S3 permits */
    let theirs = bob.get_object().bucket(&bucket).key(key).send().await;
    assert!(theirs.is_err(),
        "bob read alice's object;  the premise of this test does not hold");

    /* the control:  bob can copy at all -- his own object, in his bucket */
    let put = bob.put_object()
        .bucket(&bobs_bucket).key("bobs-object")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"bob wrote this"))
        .send().await;
    assert!(put.is_ok(), "bob could not write his own object: {put:?}");
    let cp = bob.copy_object()
        .bucket(&bobs_bucket).key("bobs-copy")
        .copy_source(format!("{bobs_bucket}/bobs-object"))
        .send().await;
    assert!(cp.is_ok(), "bob could not copy his own object: {cp:?}");

    /* and cannot copy what the filesystem will not let him read */
    let stolen = bob.copy_object()
        .bucket(&bobs_bucket).key("stolen")
        .copy_source(format!("{bucket}/{key}"))
        .send().await;
    assert!(stolen.is_err(),
        "bob copied an object the filesystem refuses him;  the copy read \
         the source as the gateway");

    /* alice's copy of her own object is hers */
    let cp = alice.copy_object()
        .bucket(&bucket).key("alices-copy")
        .copy_source(format!("{bucket}/{key}"))
        .send().await;
    assert!(cp.is_ok(), "alice could not copy her own object: {cp:?}");
    let mine = alice.get_object().bucket(&bucket).key("alices-copy").send().await;
    assert!(mine.is_ok(),
        "alice could not read her own copy;  it was made as the gateway: \
         {mine:?}");
    let theirs = bob.get_object().bucket(&bucket).key("alices-copy").send().await;
    assert!(theirs.is_err(),
        "bob read alice's copy although S3 was the only thing permitting \
         it;  it is not owned by her");

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

/// NooBaa's guard on where an account's buckets go, enacted
/// (DECISIONS.md 2026-10-14):  NooBaa makes a bucket's directory as the
/// account, so an account that cannot write its `new_buckets_path`
/// cannot create there.  nsfs makes the directory as the gateway, so it
/// checks the account's access as the account before it does.
///
/// alice is mapped with `/usr` as her `new_buckets_path`, which is
/// root-owned and not writable by uid 70101:  refused.  (Not `/`, which
/// nsfs refuses to store as a path at all.)  The control is
/// `/tmp`, which anyone may write:  created.  Neither depends on a
/// fixture directory.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_new_buckets_path_requires_the_accounts_access() {
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
    remember_identities().await;
    let bind = |nbp: &'static str| {
        let me = cfg.main_user_id.clone();
        async move {
            let r = nsfs_put_identity(&[
                ("identity", me.as_str()),
                ("uid", UID_ALICE),
                ("gid", UID_ALICE),
                ("new_buckets_path", nbp),
            ]).await;
            assert_eq!(r.status, 200, "binding alice failed: {}", r.body);
        }
    };
    let status = |r: Result<_, aws_sdk_s3::error::SdkError<
        aws_sdk_s3::operation::create_bucket::CreateBucketError>>| match r {
        Ok(_) => 200,
        Err(e) => e.raw_response().map_or(0, |r| r.status().as_u16()),
    };

    bind("/usr").await;
    let refused = s3_tests_rs::fixtures::get_new_bucket_name();
    let r = status(alice.create_bucket().bucket(&refused).send().await);

    bind("/tmp").await;
    let allowed = s3_tests_rs::fixtures::get_new_bucket_name();
    let ok = status(alice.create_bucket().bucket(&allowed).send().await);
    if ok == 200 {
        let _ = alice.delete_bucket().bucket(&allowed).send().await;
    }
    unbind_identities().await;

    assert_eq!(r, 403, "alice created a bucket where she cannot write");
    assert_eq!(ok, 200, "alice was refused where she can write");
}

/// A nested `run_as` cannot run work as the wrong identity.
///
/// `run_as` takes a fast path when its thread is already
/// impersonating, because enqueueing to the helper pool from a pool
/// thread can deadlock.  That path used to be taken for *any*
/// identity, so a nested call asking for a different one ran as the
/// outer one and returned success -- the operation worked, under the
/// wrong uid, and nothing said so.  The multipart cache's eviction
/// reaches `run_as` that way.
///
/// Both polarities, from one hint call, and each asserted on the uid
/// the inner call actually observed rather than on its status alone:
/// a nested call for the same identity must still run *and see that
/// identity*, and one for another identity must be refused with
/// `-EDEADLK`.  Asserting only the refusal would pass against a
/// `run_as` that had simply stopped working.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_nested_run_as_refuses_a_different_identity() {
    if !nsfs_only().await {
        return;
    }
    let cfg = get_config();
    let r = s3_tests_rs::admin::driver_hint_results(
        "nested-identity",
        &[("outer", &s3_tests_rs::admin::q(&cfg.main_user_id)),
          ("inner", &s3_tests_rs::admin::q(&cfg.alt_user_id))]).await;
    let m = match r {
        Some(m) => m,
        /* no admin access, or a driver without the hint */
        None => return,
    };
    match m.get("verdict").map(String::as_str) {
        Some("ran") => {}
        /* impersonation off, or the fixture's users carry no identity:
         * the nested path is unreachable, so there is nothing to assert */
        _ => return,
    }

    let outer = m.get("outer_uid").cloned().unwrap_or_default();
    let inner = m.get("inner_uid").cloned().unwrap_or_default();
    assert_ne!(outer, inner,
               "the two identities must differ for this to test anything");

    assert_eq!(m.get("same").map(String::as_str), Some(outer.as_str()),
               "a nested run_as for the identity in force must run, and see \
                it:  outer={outer} inner={inner} results={m:?}");
    assert_eq!(m.get("different").map(String::as_str), Some("-35"),
               "a nested run_as for another identity must be refused with \
                -EDEADLK, not run as the outer one:  outer={outer} \
                inner={inner} results={m:?}");
}
