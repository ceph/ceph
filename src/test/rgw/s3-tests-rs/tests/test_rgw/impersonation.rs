//! Serving a request as the POSIX identity that made it.
//!
//! These need a gateway started with `rgw_nsfs_impersonate` on and
//! the capability present — `rgw-vstart.sh --with-impersonation` —
//! so they skip when the backend reports impersonation off rather
//! than failing on every run that did not ask for it.
//!
//! The fixture ids come from that same option:  group `rgwfix`
//! (70010), `rgwalice` (70101) in it, `rgwbob` (70102) not.

use s3_tests_rs::access_model::{Access, Class};
use s3_tests_rs::admin::{nsfs_delete_identity, nsfs_put_identity,
                         SavedIdentities};
use s3_tests_rs::client::{get_alt_client, get_client};
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};
use s3_tests_rs::hints::{clear_hint, with_hint};
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

    /* The subject is the permission check, and it holds at every
     * access model:  alice's object is alice's, so bob's read of it is
     * denied.  What the gateway then does about that is the model's
     * business, which `judge` asserts. */
    let access = Access::ask(Class::Data, &bucket, key,
                             &cfg.alt_user_id, "r").await;
    assert!(access.filesystem_refuses(),
        "the filesystem permits bob to read an object alice wrote;  \
         did not cover creation, so the object is not owned by her");
    access.judge(bob.get_object().bucket(&bucket).key(key).send().await);

    unbind_identities().await;
}

/// Which authority decides an object's bytes, asserted rather than
/// inferred from whichever tests happen to read data.
///
/// `rgw_nsfs_access_model` decides what the gateway does with an
/// operation S3 authorised and the permission check denied.  For an
/// object's tags and ACL, `s3-metadata` and `s3-native` perform it
/// and `posix-strict` refuses;  for the object's bytes, only
/// `s3-native` performs it, which is what the posix driver and every
/// other RGW backend do.  This test asserts the data line at the
/// configured model, then switches to `s3-native` and asserts that
/// the same read succeeds -- so the refusals are evidence of the
/// model rather than of the fixture.
///
/// The data line is asserted here because it has already been broken
/// once, at a model that was supposed to refuse.  On 2026-10-16 the
/// first implementation opened the object as the gateway for an
/// attribute read and left that descriptor cached on the entry;  the
/// data path found it already open and served bob bytes the
/// he had been denied.  The guard written for that case never
/// ran -- it keyed on a flag the real path did not set -- so the only
/// thing that caught it was an unrelated acl test.  This test exists
/// so that the next person to widen a retry here is told by name.
///
/// Both classes matter.  The metadata half must *succeed* under
/// `s3-metadata`, or a gateway that simply refuses everything would
/// pass the data half and look correct.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_data_follows_the_access_model() {
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
    let body = b"alice wrote this";

    alice.put_object()
        .bucket(&bucket).key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(body))
        .send().await
        .expect("alice could not write her own object");

    let tagging = aws_sdk_s3::types::Tagging::builder()
        .set_tag_set(Some(vec![aws_sdk_s3::types::Tag::builder()
                               .key("k").value("v").build().unwrap()]))
        .build().unwrap();
    alice.put_object_tagging()
        .bucket(&bucket).key(key).tagging(tagging).send().await
        .expect("alice could not tag her own object");

    /* S3 grants bob everything;  only the filesystem can still object */
    let resource = make_arn_resource(&format!("{bucket}/*"));
    let doc = make_json_policy(
        "s3:*", &resource,
        Some(serde_json::json!({"AWS": [format!("arn:aws:iam:::user/{}",
                                                cfg.alt_user_id)]})),
        None, None);
    alice.put_bucket_policy().bucket(&bucket).policy(&doc).send().await
        .expect("could not grant bob s3:*");

    /* the control: without it, a denial below could just mean the
     * object is unreadable by anyone */
    alice.get_object().bucket(&bucket).key(key).send().await
        .expect("alice could not read her own object");

    let model = s3_tests_rs::features::cross_account_metadata().await;

    /* the metadata half -- succeeds only where the model says so.
     * Without it a gateway that refused everything would pass the
     * data half and look correct. */
    let tags = s3_tests_rs::unless_posix_governs_metadata!(
        model,
        bob.get_object_tagging().bucket(&bucket).key(key).send().await);
    if let Some(t) = tags {
        assert_eq!(t.tag_set().len(), 1,
            "bob read the tags but not the ones alice wrote");
    }

    /* the data half -- the shared invariant, so that adding a data
     * operation to it covers every test that calls it */
    let data_model = s3_tests_rs::features::cross_account_data().await;
    let tested = s3_tests_rs::access_model::assert_data_follows_the_filesystem(
        &alice, &bob, &cfg.alt_user_id, &bucket, key, body, data_model).await;
    if !tested {
        eprintln!("either the configured model permits the gateway to \
                   serve the bytes itself, or the filesystem permits bob \
                   this object;  there is no line to test");
        unbind_identities().await;
        return;
    }

    /* A positive control:  the same client, the same call, on an
     * object bob is entitled to.
     *
     * Without it, every refusal above is equally explained by "bob
     * cannot read anything" -- a broken client, a broken fixture, a
     * gateway refusing the whole bucket.  A test that only ever sees
     * denials cannot tell those apart from the thing it means to
     * assert. */
    let bobs_key = "bobs-own-object";
    bob.put_object()
        .bucket(&bucket).key(bobs_key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"bob wrote this"))
        .send().await
        .expect("bob could not write his own object;  the fixture is wrong, \
                 not the model");
    bob.get_object().bucket(&bucket).key(bobs_key).send().await
        .expect("bob could not read his own object;  a refusal above says \
                 nothing");

    /* The other polarity, and why it is needed.
     *
     * An assertion that cannot be made to fail asserts nothing.
     * Under the configured model the gateway must not have performed
     * any of the data operations above itself, and the counts are
     * what distinguish "refused" from "never asked";  switching to
     * `s3-native` and seeing the same read succeed is what makes the
     * refusals evidence of the model rather than of the fixture.
     *
     * Restored before the assertions, so a failure cannot leave the
     * run measuring a model nobody configured. */
    let before = s3_tests_rs::access_model::served_counts().await;
    if before.is_none() {
        eprintln!("no access-model hint;  the counters cannot be checked");
        unbind_identities().await;
        return;
    }
    let (_, data_before) = before.unwrap();

    /* now permit it, deliberately, and confirm the gateway *does*
     * serve the bytes -- and says that is why.  An invariant that
     * cannot be violated on demand is an invariant nothing has
     * tested:  the first guard written for this line never ran at
     * all, because it keyed on a flag the real path did not set. */
    let had = s3_tests_rs::access_model::set_access_model("s3-native")
        .await;
    assert!(had.is_some(), "the access model could not be switched");

    let permitted = bob.get_object().bucket(&bucket).key(key).send().await;
    let after = s3_tests_rs::access_model::served_counts().await;
    s3_tests_rs::access_model::restore_access_model().await;

    assert!(permitted.is_ok(),
        "s3-native did not let bob read the bytes, so the refusals \
         asserted above are not evidence:  they would hold whatever the \
         model said");
    let (_, data_after) = after.expect("the counters went away");
    assert!(data_after > data_before,
        "bob's read succeeded but the gateway did not record performing \
         it itself ({data_before} -> {data_after});  the success is not \
         attributable to the model, so the fixture is not proving what \
         it claims");

    /* and the configured model is back */
    let after_restore =
        s3_tests_rs::access_model::model_for("data").await;
    assert_eq!(after_restore.as_deref(), Some("refuse"),
        "the configured model was not restored;  every later test in this \
         run is measuring something nobody configured");
    let refused_again = bob.get_object().bucket(&bucket).key(key).send().await;
    assert!(refused_again.is_err(),
        "the restore did not take:  bob can still read alice's bytes");

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

    /* each is owned by whoever wrote it, which the filesystem says
     * and no access model changes */
    let hers_to_bob = Access::ask(Class::Data, &bucket, hers,
                                  &cfg.alt_user_id, "r").await;
    assert!(hers_to_bob.filesystem_refuses(),
        "the filesystem permits bob to read alice's nested object;  it is \
         owned by her");
    hers_to_bob.judge(
        bob.get_object().bucket(&bucket).key(hers).send().await);

    /* The other direction is the permission check and nothing else.  The policy above grants *bob* s3:GetObject;  alice has no
     * grant on bob's object, so a GET of it is refused by RGW's own
     * authorization before any file is opened -- which would pass with
     * impersonation off and prove nothing.  So ask the kernel and do
     * not read:  there is no S3-authorised operation here to judge. */
    let his_to_alice = Access::ask(Class::Data, &bucket, his,
                                   &cfg.main_user_id, "r").await;
    assert!(his_to_alice.filesystem_refuses(),
        "the filesystem permits alice to read bob's nested object;  it is \
         owned by him");

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

    let access = Access::ask(Class::Data, &bucket, key,
                             &cfg.alt_user_id, "r").await;
    assert!(access.filesystem_refuses(),
        "the filesystem permits bob to read the object alice's upload \
         it was made as the gateway");
    access.judge(bob.get_object().bucket(&bucket).key(key).send().await);

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

    /* the premise:  bob's read is denied by the object's mode, though
     * S3 permits it */
    let access = Access::ask(Class::Data, &bucket, key,
                             &cfg.alt_user_id, "r").await;
    assert!(access.filesystem_refuses(),
        "the filesystem permits bob to read alice's object;  the premise \
         test does not hold");
    access.judge(bob.get_object().bucket(&bucket).key(key).send().await);

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

    /* and the copy follows the same check as the read:  a copy is a
     * read of the source, so a model that refuses bob the bytes
     * refuses him the copy, and one that serves them serves it */
    access.judge(bob.copy_object()
        .bucket(&bobs_bucket).key("stolen")
        .copy_source(format!("{bucket}/{key}"))
        .send().await);

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
    let hers = Access::ask(Class::Data, &bucket, "alices-copy",
                           &cfg.alt_user_id, "r").await;
    assert!(hers.filesystem_refuses(),
        "the filesystem permits bob to read alice's copy;  it is not owned \
         by her");
    hers.judge(
        bob.get_object().bucket(&bucket).key("alices-copy").send().await);

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

    let cfg = get_config();
    let alice = get_client();
    let bucket = "impersonate-fixture";

    /* with the gating group */
    bind_identities(Some(GID_SHARED)).await;
    let open = alice.get_object().bucket(bucket).key("open.txt")
        .send().await;
    assert!(open.is_ok(),
        "the control object was unreadable;  the fixture or the gateway \
         is wrong, not the group vector: {open:?}");
    /* the vector's effect is in the permission check, which is
     * the same answer at every access model -- so this reads the
     * vector itself and not what the gateway does about it */
    let gated = Access::ask(Class::Data, bucket, "grouped.txt",
                            &cfg.main_user_id, "r").await;
    assert!(!gated.filesystem_refuses(),
        "holding gid {GID_SHARED} did not grant the read");
    gated.judge(alice.get_object().bucket(bucket).key("grouped.txt")
        .send().await);

    /* same identity, no group */
    bind_identities(None).await;
    let open = alice.get_object().bucket(bucket).key("open.txt")
        .send().await;
    assert!(open.is_ok(),
        "the control object became unreadable without the group; \
         something other than the group vector changed: {open:?}");
    let gated = Access::ask(Class::Data, bucket, "grouped.txt",
                            &cfg.main_user_id, "r").await;
    assert!(gated.filesystem_refuses(),
        "the filesystem permitted the read without the group in the \
         vector — an absent list left the gateway's own groups in place \
         rather than clearing");
    gated.judge(alice.get_object().bucket(bucket).key("grouped.txt")
        .send().await);

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

/// A raised capability is effective for one call and gone after it,
/// and cannot be raised while impersonating.
///
/// The raise is the whole of what (n) adds to the gateway's
/// authority, and the only site that uses it -- the promote's residue
/// -- needs a version file owned by a uid no identity row claims, so
/// no S3 sequence reaches it.  The hint asks the primitive directly
/// and reports what the kernel said.
///
/// Four assertions, and the raise itself is the least of them.  That
/// the capability is **not** effective before the call is what the
/// startup lowering is for, and a gateway that never lowered would
/// pass every other check here while serving every request with
/// `CAP_DAC_OVERRIDE`.  That it is gone after the call is the one
/// that would otherwise leak:  a thread returning to the frontend's
/// pool still holding it serves everything that follows with it.  The
/// last is the refusal while impersonating, where the capability
/// would be the *identity's* rather than the gateway's.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_raise_is_effective_for_one_call_and_dropped_after() {
    if !nsfs_only().await {
        return;
    }
    let cfg = get_config();
    let r = s3_tests_rs::admin::driver_hint_results(
        "raise",
        &[("cap", "promote"),
          ("user", &s3_tests_rs::admin::q(&cfg.main_user_id))]).await;
    let m = match r {
        Some(m) => m,
        /* no admin access, or a driver without the hint */
        None => return,
    };

    match m.get("result").map(String::as_str) {
        Some("ran") => {
            assert_eq!(
                m.get("before").map(String::as_str), Some("0"),
                "the capability was effective before the raise, so the \
                 startup lowering did not happen and every ordinary \
                 request holds it:  results={m:?}");
            assert_eq!(
                m.get("inside").map(String::as_str), Some("1"),
                "the raised capability was not effective inside the \
                 call:  results={m:?}");
            assert_eq!(
                m.get("after").map(String::as_str), Some("0"),
                "a raised capability outlived the call that raised it, \
                 so every later request on this thread holds it:  \
                 results={m:?}");
        }
        /* a gateway started without the file capabilities cannot raise
         * one.  That is the deployment's answer, not the primitive's,
         * and the two states that do not need the capability still
         * hold. */
        Some("not-permitted") => {
            assert_eq!(
                m.get("before").map(String::as_str), Some("0"),
                "the capability is effective on an ordinary request \
                 thread:  results={m:?}");
            assert_eq!(
                m.get("after").map(String::as_str), Some("0"),
                "a refused raise left the capability effective:  \
                 results={m:?}");
        }
        _ => return,
    }

    match m.get("impersonating").map(String::as_str) {
        /* -EPERM:  raising on an impersonating thread would hand the
         * capability to the identity */
        Some("-1") => {}
        /* impersonation off, or the fixture's user carries no identity:
         * that arm is unreachable, and the three above still hold */
        Some("not-impersonating") | Some("no-identity") | None => {}
        other => panic!(
            "a raise on an impersonating thread returned {other:?} \
             rather than -EPERM:  results={m:?}"),
    }
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
    match m.get("result").map(String::as_str) {
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

/// Is the gateway producing mtime-ino etags rather than digests?
///
/// Asked of the gateway, not inferred:  `rgw_non_md5_etag` is off by
/// default, and an etag's shape is not something a client has a
/// second opinion about.
async fn non_md5_etag() -> bool {
    match features().await {
        Some(f) => f.get_bool("non_md5_etag").unwrap_or(false),
        None => false,
    }
}


/// The non-md5 etag is *recorded on the object*, not just reported.
///
/// Under `rgw_non_md5_etag` the etag is the mtime-ino string, which
/// cannot be known until the file has been linked and stat'ed -- so
/// it is written after `NSFSAtomicWriter::complete()` has already
/// persisted the attribute map, by an `fsetxattr` of its own inside
/// `NSFSObject::link_temp_file()`.  That write used to run on the
/// calling thread with its result discarded, so under impersonation
/// the kernel refused it on a 0660 file the gateway does not own.
/// What the object then keeps is not a digest -- with this option on
/// nothing computes one -- but `RGWPutObj::execute()`'s opaque
/// `mtime-<ns>-req-<id>` form, which nsfs is supposed to replace.
/// The client is handed the mtime-ino one regardless, because
/// `RGWPutObj` reads the etag back out of the map the writer returns
/// and the writer puts it there whether or not the disk write took.
///
/// A PUT and a HEAD are therefore the whole test:  the two etags must
/// agree.  Nothing else in the suite can see this -- the option is
/// off in every other run -- which is why `--with-non-md5-etag`
/// exists.
///
/// **Both polarities, against one build.**  `inject-etag-as-gateway`
/// reproduces the defect in the running gateway, so the test can be
/// watched failing without deleting the fix and rebuilding.
/// Asserting only that the etags agree
/// would pass just as well against a gateway that had stopped
/// synthesizing them at all.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_the_non_md5_etag_is_recorded_as_the_requester() {
    const INJECT: &str = "inject-etag-as-gateway";
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
    if !non_md5_etag().await {
        eprintln!("rgw_non_md5_etag is off;  start with \
                   --with-non-md5-etag");
        return;
    }
    bind_identities(None).await;

    let alice = get_client();
    let bucket = get_new_bucket(Some(&alice)).await;

    async fn put_then_head(c: &aws_sdk_s3::Client, bucket: &str, key: &str)
        -> Result<(String, String), String>
    {
        let put = c.put_object()
            .bucket(bucket).key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"bytes"))
            .send().await
            .map_err(|e| format!("put {key} failed: {e:?}"))?;
        let head = c.head_object()
            .bucket(bucket).key(key)
            .send().await
            .map_err(|e| format!("head {key} failed: {e:?}"))?;
        Ok((put.e_tag().unwrap_or_default().to_string(),
            head.e_tag().unwrap_or_default().to_string()))
    }

    /* On the way in, because a hint is gateway-wide:  one left behind
     * by a test the harness killed would otherwise decide this one's
     * result.  `with_hint` covers the panicking path;  nothing covers
     * a process that dies without unwinding. */
    clear_hint(INJECT).await;
    let fixed = put_then_head(&alice, &bucket, "recorded").await;

    let injected = with_hint(INJECT, &[("enable", "true")],
                             put_then_head(&alice, &bucket, "diverged")).await;

    unbind_identities().await;

    let (put_etag, head_etag) = fixed.unwrap_or_else(|e| panic!("{e}"));
    let (bad_put, bad_head) = injected.unwrap_or_else(|e| panic!("{e}"));

    assert!(!put_etag.is_empty(),
            "the PUT returned no etag at all");
    assert_eq!(put_etag, head_etag,
               "the etag the client was given is not the one recorded on \
                the object:  PUT said {put_etag}, HEAD says {head_etag}");

    assert!(!bad_head.is_empty(),
            "the HEAD under injection returned no etag at all, so the \
             comparison below would hold for the wrong reason");
    assert_ne!(bad_put, bad_head,
               "with inject-etag-as-gateway on, the attribute write should \
                be refused and the object should keep the opaque etag \
                RGWPutObj wrote -- \
                the two agreeing means this test cannot fail:  PUT said \
                {bad_put}, HEAD says {bad_head}");
}
