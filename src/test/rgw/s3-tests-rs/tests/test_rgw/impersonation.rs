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
use s3_tests_rs::admin::{driver_hint_results, nsfs_delete_identity, nsfs_put_identity};
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::{cross_account_data, features};
use s3_tests_rs::fixtures::{
    check_configure_versioning_retry, get_new_bucket, PrivateUser, TestGuard,
};
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

/// alice and bob, each a user of this module's own, bound to the
/// fixture's POSIX identities.
///
/// Not the suite's `main`/`alt`.  This module used to rebind *their*
/// identity rows while 590 other tests authenticate as them, and a row
/// changed for the length of one test is a 403 for whoever else is
/// mid-request -- a correct 403, since no identity means denial, which
/// is why it read as noise for a week.
///
/// `tag` names the test, not the module:  nextest runs each test in
/// its own process, so two of them would otherwise create and delete
/// the same private user and this module would interfere with itself.
///
/// `alice` carries the gating group, `bob` carries none — which is
/// the difference the group test turns on, and is also the
/// `Credentials::groups` empty-vector case from the resolver.
async fn alice_and_bob(tag: &str, alice_groups: Option<&str>)
    -> Option<(PrivateUser, PrivateUser)> {
    let alice = PrivateUser::create(&format!("imp{tag}a")).await?;
    let bob = PrivateUser::create(&format!("imp{tag}b")).await?;
    if !alice.bind(UID_ALICE, UID_ALICE, alice_groups).await {
        eprintln!("binding alice failed");
        return None;
    }
    if !bob.bind(UID_BOB, UID_BOB, None).await {
        eprintln!("binding bob failed");
        return None;
    }
    Some((alice, bob))
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
/// bob is then denied by the kernel with nothing seeded.
///
/// The bucket policy is the whole reason this test means anything.
/// Without it bob is denied by RGW's own authorization before any
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
    let Some((alice_u, bob_u)) = alice_and_bob("iso", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let (alice, bob) = (&alice_u.client, &bob_u.client);
    let bucket = get_new_bucket(Some(alice)).await;
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
                                                bob_u.id)]})),
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
     * business, which `expect` asserts. */
    let access = Access::ask(Class::Data, &bucket, key,
                             &bob_u.id, "r").await;
    assert!(access.denied(),
        "the filesystem permits bob to read an object alice wrote;  \
         did not cover creation, so the object is not owned by her");
    access.expect(bob.get_object().bucket(&bucket).key(key).send().await);

}

/// Which authority decides an object's bytes, asserted rather than
/// inferred from whichever tests happen to read data.
///
/// `rgw_nsfs_access_model` decides what the gateway does with an
/// operation S3 authorised and the permission check denied.
/// `posix-strict` denies it, for the object's bytes and for its tags
/// and ACL alike;  `s3-native` performs it, which is what the posix
/// driver and every other RGW backend do.  This test asserts the data line at the
/// configured model, then switches to `s3-native` and asserts that
/// the same read succeeds -- so the denials are evidence of the
/// model rather than of the fixture.
///
/// The data line is asserted here because it has already been broken
/// once, at a model that was supposed to deny.  On 2026-10-16 the
/// first implementation opened the object as the gateway for an
/// attribute read and left that descriptor cached on the entry;  the
/// data path found it already open and served bob bytes the
/// he had been denied.  The guard written for that case never
/// ran -- it keyed on a flag the real path did not set -- so the only
/// thing that caught it was an unrelated acl test.  This test exists
/// so that the next person to widen a retry here is told by name.
///
/// Both classes matter.  The metadata half must *succeed* where the
/// model performs it, or a gateway that simply denies everything would
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
    let Some((alice_u, bob_u)) = alice_and_bob("line", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let (alice, bob) = (&alice_u.client, &bob_u.client);
    let bucket = get_new_bucket(Some(alice)).await;
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
                                                bob_u.id)]})),
        None, None);
    alice.put_bucket_policy().bucket(&bucket).policy(&doc).send().await
        .expect("could not grant bob s3:*");

    /* the control: without it, a denial below could just mean the
     * object is unreadable by anyone */
    alice.get_object().bucket(&bucket).key(key).send().await
        .expect("alice could not read her own object");

    let model = s3_tests_rs::features::cross_account_metadata().await;

    /* the metadata half -- succeeds only where the model says so.
     * Without it a gateway that denied everything would pass the
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
        &alice, &bob, &bob_u.id, &bucket, key, body, data_model).await;
    if !tested {
        eprintln!("either the configured model permits the gateway to \
                   serve the bytes itself, or the filesystem permits bob \
                   this object;  there is no line to test");
            return;
    }

    /* A positive control:  the same client, the same call, on an
     * object bob is entitled to.
     *
     * Without it, every denial above is equally explained by "bob
     * cannot read anything" -- a broken client, a broken fixture, a
     * gateway denying the whole bucket.  A test that only ever sees
     * denials cannot tell those apart from the thing it means to
     * assert.
     *
     * In a bucket of bob's own:  alice's bucket directory is hers at
     * 0770, so bob's create there is itself the cross-identity
     * question, which NooBaa refuses too. */
    let bobs_bucket = get_new_bucket(Some(bob)).await;
    let bobs_key = "bobs-own-object";
    bob.put_object()
        .bucket(&bobs_bucket).key(bobs_key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"bob wrote this"))
        .send().await
        .expect("bob could not write his own object;  the fixture is wrong, \
                 not the model");
    bob.get_object().bucket(&bobs_bucket).key(bobs_key).send().await
        .expect("bob could not read his own object;  a denial above says \
                 nothing");

    /* The other polarity, and why it is needed.
     *
     * An assertion that cannot be made to fail asserts nothing.
     * Under the configured model the gateway must not have performed
     * any of the data operations above itself, and the counts are
     * what distinguish "denied" from "never asked";  switching to
     * `s3-native` and seeing the same read succeed is what makes the
     * denials evidence of the model rather than of the fixture.
     *
     * Restored before the assertions, so a failure cannot leave the
     * run measuring a model nobody configured. */
    let before = s3_tests_rs::access_model::served_counts().await;
    if before.is_none() {
        eprintln!("no access-model hint;  the counters cannot be checked");
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
        "s3-native did not let bob read the bytes, so the denials \
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
    let denied_again = bob.get_object().bucket(&bucket).key(key).send().await;
    assert!(denied_again.is_err(),
        "the restore did not take:  bob can still read alice's bytes");

}

/// A nested key:  its directories belong to the identity that made
/// them, and writing beside another is the access model's.
///
/// A key's directories are made by their writer at NooBaa's mode,
/// `0770` (DECISIONS 2026-10-20), so alice's PUT makes `a/b/` hers.
/// bob, granted the write by bucket policy, then writes beside her, and
/// the filesystem denies him creating there whatever the model, because
/// the directory is hers.  What the gateway does about it is the access
/// model's:  `posix-strict` lets the denial stand, as
/// NooBaa does, and `s3-native` performs the write.  Where bob's write
/// is served, S3 grants each the other's read and the filesystem then
/// denies both -- the isolation test's argument, one level down.
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
    let Some((alice_u, bob_u)) = alice_and_bob("nested", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let (alice, bob) = (&alice_u.client, &bob_u.client);
    let bucket = get_new_bucket(Some(alice)).await;
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
                                          bob_u.id)]},
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": resource,
        }],
    }).to_string();
    let pol = alice.put_bucket_policy()
        .bucket(&bucket).policy(&doc).send().await;
    assert!(pol.is_ok(), "could not grant bob read and write: {pol:?}");

    /* a/b/ is alice's, so the filesystem denies bob creating in it at
     * every access model;  only what the gateway does next varies */
    let create = Access::ask(Class::Data, &bucket, his, &bob_u.id, "c").await;
    assert!(create.denied(),
        "the filesystem permits bob to create in a/b/;  the directory \
         was made by alice's request, so it should be hers");
    let wrote = create.expect(bob.put_object()
        .bucket(&bucket).key(his)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"bob wrote this"))
        .send().await).is_some();

    /* the controls:  each reads back what they wrote */
    let mine = alice.get_object().bucket(&bucket).key(hers).send().await;
    assert!(mine.is_ok(), "alice could not read her own nested object: {mine:?}");
    if wrote {
        let mine = bob.get_object().bucket(&bucket).key(his).send().await;
        assert!(mine.is_ok(), "bob could not read his own nested object: {mine:?}");
    }

    /* each is owned by whoever wrote it, which the filesystem says
     * and no access model changes */
    let hers_to_bob = Access::ask(Class::Data, &bucket, hers,
                                  &bob_u.id, "r").await;
    assert!(hers_to_bob.denied(),
        "the filesystem permits bob to read alice's nested object;  it is \
         owned by her");
    hers_to_bob.expect(
        bob.get_object().bucket(&bucket).key(hers).send().await);

    /* The other direction is the permission check and nothing else.  The policy above grants *bob* s3:GetObject;  alice has no
     * grant on bob's object, so a GET of it is denied by RGW's own
     * authorization before any file is opened -- which would pass with
     * impersonation off and prove nothing.  So ask the kernel and do
     * not read:  there is no S3-authorised operation here to judge. */
    if wrote {
        let his_to_alice = Access::ask(Class::Data, &bucket, his,
                                       &alice_u.id, "r").await;
        assert!(his_to_alice.denied(),
            "the filesystem permits alice to read bob's nested object;  it \
             is owned by him");
    }

}

/// A completed multipart upload belongs to the identity that uploaded it.
///
/// Completion assembles the parts into a new file, or links in a data
/// file the parts were written to, and publishes it.  Unless that runs
/// as the uploader the object is the gateway's, and the filesystem then
/// denies the uploader's read of it.  The isolation test's argument
/// again:  bucket policy grants bob the read, so only the filesystem can
/// deny it.
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
    let Some((alice_u, bob_u)) = alice_and_bob("upload", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let (alice, bob) = (&alice_u.client, &bob_u.client);
    let bucket = get_new_bucket(Some(alice)).await;
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
                                                bob_u.id)]})),
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
                             &bob_u.id, "r").await;
    assert!(access.denied(),
        "the filesystem permits bob to read the object alice's upload \
         it was made as the gateway");
    access.expect(bob.get_object().bucket(&bucket).key(key).send().await);

}

/// A copy is the authenticated user's:  it reads the source as them,
/// and it belongs to them.
///
/// The read half is the one with teeth.  Bucket policy grants bob
/// `s3:GetObject` on alice's object and the filesystem denies his read
/// of it, as in the isolation test.  A copy that read the source as the
/// gateway would hand him the same bytes as an object of his own;  so
/// the filesystem must deny his copy of it too.  The ownership half:
/// alice's copy of her own object is hers, so she reads it, and the
/// filesystem denies bob's read of the copy, which policy grants.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_impersonation_copies_as_the_authenticated_user() {
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
    let Some((alice_u, bob_u)) = alice_and_bob("copy", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let (alice, bob) = (&alice_u.client, &bob_u.client);
    let bucket = get_new_bucket(Some(alice)).await;
    let bobs_bucket = get_new_bucket(Some(bob)).await;
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
                                                bob_u.id)]})),
        None, None);
    let pol = alice.put_bucket_policy()
        .bucket(&bucket).policy(&doc).send().await;
    assert!(pol.is_ok(), "could not grant bob s3:GetObject: {pol:?}");

    /* the premise:  bob's read is denied by the object's mode, though
     * S3 permits it */
    let access = Access::ask(Class::Data, &bucket, key,
                             &bob_u.id, "r").await;
    assert!(access.denied(),
        "the filesystem permits bob to read alice's object;  the premise \
         test does not hold");
    access.expect(bob.get_object().bucket(&bucket).key(key).send().await);

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
     * read of the source, so a model that denies bob the bytes
     * denies him the copy, and one that serves them serves it */
    access.expect(bob.copy_object()
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
                           &bob_u.id, "r").await;
    assert!(hers.denied(),
        "the filesystem permits bob to read alice's copy;  it is not owned \
         by her");
    hers.expect(
        bob.get_object().bucket(&bucket).key("alices-copy").send().await);

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

    let bucket = "impersonate-fixture";

    /* `impersonate-fixture` and its files are made by rgw-vstart.sh as
     * the main user -- `grouped.txt` at `0040 root:rgwfix` needs root,
     * which no test has -- and no other test reads that bucket.  So
     * the main user grants a user of this test's own the read, and the
     * row this test rebinds is that user's, not the main user's. */
    let Some(reader) = PrivateUser::create("impgroups").await else {
        eprintln!("could not make this test's own user");
        return;
    };
    let resource = make_arn_resource(&format!("{bucket}/*"));
    let doc = make_json_policy(
        "s3:GetObject", &resource,
        Some(serde_json::json!({"AWS": [format!("arn:aws:iam:::user/{}",
                                                reader.id)]})),
        None, None);
    let main = get_client();
    main.put_bucket_policy().bucket(bucket).policy(&doc).send().await
        .expect("could not grant the reader s3:GetObject");
    assert!(reader.bind(UID_ALICE, UID_ALICE, Some(GID_SHARED)).await,
        "binding the reader with the gating group failed");
    let alice = &reader.client;
    let open = alice.get_object().bucket(bucket).key("open.txt")
        .send().await;
    assert!(open.is_ok(),
        "the control object was unreadable;  the fixture or the gateway \
         is wrong, not the group vector: {open:?}");
    /* the vector's effect is in the permission check, which is
     * the same answer at every access model -- so this reads the
     * vector itself and not what the gateway does about it */
    let gated = Access::ask(Class::Data, bucket, "grouped.txt",
                            &reader.id, "r").await;
    assert!(!gated.denied(),
        "holding gid {GID_SHARED} did not grant the read");
    gated.expect(alice.get_object().bucket(bucket).key("grouped.txt")
        .send().await);

    /* the same identity, no group */
    assert!(reader.bind(UID_ALICE, UID_ALICE, None).await,
        "rebinding the reader without the group failed");
    let open = alice.get_object().bucket(bucket).key("open.txt")
        .send().await;
    assert!(open.is_ok(),
        "the control object became unreadable without the group; \
         something other than the group vector changed: {open:?}");
    let gated = Access::ask(Class::Data, bucket, "grouped.txt",
                            &reader.id, "r").await;
    assert!(gated.denied(),
        "the filesystem permitted the read without the group in the \
         vector — an absent list left the gateway's own groups in place \
         rather than clearing");
    gated.expect(alice.get_object().bucket(bucket).key("grouped.txt")
        .send().await);

    let _ = main.delete_bucket_policy().bucket(bucket).send().await;
}

/// An identity with no record is denied, not served as the gateway.
///
/// Section 4 of the design makes this an invariant rather than a
/// policy:  a valid account always has an identity row, so one
/// without is a provisioning fault.  The alternative — falling back
/// to the gateway's own credentials — would give an unprovisioned
/// user the daemon's filesystem reach while every provisioned one
/// was confined, which is the escalation the whole design rules out.
///
/// Both polarities, and in this order:  the write must succeed with
/// a record, so that its failure without one is attributable to the
/// record and not to anything else about the request.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_impersonation_denies_an_unprovisioned_identity() {
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
    let Some((alice_u, _bob_u)) = alice_and_bob("unprov", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let alice = &alice_u.client;
    let bucket = get_new_bucket(Some(alice)).await;

    /* the control */
    let ok = alice.put_object()
        .bucket(&bucket).key("provisioned")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send().await;
    assert!(ok.is_ok(), "a provisioned identity could not write: {ok:?}");

    /* take the record away and try the same thing */
    let _ = nsfs_delete_identity(&alice_u.id).await;
    let denied = alice.put_object()
        .bucket(&bucket).key("unprovisioned")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send().await;
    assert!(denied.is_err(),
        "an identity with no identity row was served;  the request ran as \
         the gateway rather than being denied");

    /* and reads too, not only writes */
    let read = alice.get_object().bucket(&bucket).key("provisioned")
        .send().await;
    assert!(read.is_err(),
        "an identity with no identity row read an object");

}

/// NooBaa's guard on where an account's buckets go, enacted
/// (DECISIONS.md 2026-10-14):  NooBaa makes a bucket's directory as the
/// account, so an account that cannot write its `new_buckets_path`
/// cannot create there.  nsfs makes the directory as the gateway, so it
/// checks the account's access as the account before it does.
///
/// alice is mapped with `/usr` as her `new_buckets_path`, which is
/// root-owned and not writable by uid 70101:  denied.  (Not `/`, which
/// nsfs rejects as a path at all.)  The control is
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
    let Some((alice_u, _bob_u)) = alice_and_bob("nbp", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let alice = &alice_u.client;
    let bind = |nbp: &'static str| {
        let me = alice_u.id.clone();
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
    let denied = s3_tests_rs::fixtures::get_new_bucket_name();
    let r = status(alice.create_bucket().bucket(&denied).send().await);

    bind("/tmp").await;
    let allowed = s3_tests_rs::fixtures::get_new_bucket_name();
    let ok = status(alice.create_bucket().bucket(&allowed).send().await);
    if ok == 200 {
        let _ = alice.delete_bucket().bucket(&allowed).send().await;
    }

    assert_eq!(r, 403, "alice created a bucket where she cannot write");
    assert_eq!(ok, 200, "alice was denied where she can write");
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
/// last is the rejection while impersonating, where the capability
/// would be the *identity's* rather than the gateway's.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_raise_is_effective_for_one_call_and_dropped_after() {
    if !nsfs_only().await {
        return;
    }
    let r = s3_tests_rs::admin::driver_hint_results(
        "raise",
        &[("cap", "promote"),
          ("user", &s3_tests_rs::admin::q(&get_config().main_user_id))]).await;
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
                "a failed raise left the capability effective:  \
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
/// identity*, and one for another identity must fail with
/// `-EDEADLK`.  Asserting only the failure would pass against a
/// `run_as` that had simply stopped working.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_nested_run_as_rejects_a_different_identity() {
    if !nsfs_only().await {
        return;
    }
    let r = s3_tests_rs::admin::driver_hint_results(
        "nested-identity",
        &[("outer", &s3_tests_rs::admin::q(&get_config().main_user_id)),
          ("inner", &s3_tests_rs::admin::q(&get_config().alt_user_id))]).await;
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
               "a nested run_as for another identity must fail with \
                -EDEADLK, not run as the outer one:  outer={outer} \
                inner={inner} results={m:?}");
}

/// Is the gateway producing mtime-ino etags rather than digests?
///


/// The non-md5 etag is *recorded on the object*, not just reported.
///
/// Under `rgw_non_md5_etag` the etag is the mtime-ino string, which
/// cannot be known until the file has been linked and stat'ed -- so
/// it is written after `NSFSAtomicWriter::complete()` has already
/// persisted the attribute map, by an `fsetxattr` of its own inside
/// `NSFSObject::link_temp_file()`.  That write used to run on the
/// calling thread with its result discarded, so under impersonation
/// the kernel denied it on a 0660 file the gateway does not own.
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
async fn test_the_non_md5_etag_is_recorded_as_the_authenticated_user() {
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
    if !s3_tests_rs::features::non_md5_etag().await {
        eprintln!("rgw_non_md5_etag is off;  start with \
                   --with-non-md5-etag");
        return;
    }
    let Some((alice_u, _bob_u)) = alice_and_bob("etag", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let alice = &alice_u.client;
    let bucket = get_new_bucket(Some(alice)).await;

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
                be denied and the object should keep the opaque etag \
                RGWPutObj wrote -- \
                the two agreeing means this test cannot fail:  PUT said \
                {bad_put}, HEAD says {bad_head}");
}

/* DeleteBucket under impersonation:  who acts at each layer follows
 * who had to be able to create it (DECISIONS.md 2026-10-24).  The
 * bucket directory is checked as the authenticated user;  what is below
 * it is removed as that user;  its entry in a parent the account owns
 * is removed as that user, and in the base path by the gateway.  Under
 * `posix-strict` a refusal at any of that user's layers is 403, as
 * NooBaa's `delete_uls()`;  under `s3-native` the gateway performs it. */

async fn bucket_exists(client: &aws_sdk_s3::Client, bucket: &str) -> bool {
    client.head_bucket().bucket(bucket).send().await.is_ok()
}

/// bob, granted `s3:DeleteBucket` by alice's policy, deletes her empty
/// bucket.  Her bucket directory is hers at 0770 and bob is not in her
/// group, so the filesystem refuses him at the bucket directory:  403
/// under `posix-strict`, and deleted under `s3-native`.
///
/// The policy is what makes the refusal the filesystem's:  without it
/// RGW denies bob before the driver is reached, under every model.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_delete_bucket_by_another_identity_follows_the_access_model() {
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
    let Some((alice_u, bob_u)) = alice_and_bob("dbx", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let (alice, bob) = (&alice_u.client, &bob_u.client);
    let bucket = get_new_bucket(Some(alice)).await;

    let doc = make_json_policy(
        "s3:DeleteBucket", &make_arn_resource(&bucket),
        Some(serde_json::json!({"AWS": [format!("arn:aws:iam:::user/{}",
                                                bob_u.id)]})),
        None, None);
    alice.put_bucket_policy().bucket(&bucket).policy(&doc).send().await
        .expect("could not grant bob s3:DeleteBucket");

    /* the request-time check:  write and search on the bucket
     * directory, as bob */
    let access = Access::ask(Class::Data, &bucket, "k", &bob_u.id, "c").await;
    assert!(access.denied(),
        "the filesystem permits bob to write alice's bucket directory;  \
         it is not hers at 0770");
    let deleted = access.expect(bob.delete_bucket().bucket(&bucket).send().await)
        .is_some();

    let survives = bucket_exists(alice, &bucket).await;
    if access.served() == Some(true) {
        assert!(deleted && !survives, "the model performs the delete, and \
                the bucket is still there");
    } else if access.served() == Some(false) {
        assert!(survives, "bob's delete was refused, and the bucket is gone");
    }
    if survives {
        alice.delete_bucket().bucket(&bucket).send().await
            .expect("alice could not delete her own bucket");
    }
}

/// alice's bucket under a location of her own, whose write she then
/// loses.  She may still use the bucket directory, which is hers, but
/// removing its entry needs write on the location, which NooBaa's
/// `rmdir` as the account asks:  403 under `posix-strict`, deleted
/// under `s3-native`.
///
/// The location is the `unplaced-directory` hint's `as=` directory,
/// made for her and changed as her, so the test takes no privilege.
/// The control is the same delete after write is restored.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_delete_bucket_needs_write_on_the_accounts_location() {
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
    let Some((alice_u, _bob_u)) = alice_and_bob("dbl", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let alice = &alice_u.client;
    let loc = s3_tests_rs::fixtures::get_new_bucket_name();
    let made = driver_hint_results("unplaced-directory", &[
        ("op", "create"), ("name", &loc), ("as", &alice_u.id),
        ("mode", "0770")]).await
        .expect("could not make alice a location");
    let path = made.get("path").cloned().expect("the location's path");
    let r = nsfs_put_identity(&[
        ("identity", alice_u.id.as_str()),
        ("uid", UID_ALICE), ("gid", UID_ALICE),
        ("new_buckets_path", path.as_str()),
    ]).await;
    assert_eq!(r.status, 200, "binding alice's location failed: {}", r.body);

    let bucket = s3_tests_rs::fixtures::get_new_bucket_name();
    alice.create_bucket().bucket(&bucket).send().await
        .expect("alice could not create a bucket in her own location");

    /* she loses write on the location, and keeps the bucket */
    driver_hint_results("unplaced-directory", &[
        ("op", "mode"), ("name", &loc), ("as", &alice_u.id),
        ("mode", "0550")]).await
        .expect("could not take write from alice's location");
    let r = alice.delete_bucket().bucket(&bucket).send().await;
    let model = cross_account_data().await;
    match model {
        Some(false) => {
            s3_tests_rs::assert_s3_err!(r, 403, "AccessDenied");
            assert!(bucket_exists(alice, &bucket).await,
                "the delete was refused, and the bucket is gone");
        }
        Some(true) => {
            assert!(r.is_ok(), "the model performs the delete: {r:?}");
            assert!(!bucket_exists(alice, &bucket).await,
                "the delete succeeded, and the bucket is still there");
        }
        None => {}
    }

    /* the control, and the cleanup:  write restored, the delete goes */
    driver_hint_results("unplaced-directory", &[
        ("op", "mode"), ("name", &loc), ("as", &alice_u.id),
        ("mode", "0770")]).await
        .expect("could not restore alice's location");
    if bucket_exists(alice, &bucket).await {
        alice.delete_bucket().bucket(&bucket).send().await
            .expect("alice could not delete her bucket with write restored");
    }
    let _ = driver_hint_results("unplaced-directory", &[
        ("op", "remove"), ("name", &loc), ("as", &alice_u.id)]).await;
}

/// The base path is the gateway's, and the gateway made alice's bucket
/// directory there for her, so the gateway removes its entry:  her
/// delete of her own empty bucket succeeds under every model, the same
/// as at creation.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_the_gateway_unlinks_a_base_path_bucket() {
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
    let Some((alice_u, _bob_u)) = alice_and_bob("dbb", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let alice = &alice_u.client;
    let bucket = get_new_bucket(Some(alice)).await;
    alice.put_object().bucket(&bucket).key("k")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send().await.expect("alice could not write her own bucket");
    alice.delete_object().bucket(&bucket).key("k").send().await
        .expect("alice could not delete her own object");

    alice.delete_bucket().bucket(&bucket).send().await
        .expect("alice could not delete her own empty bucket");
    assert!(!bucket_exists(alice, &bucket).await,
        "the delete succeeded, and the bucket is still there");
}

/// bob, granted `s3:*` by alice's policy, copies her object onto itself
/// in a versioned bucket, replacing its metadata.  The self-copy
/// demotes the current version and then clones it back into her key
/// directory, which is hers at 0770, so the clone's create and publish
/// are bob's to be denied:  403 under `posix-strict`, served under
/// `s3-native`, where the gateway performs them.
///
/// Under every model the key still has its current entry on disk, and
/// whose it is follows who acted:  bob's where his copy was served, as
/// the gateway gives a raised create to its authenticated user, and alice's
/// where it was refused.  The current entry is checked because the
/// demote runs before the clone, so a clone that fails must not leave
/// the key without one.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_versioned_self_copy_follows_the_access_model() {
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
    let Some((alice_u, bob_u)) = alice_and_bob("vsc", None).await else {
        eprintln!("could not make this module's own users");
        return;
    };
    let (alice, bob) = (&alice_u.client, &bob_u.client);
    let bucket = get_new_bucket(Some(alice)).await;
    check_configure_versioning_retry(alice, &bucket, "Enabled", "Enabled").await;
    let key = "k";
    alice.put_object().bucket(&bucket).key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"alice's"))
        .send().await
        .expect("alice could not write her own object");

    let doc = make_json_policy(
        "s3:*", &make_arn_resource(&format!("{bucket}/*")),
        Some(serde_json::json!({"AWS": [format!("arn:aws:iam:::user/{}",
                                                bob_u.id)]})),
        None, None);
    alice.put_bucket_policy().bucket(&bucket).policy(&doc).send().await
        .expect("could not grant bob s3:*");

    /* the clone's create, as bob, in alice's key directory */
    let access = Access::ask(Class::Data, &bucket, key, &bob_u.id, "c").await;
    assert!(access.denied(),
        "the filesystem permits bob to create in alice's key directory;  \
         it is not hers at 0770");
    let copied = access.expect(bob.copy_object()
        .bucket(&bucket).key(key)
        .copy_source(format!("{bucket}/{key}"))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .metadata("by", "bob")
        .send().await)
        .is_some();

    let on_disk = driver_hint_results("symlink",
        &[("op", "stat"), ("bucket", &bucket), ("key", key)]).await
        .expect("the symlink hint did not answer;  rgw_driver_debug_apis");
    assert_eq!(on_disk.get("kind").map(String::as_str), Some("file"),
        "the key has no current entry after bob's self-copy");
    let want = if copied { UID_BOB } else { UID_ALICE };
    assert_eq!(on_disk.get("uid").map(String::as_str), Some(want),
        "the current entry is not the acting principal's");
}
