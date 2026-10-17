//! The invariant the nsfs access model rests on, as something any
//! test can assert.
//!
//! `rgw_nsfs_cross_account_permission` decides what the gateway does
//! when the filesystem refuses an operation S3 authorised.  Under
//! `s3-metadata` it performs the object's *metadata* work itself --
//! tags and ACL.  It must never do that for the object's **bytes**,
//! at any value of the setting, and it has already been broken once:
//! on 2026-10-16 an attribute read left a gateway-opened descriptor
//! cached on the object and a data read found it, serving bytes the
//! filesystem had refused.
//!
//! The invariant is therefore worth asserting from more than one
//! place -- any test that arranges a cross-identity object can call
//! [`assert_data_follows_the_filesystem`], and adding a data
//! operation to the list below covers every such test at once.

use aws_sdk_s3::Client;

use crate::admin::{driver_hint_results, fs_access};

/// Point the gateway at another access model for the rest of the
/// process, returning the one it had.
///
/// `posix-strict`, `s3-metadata`, `default`, or `data-on-s3s-word`,
/// which is wrong on purpose so that a test can watch itself fail.
/// `None` means the gateway has no such hint, which is every backend
/// but nsfs.
///
/// **A caller that sets this must put it back**, unconditionally, or
/// every later test in the run is measuring a model nobody asked
/// for.  [`restore_access_model`] is the pair.
pub async fn set_access_model(which: &str) -> Option<String> {
    let r = driver_hint_results("access-model", &[("set", which)]).await?;
    r.get("had").cloned()
}

/// Put the configured model back, whatever happened above.
pub async fn restore_access_model() {
    let _ = driver_hint_results("access-model", &[("set", "default")]).await;
}

/// How many operations the gateway has performed on the access
/// model's say-so after the filesystem refused the requester,
/// as `(object_attrs, data)`.
///
/// This is what makes a success attributable.  A test that sees a
/// read succeed cannot otherwise tell whether the model permitted it
/// or the filesystem never objected and the fixture is wrong -- and
/// those look identical from the client.
///
/// Compare across an operation rather than against zero:  the
/// gateway is shared, and another test may be running.
pub async fn served_counts() -> Option<(u64, u64)> {
    let r = driver_hint_results("access-model", &[]).await?;
    let attrs = r.get("attrs_served")?.parse().ok()?;
    let data = r.get("data_served")?.parse().ok()?;
    Some((attrs, data))
}

/// What the gateway says it does for a class of operation:  `refuse`
/// or `as_gateway_raised`.  Asked rather than inferred, because both
/// models answer alike for every request where S3 and POSIX agree.
pub async fn verdict_for(class: &str) -> Option<String> {
    let r = driver_hint_results("access-model", &[]).await?;
    r.get(class).cloned()
}

/// One data operation, named so a failure says which.
///
/// The list, not a single GET, is the point: "data" is every path
/// that moves an object's bytes, and a model that leaked through one
/// of them while refusing another would be just as wrong.
#[derive(Debug)]
pub enum DataOp {
    Get,
    Overwrite,
    RangeGet,
    CopyFrom,
}

/// Assert that **no** access model lets `other` touch the bytes of an
/// object the filesystem refuses it, and that the owner's object is
/// unchanged afterwards.
///
/// Says nothing about metadata: that is the model's business and
/// `unless_posix_governs_metadata!` is where it is asserted.
///
/// Returns `false` when the filesystem *permits* `other` the object,
/// in which case there is no line here to test and the caller should
/// say so rather than assert.
pub async fn assert_data_follows_the_filesystem(
    owner: &Client,
    other: &Client,
    other_user_id: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
    model: Option<bool>,
) -> bool {
    /* the filesystem's own answer, asked of the gateway rather than
     * inferred from the fixture's uids */
    if !fs_access(bucket, key, other_user_id, "r").await.refuses() {
        return false;
    }

    for op in [DataOp::Get, DataOp::RangeGet, DataOp::Overwrite,
               DataOp::CopyFrom] {
        let refused = match op {
            DataOp::Get =>
                other.get_object().bucket(bucket).key(key)
                    .send().await.is_err(),
            DataOp::RangeGet =>
                other.get_object().bucket(bucket).key(key)
                    .range("bytes=0-1").send().await.is_err(),
            DataOp::Overwrite =>
                other.put_object().bucket(bucket).key(key)
                    .body(aws_sdk_s3::primitives::ByteStream::from_static(
                        b"not yours"))
                    .send().await.is_err(),
            DataOp::CopyFrom =>
                other.copy_object()
                    .bucket(bucket).key(format!("{key}-copy"))
                    .copy_source(format!("{bucket}/{key}"))
                    .send().await.is_err(),
        };
        assert!(refused,
            "{op:?} moved bytes the filesystem refused this identity, \
             access model = {model:?};  no model permits that");
    }

    /* and the owner's object is what the owner wrote.  A write
     * reported as refused that landed anyway would pass every check
     * above. */
    let after = owner.get_object().bucket(bucket).key(key).send().await
        .expect("the owner lost their own object");
    let got = after.body.collect().await.unwrap().into_bytes();
    assert_eq!(&got[..], body, "the object changed under its owner");
    true
}
