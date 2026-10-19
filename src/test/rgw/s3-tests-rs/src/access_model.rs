//! The invariant the nsfs access model rests on, as something any
//! test can assert.
//!
//! `rgw_nsfs_access_model` decides what the gateway does when the
//! filesystem refuses an operation S3 authorised:  `posix-strict`
//! refuses it too, `s3-metadata` performs the object's *metadata*
//! work itself -- tags and ACL -- and `s3-native` performs the data
//! work as well, which is what the posix driver and every other RGW
//! backend do.
//!
//! **Under the first two it must never do that for the object's
//! bytes**, and it has already been broken once:
//! on 2026-10-16 an attribute read left a gateway-opened descriptor
//! cached on the object and a data read found it, serving bytes the
//! had been denied.
//!
//! The invariant is therefore worth asserting from more than one
//! place -- any test that arranges a cross-identity object can call
//! [`assert_data_follows_the_filesystem`], and adding a data
//! operation to the list below covers every such test at once.

use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response;

use crate::admin::{driver_hint_results, fs_access, FsAccess};

/// The class of operation, as the driver names it
/// (`nsfs::AccessClass`):  an object's bytes, or its tags and ACL.
///
/// The classes differ because the models do:  `s3-metadata` performs
/// an object's metadata work for an identity the filesystem refused
/// and leaves its bytes alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Data,
    ObjectAttrs,
}

/// What the configuration does with one identity's access to one
/// object, for one class of operation.
///
/// Two questions answered together, because a test asking only one of
/// them asserts the wrong thing.  The first is the filesystem's own
/// check, got from [`fs_access`], which has the gateway ask the
/// kernel as that identity -- independent of the access model, so it
/// is the same answer at every value of it.  The second is whether
/// the model has the gateway perform what the filesystem refused.
///
/// **One rule covers both classes and every model**:  where the
/// filesystem permits it the gateway serves it, and where the
/// filesystem refuses it the model decides.  A test that consults
/// only the filesystem is asserting one model's behaviour against
/// whatever model the run configured;  a test that consults only the
/// model is assuming a refusal its fixture may not have arranged.
#[derive(Debug, Clone, Copy)]
pub struct Access {
    class: Class,
    check: FsAccess,
    /// Whether the model has the gateway perform this class itself.
    /// `None` is unknown -- no model reported and none declared --
    /// and asserts nothing, because a suite that guesses here
    /// asserts the opposite of the truth against half the backends it
    /// meets.
    model: Option<bool>,
}

impl Access {
    /// Ask the gateway about `user`'s access to `key` in `bucket`,
    /// `mode` being `r` or `w`.  `user` is the request's user,
    /// `tenant$id` for a tenant's, empty for anonymous.
    ///
    /// Ask after the object exists:  there is nothing to check on a
    /// file that is not there.
    pub async fn ask(class: Class, bucket: &str, key: &str,
                     user: &str, mode: &str) -> Access {
        let model = match class {
            Class::Data => crate::features::cross_account_data().await,
            Class::ObjectAttrs =>
                crate::features::cross_account_metadata().await,
        };
        Access {
            class,
            check: fs_access(bucket, key, user, mode).await,
            model,
        }
    }

    /// Whether the permission check denied it, whatever the gateway
    /// does about
    /// it.  The subject of a test about ownership or the group
    /// vector:  that alice's object is alice's holds at every access
    /// model, and only what the gateway does next varies.
    pub fn filesystem_refuses(&self) -> bool {
        self.check.denied()
    }

    /// Whether the gateway serves it.  `None` is unknown.
    pub fn served(&self) -> Option<bool> {
        if !self.check.denied() {
            return Some(true);
        }
        self.model
    }

    /// Judge a result against that:  AccessDenied where the
    /// configuration refuses it, the output where it serves it, and
    /// nothing asserted where it is unknown -- `Some` only where
    /// something was served, so a test checks what it read only where
    /// it read.
    pub fn judge<T, E>(&self, r: Result<T, SdkError<E, Response>>)
        -> Option<T>
    where
        E: ProvideErrorMetadata + std::fmt::Debug,
    {
        match self.served() {
            Some(true) => Some(r.unwrap_or_else(|e| panic!(
                "the configuration serves this {:?} operation and the \
                 gateway refused it: {e:?}", self.class))),
            Some(false) => {
                crate::assert_s3_err!(r, 403, "AccessDenied");
                None
            }
            None => r.ok(),
        }
    }
}

/// Point the gateway at another access model for the rest of the
/// process, returning the one it had.
///
/// `posix-strict`, `s3-metadata`, `s3-native`, or `default`.
/// Switching to `s3-native` is how a test asserting the byte line
/// gets its other polarity:  the same read the strict models refuse
/// is served there, so the refusal is shown to be the model's doing
/// and not the fixture's.  `None` means the gateway has no such
/// hint, which is every backend but nsfs.
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

/// How many operations the gateway has performed itself after the
/// filesystem refused the requester, which the access model permits,
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
pub async fn model_for(class: &str) -> Option<String> {
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
/// object it is denied, and that the owner's object is
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
    data_model: Option<bool>,
) -> bool {
    /* Not an invariant under a model that permits the gateway to read
     * and write the bytes itself, which it does on purpose.  Passed
     * in rather than read here, so the caller states which model it is
     * asserting against -- and `features::cross_account_data()` is the
     * one that answers for this class, not the metadata one. */
    if data_model != Some(false) {
        return false;
    }

    /* the permission check, asked of the gateway rather than
     * inferred from the fixture's uids */
    if !fs_access(bucket, key, other_user_id, "r").await.denied() {
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
            "{op:?} moved bytes this identity was denied, \
             access model = {data_model:?};  no strict model permits that");
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
