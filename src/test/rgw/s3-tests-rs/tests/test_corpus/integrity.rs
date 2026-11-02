//! Does the reported ETag describe the bytes?
//!
//! An axis invariant, not a diff row.  The gateway's ETag is either a
//! digest of the content or a token for the file instance, and only the
//! first says anything about the bytes.  So the check is conditional on
//! the regime, and in the regime where a digest is promised this is the
//! only thing in the corpus that reads an object's content at all --
//! the manifest records what S3 says and the inventory records what is
//! on disk, neither of which witnesses a byte of the object.
//!
//! **The suite pays for this, not the gateway.**  Validating a digest
//! means reading every object end to end, which is free on a corpus of
//! a few small objects and close to unacceptable during a field
//! upgrade.  That is why it lives here and why conversion has no
//! validation pass:  `DECISIONS.md` 2026-10-09.
//!
//! The regime comes from `features::non_md5_etag()`:  the gateway's
//! report where it makes one, and the suite's own `non_md5_etag`
//! config otherwise, which already means exactly this -- "ETags are
//! unique-per-write tokens rather than content MD5s" (`config.rs:44`)
//! -- rather than from a second setting of the corpus's own.

use md5::{Digest, Md5};

use crate::manifest::Manifest;

/// `true` when the gateway promises a digest.
pub async fn digest_regime() -> bool {
    !s3_tests_rs::features::non_md5_etag().await
}

/// A one-line name for the regime, recorded so a capture carries its
/// own provenance.  Two captures taken under different regimes are not
/// comparable, and the manifest should say which it is rather than
/// leave it to be inferred from the values.
pub async fn regime() -> &'static str {
    if digest_regime().await { "digest" } else { "synthetic" }
}

/// Whether the ETag is this function's business.
pub enum EtagCheck<'a> {
    /// A plain object:  in the digest regime the ETag is the body's
    /// digest, and `None` here means the gateway reported none at all,
    /// which is itself a defect.
    Plain(Option<&'a str>),
    /// A multipart object.  Its ETag is a digest of digests and is
    /// checked against `expected_multipart_etag` where the part layout
    /// is known;  the body is still read and still compared.
    Composite,
}

/// The hex MD5 of some bytes.  The corpus uses it on bodies it is
/// about to write, so it can later assert what it reads back.
pub fn md5_hex(bytes: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

/// GET the object, digest the body, record it, and check the ETag.
///
/// Records `body-md5` whatever the regime:  the fact is about the
/// object and costs one GET, and a capture that has it can be
/// re-examined without the tree.  The *assertion* is what varies.
///
/// Multipart objects go to `expected_multipart_etag` instead.  An S3
/// multipart ETag is a digest of digests with a `-<parts>` suffix, so
/// comparing it to the digest of the assembled body is wrong -- but
/// the corpus knows its own part layout, so the composite is
/// computable rather than unavailable.
pub async fn check_body_digest(
    client: &aws_sdk_s3::Client,
    b: &str,
    key: &str,
    etag: EtagCheck<'_>,
    /* the digest of what the corpus wrote, when it knows it.  This
     * comparison needs no ETag and holds in either regime, which makes
     * it the only integrity check here that survives a gateway with
     * synthetic ETags. */
    expected_body: Option<&str>,
    m: &mut Manifest,
    problems: &mut Vec<String>,
) {
    let got = match client.get_object().bucket(b).key(key).send().await {
        Ok(r) => r,
        Err(e) => {
            problems.push(format!(
                "{key:?} could not be read back for digesting: {}",
                e.into_service_error().meta().code().unwrap_or("Unknown")
            ));
            return;
        }
    };
    let body = match got.body.collect().await {
        Ok(b) => b.into_bytes(),
        Err(e) => {
            problems.push(format!("{key:?} body did not stream: {e}"));
            return;
        }
    };

    let mut h = Md5::new();
    h.update(&body);
    let digest = format!("{:x}", h.finalize());
    m.object(key, "body-md5", &digest);
    m.object(key, "body-len", &body.len().to_string());

    /* The strongest check here, and the one that needs no metadata at
     * all:  the corpus wrote these bytes, so reading back anything
     * else is data loss whatever the ETag says about it. */
    if let Some(want) = expected_body {
        if want != digest {
            problems.push(format!(
                "{key:?} was written with body digest {want:?} and reads \
                 back as {digest:?} -- the bytes changed, which no ETag \
                 regime makes acceptable"
            ));
        }
    }

    let tag = match etag {
        EtagCheck::Composite => return,
        EtagCheck::Plain(Some(t)) => t.trim_matches('"'),
        EtagCheck::Plain(None) => {
            problems.push(format!("{key:?} reported no ETag at all"));
            return;
        }
    };

    if digest_regime().await {
        if tag != digest {
            problems.push(format!(
                "{key:?} reports ETag {tag:?} and its body digests to \
                 {digest:?} -- in the digest regime those must agree, so \
                 either the ETag does not describe the content or the \
                 content changed under it"
            ));
        }
        return;
    }

    /* Synthetic regime.  There is no digest to agree with, so assert
     * the two things the regime does promise:  the value is not the
     * content digest, and it carries a dash, which is what makes an
     * SDK skip client-side MD5 validation.  Asserting both polarities
     * is the point -- a check that cannot fail here would pass a
     * gateway that had quietly started digesting. */
    if tag == digest {
        problems.push(format!(
            "{key:?} reports the content MD5 {tag:?} while the suite is \
             configured for synthetic ETags"
        ));
    }
    if !tag.contains('-') {
        problems.push(format!(
            "{key:?} reports ETag {tag:?}, which carries no dash, so an \
             SDK will try to validate it as a digest"
        ));
    }
}

/// The composite ETag an S3 multipart object should carry.
///
/// `hex(MD5(d1 || d2 || .. || dN)) + "-" + N`, where each `di` is the
/// **raw** 16-byte digest of part `i` -- concatenated as bytes, not as
/// hex, which is the usual way to get this wrong.
///
/// **Computed from the bytes the corpus wrote, not from the part ETags
/// the gateway reported.**  Deriving it from the reported part ETags
/// would make the check circular:  a gateway that digested every part
/// wrongly but consistently would still agree with itself.  The corpus
/// writes `fill` repeated `sizes[i]` times for each part, so it can
/// reproduce every part body exactly.
///
/// For an arbitrary object this is not available -- the part
/// boundaries are gone once the upload completes, and recovering them
/// needs `GetObjectAttributes` with `ObjectParts`, which not every
/// backend answers. The corpus is not in that position: it chose the
/// layout.
pub fn expected_multipart_etag(sizes: &[usize], fill: u8) -> String {
    let mut outer = Md5::new();
    for sz in sizes {
        let mut part = Md5::new();
        part.update(vec![fill; *sz]);
        outer.update(part.finalize());
    }
    format!("{:x}-{}", outer.finalize(), sizes.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    /* The one-part case against a digest computed by hand, so the
     * concatenation is known to be of raw bytes rather than hex:  a
     * single part of 3 bytes of `m` has part digest
     * MD5("mmm"), and the composite is MD5 of those 16 bytes. */
    #[test]
    fn composite_concatenates_raw_digests() {
        let mut inner = Md5::new();
        inner.update(b"mmm");
        let raw = inner.finalize();
        let mut outer = Md5::new();
        outer.update(raw);
        let want = format!("{:x}-1", outer.finalize());
        assert_eq!(expected_multipart_etag(&[3], b'm'), want);

        /* and the hex-concatenation mistake produces a different
         * value, so the test above is not vacuous */
        let mut wrong = Md5::new();
        wrong.update(format!("{raw:x}").as_bytes());
        assert_ne!(format!("{:x}-1", wrong.finalize()), want);
    }
}
