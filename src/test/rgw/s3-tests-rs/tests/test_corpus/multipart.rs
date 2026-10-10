//! Tier 1 -- the multipart axis.
//!
//! Three things this axis is for that the others are not:
//!
//!   * **The staging layout is a disk artifact.**  An upload in flight
//!     has files and attributes of its own, in a layout that differs
//!     between the two formats, and conversion has to re-stage it
//!     rather than abort it.  The inventory is the only half that can
//!     see that, and it is the reason `UploadState::InFlight` is a
//!     value rather than a cleanup step.
//!   * **512 and 513 parts** straddle the point where the staging
//!     attribute overflowed.  Named, not generated.
//!   * **A completed object lands on a warm listing cache.**  The
//!     bucket is listed before the uploads complete, deliberately, so
//!     that what the completion path puts into the cache is what the
//!     final listing reads.  A cold listing refills from the store and
//!     reads correctly whatever the write path did -- which is how two
//!     of these defects stayed hidden.
//!
//! Part bodies are 64 bytes against `rgw_multipart_min_part_size=32`,
//! so 513 parts is 33 KB rather than 2.5 GB.  The corpus is about
//! shape, and scale is its own tier.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedPart;
use s3_tests_rs::client::get_client;

use crate::generate::{ensure_bucket, env_or, out_dir};
use crate::manifest::Manifest;
use crate::names::bucket;
use crate::shapes::{PartCount, PartPattern, UploadState, MPU_COVERAGE};

/// The part size the minimum allows, doubled so `ShortFinal` has room
/// to halve it and stay legal.
const BASE_PART: usize = 64;

/// What every part is filled with.  Named because the integrity check
/// reproduces the part bodies from it.
const FILL: u8 = b'm';

struct Upload {
    key: String,
    parts: usize,
    total: usize,
    state: UploadState,
    upload_id: String,
    /// the part layout, kept so the composite ETag can be computed
    /// from the bodies this axis wrote rather than from what the
    /// gateway reported for them
    sizes: Vec<usize>,
}

/// Create, upload every part, and leave the upload in `state`.
async fn stage_one(
    client: &aws_sdk_s3::Client,
    b: &str,
    pc: PartCount,
    pp: PartPattern,
    st: UploadState,
    m: &mut Manifest,
) -> Upload {
    let key = format!("m-{}-{}-{}", pc.label(), pp.label(), st.label());
    let sizes = pp.sizes(pc.count(), BASE_PART);
    let total: usize = sizes.iter().sum();

    m.object(&key, "declared-parts", &pc.count().to_string());
    m.object(&key, "declared-size", &total.to_string());
    m.object(&key, "part-pattern", pp.label());
    m.object(&key, "upload-state", st.label());

    /* An observing pass stages nothing.  Everything above is the
     * model -- the key, the part layout and the state come from the
     * axis's own matrix -- so the descriptor is returned without the
     * S3 work, and the readers below find whatever the first gateway
     * left in the tree.  The upload id is the one field that is an
     * outcome, so it is empty and `list_parts` is not asked. */
    if crate::generate::observing() {
        return Upload { key, parts: pc.count(), total, state: st,
                        upload_id: String::new(), sizes };
    }

    let create = client
        .create_multipart_upload()
        .bucket(b)
        .key(&key)
        .content_type("application/octet-stream")
        .customize()
        .mutate_request(s3_tests_rs::http::zero_content_length)
        .send()
        .await
        .expect("create_multipart_upload");
    let upload_id = create.upload_id().unwrap().to_string();

    let mut done = Vec::new();
    for (i, sz) in sizes.iter().enumerate() {
        let n = (i + 1) as i32;
        let r = client
            .upload_part()
            .bucket(b)
            .key(&key)
            .upload_id(&upload_id)
            .part_number(n)
            .body(ByteStream::from(vec![FILL; *sz]))
            .send()
            .await
            .expect("upload_part");
        done.push(
            CompletedPart::builder()
                .e_tag(r.e_tag().unwrap())
                .part_number(n)
                .build(),
        );
    }

    match st {
        UploadState::Completed => {
            client
                .complete_multipart_upload()
                .bucket(b)
                .key(&key)
                .upload_id(&upload_id)
                .multipart_upload(
                    aws_sdk_s3::types::CompletedMultipartUpload::builder()
                        .set_parts(Some(done))
                        .build(),
                )
                .send()
                .await
                .expect("complete_multipart_upload");
        }
        UploadState::Aborted => {
            client
                .abort_multipart_upload()
                .bucket(b)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await
                .expect("abort_multipart_upload");
        }
        UploadState::InFlight => {}
    }

    Upload { key, parts: pc.count(), total, state: st, upload_id, sizes }
}

/// What `ListParts` says about an upload still in flight.  A completed
/// or aborted upload is not asked:  whether `ListParts` should answer
/// after completion is an open question, recorded in the punch list,
/// and the corpus must not assume an answer to it.
/* `ListParts` needs an upload id, which only a pass that created the
 * upload has;  an observing pass skips it. */
async fn list_parts_into(
    client: &aws_sdk_s3::Client,
    b: &str,
    u: &Upload,
    m: &mut Manifest,
) -> usize {
    let r = client
        .list_parts()
        .bucket(b)
        .key(&u.key)
        .upload_id(&u.upload_id)
        .send()
        .await
        .expect("list_parts on an upload still in flight");
    let mut n = 0;
    for p in r.parts() {
        n += 1;
        if let (Some(num), Some(sz)) = (p.part_number(), p.size()) {
            m.object(&u.key, &format!("part.{num:05}.size"), &sz.to_string());
        }
    }
    m.object(&u.key, "parts-listed", &n.to_string());
    n
}

#[tokio::test]
async fn corpus_coverage_multipart() {
    let tier = env_or("CORPUS_TIER", "coverage");
    if tier != "coverage" {
        return;
    }
    let client = get_client();
    let b = bucket(&tier, "mpu");
    ensure_bucket(&client, &b).await;

    let mut m = Manifest::new();
    m.bucket(&b, "tier", &tier);

    /* Warm the listing cache before any upload completes.
     *
     * Deliberate:  a listing that misses refills from the store and
     * derives every field correctly, so it reads the same whatever the
     * completion path wrote into the cache.  Two defects lived in that
     * gap.  The sentinel object is here only to give the first listing
     * something to find. */
    if !crate::generate::observing() {
        client
            .put_object()
            .bucket(&b)
            .key("m-warm-sentinel")
            .body(ByteStream::from(b"warm".to_vec()))
            .send()
            .await
            .expect("sentinel put");
    }
    /* The warming listing itself is a read, so an observing pass still
     * makes it -- the cache is warmed by the first listing either way,
     * and the sentinel exists only to give that listing something to
     * find.  It is already in the tree by then. */
    let warm = client
        .list_objects_v2()
        .bucket(&b)
        .fetch_owner(true)
        .send()
        .await
        .expect("warming listing");
    m.bucket(&b, "warmed-with", &warm.key_count().unwrap_or(0).to_string());

    let mut uploads = Vec::new();
    for (pc, pp, st) in MPU_COVERAGE {
        uploads.push(stage_one(&client, &b, *pc, *pp, *st, &mut m).await);
    }

    /* in-flight uploads, as S3 reports them */
    let mpus = client
        .list_multipart_uploads()
        .bucket(&b)
        .send()
        .await
        .expect("list_multipart_uploads");
    let open: std::collections::BTreeSet<String> =
        mpus.uploads().iter().filter_map(|u| u.key().map(String::from)).collect();
    for k in &open {
        m.object(k, "listed-in-flight", "yes");
    }
    m.bucket(&b, "uploads.in-flight", &open.len().to_string());

    for u in &uploads {
        if u.state == UploadState::InFlight {
            if !crate::generate::observing() {
                list_parts_into(&client, &b, u, &mut m)
                    .await;
            }
        }
    }

    /* the completed objects, on the warm cache */
    let listed = client
        .list_objects_v2()
        .bucket(&b)
        .fetch_owner(true)
        .send()
        .await
        .expect("final listing");
    let mut seen = std::collections::BTreeMap::new();
    for o in listed.contents() {
        let k = o.key().unwrap_or_default().to_string();
        let owner = o.owner().and_then(|ow| ow.id()).unwrap_or("").to_string();
        if let Some(sz) = o.size() {
            m.object(&k, "listed-size", &sz.to_string());
        }
        if let Some(e) = o.e_tag() {
            m.object(&k, "listed-etag", e);
        }
        m.object(&k, "listed-owner",
                 if owner.is_empty() { "<none>" } else { &owner });
        seen.insert(k, (o.size(), o.e_tag().map(String::from), owner));
    }

    /* The assembled bytes, which the composite ETag does not cover:
     * that ETag agrees with the *part* digests, so an assembly which
     * took the right parts in the wrong order -- or dropped one and
     * padded to length -- can still carry the expected value.  Every
     * part is the same fill, so a completed object should read back as
     * `total` bytes of it.  No ETag is involved, so this holds in
     * either regime.
     *
     * Before the manifest is rendered, because it records facts. */
    let mut body_problems: Vec<String> = Vec::new();
    for u in &uploads {
        if !matches!(u.state, UploadState::Completed) {
            continue;
        }
        crate::integrity::check_body_digest(
            &client,
            &b,
            &u.key,
            crate::integrity::EtagCheck::Composite,
            Some(&crate::integrity::md5_hex(&vec![FILL; u.total])),
            &mut m,
            &mut body_problems,
        )
        .await;
    }

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-mpu.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-mpu"));

    eprintln!(
        "corpus {tier}/mpu: {} uploads, {} in flight, {} listed, {} facts -> {}",
        uploads.len(),
        open.len(),
        seen.len(),
        m.len(),
        path.display()
    );

    /* Reference-free properties.  Each holds of any S3 implementation
     * and none needs a second gateway to compare against. */
    let mut problems = mode_problems;
    problems.extend(body_problems);

    for u in &uploads {
        match u.state {
            UploadState::Completed => {
                let Some((size, etag, owner)) = seen.get(&u.key) else {
                    problems.push(format!(
                        "{:?} completed and is not listed", u.key
                    ));
                    continue;
                };
                if *size != Some(u.total as i64) {
                    problems.push(format!(
                        "{:?} is {} parts totalling {} bytes and lists {:?}",
                        u.key, u.parts, u.total, size
                    ));
                }
                /* the part count is the etag's suffix, which is how a
                 * client tells a multipart object from a plain one
                 *
                 * NooBaa does not produce an S3 multipart ETag at all.
                 * Measured at the pin:  every completed upload comes
                 * back `mtime-<base36>-ino-<base36>` -- an identity
                 * token for the file instance, with no digest and no
                 * part count.  Gated rather than fixed, because it is
                 * what their implementation does;  the consequence for
                 * adoption is in the punch list, and it is that an
                 * adopted object either changes ETag under clients or
                 * we store NooBaa's verbatim. */
                #[cfg(not(feature = "fails_on_noobaa"))]
                {
                    let e = etag.clone().unwrap_or_default();
                    let suffix = format!("-{}", u.parts);
                    if !e.trim_matches('"').ends_with(&suffix) {
                        problems.push(format!(
                            "{:?} has {} parts and etag {e}, which does not \
                             end {suffix:?}",
                            u.key, u.parts
                        ));
                    }

                    /* And the whole composite, not only its suffix.
                     * The corpus chose the part layout and writes a
                     * known fill, so `hex(MD5(d1||..||dN))-N` is
                     * computable here -- which is what makes this a
                     * check on the assembly rather than on the shape
                     * of the string.  Only in the digest regime:  a
                     * synthetic ETag has no digest to agree with. */
                    if crate::integrity::digest_regime() {
                        let want = crate::integrity::expected_multipart_etag(
                            &u.sizes, FILL,
                        );
                        let got = e.trim_matches('"');
                        if got != want {
                            problems.push(format!(
                                "{:?} reports etag {got:?};  its {} parts of \
                                 {:?} bytes compose to {want:?}",
                                u.key, u.parts, u.sizes
                            ));
                        }
                    }

                }
                if owner.is_empty() || owner == "unknown" {
                    problems.push(format!(
                        "{:?} lists owner {:?} after completing on a warm cache",
                        u.key,
                        if owner.is_empty() { "<none>" } else { owner }
                    ));
                }
                if open.contains(&u.key) {
                    problems.push(format!(
                        "{:?} completed and is still listed in flight", u.key
                    ));
                }
            }
            UploadState::InFlight => {
                if !open.contains(&u.key) {
                    problems.push(format!(
                        "{:?} is in flight and ListMultipartUploads omits it",
                        u.key
                    ));
                }
                if seen.contains_key(&u.key) {
                    problems.push(format!(
                        "{:?} is in flight and listed as an object", u.key
                    ));
                }
            }
            UploadState::Aborted => {
                if open.contains(&u.key) {
                    problems.push(format!(
                        "{:?} was aborted and is still listed in flight", u.key
                    ));
                }
                if seen.contains_key(&u.key) {
                    problems.push(format!(
                        "{:?} was aborted and is listed as an object", u.key
                    ));
                }
            }
        }
    }

    assert!(
        problems.is_empty(),
        "the multipart corpus disagrees with what the gateway did:\n  {}",
        problems.join("\n  ")
    );
}
