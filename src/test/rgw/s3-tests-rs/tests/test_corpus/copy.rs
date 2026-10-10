//! Tier 1 -- the copy axis.
//!
//! Copy is the last artifact-creating path with no coverage here, and
//! the one with an open question against it:  `POSIXObject`'s and
//! `NSFSObject`'s copy paths fill the destination's listing-cache entry
//! from the *destination object's in-memory attributes*, which for a
//! new object hold no ACL.  The same shape in the PUT and multipart
//! paths produced entries with no owner, and the multipart axis settled
//! those two by completing into a bucket it had listed first.  This
//! does the same for copy.
//!
//! So the bucket is listed before any copy runs.  A listing that misses
//! refills from the store and derives every field correctly, so it
//! reads the same whatever the copy path wrote into the cache;  warming
//! it first is what makes the final listing report what copy actually
//! did.
//!
//! Two more reasons for this axis:  `FSStrategy::clone_file()` is on
//! the copy path and was one of the creates that asked for 0644, and a
//! multipart source has a `-<parts>` ETag that a copy re-assembles,
//! so what the copy's ETag becomes is a question rather than an
//! assumption.  It is captured, not asserted.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{MetadataDirective, TaggingDirective};
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::check_configure_versioning_retry;

use crate::generate::{ensure_bucket, env_or, out_dir};
use crate::manifest::Manifest;
use crate::names::bucket;
use crate::shapes::{CopyDest, CopySource, Directive, COPY_COVERAGE};

const PART: usize = 64;

struct Copied {
    src_key: String,
    dst_key: String,
    dst_bucket: String,
    declared_size: usize,
    meta: Directive,
    refused: Option<String>,
}

/// Write the source object for one case and return its size.
async fn make_source(
    client: &aws_sdk_s3::Client,
    b: &str,
    key: &str,
    shape: CopySource,
) -> usize {
    /* An observing pass creates no sources.  The *size* each shape
     * declares is derived, so it is returned without the write -- and
     * writing them would be worse than useless:  re-putting a source
     * undoes a Replace copy the first gateway made onto it, which is
     * exactly what the self-copy case exists to catch.  The comparison
     * found that rather than a review. */
    if crate::generate::observing() {
        return match shape {
            CopySource::Empty => 0,
            CopySource::Plain => 128,
            CopySource::Multipart => PART * 2,
        };
    }
    match shape {
        CopySource::Empty => {
            client
                .put_object()
                .bucket(b)
                .key(key)
                .body(ByteStream::from(Vec::new()))
                .metadata("srcmeta", "from-source")
                .content_type("text/plain")
                .tagging("ck=cv")
                .send()
                .await
                .expect("source put");
            0
        }
        CopySource::Plain => {
            let body = vec![b'c'; 128];
            client
                .put_object()
                .bucket(b)
                .key(key)
                .body(ByteStream::from(body.clone()))
                .metadata("srcmeta", "from-source")
                .content_type("text/plain")
                .tagging("ck=cv")
                .send()
                .await
                .expect("source put");
            body.len()
        }
        CopySource::Multipart => {
            let create = client
                .create_multipart_upload()
                .bucket(b)
                .key(key)
                .metadata("srcmeta", "from-source")
                .content_type("text/plain")
                .tagging("ck=cv")
                .customize()
                .mutate_request(s3_tests_rs::http::zero_content_length)
                .send()
                .await
                .expect("create_multipart_upload");
            let upload_id = create.upload_id().unwrap().to_string();
            let mut done = Vec::new();
            for n in 1..=2i32 {
                let r = client
                    .upload_part()
                    .bucket(b)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(n)
                    .body(ByteStream::from(vec![b'c'; PART]))
                    .send()
                    .await
                    .expect("upload_part");
                done.push(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .e_tag(r.e_tag().unwrap())
                        .part_number(n)
                        .build(),
                );
            }
            client
                .complete_multipart_upload()
                .bucket(b)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(
                    aws_sdk_s3::types::CompletedMultipartUpload::builder()
                        .set_parts(Some(done))
                        .build(),
                )
                .send()
                .await
                .expect("complete_multipart_upload");
            PART * 2
        }
    }
}

/* posix: a copy into an unversioned bucket is never added to the
 * listing cache, so the object is on disk, answerable by HEAD, and
 * absent from ListObjectsV2 for as long as that bucket's cache stays
 * warm.  POSIXObject::copy() does not touch the cache at all -- its
 * three cache updates are in delete_object(), link_temp_file() and
 * POSIXMultipartUpload::complete().  Measured here on all six
 * non-self copies.  nsfs had the same defect in its own shape and is
 * fixed;  posix is left for rgw-standalone, which owns that line. */
#[cfg_attr(feature = "fails_on_posix",
           ignore = "posix: a copy is not added to the listing cache")]
#[tokio::test]
async fn corpus_coverage_copy() {
    let tier = env_or("CORPUS_TIER", "coverage");
    if tier != "coverage" {
        return;
    }
    let client = get_client();
    let b = bucket(&tier, "copy");
    let other = bucket(&tier, "copy-dst");
    /* A versioned destination, because the driver's copy path forks on
     * it and only that branch hand-builds the new object's cache
     * entry. */
    let versioned = bucket(&tier, "copy-ver");
    ensure_bucket(&client, &b).await;
    ensure_bucket(&client, &other).await;
    ensure_bucket(&client, &versioned).await;
    if !crate::generate::observing() {
        check_configure_versioning_retry(&client, &versioned, "Enabled",
                                        "Enabled").await;
    }

    let mut m = Manifest::new();
    m.bucket(&b, "tier", &tier);
    m.bucket(&other, "tier", &tier);
    m.bucket(&versioned, "tier", &tier);
    m.bucket(&versioned, "versioning", "Enabled");

    /* sources first, then warm the cache on both buckets, then copy --
     * so what the copy path writes into the cache is what the final
     * listing reads */
    let mut cases = Vec::new();
    for (i, (shape, dest, meta, tags)) in COPY_COVERAGE.iter().enumerate() {
        let src_key = format!("c{i:02}-{}-src", shape.label());
        let size = make_source(&client, &b, &src_key, *shape).await;
        m.object(&src_key, "declared-size", &size.to_string());
        m.object(&src_key, "source-shape", shape.label());
        cases.push((i, *shape, *dest, *meta, *tags, src_key, size));
    }

    for bk in [&b, &other, &versioned] {
        let w = client
            .list_objects_v2()
            .bucket(bk)
            .fetch_owner(true)
            .send()
            .await
            .expect("warming listing");
        m.bucket(bk, "warmed-with", &w.key_count().unwrap_or(0).to_string());
    }

    let mut copies = Vec::new();
    for (i, _shape, dest, meta, tags, src_key, size) in cases {
        let (dst_bucket, dst_key) = match dest {
            CopyDest::SameBucket => (b.clone(), format!("c{i:02}-dst")),
            CopyDest::OtherBucket => (other.clone(), format!("c{i:02}-dst")),
            CopyDest::Versioned => (versioned.clone(), format!("c{i:02}-dst")),
            CopyDest::SelfSame => (b.clone(), src_key.clone()),
        };

        let mut req = client
            .copy_object()
            .bucket(&dst_bucket)
            .key(&dst_key)
            .copy_source(format!("{b}/{src_key}"));
        req = match meta {
            Directive::Copy => req.metadata_directive(MetadataDirective::Copy),
            Directive::Replace => req
                .metadata_directive(MetadataDirective::Replace)
                .metadata("dstmeta", "from-copy")
                .content_type("application/octet-stream"),
        };
        req = match tags {
            Directive::Copy => req.tagging_directive(TaggingDirective::Copy),
            Directive::Replace => req
                .tagging_directive(TaggingDirective::Replace)
                .tagging("dk=dv"),
        };

        m.object(&dst_key, "copy.dest", dest.label());
        m.object(&dst_key, "copy.metadata-directive", meta.label());
        m.object(&dst_key, "copy.tagging-directive", tags.label());
        m.object(&dst_key, "copy.source", &src_key);

        /* An observing pass makes no copies.  The four facts above are
         * the model -- destination, directives and source all come from
         * the axis's matrix -- and what it cannot have is what the copy
         * reported, which is why `copy.reported-*` are outcome facts.
         *
         * It falls through rather than skipping the iteration:  the
         * descriptor has to reach `copies` so the destinations the
         * first gateway left are still read back below.  Skipping it
         * emptied that list and silently dropped 47 facts. */
        let refused = if crate::generate::observing() {
            None
        } else {
            match req.send().await {
            Ok(r) => {
                if let Some(cor) = r.copy_object_result() {
                    if let Some(e) = cor.e_tag() {
                        m.object(&dst_key, "copy.reported-etag", e);
                    }
                }
                if let Some(v) = r.version_id() {
                    m.object(&dst_key, "copy.reported-version-id", v);
                }
                None
            }
            Err(e) => {
                let svc = e.into_service_error();
                let code = svc.meta().code().unwrap_or("Unknown").to_string();
                m.object(&dst_key, "copy.refused", &code);
                Some(code)
            }
            }
        };

        copies.push(Copied {
            src_key,
            dst_key,
            dst_bucket,
            declared_size: size,
            meta,
            refused,
        });
    }

    /* what the two buckets say afterwards, on the warm cache */
    let mut listed = std::collections::BTreeMap::new();
    for bk in [&b, &other, &versioned] {
        let page = client
            .list_objects_v2()
            .bucket(bk)
            .fetch_owner(true)
            .send()
            .await
            .expect("final listing");
        for o in page.contents() {
            let k = o.key().unwrap_or_default().to_string();
            let owner = o.owner().and_then(|ow| ow.id()).unwrap_or("").to_string();
            m.object(&k, "listed-size", &o.size().unwrap_or(-1).to_string());
            m.object(&k, "listed-owner",
                     if owner.is_empty() { "<none>" } else { &owner });
            listed.insert((bk.clone(), k), (o.size(), owner));
        }
    }

    /* and what HEAD says about each copy, which is where the
     * directives show up */
    for c in &copies {
        if c.refused.is_some() {
            continue;
        }
        if let Ok(h) = client
            .head_object()
            .bucket(&c.dst_bucket)
            .key(&c.dst_key)
            .send()
            .await
        {
            if let Some(n) = h.content_length() {
                m.object(&c.dst_key, "head-size", &n.to_string());
            }
            if let Some(e) = h.e_tag() {
                m.object(&c.dst_key, "head-etag", e);
            }
            if let Some(ct) = h.content_type() {
                m.object(&c.dst_key, "head-content-type", ct);
            }
            if let Some(meta) = h.metadata() {
                for (k, v) in meta {
                    m.object(&c.dst_key, &format!("head-meta.{k}"), v);
                }
            }
        }
    }

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-copy.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mut mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-copy"));
    mode_problems
        .extend(crate::inventory::capture(&other, &dir, &format!("{tier}-copy-dst")));
    mode_problems
        .extend(crate::inventory::capture(&versioned, &dir, &format!("{tier}-copy-ver")));

    eprintln!(
        "corpus {tier}/copy: {} copies, {} refused, {} facts -> {}",
        copies.len(),
        copies.iter().filter(|c| c.refused.is_some()).count(),
        m.len(),
        path.display()
    );
    for c in &copies {
        if let Some(code) = &c.refused {
            eprintln!("  refused {:?} -> {:?}: {code}", c.src_key, c.dst_key);
        }
    }

    /* Reference-free properties. */
    let mut problems = mode_problems;
    for c in &copies {
        if c.refused.is_some() {
            /* A refusal is a fact, not a failure -- self-copy without
             * REPLACE is refused by S3 itself, and this backend has a
             * recorded gap there.  The manifest records the code. */
            continue;
        }
        let Some((size, owner)) = listed.get(&(c.dst_bucket.clone(), c.dst_key.clone()))
        else {
            problems.push(format!(
                "{:?} was copied and is not listed in {:?}", c.dst_key, c.dst_bucket
            ));
            continue;
        };
        if *size != Some(c.declared_size as i64) {
            problems.push(format!(
                "{:?} copies {} bytes from {:?} and lists {:?}",
                c.dst_key, c.declared_size, c.src_key, size
            ));
        }
        if owner.is_empty() || owner == "unknown" {
            problems.push(format!(
                "{:?} lists owner {:?} after a copy onto a warm cache",
                c.dst_key,
                if owner.is_empty() { "<none>" } else { owner }
            ));
        }
        /* The metadata directive is the one copy-specific contract
         * that is checkable without a reference:  COPY must carry the
         * source's user metadata across, REPLACE must not.
         *
         * The self-copy is asserted like any other now.  REPLACE onto
         * the object itself used to *merge* -- the replacement applied
         * and the source's user metadata survived with it -- because
         * the attribute write merges and nothing removed the keys the
         * request had dropped.  That is what this case exists to
         * catch. */
        let rendered = m.render();
        let has_src = rendered.contains(&format!(
            "object\t{}\thead-meta.srcmeta\t", crate::manifest::encode(&c.dst_key)));
        let has_dst = rendered.contains(&format!(
            "object\t{}\thead-meta.dstmeta\t", crate::manifest::encode(&c.dst_key)));
        match c.meta {
            Directive::Copy if !has_src => problems.push(format!(
                "{:?} was copied with MetadataDirective::Copy and lost the \
                 source's user metadata", c.dst_key)),
            Directive::Replace if has_src => problems.push(format!(
                "{:?} was copied with MetadataDirective::Replace and kept the \
                 source's user metadata", c.dst_key)),
            Directive::Replace if !has_dst => problems.push(format!(
                "{:?} was copied with MetadataDirective::Replace and does not \
                 carry the replacement metadata", c.dst_key)),
            _ => {}
        }
    }

    assert!(
        problems.is_empty(),
        "the copy corpus disagrees with what the gateway did:\n  {}",
        problems.join("\n  ")
    );
}
