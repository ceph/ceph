//! Tier 1 — coverage.  One artifact per axis value, every value once.
//!
//! The corpus expects a gateway it may create buckets on and whose
//! root it is allowed to leave populated.  Nothing here cleans up:  the
//! artifacts *are* the product, and the next pass converts them.

use std::path::PathBuf;

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::error::extract_s3_error;

use crate::manifest::Manifest;
use crate::names::{bucket, KeyClass};

pub fn env_or(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

pub fn out_dir() -> PathBuf {
    PathBuf::from(env_or("CORPUS_OUT", "target/corpus"))
}

/// Create the bucket if it is not there.  Stable names are the point,
/// so a rerun meets its own buckets;  that is tolerated rather than
/// treated as an error, and a corpus run against a dirty root is the
/// operator's problem to notice.
pub async fn ensure_bucket(client: &aws_sdk_s3::Client, name: &str) {
    match client.create_bucket().bucket(name).send().await {
        Ok(_) => {}
        Err(e) => {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("BucketAlreadyOwnedByYou")
                    || msg.contains("BucketAlreadyExists"),
                "could not create {name}: {msg}"
            );
        }
    }
}

/// The coverage tier for key naming:  one object per key class, each
/// with a body whose length identifies it, and the manifest recording
/// what S3 then says.
/* posix: a key whose first component starts with a dot -- ".folder",
 * ".fofopidoo", ".versions/k-under-versions" -- is accepted by the PUT
 * and then dropped by the cold listing path, so the object exists and
 * no listing shows it.  Reproducible either way:  it fails on a
 * freshly cleaned tree and passes on the next run against the same
 * one, because fill_cache() filters those names and add_entry() does
 * not.  nsfs refuses the same keys with InvalidObjectName. */
#[cfg_attr(feature = "fails_on_posix",
           ignore = "posix: dot-prefixed key accepted, then not listed")]
#[tokio::test]
async fn corpus_coverage_keys() {
    let tier = env_or("CORPUS_TIER", "coverage");
    if tier != "coverage" {
        eprintln!("CORPUS_TIER={tier};  not this tier");
        return;
    }

    let client = get_client();
    let b = bucket(&tier, "keys");
    ensure_bucket(&client, &b).await;

    let mut m = Manifest::new();
    m.bucket(&b, "tier", &tier);

    let mut written: Vec<String> = Vec::new();
    let mut refused = Vec::new();

    for class in KeyClass::COVERAGE {
        let key = class.key();
        let body = format!("{:?}", class);
        let put = client
            .put_object()
            .bucket(&b)
            .key(&key)
            .body(ByteStream::from(body.clone().into_bytes()))
            .send()
            .await;

        match put {
            Ok(_) => {
                written.push(key.clone());
                m.object(&key, "class", &format!("{class:?}"));
                m.object(&key, "rationale", class.rationale());
            }
            Err(e) => {
                /* A refusal is a fact about the gateway, not a failure
                 * of the corpus:  a key that is also a prefix cannot be
                 * written to nsfs at all, and a 1024-byte key in one
                 * component exceeds NAME_MAX.  Recording the refusal is
                 * the point of generating them. */
                let info = extract_s3_error(&e);
                m.object(&key, "put-refused", &info.error_code);
                m.object(&key, "put-status", &info.status.to_string());
                if let Some(msg) = &info.message {
                    m.object(&key, "put-message", msg);
                }
                refused.push((
                    key.clone(),
                    format!("{} {}", info.status, info.error_code),
                ));
            }
        }
    }

    /* What S3 says about what landed */
    let mut listed: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(&b).max_keys(1000);
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let page = req.send().await.expect("the corpus bucket should list");
        for o in page.contents() {
            let key = o.key().unwrap_or_default();
            listed.push(key.to_string());
            m.object(key, "listed", "yes");
            if let Some(sz) = o.size() {
                m.object(key, "size", &sz.to_string());
            }
            if let Some(e) = o.e_tag() {
                m.object(key, "etag", e);
            }
        }
        for p in page.common_prefixes() {
            if let Some(pfx) = p.prefix() {
                m.fact("prefix", pfx, "listed", "yes");
            }
        }
        if page.is_truncated() == Some(true) {
            token = page.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-keys.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-keys"));
    assert!(
        mode_problems.is_empty(),
        "an object on disk does not carry the published mode:\n  {}",
        mode_problems.join("\n  ")
    );

    eprintln!(
        "corpus {tier}/keys: {} of {} keys written, {} refused, {} facts -> {}",
        written.len(),
        KeyClass::COVERAGE.len(),
        refused.len(),
        m.len(),
        path.display()
    );
    for (k, c) in &refused {
        eprintln!("  refused {k:?}: {c}");
    }

    /* An empty manifest means nothing was generated and any later
     * comparison would pass vacuously. */
    assert!(!m.is_empty(), "the corpus produced no facts");

    check_listing_invariants(&written, &listed);
}

/// Properties true of any S3 implementation, so they need no reference
/// gateway and hold before the NooBaa half of the harness exists.
///
/// This is what separates the corpus from a recorder.  A *refusal* is a
/// fact about a backend and is recorded rather than failed -- a key a
/// gateway cannot write is something the corpus exists to capture.  A
/// listing that disagrees with what was written is not a fact about
/// anything;  it is a defect, and the run should say so without waiting
/// for a second gateway to compare against.
fn check_listing_invariants(written: &[String], listed: &[String]) {
    let mut problems: Vec<String> = Vec::new();

    /* No key the listing returns is empty.  An empty key is not a
     * valid S3 object name and cannot be addressed, copied or
     * deleted, so a client meeting one has no way forward. */
    let empties = listed.iter().filter(|k| k.is_empty()).count();
    if empties > 0 {
        problems.push(format!("{empties} listed key(s) are empty"));
    }

    /* No key is listed twice.  One object, one entry. */
    let mut seen = std::collections::BTreeMap::new();
    for k in listed {
        *seen.entry(k.as_str()).or_insert(0usize) += 1;
    }
    for (k, n) in &seen {
        if *n > 1 {
            problems.push(format!("{k:?} listed {n} times"));
        }
    }

    /* Everything written is listed:  catches silent loss.
     *
     * NooBaa hides its own scaffolding by prefix match, so three keys
     * this axis deliberately generates are accepted and then never
     * listed:  `.folder`, anything under `.versions/`, and anything
     * under their `.noobaa-nsfs` temp prefix.  The KeyClass variants
     * are named for exactly that -- ReservedFolder, ReservedVersions,
     * ReservedTemp -- so the behaviour is the expectation rather than
     * a surprise, and the gate records it instead of asserting against
     * it.  The other eleven keys still have to list, which is what
     * makes the gate worth having over ignoring the axis. */
    for w in written {
        if !seen.contains_key(w.as_str()) {
            #[cfg(feature = "fails_on_noobaa")]
            if w.starts_with(".folder")
                || w.starts_with(".versions/")
                || w.starts_with(".noobaa-nsfs")
            {
                continue;
            }
            problems.push(format!("{w:?} was written and is not listed"));
        }
    }

    /* Everything listed was written:  catches phantoms, which is how a
     * reserved-name collision surfaces -- one file on disk emitted
     * under a second, invented key. */
    let wset: std::collections::BTreeSet<&str> =
        written.iter().map(|s| s.as_str()).collect();
    for k in seen.keys() {
        if !wset.contains(k) {
            problems.push(format!("{k:?} is listed and was never written"));
        }
    }

    assert!(
        problems.is_empty(),
        "the listing disagrees with what the corpus wrote:\n  {}",
        problems.join("\n  ")
    );
}
