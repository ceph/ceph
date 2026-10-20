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
    PathBuf::from(env_or(crate::env::OUT, "target/corpus"))
}

/// Note on the manifest how this capture was taken.
///
/// Two captures of one tree are comparable;  a generated one and an
/// observed one are not, because only the first created what it
/// describes -- so the facts a write produces are present in one and
/// structurally absent from the other.  The
/// `expected-differences.tsv` row for `put-*` is keyed on this.
pub fn note_mode(m: &mut Manifest, bucket: &str) {
    m.bucket(bucket, "capture-mode",
             if observing() { "observe" } else { "generate" });
}

/// Record what is there;  create nothing.
///
/// The second half of a comparison reads the tree the *first* gateway
/// wrote.  An axis that created its objects first would describe its
/// own, which says nothing about that tree -- and over a tree being
/// served read-only the creation fails outright.
///
/// **The model stays the driver.**  The alternative was to take the
/// first capture's manifest as a work list and re-ask each fact of the
/// second gateway, which needs no per-axis change -- but then the
/// artifact becomes the specification and the parameter matrix stops
/// being the authority.  Keeping it here means the axis still knows
/// the shape it is looking for, still drives both artifacts, and still
/// asserts at capture time rather than deferring everything to a
/// diff.
///
/// What an observing axis keeps:  every fact it records, and every
/// assertion about the *tree*.  What it drops:  assertions about its
/// own writing, which have no subject here.
pub fn observing() -> bool {
    matches!(env_or(crate::env::OBSERVE, "0").as_str(),
             "1" | "on" | "true" | "yes")
}

/// The bucket's owner, and what is wrong with its ownership.
///
/// An object that records no owner is its bucket owner's -- NooBaa
/// gives every object its bucket's `owner_account`, and nsfs carries
/// that rule through adoption -- and an object RGW writes into an
/// adopted bucket is the bucket owner's too, because adoption makes the
/// bucket BucketOwnerEnforced.  So in this corpus every listed owner
/// is the bucket's, and the axes compare against what this returns.
///
/// The owner is read with GetBucketAcl;  the object-ownership control
/// is recorded as a fact.  In observe mode the tree was written by
/// another gateway and adopted, and two things must hold of it:  the
/// control is BucketOwnerEnforced, and the owner is the account the
/// corpus runs as -- the account NooBaa's record names, imported.  A
/// bucket whose record was not imported keeps NooBaa's raw id as its
/// owner, so the second check can fail.  In generate mode the corpus
/// makes its own buckets and RGW's defaults stand.
pub async fn bucket_ownership(
    client: &aws_sdk_s3::Client,
    m: &mut Manifest,
    b: &str,
) -> (String, Vec<String>) {
    let mut problems = Vec::new();
    let owner = match client.get_bucket_acl().bucket(b).send().await {
        Ok(r) => r.owner().and_then(|o| o.id()).unwrap_or("").to_string(),
        Err(e) => {
            problems.push(format!("GetBucketAcl on {b:?} failed: {e:?}"));
            String::new()
        }
    };
    if owner.is_empty() && problems.is_empty() {
        problems.push(format!("GetBucketAcl on {b:?} names no owner"));
    }

    let controls = match client.get_bucket_ownership_controls().bucket(b).send().await {
        Ok(r) => r
            .ownership_controls()
            .and_then(|c| c.rules().first())
            .map(|r| r.object_ownership().as_str().to_string())
            .unwrap_or_else(|| "<none>".to_string()),
        Err(e) => e
            .into_service_error()
            .meta()
            .code()
            .unwrap_or("Unknown")
            .to_string(),
    };
    m.bucket(b, "ownership-controls", &controls);
    if observing() && controls != "BucketOwnerEnforced" {
        problems.push(format!(
            "{b:?} is an adopted bucket and its object ownership is \
             {controls:?}, not BucketOwnerEnforced"
        ));
    }
    if observing() && !owner.is_empty() {
        /* The account, not the user:  ListBuckets names the caller's
         * user, and an account's bucket is owned by the account. */
        let cfg = s3_tests_rs::config::get_config();
        let caller = s3_tests_rs::client::build_sts_client(
            &cfg.main_access_key, &cfg.main_secret_key)
            .get_caller_identity()
            .send()
            .await;
        match caller {
            Ok(r) => {
                let account = r.account().unwrap_or("").to_string();
                if owner != account {
                    problems.push(format!(
                        "{b:?} is an adopted bucket owned by {owner:?}, not by \
                         the account the corpus runs as ({account:?})"
                    ));
                }
            }
            Err(e) => problems.push(format!(
                "GetCallerIdentity failed, so {b:?}'s owner cannot be \
                 checked: {:?}", e.into_service_error()
            )),
        }
    }
    (owner, problems)
}

/// Create the bucket if it is not there.  Stable names are the point,
/// so a rerun meets its own buckets;  that is tolerated rather than
/// treated as an error, and a corpus run against a dirty root is the
/// operator's problem to notice.
pub async fn ensure_bucket(client: &aws_sdk_s3::Client, name: &str) {
    /* An observing pass describes a tree another gateway wrote, so it
     * creates nothing -- not even the bucket.  Here rather than in each
     * axis, because every axis opens this way and they would otherwise
     * have to be kept in step by hand. */
    if observing() {
        return;
    }
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
 * not.  nsfs rejects the same keys with InvalidObjectName. */
#[cfg_attr(feature = "fails_on_posix",
           ignore = "posix: dot-prefixed key accepted, then not listed")]
#[tokio::test]
async fn corpus_coverage_keys() {
    keys_axis("keys", KeyClass::COVERAGE).await;
}

/// The maximum-length keys, in their own bucket:  see
/// `KeyClass::MAX_LENGTH`.
#[tokio::test]
async fn corpus_coverage_keys_maxlen() {
    keys_axis("keys-maxlen", KeyClass::MAX_LENGTH).await;
}

/// One key-naming bucket, `name`, holding `classes`.
async fn keys_axis(name: &str, classes: &[KeyClass]) {
    let tier = env_or(crate::env::TIER, "coverage");
    if tier != "coverage" {
        eprintln!("CORPUS_TIER={tier};  not this tier");
        return;
    }

    let client = get_client();
    let b = bucket(&tier, name);
    ensure_bucket(&client, &b).await;

    let mut m = Manifest::new();
    m.bucket(&b, "tier", &tier);
    note_mode(&mut m, &b);

    let mut written: Vec<String> = Vec::new();
    let mut errors = Vec::new();
    /* What was written, so what is read back can be compared to it.
     * The bodies are derived from the class, so this is knowledge the
     * axis already has rather than state it has to keep. */
    let mut wrote_digest: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    for class in classes {
        let key = class.key();
        let body = format!("{:?}", class);

        /* What the *model* says about this key, recorded in either
         * mode:  the class and its rationale come from `KeyClass`, not
         * from the gateway, so an observing pass has them too.  Only
         * the outcome of a write is a fact a write produces. */
        m.object(&key, "class", &format!("{class:?}"));
        m.object(&key, "rationale", class.rationale());

        /* The body is derived from the class as well, so an observing
         * pass knows what the bytes should be without having written
         * them -- which is what lets the content check below survive. */
        wrote_digest.insert(key.clone(),
                            crate::integrity::md5_hex(body.as_bytes()));
        if observing() {
            continue;
        }

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
            }
            Err(e) => {
                /* The gateway's error code is recorded like any other
                 * fact, and the axis carries on:  a key that is also a
                 * prefix cannot be written to nsfs at all, and a
                 * 1024-byte key in one component exceeds NAME_MAX.
                 * Recording the code is the point of generating them. */
                let info = extract_s3_error(&e);
                m.object(&key, "put-error", &info.error_code);
                m.object(&key, "put-status", &info.status.to_string());
                if let Some(msg) = &info.message {
                    m.object(&key, "put-message", msg);
                }
                errors.push((
                    key.clone(),
                    format!("{} {}", info.status, info.error_code),
                ));
            }
        }
    }

    /* What S3 says about what landed */
    let mut listed: Vec<String> = Vec::new();
    /* Kept rather than only recorded:  the integrity check needs the
     * ETag the gateway reported for the key it reported it for. */
    let mut reported: Vec<(String, String)> = Vec::new();
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
                reported.push((key.to_string(), e.to_string()));
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

    /* Does the ETag describe the bytes?  One GET per listed key, which
     * is the only read of an object's content anywhere in the corpus.
     * Recorded into the manifest before it is rendered, and the
     * failures folded into the axis's own assertion below. */
    m.bucket(&b, "etag-regime", crate::integrity::regime());
    let mut integrity: Vec<String> = Vec::new();
    for (key, etag) in &reported {
        crate::integrity::check_body_digest(
            &client,
            &b,
            key,
            crate::integrity::EtagCheck::Plain(Some(etag)),
            wrote_digest.get(key.as_str()).map(String::as_str),
            &mut m,
            &mut integrity,
        )
        .await;
    }

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-{name}.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-{name}"));
    assert!(
        mode_problems.is_empty(),
        "an object on disk does not carry the published mode:\n  {}",
        mode_problems.join("\n  ")
    );

    /* A client's object whose name starts with NooBaa's temp prefix is
     * an ordinary object under RGW (DECISIONS.md 2026-10-12), though
     * NooBaa's listing hides it.  Asserted in observe mode, where the
     * tree is adopted:  it must list, and listing it puts it through
     * the content check below. */
    if observing() && classes.contains(&KeyClass::ReservedTemp) {
        let key = KeyClass::ReservedTemp.key();
        assert!(listed.contains(&key),
            "{key:?} is not listed:  an object NooBaa masked by its temp \
             prefix must be an ordinary object after adoption");
    }

    assert!(
        integrity.is_empty(),
        "the reported ETag does not describe the object's bytes:\n  {}",
        integrity.join("\n  ")
    );


    eprintln!(
        "corpus {tier}/{name}: {} of {} keys written, {} errors, {} facts -> {}",
        written.len(),
        classes.len(),
        errors.len(),
        m.len(),
        path.display()
    );
    for (k, c) in &errors {
        eprintln!("  error {k:?}: {c}");
    }

    /* An empty manifest means nothing was generated and any later
     * comparison would pass vacuously. */
    assert!(!m.is_empty(), "the corpus produced no facts");

    /* Not asked of an observing pass:  it wrote nothing, so there is
     * no "what was written" for the listing to agree with.  The facts
     * and the content check above are what it is here for;  whether
     * the tree holds the right keys is the diff's question, and the
     * expected-difference list is where a missing key is named. */
    if !observing() {
        check_listing_invariants(&written, &listed);
    }
}

/// Properties true of any S3 implementation, so they need no reference
/// gateway and hold before the NooBaa half of the harness exists.
///
/// This is what separates the corpus from a recorder.  A gateway's error
/// code is a fact about a backend:  it is recorded, and the axis carries
/// on -- a key a gateway cannot write is something the corpus exists to
/// capture.  A
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
     * NooBaa accepts three keys this axis deliberately generates and
     * then never lists them:  `.folder`, anything under `.versions/`,
     * and anything starting with `.noobaa-nsfs`.  The last is a NooBaa
     * defect:  its temp directory is `.noobaa-nsfs_<bucket id>`, but
     * its listing hides the whole prefix, so a client's object named
     * that way is stored and lost from listings.  RGW lists it, which is
     * correct (DECISIONS.md 2026-10-12).  This gate, under
     * `fails_on_noobaa` only, tolerates NooBaa's behaviour so a NooBaa
     * capture still runs;  it is not a requirement, and no run against
     * RGW may require those keys hidden.  The other keys still have to
     * list, which is what makes the gate worth having over ignoring the
     * axis. */
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
