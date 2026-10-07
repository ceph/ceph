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

fn env_or(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

fn out_dir() -> PathBuf {
    PathBuf::from(env_or("CORPUS_OUT", "target/corpus"))
}

/// Create the bucket if it is not there.  Stable names are the point,
/// so a rerun meets its own buckets;  that is tolerated rather than
/// treated as an error, and a corpus run against a dirty root is the
/// operator's problem to notice.
async fn ensure_bucket(client: &aws_sdk_s3::Client, name: &str) {
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

    let mut made = 0usize;
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
                made += 1;
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
    let mut token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(&b).max_keys(1000);
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let page = req.send().await.expect("the corpus bucket should list");
        for o in page.contents() {
            let key = o.key().unwrap_or_default();
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

    eprintln!(
        "corpus {tier}/keys: {made} of {} keys written, {} refused, {} facts -> {}",
        KeyClass::COVERAGE.len(),
        refused.len(),
        m.len(),
        path.display()
    );
    for (k, c) in &refused {
        eprintln!("  refused {k:?}: {c}");
    }

    /* The corpus is the product, so the only assertion is that it is
     * not empty -- an empty manifest means nothing was generated and
     * the later comparison would pass vacuously. */
    assert!(!m.is_empty(), "the corpus produced no facts");
}
