//! Tier 1 — the shape axes.  Size, user metadata, tags.
//!
//! Separate from the key-naming tier because the manifest wants
//! different facts:  naming is answered by a listing, shape is answered
//! by HEAD and GetObjectTagging per object.  A listing does not carry
//! user metadata or a tag set, so a corpus that only lists cannot tell
//! whether either survived.

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::error::extract_s3_error;

use crate::generate::{ensure_bucket, env_or, out_dir};
use crate::manifest::Manifest;
use crate::names::bucket;
use crate::shapes::{MetaClass, SizeClass, TagClass};

/// Record what S3 says about one object:  the facts a listing omits.
async fn head_into(
    client: &aws_sdk_s3::Client,
    b: &str,
    key: &str,
    m: &mut Manifest,
) {
    match client.head_object().bucket(b).key(key).send().await {
        Ok(h) => {
            if let Some(n) = h.content_length() {
                m.object(key, "size", &n.to_string());
            }
            if let Some(e) = h.e_tag() {
                m.object(key, "etag", e);
            }
            if let Some(c) = h.content_type() {
                m.object(key, "content-type", c);
            }
            if let Some(c) = h.content_encoding() {
                m.object(key, "content-encoding", c);
            }
            if let Some(sc) = h.storage_class() {
                m.object(key, "storage-class", sc.as_str());
            }
            /* user metadata, one fact per header so a diff names the
             * header that differs rather than the object */
            if let Some(meta) = h.metadata() {
                for (k, v) in meta {
                    m.object(key, &format!("meta.{k}"), v);
                }
            }
        }
        Err(e) => {
            let info = extract_s3_error(&e);
            m.object(key, "head-failed", &info.error_code);
            m.object(key, "head-status", &info.status.to_string());
        }
    }

    match client.get_object_tagging().bucket(b).key(key).send().await {
        Ok(t) => {
            let set = t.tag_set();
            m.object(key, "tags.count", &set.len().to_string());
            for tag in set {
                m.object(key, &format!("tag.{}", tag.key()), tag.value());
            }
        }
        Err(e) => {
            let info = extract_s3_error(&e);
            m.object(key, "tagging-failed", &info.error_code);
        }
    }
}

#[tokio::test]
async fn corpus_coverage_objects() {
    let tier = env_or("CORPUS_TIER", "coverage");
    if tier != "coverage" {
        eprintln!("CORPUS_TIER={tier};  not this tier");
        return;
    }

    let client = get_client();
    let b = bucket(&tier, "objects");
    ensure_bucket(&client, &b).await;

    let mut m = Manifest::new();
    m.bucket(&b, "tier", &tier);

    let mut written: Vec<String> = Vec::new();
    let mut refused: Vec<(String, String)> = Vec::new();

    /* size */
    for sc in SizeClass::COVERAGE {
        let key = sc.key();
        let r = client
            .put_object()
            .bucket(&b)
            .key(&key)
            .body(ByteStream::from(sc.body()))
            .send()
            .await;
        record(&mut m, &mut written, &mut refused, &key, r, |m| {
            m.object(&key, "class", &format!("{sc:?}"));
            m.object(&key, "declared-len", &sc.len().to_string());
        });
    }

    /* user metadata */
    for mc in MetaClass::COVERAGE {
        let key = mc.key();
        let mut req = client
            .put_object()
            .bucket(&b)
            .key(&key)
            .body(ByteStream::from(b"m".to_vec()));
        for (k, v) in mc.headers() {
            req = req.metadata(k, v);
        }
        let r = req.send().await;
        record(&mut m, &mut written, &mut refused, &key, r, |m| {
            m.object(&key, "class", &format!("{mc:?}"));
            m.object(&key, "rationale", mc.rationale());
            m.object(&key, "declared-headers",
                     &mc.headers().len().to_string());
        });
    }

    /* tags */
    for tc in TagClass::COVERAGE {
        let key = tc.key();
        let mut req = client
            .put_object()
            .bucket(&b)
            .key(&key)
            .body(ByteStream::from(b"t".to_vec()));
        if let Some(h) = tc.header() {
            req = req.tagging(h);
        }
        let r = req.send().await;
        record(&mut m, &mut written, &mut refused, &key, r, |m| {
            m.object(&key, "class", &format!("{tc:?}"));
            m.object(&key, "declared-tags", &tc.count().to_string());
        });
    }

    for key in &written {
        head_into(&client, &b, key, &mut m).await;
    }

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-objects.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-objects"));

    eprintln!(
        "corpus {tier}/objects: {} written, {} refused, {} facts -> {}",
        written.len(),
        refused.len(),
        m.len(),
        path.display()
    );
    for (k, c) in &refused {
        eprintln!("  refused {k:?}: {c}");
    }

    assert!(!m.is_empty(), "the corpus produced no facts");

    /* What a shape axis can assert without a reference gateway:  an
     * object that was written answers HEAD.  A write reported as
     * succeeding and a HEAD that then fails is the same class of
     * disagreement the listing invariants catch. */
    let mut problems = Vec::new();
    for key in &written {
        let rendered = m.render();
        let needle = format!("object\t{}\thead-failed\t", crate::manifest::encode(key));
        if rendered.contains(&needle) {
            problems.push(format!("{key:?} was written and HEAD fails"));
        }
    }
    problems.extend(mode_problems);

    assert!(
        problems.is_empty(),
        "the corpus disagrees with what the gateway did:\n  {}",
        problems.join("\n  ")
    );
}

/// Record a put's outcome:  facts on success, the error as a fact on
/// refusal.  A refusal is something the corpus is here to capture.
fn record<T, E>(
    m: &mut Manifest,
    written: &mut Vec<String>,
    refused: &mut Vec<(String, String)>,
    key: &str,
    r: Result<T, aws_smithy_runtime_api::client::result::SdkError<
        E, aws_smithy_runtime_api::http::Response>>,
    on_ok: impl FnOnce(&mut Manifest),
) where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::fmt::Debug,
{
    match r {
        Ok(_) => {
            written.push(key.to_string());
            on_ok(m);
        }
        Err(e) => {
            let info = extract_s3_error(&e);
            m.object(key, "put-refused", &info.error_code);
            m.object(key, "put-status", &info.status.to_string());
            refused.push((
                key.to_string(),
                format!("{} {}", info.status, info.error_code),
            ));
        }
    }
}
