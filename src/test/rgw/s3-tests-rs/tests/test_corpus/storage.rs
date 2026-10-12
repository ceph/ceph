//! Tier 1 -- the storage class axis.
//!
//! The axis nothing had exercised, and the reason is worth stating:
//! every object this corpus wrote before now was `STANDARD`, and RGW
//! stores no attribute for the default class.  So there has never been
//! a `user.rgw.storage_class` on disk for conversion to translate, and
//! the whole path was untested by construction rather than by
//! oversight.
//!
//! Three things it is after.
//!
//! **The attribute.**  A non-default class writes
//! `user.nsfs.rgw.storage_class` in our format and `user.storage_class`
//! in theirs -- note *theirs* is not under `user.noobaa.`, the one
//! name of theirs that is not, which `xattr_strategy.cc` special-cases
//! because otherwise it falls through to the metadata rule and comes
//! back to clients as `x-amz-meta-storage_class`.
//!
//! **The listing row.**  Every nsfs cache-entry builder sets
//! `bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD` as a literal.
//! If that is what `ListObjectsV2` reports, an object with a
//! non-default class on disk lists as STANDARD -- the same shape as
//! the defects found in the versioning, multipart and copy axes, where
//! the row carried less than the object.  This axis is written to
//! catch it.
//!
//! **What only their gateway can produce.**  `GLACIER_DA` is a value
//! NooBaa wrote and later renamed;  it cannot be created from our
//! side at all.  It is in the axis so the gap is named rather than
//! invisible.

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::configured_storage_classes;

use crate::generate::{ensure_bucket, env_or, note_mode, observing, out_dir};
use crate::manifest::Manifest;
use crate::names::bucket;
use crate::shapes::{StorageClass, STORAGE_COVERAGE};

/// A non-default class to try.
///
/// `configured_storage_classes()` is the suite's own helper -- it
/// seeds STANDARD and appends whatever `storage_classes` in
/// `[s3 main]` names -- and the lifecycle transition tests already
/// use it.  Read from config rather than hardcoded because which
/// classes exist is a property of the deployment.
///
/// nsfs accepts any class a client asks for and now records it, so
/// when nothing is configured this falls back to a name that is valid
/// S3 but not STANDARD:  the point here is that the *attribute* path
/// is exercised, not that the cluster has a placement target.
fn configured_class() -> String {
    let sc = configured_storage_classes();
    sc.iter()
        .find(|c| !c.eq_ignore_ascii_case("STANDARD"))
        .cloned()
        .unwrap_or_else(|| "GLACIER".to_string())
}

/* posix accepts `x-amz-storage-class` and discards it:  the op layer
 * assigns the header without validating, the only check anywhere is in
 * rados's zone-placement lookup, and posix needs no pool -- so nothing
 * stores it.  A client asking for GLACIER gets a 200, HEAD reports no
 * class, and the listing says STANDARD.  Measured here on both halves.
 *
 * nsfs stores it as user.nsfs.rgw.storage_class and reports it from
 * both HEAD and the listing.  posix is left for rgw-standalone, which
 * owns that line. */
#[cfg_attr(feature = "fails_on_posix",
           ignore = "posix: a requested storage class is accepted and discarded")]
#[tokio::test]
async fn corpus_coverage_storage_class() {
    let tier = env_or(crate::env::TIER, "coverage");
    if tier != "coverage" {
        return;
    }
    let client = get_client();
    let b = bucket(&tier, "storage");
    ensure_bucket(&client, &b).await;

    let mut m = Manifest::new();
    m.bucket(&b, "tier", &tier);
    note_mode(&mut m, &b);

    let configured = configured_class();
    m.bucket(&b, "storage-classes.configured", &configured);

    let mut written: Vec<(StorageClass, String, Option<String>)> = Vec::new();

    for class in STORAGE_COVERAGE {
        let key = format!("sc-{}", class.label());
        m.object(&key, "class", class.label());
        m.object(&key, "rationale", class.rationale());

        let asked: Option<String> = match class {
            StorageClass::Standard => None,
            StorageClass::Configured => Some(configured.clone()),
            StorageClass::TheirUnmigrated => {
                m.object(&key, "not-exercised",
                         "GLACIER_DA can only be written by their gateway");
                continue;
            }
        };

        let mut req = client
            .put_object()
            .bucket(&b)
            .key(&key)
            .body(ByteStream::from(b"storage".to_vec()));
        if let Some(c) = &asked {
            req = req.storage_class(aws_sdk_s3::types::StorageClass::from(c.as_str()));
        }

        if observing() {
            /* The key and the class asked for are both derived, so an
             * observing pass has them without the write.
             * `declared-class` goes with them:  it is what the axis
             * asked for, not what the gateway answered. */
            m.object(&key, "declared-class",
                     asked.as_deref().unwrap_or("STANDARD"));
            written.push((*class, key, asked));
            continue;
        }
        match req.send().await {
            Ok(_) => {
                m.object(&key, "declared-class",
                         asked.as_deref().unwrap_or("STANDARD"));
                written.push((*class, key, asked));
            }
            Err(e) => {
                let code = e
                    .into_service_error()
                    .meta()
                    .code()
                    .unwrap_or("Unknown")
                    .to_string();
                m.object(&key, "refused", &code);
            }
        }
    }

    /* HEAD says what the object carries;  the listing says what the row
     * carries.  They are different questions and this axis exists
     * because they can disagree.
     *
     * An observing pass did not make the writes, so an object it derived
     * may never have been written:  NooBaa refuses a class it does not
     * support, and its manifest records `refused`.  Such an object is
     * recorded absent here and not asserted on;  comparing the two
     * manifests is the diff stage's job. */
    let mut absent = std::collections::BTreeSet::new();
    for (_class, key, _asked) in &written {
        match client.head_object().bucket(&b).key(key).send().await {
            Ok(h) => m.object(
                key,
                "head-class",
                h.storage_class().map(|s| s.as_str()).unwrap_or("<none>"),
            ),
            Err(e) if observing() && e.as_service_error()
                .is_some_and(|s| s.is_not_found()) => {
                m.object(key, "observed", "absent");
                absent.insert(key.clone());
            }
            Err(_) => {}
        }
    }
    let listed = client
        .list_objects_v2()
        .bucket(&b)
        .send()
        .await
        .expect("listing");
    let mut rows = std::collections::BTreeMap::new();
    for o in listed.contents() {
        let k = o.key().unwrap_or_default().to_string();
        let sc = o.storage_class().map(|s| s.as_str().to_string());
        m.object(&k, "listed-class", sc.as_deref().unwrap_or("<none>"));
        rows.insert(k, sc);
    }

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-storage.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-storage"));

    eprintln!(
        "corpus {tier}/storage: configured class {:?}, {} written, {} facts -> {}",
        configured,
        written.len(),
        m.len(),
        path.display()
    );

    /* Reference-free properties. */
    let mut problems = mode_problems;
    let rendered = m.render();
    let has = |key: &str, field: &str| -> bool {
        rendered.contains(&format!(
            "object\t{}\t{}\t",
            crate::manifest::encode(key),
            field
        ))
    };

    for (class, key, asked) in &written {
        if absent.contains(key) {
            continue;
        }
        match asked {
            None => {
                /* The default.  Nothing should claim otherwise, and no
                 * attribute should be on disk for it. */
                if !has(key, "head-class") {
                    continue;
                }
            }
            Some(want) => {
                /* What was asked for is what both HEAD and the listing
                 * must report.  The listing is the interesting half. */
                let hc = format!("object\t{}\thead-class\t{}\n",
                                 crate::manifest::encode(key),
                                 crate::manifest::encode(want));
                if !rendered.contains(&hc) {
                    problems.push(format!(
                        "{key:?} was written {want} and HEAD does not report it"
                    ));
                }
                match rows.get(key.as_str()) {
                    Some(Some(got)) if got == want => {}
                    Some(got) => problems.push(format!(
                        "{key:?} was written {want} and the listing reports {:?} \
                         -- the listing row carries less than the object",
                        got.as_deref().unwrap_or("<none>")
                    )),
                    None => problems.push(format!("{key:?} is not listed")),
                }
            }
        }
        let _ = class;
    }

    assert!(
        problems.is_empty(),
        "the storage class corpus disagrees with what the gateway did:\n  {}",
        problems.join("\n  ")
    );
}
