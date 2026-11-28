//! Tier 1 — the versioning axis.
//!
//! Versioning is a property of the bucket, so each state gets its own:
//! a corpus that flipped one bucket between states would record
//! artifacts whose history depended on the order they were written in.
//!
//! The facts live in `ListObjectVersions`, not in `ListObjectsV2` — a
//! plain listing shows neither a version id nor a delete marker, so a
//! corpus that only lists cannot tell whether a version survived.
//!
//! **Version ids are inode-derived** (`mtime-<b36>-ino-<b36>`), so they
//! are stable within one tree and differ between two.  That is fine for
//! the comparison this corpus is for -- NooBaa writes the tree, RGW
//! reads it, RGW converts it, all one tree -- and it is why comparing
//! two *separately generated* trees would need them masked.
//! `docs/NSFS_CORPUS.md` §7 records the limit.

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::check_configure_versioning_retry;

use crate::generate::{ensure_bucket, env_or, note_mode, observing, out_dir};
use crate::manifest::Manifest;
use crate::names::bucket;

/// Record an entry's owner, and flag it when there is not one.
///
/// RGW emits an `<Owner>` for a delete marker as well as for a version
/// -- `rgw_rest_s3.cc` calls `dump_owner()` outside the
/// `is_delete_marker()` guard, which suppresses only ETag, Size and
/// StorageClass.  On this backend that field reads from the recorded
/// ACL and nothing else, and the driver substitutes the literal string
/// `unknown` when there is no ACL to read.  So `unknown` is the shape
/// the defect takes:  a DELETE of a key that does not exist still
/// writes a marker, and there was no source ACL to copy onto it.
fn note_owner(
    m: &mut Manifest,
    key: &str,
    vid: &str,
    id: Option<&str>,
    bucket_owner: &str,
    offenders: &mut Vec<String>,
) {
    let id = id.unwrap_or("");
    m.object(key, &format!("version.{vid}.owner"),
             if id.is_empty() { "<none>" } else { id });
    /* Every owner in this corpus is the bucket's;  see
     * generate::bucket_ownership().  `unknown` and an empty owner are
     * the shapes the old defects took, and both fail this. */
    if id != bucket_owner {
        offenders.push(format!(
            "{key:?} version {vid:?} has owner {:?}, not the bucket's {:?}",
            if id.is_empty() { "<none>" } else { id }, bucket_owner
        ));
    }
}

/// Record every version and delete marker the bucket reports.
async fn list_versions_into(
    client: &aws_sdk_s3::Client,
    b: &str,
    m: &mut Manifest,
    owners: &mut Vec<String>,
) -> Vec<(String, String)> {
    let mut seen = Vec::new();
    let page = client
        .list_object_versions()
        .bucket(b)
        .send()
        .await
        .expect("a corpus bucket should list its versions");
    let (bucket_owner, ownership) =
        crate::generate::bucket_ownership(client, m, b).await;
    owners.extend(ownership);

    for v in page.versions() {
        let key = v.key().unwrap_or_default();
        let vid = v.version_id().unwrap_or_default();
        m.object(key, &format!("version.{vid}.is-latest"),
                 &v.is_latest().unwrap_or(false).to_string());
        if let Some(sz) = v.size() {
            m.object(key, &format!("version.{vid}.size"), &sz.to_string());
        }
        note_owner(m, key, vid, v.owner().and_then(|o| o.id()),
                   &bucket_owner, owners);
        seen.push((key.to_string(), vid.to_string()));
    }
    for d in page.delete_markers() {
        let key = d.key().unwrap_or_default();
        let vid = d.version_id().unwrap_or_default();
        m.object(key, &format!("version.{vid}.delete-marker"), "true");
        m.object(key, &format!("version.{vid}.is-latest"),
                 &d.is_latest().unwrap_or(false).to_string());
        note_owner(m, key, vid, d.owner().and_then(|o| o.id()),
                   &bucket_owner, owners);
        seen.push((key.to_string(), vid.to_string()));
    }
    m.bucket(b, "versions.listed", &seen.len().to_string());
    seen
}

/// Write `n` versions of one key, returning the version ids the puts
/// reported.  Those are what the listing is checked against.
async fn put_versions(
    client: &aws_sdk_s3::Client,
    b: &str,
    key: &str,
    n: usize,
) -> Vec<String> {
    let mut ids = Vec::new();
    for i in 0..n {
        let r = client
            .put_object()
            .bucket(b)
            .key(key)
            .body(ByteStream::from(format!("v{i}").into_bytes()))
            .send()
            .await
            .expect("a versioned put should succeed");
        if let Some(v) = r.version_id() {
            ids.push(v.to_string());
        }
    }
    ids
}

async fn run(tier: &str, label: &str, status: &str) {
    let client = get_client();
    let b = bucket(tier, label);
    ensure_bucket(&client, &b).await;
    /* Setting the versioning state is a *write* to the bucket, so an
     * observing pass does not do it -- the state it describes was put
     * there by the first gateway.  Left unguarded it put a version
     * into the tree it was meant only to read, which the comparison
     * caught as 3 versions against 2. */
    if !observing() {
        check_configure_versioning_retry(&client, &b, status, status).await;
    }

    let mut m = Manifest::new();
    m.bucket(&b, "tier", tier);
    m.bucket(&b, "versioning", status);
    note_mode(&mut m, &b);

    /* what the puts reported, to check the listing against */
    let mut reported: Vec<(String, String)> = Vec::new();

    /* An observing pass makes no versions and no delete markers.  The
     * ids are outcomes -- discovered from ListObjectVersions below
     * rather than returned by a put it did not make -- but how many
     * versions a key is *supposed* to have is the axis's own fact and
     * is recorded either way. */
    for (key, n) in [("v-one", 1usize), ("v-three", 3)] {
        if !observing() {
            for id in put_versions(&client, &b, key, n).await {
                reported.push((key.to_string(), id));
            }
        }
        m.object(key, "declared-versions", &n.to_string());
    }

    /* a delete marker over nothing, and one over versions */
    for key in ["v-dm-only", "v-dm-over-versions"] {
        if !observing() {
            if key.ends_with("versions") {
                for id in put_versions(&client, &b, key, 2).await {
                    reported.push((key.to_string(), id));
                }
            }
            let r = client
                .delete_object()
                .bucket(&b)
                .key(key)
                .send()
                .await
                .expect("delete should succeed on a versioned bucket");
            if let Some(v) = r.version_id() {
                reported.push((key.to_string(), v.to_string()));
                m.object(key, "delete-marker-id-reported", "yes");
            }
        }
        m.object(key, "delete-requested", "yes");
    }

    let mut ownerless = Vec::new();
    let listed = list_versions_into(&client, &b, &mut m, &mut ownerless).await;

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-{label}.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-{label}"));

    eprintln!(
        "corpus {tier}/{label}: versioning={status}, {} reported, {} listed, {} facts -> {}",
        reported.len(),
        listed.len(),
        m.len(),
        path.display()
    );

    /* Reference-free properties.  A version the gateway *told the
     * client about* must be findable afterwards, and no entry may
     * appear twice -- both hold of any S3 implementation. */
    let mut problems = mode_problems;
    problems.extend(ownerless);
    for (k, v) in &reported {
        if !listed.iter().any(|(lk, lv)| lk == k && lv == v) {
            problems.push(format!(
                "{k:?} version {v:?} was reported by a put and is not listed"
            ));
        }
    }
    let mut counts = std::collections::BTreeMap::new();
    for e in &listed {
        *counts.entry(e.clone()).or_insert(0usize) += 1;
    }
    for (e, n) in &counts {
        if *n > 1 {
            problems.push(format!("{:?} version {:?} listed {n} times", e.0, e.1));
        }
    }
    assert!(
        problems.is_empty(),
        "the version listing disagrees with what the corpus wrote:\n  {}",
        problems.join("\n  ")
    );
}

/* posix: a version the PUT reported is absent from ListObjectVersions.
 * Not the mode half of this, which is fixed -- the versions themselves
 * do not come back. */
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
#[tokio::test]
async fn corpus_coverage_versioning_enabled() {
    let tier = env_or(crate::env::TIER, "coverage");
    if tier != "coverage" {
        return;
    }
    run(&tier, "ver-enabled", "Enabled").await;
}

/// Suspended is versioned *and* suspended, not unversioned:  the bucket
/// still holds the versions it had, and a write under suspension
/// becomes the null version rather than a new one.
/* posix: suspending versioning on a bucket that has versions errors
 * outright, so this one cannot reach its assertions at all. */
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: versioning WIP")]
#[tokio::test]
async fn corpus_coverage_versioning_suspended() {
    let tier = env_or(crate::env::TIER, "coverage");
    if tier != "coverage" {
        return;
    }
    let client = get_client();
    let b = bucket(&tier, "ver-suspended");
    ensure_bucket(&client, &b).await;

    let mut m = Manifest::new();
    m.bucket(&b, "tier", &tier);
    m.bucket(&b, "versioning", "Suspended");
    note_mode(&mut m, &b);

    /* enable, write a real version, then suspend and write again:  the
     * second write is the null version, which is the artifact this
     * bucket exists for.  All of it is writing -- including setting the
     * versioning state, which is a write to the bucket -- so an
     * observing pass does none of it and reads what the first gateway
     * left.  Unguarded it put a version into the tree it was meant only
     * to read, which the comparison caught as three versions against
     * two. */
    if !observing() {
        check_configure_versioning_retry(&client, &b, "Enabled", "Enabled")
            .await;
        let pre = put_versions(&client, &b, "v-across-suspend", 1).await;
        check_configure_versioning_retry(&client, &b, "Suspended",
                                         "Suspended").await;
        let post = put_versions(&client, &b, "v-across-suspend", 1).await;
        m.object("v-across-suspend", "versions-before-suspend",
                 &pre.len().to_string());
        m.object("v-across-suspend", "versions-after-suspend",
                 &post.len().to_string());
    }

    let mut ownerless = Vec::new();
    let listed = list_versions_into(&client, &b, &mut m, &mut ownerless).await;

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-ver-suspended.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-ver-suspended"));

    eprintln!(
        "corpus {tier}/ver-suspended: {} listed, {} facts -> {}",
        listed.len(),
        m.len(),
        path.display()
    );

    assert!(
        !listed.is_empty(),
        "a suspended bucket lost the versions it held while enabled"
    );

    let mut problems = mode_problems;
    problems.extend(ownerless);
    assert!(
        problems.is_empty(),
        "the suspended bucket's corpus breaks an invariant:\n  {}",
        problems.join("\n  ")
    );
}
