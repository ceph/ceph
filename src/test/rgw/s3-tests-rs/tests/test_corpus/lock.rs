//! Tier 1 -- the object lock axis.
//!
//! The axis where the two formats differ in shape rather than in
//! spelling.  NooBaa writes three plain attributes -- legal hold as the
//! literal `ON`/`OFF`, retention as a mode and a date -- against our
//! ceph-encoded `RGWObjectLegalHold` and our single retention
//! attribute.  So a conversion here is a value rewrite, not a rename,
//! and the disk inventory is the half that can see whether the right
//! names ended up on the file.
//!
//! `NOOBAA_ONDISK_STABILITY.md` names object lock as the part of their
//! layout still moving, which makes this the axis most likely to need
//! revisiting against a newer baseline.
//!
//! Object lock needs its own bucket:  it can only be enabled at
//! creation and cannot be turned off, so it cannot share with the other
//! axes.  Enabling it enables versioning too.

use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled,
    ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetentionMode, ObjectLockRule,
};
use s3_tests_rs::client::get_client;

use crate::generate::{env_or, out_dir};
use crate::manifest::Manifest;
use crate::names::bucket;
use crate::shapes::{LockClass, LOCK_COVERAGE, RETAIN_UNTIL_SECS};

/// Create the bucket with lock enabled, tolerating one that is already
/// there from an earlier run.
async fn ensure_lock_bucket(client: &aws_sdk_s3::Client, name: &str) {
    match client
        .create_bucket()
        .bucket(name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
    {
        Ok(_) => {}
        Err(e) => {
            let code = e
                .into_service_error()
                .meta()
                .code()
                .unwrap_or("")
                .to_string();
            assert!(
                code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists",
                "could not create the lock bucket {name:?}: {code}"
            );
        }
    }
}

/* The PUT header and the retention record use different enums for the
 * same two values, so both conversions are spelled out. */
fn put_mode_of(s: &str) -> ObjectLockMode {
    match s {
        "COMPLIANCE" => ObjectLockMode::Compliance,
        _ => ObjectLockMode::Governance,
    }
}

fn default_mode_of(s: &str) -> ObjectLockRetentionMode {
    match s {
        "COMPLIANCE" => ObjectLockRetentionMode::Compliance,
        _ => ObjectLockRetentionMode::Governance,
    }
}

/* NooBaa returns `501 NotImplemented` to PutObjectLockConfiguration at
 * the pin, so the axis cannot create the bucket it needs and reaches
 * none of its assertions.  Per-object retention is a different
 * surface -- NooBaa does keep that in xattrs -- and only the
 * bucket-level configuration returns 501. */
#[cfg_attr(feature = "fails_on_noobaa",
           ignore = "noobaa: PutObjectLockConfiguration is NotImplemented")]
#[tokio::test]
async fn corpus_coverage_object_lock() {
    let tier = env_or(crate::env::TIER, "coverage");
    if tier != "coverage" {
        return;
    }
    let client = get_client();
    let b = bucket(&tier, "lock");
    ensure_lock_bucket(&client, &b).await;

    let mut m = Manifest::new();
    m.bucket(&b, "tier", &tier);
    m.bucket(&b, "object-lock", "Enabled");

    /* The bucket default is configured *between* the two groups, not
     * before both.
     *
     * A default rule applies to every object written after it that
     * declares no retention of its own -- which, written first, would
     * have given the "no lock" artifact a GOVERNANCE retention and
     * made its name a lie.  The disk inventory is what caught that:
     * every object carried `user.nsfs.rgw.object-retention`, including
     * the ones that asked for nothing.  So the unlocked and
     * explicitly-locked objects go in first, then the rule, then the
     * one object that is there to inherit it. */
    let mut written = Vec::new();
    for class in LOCK_COVERAGE {
        if *class == LockClass::BucketDefault {
            continue;
        }
        let key = format!("l-{}", class.label());
        let mut req = client
            .put_object()
            .bucket(&b)
            .key(&key)
            .body(ByteStream::from(b"locked".to_vec()));

        /* Declared at PUT where the class declares it.  BucketDefault
         * deliberately sends nothing, so what it ends up with is the
         * bucket's rule rather than anything this request said. */
        if let Some(mode) = class.retention() {
            req = req
                .object_lock_mode(put_mode_of(mode))
                .object_lock_retain_until_date(DateTime::from_secs(RETAIN_UNTIL_SECS));
        }
        if let Some(h) = class.legal_hold() {
            req = req.object_lock_legal_hold_status(if h == "ON" {
                ObjectLockLegalHoldStatus::On
            } else {
                ObjectLockLegalHoldStatus::Off
            });
        }

        let r = req.send().await.expect("locked put");
        let vid = r.version_id().map(String::from).unwrap_or_default();
        m.object(&key, "declared-lock", class.label());
        if let Some(mode) = class.retention() {
            m.object(&key, "declared-retention-mode", mode);
            m.object(&key, "declared-retain-until", &RETAIN_UNTIL_SECS.to_string());
        }
        if let Some(h) = class.legal_hold() {
            m.object(&key, "declared-legal-hold", h);
        }
        written.push((*class, key, vid));
    }

    let conf = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(default_mode_of("GOVERNANCE"))
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build();
    /* Observing is not available here, and saying so beats half-doing
     * it.  The axis's subject is a bucket-level configuration, and a
     * bucket NooBaa fails to create -- it returns 501 to
     * PutObjectLockConfiguration -- is not in the tree to be
     * described.  The expected-difference list carries that as
     * `run coverage-lock absent`. */
    if crate::generate::observing() {
        eprintln!("corpus {tier}/lock: observing, and there is nothing to \
                   observe -- the axis's bucket is created by the axis");
        return;
    }

    client
        .put_object_lock_configuration()
        .bucket(&b)
        .object_lock_configuration(conf)
        .send()
        .await
        .expect("put_object_lock_configuration");
    m.bucket(&b, "default-retention.mode", "GOVERNANCE");
    m.bucket(&b, "default-retention.days", "1");

    {
        let key = format!("l-{}", LockClass::BucketDefault.label());
        let r = client
            .put_object()
            .bucket(&b)
            .key(&key)
            .body(ByteStream::from(b"locked".to_vec()))
            .send()
            .await
            .expect("inheriting put");
        m.object(&key, "declared-lock", LockClass::BucketDefault.label());
        written.push((
            LockClass::BucketDefault,
            key,
            r.version_id().map(String::from).unwrap_or_default(),
        ));
    }

    /* What S3 reports back.  Both calls answer per object, and an
     * object with nothing set answers with an error rather than an
     * empty value -- recorded as a fact, since which error is a
     * question this corpus must not presume. */
    let mut problems: Vec<String> = Vec::new();
    for (class, key, _vid) in &written {
        match client
            .get_object_retention()
            .bucket(&b)
            .key(key)
            .send()
            .await
        {
            Ok(r) => {
                if let Some(ret) = r.retention() {
                    if let Some(mode) = ret.mode() {
                        m.object(key, "retention.mode", mode.as_str());
                    }
                    if let Some(d) = ret.retain_until_date() {
                        m.object(key, "retention.retain-until", &d.secs().to_string());
                    }
                }
            }
            Err(e) => {
                let code =
                    e.into_service_error().meta().code().unwrap_or("Unknown").to_string();
                m.object(key, "retention.absent", &code);
            }
        }

        match client
            .get_object_legal_hold()
            .bucket(&b)
            .key(key)
            .send()
            .await
        {
            Ok(r) => {
                if let Some(s) = r.legal_hold().and_then(|h| h.status()) {
                    m.object(key, "legal-hold.status", s.as_str());
                }
            }
            Err(e) => {
                let code =
                    e.into_service_error().meta().code().unwrap_or("Unknown").to_string();
                m.object(key, "legal-hold.absent", &code);
            }
        }

        /* HEAD carries the lock headers too, and a client reads them
         * there rather than making two more round trips. */
        if let Ok(h) = client.head_object().bucket(&b).key(key).send().await {
            if let Some(mode) = h.object_lock_mode() {
                m.object(key, "head.lock-mode", mode.as_str());
            }
            if let Some(s) = h.object_lock_legal_hold_status() {
                m.object(key, "head.legal-hold", s.as_str());
            }
            if let Some(d) = h.object_lock_retain_until_date() {
                m.object(key, "head.retain-until", &d.secs().to_string());
            }
        }

        /* Enforcement, which is the only part of this that a wrong
         * answer makes dangerous rather than untidy:  a version held by
         * COMPLIANCE retention or by a legal hold must not delete.
         * Asserted by attempting it. */
        let protected = matches!(
            class,
            LockClass::Compliance | LockClass::LegalHoldOn | LockClass::GovernanceThenHold
        );
        if protected {
            if _vid.is_empty() {
                problems.push(format!("{key:?} has no version id in a lock bucket"));
            } else {
                match client
                    .delete_object()
                    .bucket(&b)
                    .key(key)
                    .version_id(_vid.as_str())
                    .send()
                    .await
                {
                    Ok(_) => problems.push(format!(
                        "{key:?} is protected and its version deleted anyway"
                    )),
                    Err(e) => {
                        let code = e
                            .into_service_error()
                            .meta()
                            .code()
                            .unwrap_or("Unknown")
                            .to_string();
                        m.object(key, "protected-delete.error", &code);
                    }
                }
            }
        }
    }

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-lock.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-lock"));

    eprintln!(
        "corpus {tier}/lock: {} objects, {} facts -> {}",
        written.len(),
        m.len(),
        path.display()
    );

    /* Reference-free properties:  what was declared is what is
     * reported, each of the two independently. */
    problems.extend(mode_problems);
    let rendered = m.render();
    let fact = |key: &str, field: &str, value: &str| -> bool {
        rendered.contains(&format!(
            "object\t{}\t{}\t{}\n",
            crate::manifest::encode(key),
            field,
            crate::manifest::encode(value)
        ))
    };
    for (class, key, _vid) in &written {
        if let Some(mode) = class.retention() {
            if !fact(key, "retention.mode", mode) {
                problems.push(format!(
                    "{key:?} was written with {mode} retention and does not report it"
                ));
            }
            if !fact(key, "retention.retain-until", &RETAIN_UNTIL_SECS.to_string()) {
                problems.push(format!(
                    "{key:?} does not report the retain-until date it was given"
                ));
            }
        }
        if let Some(h) = class.legal_hold() {
            if !fact(key, "legal-hold.status", h) {
                problems.push(format!(
                    "{key:?} was written with legal hold {h} and does not report it"
                ));
            }
        }
        /* The two are independent:  an object carrying both must report
         * both, which is what GovernanceThenHold is for. */
        if *class == LockClass::GovernanceThenHold
            && !(fact(key, "retention.mode", "GOVERNANCE")
                && fact(key, "legal-hold.status", "ON"))
        {
            problems.push(format!(
                "{key:?} carries retention and a legal hold and does not report both"
            ));
        }
        /* Only the mode is asserted for the inherited one.  Its
         * retain-until is computed from the object's own mtime plus
         * the rule's days, so it differs on every run -- recorded as a
         * fact, not compared, for the reason version ids are not.
         *
         * Skipped on posix, which does not apply a bucket's default
         * rule either:  the same gap nsfs had, and the same four
         * drivers -- rados, dbstore, daos, motr -- that do implement
         * it.  Gated here rather than by ignoring the whole axis,
         * because posix satisfies the other six assertions and they
         * are worth more as a regression guard than a tidy ignore. */
        #[cfg(not(feature = "fails_on_posix"))]
        {
            if *class == LockClass::BucketDefault
                && !fact(key, "retention.mode", "GOVERNANCE")
            {
                problems.push(format!(
                    "{key:?} declared no retention and did not inherit the \
                     bucket's rule"
                ));
            }
        }
    }

    assert!(
        problems.is_empty(),
        "the object lock corpus disagrees with what the gateway did:\n  {}",
        problems.join("\n  ")
    );
}
