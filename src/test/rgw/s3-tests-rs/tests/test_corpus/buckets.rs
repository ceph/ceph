//! Tier 1 -- the bucket configuration axis.
//!
//! Everything the other axes do not cover, because it lives on the
//! bucket rather than on an object.  NooBaa's bucket record has 22
//! fields and we read 10 of them
//! (`docs/NOOBAA_WRITE_SURFACE.md` §2), so what a conversion has to
//! carry across is decided here rather than per object.
//!
//! **The design point is attribution.**  Each configuration is applied
//! on its own and the bucket's persisted state is read before and
//! after, so the manifest records *which stored thing each
//! configuration produced* rather than only that the bucket ended up
//! with a pile of them.  `Inventory::walk()` cannot answer this -- it
//! records what is under a root and not the root itself -- which is why
//! `root_xattr_names()` exists.
//!
//! That attribution is also what makes the strong invariant possible:
//! a configuration the gateway *accepted* must have changed something
//! that persists.  Accepted-and-dropped is the shape of the known
//! `logging` and `notifications` gaps, and it is invisible to a
//! round-trip check if the value is also served from memory.
//!
//! **A bucket's configuration does not live in one place.**  The nsfs
//! driver puts it in the bucket directory's extended attributes;
//! NooBaa puts it in a record under their config store, and nothing of
//! it reaches the directory.  This axis read only the first and
//! concluded "nothing on disk holds it" for all three of tagging, CORS
//! and policy on the first capture from their gateway -- a claim about
//! storage in general, drawn from looking at one location, and wrong
//! wherever it ran.  Both locations are read now, and `CORPUS_CONFIG_
//! ROOT` names the second when there is one.  See the punch list.

use s3_tests_rs::client::get_client;

use crate::generate::{ensure_bucket, env_or, out_dir};
use crate::manifest::Manifest;
use crate::names::bucket;

/// What happened to one configuration.
struct Step {
    name: &'static str,
    accepted: bool,
    code: String,
    gained: Vec<String>,
    read_back: bool,
}

fn data_root_bucket(b: &str) -> Option<std::path::PathBuf> {
    std::env::var("CORPUS_DATA_ROOT")
        .ok()
        .map(|r| std::path::Path::new(&r).join(b))
}

/// The bucket's record in a config store, when the backend keeps one.
///
/// `<config_root>/buckets/<name>.json` is the layout.  A path, not a
/// schema:  what is inside it is read generically.
fn config_store_record(b: &str) -> Option<std::path::PathBuf> {
    std::env::var("CORPUS_CONFIG_ROOT").ok().map(|r| {
        std::path::Path::new(&r)
            .join("buckets")
            .join(format!("{b}.json"))
    })
}

/// Everything persisted about this bucket that the corpus can see,
/// each entry a name and a digest of its value.
///
/// Two locations, each prefixed so the manifest attributes a
/// configuration to where it landed rather than only to a name:
///
/// ```text
/// xattr:user.nsfs.rgw.tagset      the bucket directory's attributes
/// record:cors_configuration_rules a field of the bucket's record
/// ```
///
/// Digests rather than names, for both:  a configuration folded into
/// something the bucket already carries adds no name, and a names-only
/// comparison reads that as "stored nowhere".  NooBaa's `tag` is
/// exactly that case -- their `bucket add` writes `tag: []` and
/// PutBucketTagging modifies it in place.
///
/// Empty when neither location is being observed, which is how a run
/// remote from the gateway asserts nothing it cannot measure.
fn bucket_state(b: &str) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    if let Some(p) = data_root_bucket(b) {
        for (n, h) in crate::inventory::root_xattr_digest(&p) {
            out.push((format!("xattr:{n}"), h));
        }
    }
    if let Some(p) = config_store_record(b) {
        for (n, h) in crate::inventory::record_field_digest(&p) {
            out.push((format!("record:{n}"), h));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn corpus_coverage_bucket_config() {
    let tier = env_or("CORPUS_TIER", "coverage");
    if tier != "coverage" {
        return;
    }
    let client = get_client();
    let b = bucket(&tier, "bconf");
    ensure_bucket(&client, &b).await;

    let mut m = Manifest::new();
    m.bucket(&b, "tier", &tier);

    /* Observed at all:  either location is enough, and neither means
     * the persistence invariant is not asserted. */
    let have_disk = data_root_bucket(&b).is_some() || config_store_record(&b).is_some();
    let mut steps: Vec<Step> = Vec::new();

    /* One macro-free helper per configuration, because each has its own
     * request and response type and there is nothing to share but the
     * shape:  read the attributes, apply, read them again, read the
     * configuration back. */
    macro_rules! step {
        ($name:literal, $clear:expr, $apply:expr, $read:expr) => {{
            /* Cleared first, so the before/after comparison is always a
             * real transition.
             *
             * Without this the axis is not idempotent:  a second run
             * against the same tree re-applies identical values, the
             * bytes on disk do not change, and every step reports
             * "added no attribute" -- which reads as the
             * accepted-and-dropped defect the invariant exists to
             * catch.  Found by the parallel baseline run, which reused
             * the tree a targeted run had already configured.
             *
             * Clearing rather than varying the values, because
             * run-varying values would stop the manifests diffing. */
            let _: Result<(), String> = $clear;
            let before = bucket_state(&b);
            let outcome: Result<(), String> = $apply;
            let (accepted, code) = match outcome {
                Ok(()) => (true, String::new()),
                Err(c) => (false, c),
            };
            let after = bucket_state(&b);
            /* added, or changed in place */
            let gained: Vec<String> = after
                .iter()
                .filter(|(n, h)| !before.iter().any(|(bn, bh)| bn == n && bh == h))
                .map(|(n, _)| n.clone())
                .collect();
            m.bucket(&b, concat!($name, ".accepted"), &accepted.to_string());
            /* Emitted whether or not it was refused.  A field that
             * appears only on failure makes a capability divergence
             * read as a removed line plus an added line rather than a
             * changed value, so the manifests of two gateways stop
             * diffing where they differ most. */
            m.bucket(&b, concat!($name, ".refused"),
                     if accepted { "<none>" } else { code.as_str() });
            for g in &gained {
                m.bucket(&b, concat!($name, ".gained"), g);
            }
            let read_back: bool = if accepted { $read } else { false };
            m.bucket(&b, concat!($name, ".reads-back"), &read_back.to_string());
            steps.push(Step {
                name: $name,
                accepted,
                code,
                gained,
                read_back,
            });
        }};
    }

    /* tagging */
    step!(
        "tagging",
        client.delete_bucket_tagging().bucket(&b).send().await
            .map(|_| ()).map_err(|_| String::new()),
        {
            let tagging = aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("bk")
                        .value("bv")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap();
            client
                .put_bucket_tagging()
                .bucket(&b)
                .tagging(tagging)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| {
                    e.into_service_error().meta().code().unwrap_or("Unknown").to_string()
                })
        },
        client
            .get_bucket_tagging()
            .bucket(&b)
            .send()
            .await
            .map(|r| r.tag_set().iter().any(|t| t.key() == "bk"))
            .unwrap_or(false)
    );

    /* cors */
    step!(
        "cors",
        client.delete_bucket_cors().bucket(&b).send().await
            .map(|_| ()).map_err(|_| String::new()),
        {
            let conf = aws_sdk_s3::types::CorsConfiguration::builder()
                .cors_rules(
                    aws_sdk_s3::types::CorsRule::builder()
                        .allowed_methods("GET")
                        .allowed_origins("*")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap();
            client
                .put_bucket_cors()
                .bucket(&b)
                .cors_configuration(conf)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| {
                    e.into_service_error().meta().code().unwrap_or("Unknown").to_string()
                })
        },
        client
            .get_bucket_cors()
            .bucket(&b)
            .send()
            .await
            .map(|r| !r.cors_rules().is_empty())
            .unwrap_or(false)
    );

    /* policy */
    step!(
        "policy",
        client.delete_bucket_policy().bucket(&b).send().await
            .map(|_| ()).map_err(|_| String::new()),
        {
            let doc = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::{b}/*"}}]}}"#
            );
            client
                .put_bucket_policy()
                .bucket(&b)
                .policy(doc)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| {
                    e.into_service_error().meta().code().unwrap_or("Unknown").to_string()
                })
        },
        client
            .get_bucket_policy()
            .bucket(&b)
            .send()
            .await
            .map(|r| r.policy().map(|p| p.contains("s3:GetObject")).unwrap_or(false))
            .unwrap_or(false)
    );

    /* lifecycle */
    step!(
        "lifecycle",
        client.delete_bucket_lifecycle().bucket(&b).send().await
            .map(|_| ()).map_err(|_| String::new()),
        {
            let rule = aws_sdk_s3::types::LifecycleRule::builder()
                .id("corpus")
                .filter(
                    aws_sdk_s3::types::LifecycleRuleFilter::builder()
                        .prefix("bconf/")
                        .build(),
                )
                .expiration(
                    aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build(),
                )
                .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
                .build()
                .unwrap();
            let conf = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(rule)
                .build()
                .unwrap();
            client
                .put_bucket_lifecycle_configuration()
                .bucket(&b)
                .lifecycle_configuration(conf)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| {
                    e.into_service_error().meta().code().unwrap_or("Unknown").to_string()
                })
        },
        client
            .get_bucket_lifecycle_configuration()
            .bucket(&b)
            .send()
            .await
            .map(|r| r.rules().iter().any(|x| x.id() == Some("corpus")))
            .unwrap_or(false)
    );

    /* default encryption */
    step!(
        "encryption",
        client.delete_bucket_encryption().bucket(&b).send().await
            .map(|_| ()).map_err(|_| String::new()),
        {
            let rule = aws_sdk_s3::types::ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    aws_sdk_s3::types::ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(aws_sdk_s3::types::ServerSideEncryption::Aes256)
                        .build()
                        .unwrap(),
                )
                .build();
            let conf = aws_sdk_s3::types::ServerSideEncryptionConfiguration::builder()
                .rules(rule)
                .build()
                .unwrap();
            client
                .put_bucket_encryption()
                .bucket(&b)
                .server_side_encryption_configuration(conf)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| {
                    e.into_service_error().meta().code().unwrap_or("Unknown").to_string()
                })
        },
        client
            .get_bucket_encryption()
            .bucket(&b)
            .send()
            .await
            .map(|r| {
                r.server_side_encryption_configuration()
                    .map(|c| !c.rules().is_empty())
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    );

    /* request payment:  captured, not expected */
    step!(
        "request-payment",
        {
            /* no delete for this one;  set the other value first */
            let back = aws_sdk_s3::types::RequestPaymentConfiguration::builder()
                .payer(aws_sdk_s3::types::Payer::BucketOwner)
                .build()
                .unwrap();
            client.put_bucket_request_payment().bucket(&b)
                .request_payment_configuration(back).send().await
                .map(|_| ()).map_err(|_| String::new())
        },
        {
            let conf = aws_sdk_s3::types::RequestPaymentConfiguration::builder()
                .payer(aws_sdk_s3::types::Payer::Requester)
                .build()
                .unwrap();
            client
                .put_bucket_request_payment()
                .bucket(&b)
                .request_payment_configuration(conf)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| {
                    e.into_service_error().meta().code().unwrap_or("Unknown").to_string()
                })
        },
        {
            /* The value, not merely that a value came back:  this call
             * answers with a default when nothing was stored, so
             * `is_some()` is true either way and says nothing. */
            match client.get_bucket_request_payment().bucket(&b).send().await {
                Ok(r) => {
                    let got = r.payer().map(|p| p.as_str().to_string())
                        .unwrap_or_default();
                    m.bucket(&b, "request-payment.payer-read", &got);
                    got == "Requester"
                }
                Err(_) => false,
            }
        }
    );

    /* logging.
     *
     * The target bucket needs a policy, not merely to exist.  S3
     * requires the log-delivery service to be granted s3:PutObject on
     * it, and RGW implements that check --
     * `verify_target_bucket_policy()` in rgw_bucket_logging.cc, which
     * cites the AWS page it comes from.  A bare target is refused with
     * 403, correctly, and reading that as "logging is unimplemented"
     * is how this axis first mis-reported it:  the permission check
     * had in fact *granted*, and the refusal was the op's own.
     *
     * Same statement the suite's own logging tests use, so the
     * condition keys match what the check wants rather than what I
     * guessed. */
    /* Derived, not random.  As `get_new_bucket_name()` it was a fresh
     * name every run, which put a per-run string in the *subject*
     * column -- so the log-target lines could not align between two
     * captures however the fields were named. */
    let log_target = bucket(&tier, "bconf-log-target");
    ensure_bucket(&client, &log_target).await;
    {
        let policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Sid":"S3ServerAccessLogsPolicy","Effect":"Allow","Principal":{{"Service":"logging.s3.amazonaws.com"}},"Action":["s3:PutObject"],"Resource":"arn:aws:s3:::{log_target}/log/*","Condition":{{"ArnLike":{{"aws:SourceArn":"arn:aws:s3:::{b}"}},"StringEquals":{{"aws:SourceAccount":"testid"}}}}}}]}}"#
        );
        match client
            .put_bucket_policy()
            .bucket(&log_target)
            .policy(policy)
            .send()
            .await
        {
            /* Both names every time, for the reason in step!():  this
             * site previously changed the *field* with the outcome,
             * `log-target-policy` on success and
             * `log-target-policy.refused` on failure, so neither line
             * had a counterpart on the other side. */
            Ok(_) => {
                m.bucket(&log_target, "log-target-policy", "set");
                m.bucket(&log_target, "log-target-policy.refused", "<none>");
            }
            Err(e) => {
                m.bucket(&log_target, "log-target-policy", "refused");
                m.bucket(
                    &log_target,
                    "log-target-policy.refused",
                    e.into_service_error().meta().code().unwrap_or("Unknown"),
                );
            }
        }
    }
    step!(
        "logging",
        Ok(()),
        {
            let status = aws_sdk_s3::types::BucketLoggingStatus::builder()
                .logging_enabled(
                    aws_sdk_s3::types::LoggingEnabled::builder()
                        .target_bucket(&log_target)
                        .target_prefix("log/")
                        .build()
                        .unwrap(),
                )
                .build();
            client
                .put_bucket_logging()
                .bucket(&b)
                .bucket_logging_status(status)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| {
                    e.into_service_error().meta().code().unwrap_or("Unknown").to_string()
                })
        },
        client
            .get_bucket_logging()
            .bucket(&b)
            .send()
            .await
            .map(|r| r.logging_enabled().is_some())
            .unwrap_or(false)
    );

    let dir = out_dir();
    std::fs::create_dir_all(&dir).expect("manifest directory");
    let path = dir.join(format!("{tier}-bconf.manifest"));
    std::fs::write(&path, m.render()).expect("write manifest");

    let mode_problems =
        crate::inventory::capture(&b, &dir, &format!("{tier}-bconf"));

    eprintln!(
        "corpus {tier}/bconf: {} configurations, {} accepted, {} facts -> {}",
        steps.len(),
        steps.iter().filter(|s| s.accepted).count(),
        m.len(),
        path.display()
    );
    for s in &steps {
        if !s.accepted {
            eprintln!("  refused {}: {}", s.name, s.code);
        } else {
            eprintln!(
                "  {}: reads-back={} gained {:?}",
                s.name, s.read_back, s.gained
            );
        }
    }

    /* Reference-free properties. */
    let mut problems = mode_problems;
    for s in &steps {
        if !s.accepted {
            /* A refusal is a fact.  Which configurations this backend
             * supports is not this corpus's to assert -- the suite's
             * fails_on_nsfs gates are where that is stated. */
            continue;
        }
        if !s.read_back {
            problems.push(format!(
                "{:?} was accepted and does not read back", s.name
            ));
        }
        /* The strong one:  accepted means persisted.  A configuration
         * that lands nowhere survives only as long as whatever memory
         * is serving it, and a round-trip check cannot tell the two
         * apart.
         *
         * "Nowhere" means neither location -- not the bucket
         * directory's attributes and not a record field.  Asserting it
         * of one location was this axis's own defect. */
        if have_disk && s.gained.is_empty() {
            problems.push(format!(
                "{:?} was accepted and changed nothing that persists, \
                 in neither the bucket's attributes nor its record",
                s.name
            ));
        }
    }

    assert!(
        problems.is_empty(),
        "the bucket configuration corpus disagrees with what the gateway did:\n  {}",
        problems.join("\n  ")
    );
}
