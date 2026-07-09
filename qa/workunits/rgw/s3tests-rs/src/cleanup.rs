use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};

use crate::client::{get_alt_client, get_client, get_tenant_client};
use crate::config::get_config;

const NUKE_BATCH_SIZE: i32 = 1000;

pub async fn nuke_bucket(client: &S3Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Optimistic: try deleting the bucket directly. If empty, this avoids
    // the expensive list_object_versions flow entirely.
    match client.delete_bucket().bucket(bucket).send().await {
        Ok(_) => return Ok(()),
        Err(e) => {
            let code = e
                .as_service_error()
                .and_then(|se| se.code())
                .unwrap_or("");
            if code == "NoSuchBucket" {
                return Ok(());
            }
            if code != "BucketNotEmpty" {
                return Err(e.into());
            }
        }
    }

    let mut max_retain_date: Option<DateTime<Utc>> = None;

    let mut key_marker: Option<String> = None;
    let mut version_id_marker: Option<String> = None;

    loop {
        let mut req = client
            .list_object_versions()
            .bucket(bucket)
            .max_keys(NUKE_BATCH_SIZE);
        if let Some(ref km) = key_marker {
            req = req.key_marker(km);
        }
        if let Some(ref vm) = version_id_marker {
            req = req.version_id_marker(vm);
        }

        let listing = req.send().await?;
        let is_truncated = listing.is_truncated().unwrap_or(false);
        key_marker = listing.next_key_marker().map(|s| s.to_string());
        version_id_marker = listing.next_version_id_marker().map(|s| s.to_string());

        let mut objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = Vec::new();

        for v in listing.versions() {
            objects.push(
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(v.key().unwrap_or_default())
                    .version_id(v.version_id().unwrap_or_default())
                    .build()?,
            );
        }
        for dm in listing.delete_markers() {
            objects.push(
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(dm.key().unwrap_or_default())
                    .version_id(dm.version_id().unwrap_or_default())
                    .build()?,
            );
        }

        if objects.is_empty() {
            break;
        }

        let delete = aws_sdk_s3::types::Delete::builder()
            .set_objects(Some(objects))
            .quiet(true)
            .build()?;

        let result = client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .bypass_governance_retention(true)
            .send()
            .await?;

        for err in result.errors() {
            if err.code() != Some("AccessDenied") {
                continue;
            }
            if let (Some(key), Some(vid)) = (err.key(), err.version_id()) {
                if let Ok(res) = client
                    .get_object_retention()
                    .bucket(bucket)
                    .key(key)
                    .version_id(vid)
                    .send()
                    .await
                {
                    if let Some(retention) = res.retention() {
                        if let Some(retain_until) = retention.retain_until_date() {
                            let secs = retain_until.secs();
                            let nanos = retain_until.subsec_nanos();
                            if let Some(dt) =
                                DateTime::from_timestamp(secs, nanos as u32)
                            {
                                if max_retain_date.is_none()
                                    || max_retain_date.unwrap() < dt
                                {
                                    max_retain_date = Some(dt);
                                }
                            }
                        }
                    }
                }
            }
        }

        if !is_truncated {
            break;
        }
    }

    if let Some(retain_date) = max_retain_date {
        let now = Utc::now();
        if retain_date > now {
            let delta = retain_date - now;
            if delta.num_seconds() > 60 {
                return Err(format!(
                    "bucket {bucket} still has objects locked for {} more seconds",
                    delta.num_seconds()
                )
                .into());
            }
            eprintln!(
                "nuke_bucket {bucket} waiting {} seconds for object locks to expire",
                delta.num_seconds()
            );
            tokio::time::sleep(std::time::Duration::from_secs(
                delta.num_seconds() as u64,
            ))
            .await;
        }

        // retry deletes after retention expires
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;
        loop {
            let mut req = client
                .list_object_versions()
                .bucket(bucket)
                .max_keys(NUKE_BATCH_SIZE);
            if let Some(ref km) = key_marker {
                req = req.key_marker(km);
            }
            if let Some(ref vm) = version_id_marker {
                req = req.version_id_marker(vm);
            }

            let listing = req.send().await?;
            let is_truncated = listing.is_truncated().unwrap_or(false);
            key_marker = listing.next_key_marker().map(|s| s.to_string());
            version_id_marker = listing.next_version_id_marker().map(|s| s.to_string());

            let mut objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = Vec::new();
            for v in listing.versions() {
                objects.push(
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(v.key().unwrap_or_default())
                        .version_id(v.version_id().unwrap_or_default())
                        .build()?,
                );
            }
            for dm in listing.delete_markers() {
                objects.push(
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(dm.key().unwrap_or_default())
                        .version_id(dm.version_id().unwrap_or_default())
                        .build()?,
                );
            }

            if objects.is_empty() {
                break;
            }

            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()?;

            client
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .bypass_governance_retention(true)
                .send()
                .await?;

            if !is_truncated {
                break;
            }
        }
    }

    client.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}

pub async fn nuke_prefixed_buckets(client: &S3Client, prefix: &str) {
    let response = match client.list_buckets().send().await {
        Ok(r) => r,
        Err(_) => return,
    };

    let matching: Vec<String> = response
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .filter(|name| name.contains(prefix))
        .map(|s| s.to_string())
        .collect();

    let mut set = tokio::task::JoinSet::new();

    for name in matching {
        let c = client.clone();
        set.spawn(async move {
            if let Err(e) = nuke_bucket(&c, &name).await {
                eprintln!("Warning: failed to nuke {name}: {e}");
            }
        });
    }

    while set.join_next().await.is_some() {}
}

pub async fn setup() {
    let cfg = get_config();
    let client = get_client();
    let alt_client = get_alt_client();
    let tenant_client = get_tenant_client();
    let prefix = cfg.bucket_prefix.clone();
    tokio::join!(
        nuke_prefixed_buckets(&client, &prefix),
        nuke_prefixed_buckets(&alt_client, &prefix),
        nuke_prefixed_buckets(&tenant_client, &prefix),
    );
}

pub async fn teardown() {
    let cfg = get_config();
    let client = get_client();
    let alt_client = get_alt_client();
    let tenant_client = get_tenant_client();
    let prefix = cfg.bucket_prefix.clone();
    tokio::join!(
        nuke_prefixed_buckets(&client, &prefix),
        nuke_prefixed_buckets(&alt_client, &prefix),
        nuke_prefixed_buckets(&tenant_client, &prefix),
    );
}
