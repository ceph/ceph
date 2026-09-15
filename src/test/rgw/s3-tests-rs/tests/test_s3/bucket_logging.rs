use aws_sdk_s3::types::{
    BucketLoggingStatus, LoggingEnabled, PartitionDateSource, PartitionedPrefix, SimplePrefix,
    TargetObjectKeyFormat,
};
use once_cell::sync::OnceCell;
use s3_tests_rs::assert_s3_err;
use s3_tests_rs::client::{
    get_alt_client, get_client, get_iam_root_client, get_iam_root_s3client,
    get_tenant_client, get_unauthenticated_client,
};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{get_new_bucket, get_new_bucket_name, TestGuard};
use s3_tests_rs::http::{
    presign_get, raw_request, signed_request, ResponseHeaderCapture, RawResponse,
};

const EXPECTED_OBJECT_ROLL_TIME: u32 = 5;

// ── extension detection ──

static HAS_LOGGING_EXT: OnceCell<bool> = OnceCell::new();

async fn has_bucket_logging_extension() -> bool {
    if let Some(v) = HAS_LOGGING_EXT.get() {
        return *v;
    }
    let client = get_client();
    let bucket = get_new_bucket_name();
    let log_bucket = get_new_bucket_name();
    let result = client
        .put_bucket_logging()
        .bucket(&bucket)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(
                    LoggingEnabled::builder()
                        .target_bucket(&log_bucket)
                        .target_prefix("log/")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .customize()
        .mutate_request(|req| {
            inject_extension_xml(req, Some("Journal"), None, None);
        })
        .send()
        .await;
    let has_ext = match result {
        Ok(_) => true,
        Err(e) => {
            let err_str = format!("{e:?}");
            err_str.contains("NoSuchBucket")
        }
    };
    let _ = HAS_LOGGING_EXT.set(has_ext);
    has_ext
}

// ── XML injection helpers ──

fn inject_extension_xml(
    req: &mut aws_smithy_runtime_api::client::orchestrator::HttpRequest,
    logging_type: Option<&str>,
    object_roll_time: Option<u32>,
    records_batch_size: Option<u32>,
) {
    if let Some(body_bytes) = req.body().bytes() {
        let body_str = std::str::from_utf8(body_bytes).unwrap();
        let mut extra = String::new();
        if let Some(lt) = logging_type {
            extra.push_str(&format!("<LoggingType>{lt}</LoggingType>"));
        }
        if let Some(ort) = object_roll_time {
            extra.push_str(&format!("<ObjectRollTime>{ort}</ObjectRollTime>"));
        }
        if let Some(rbs) = records_batch_size {
            extra.push_str(&format!("<RecordsBatchSize>{rbs}</RecordsBatchSize>"));
        }
        if extra.is_empty() {
            return;
        }
        let new_body_str = body_str.replace("</LoggingEnabled>", &format!("{extra}</LoggingEnabled>"));
        let new_body = aws_smithy_types::body::SdkBody::from(new_body_str);
        if let Some(len) = new_body.content_length() {
            req.headers_mut().insert("content-length", len.to_string());
        }
        *req.body_mut() = new_body;
    }
}

// ── policy helpers ──

fn log_bucket_policy_json(
    log_tenant: &str,
    log_bucket: &str,
    src_tenant: &str,
    src_user: &str,
    src_buckets: &[&str],
    log_prefixes: &[&str],
) -> String {
    let mut statements = Vec::new();
    for (j, src_bucket) in src_buckets.iter().enumerate() {
        let prefix = if log_prefixes.len() == 1 {
            log_prefixes[0]
        } else {
            log_prefixes[j]
        };
        let source_account = if src_tenant.is_empty() {
            src_user.to_string()
        } else {
            format!("{src_tenant}${src_user}")
        };
        statements.push(serde_json::json!({
            "Sid": "S3ServerAccessLogsPolicy",
            "Effect": "Allow",
            "Principal": {"Service": "logging.s3.amazonaws.com"},
            "Action": ["s3:PutObject"],
            "Resource": format!("arn:aws:s3::{log_tenant}:{log_bucket}/{prefix}"),
            "Condition": {
                "ArnLike": {"aws:SourceArn": format!("arn:aws:s3::{src_tenant}:{src_bucket}")},
                "StringEquals": {
                    "aws:SourceAccount": source_account
                }
            }
        }));
    }
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": statements
    })
    .to_string()
}

async fn set_log_bucket_policy(
    client: &aws_sdk_s3::Client,
    log_bucket: &str,
    src_buckets: &[&str],
    log_prefixes: &[&str],
) {
    let cfg = get_config();
    let policy =
        log_bucket_policy_json("", log_bucket, "", &cfg.main_user_id, src_buckets, log_prefixes);
    client
        .put_bucket_policy()
        .bucket(log_bucket)
        .policy(policy)
        .send()
        .await
        .unwrap();
}

// ── put_bucket_logging with optional extensions ──

async fn put_bucket_logging_ext(
    client: &aws_sdk_s3::Client,
    src_bucket: &str,
    target_bucket: &str,
    target_prefix: &str,
    key_format: Option<TargetObjectKeyFormat>,
    logging_type: Option<&str>,
    object_roll_time: Option<u32>,
    records_batch_size: Option<u32>,
) -> String {
    let mut le = LoggingEnabled::builder()
        .target_bucket(target_bucket)
        .target_prefix(target_prefix);
    if let Some(kf) = key_format {
        le = le.target_object_key_format(kf);
    }
    let status = BucketLoggingStatus::builder()
        .logging_enabled(le.build().unwrap())
        .build();

    let capture = ResponseHeaderCapture::new();
    let needs_ext = logging_type.is_some() || object_roll_time.is_some() || records_batch_size.is_some();
    if needs_ext {
        let lt = logging_type.map(|s| s.to_string());
        client
            .put_bucket_logging()
            .bucket(src_bucket)
            .bucket_logging_status(status)
            .customize()
            .mutate_request(move |req| {
                inject_extension_xml(
                    req,
                    lt.as_deref(),
                    object_roll_time,
                    records_batch_size,
                );
            })
            .interceptor(capture.clone())
            .send()
            .await
            .unwrap();
    } else {
        client
            .put_bucket_logging()
            .bucket(src_bucket)
            .bucket_logging_status(status)
            .customize()
            .interceptor(capture.clone())
            .send()
            .await
            .unwrap();
    }
    capture.body()
}

// ── get_bucket_logging raw XML (for extension field assertions) ──

async fn get_bucket_logging_xml(client: &aws_sdk_s3::Client, bucket: &str) -> String {
    let capture = ResponseHeaderCapture::new();
    client
        .get_bucket_logging()
        .bucket(bucket)
        .customize()
        .interceptor(capture.clone())
        .send()
        .await
        .unwrap();
    capture.body()
}

fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

// ── post_bucket_logging (Ceph extension: flush) ──

async fn post_bucket_logging(bucket: &str) -> RawResponse {
    post_bucket_logging_creds(bucket, None).await
}

async fn post_bucket_logging_creds(bucket: &str, creds: Option<(&str, &str)>) -> RawResponse {
    s3_tests_rs::http::signed_request_with_creds(
        reqwest::Method::POST, bucket, "logging", None, creds,
    ).await
}

async fn flush_logs(client: &aws_sdk_s3::Client, src_bucket: &str) -> Option<String> {
    flush_logs_creds(client, src_bucket, "dummy", None).await
}

async fn flush_logs_creds(
    client: &aws_sdk_s3::Client,
    src_bucket: &str,
    dummy_key: &str,
    creds: Option<(&str, &str)>,
) -> Option<String> {
    if has_bucket_logging_extension().await {
        let resp = post_bucket_logging_creds(src_bucket, creds).await;
        if resp.status == 200 {
            return xml_text(&resp.body, "FlushedLoggingObject");
        }
    }
    tokio::time::sleep(std::time::Duration::from_secs_f64(
        EXPECTED_OBJECT_ROLL_TIME as f64 * 1.1,
    ))
    .await;
    client
        .put_object()
        .bucket(src_bucket)
        .key(dummy_key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"dummy"))
        .send()
        .await
        .unwrap();
    None
}

// ── log record parsing ──

struct StandardLogRecord {
    bucket_owner: String,
    bucket_name: String,
    request_date_time: String,
    remote_ip: String,
    requester: String,
    request_id: String,
    operation: String,
    key: String,
    request_uri: String,
    http_status: String,
    error_code: String,
    bytes_sent: String,
    object_size: String,
    total_time: String,
    turn_around_time: String,
    referrer: String,
    user_agent: String,
    version_id: String,
    host_id: String,
    sig_version: String,
    cipher_suite: String,
    auth_type: String,
    host_header: String,
    tls_version: String,
    access_point_arn: String,
    acl_required: String,
}

struct JournalLogRecord {
    bucket_owner: String,
    bucket_name: String,
    request_date_time: String,
    operation: String,
    key: String,
    object_size: String,
    version_id: String,
    etag: String,
}

fn shlex_split(record: &str) -> Vec<String> {
    let normalized = record.replace('[', "\"").replace(']', "\"");
    shell_words_split(&normalized)
}

fn shell_words_split(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in s.chars() {
        match ch {
            '"' => in_quote = !in_quote,
            ' ' if !in_quote => {
                if !current.is_empty() {
                    result.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        result.push(current);
    }
    result
}

fn parse_standard_log_record(record: &str) -> StandardLogRecord {
    let chunks = shlex_split(record);
    assert_eq!(chunks.len(), 26, "standard log record has {}, expected 26 fields", chunks.len());
    StandardLogRecord {
        bucket_owner: chunks[0].clone(),
        bucket_name: chunks[1].clone(),
        request_date_time: chunks[2].clone(),
        remote_ip: chunks[3].clone(),
        requester: chunks[4].clone(),
        request_id: chunks[5].clone(),
        operation: chunks[6].clone(),
        key: chunks[7].clone(),
        request_uri: chunks[8].clone(),
        http_status: chunks[9].clone(),
        error_code: chunks[10].clone(),
        bytes_sent: chunks[11].clone(),
        object_size: chunks[12].clone(),
        total_time: chunks[13].clone(),
        turn_around_time: chunks[14].clone(),
        referrer: chunks[15].clone(),
        user_agent: chunks[16].clone(),
        version_id: chunks[17].clone(),
        host_id: chunks[18].clone(),
        sig_version: chunks[19].clone(),
        cipher_suite: chunks[20].clone(),
        auth_type: chunks[21].clone(),
        host_header: chunks[22].clone(),
        tls_version: chunks[23].clone(),
        access_point_arn: chunks[24].clone(),
        acl_required: chunks[25].clone(),
    }
}

fn parse_journal_log_record(record: &str) -> JournalLogRecord {
    let chunks = shlex_split(record);
    assert_eq!(chunks.len(), 8, "journal log record has {}, expected 8 fields", chunks.len());
    JournalLogRecord {
        bucket_owner: chunks[0].clone(),
        bucket_name: chunks[1].clone(),
        request_date_time: chunks[2].clone(),
        operation: chunks[3].clone(),
        key: chunks[4].clone(),
        object_size: chunks[5].clone(),
        version_id: chunks[6].clone(),
        etag: chunks[7].clone(),
    }
}

fn get_record_field(record: &str, record_type: &str, field: &str) -> String {
    if record_type == "Standard" {
        let r = parse_standard_log_record(record);
        match field {
            "BucketOwner" => r.bucket_owner,
            "BucketName" => r.bucket_name,
            "Operation" => r.operation,
            "Key" => r.key,
            "AuthType" => r.auth_type,
            "ACLRequired" => r.acl_required,
            "HTTPStatus" => r.http_status,
            _ => panic!("unknown standard field: {field}"),
        }
    } else {
        let r = parse_journal_log_record(record);
        match field {
            "BucketOwner" => r.bucket_owner,
            "BucketName" => r.bucket_name,
            "Operation" => r.operation,
            "Key" => r.key,
            _ => panic!("unknown journal field: {field}"),
        }
    }
}

fn verify_records(
    records: &str,
    bucket_name: &str,
    event_type: &str,
    src_keys: &[&str],
    record_type: &str,
    expected_count: usize,
    exact_match: bool,
) -> bool {
    let mut keys_found = Vec::new();
    let mut all_keys = Vec::new();
    for line in records.lines() {
        if line.is_empty() {
            continue;
        }
        let key_field = if record_type == "Standard" {
            parse_standard_log_record(line).key.clone()
        } else {
            parse_journal_log_record(line).key.clone()
        };
        if line.contains(bucket_name) && line.contains(event_type) {
            all_keys.push(key_field.clone());
            for src_key in src_keys {
                if line.contains(src_key) {
                    keys_found.push(src_key.to_string());
                    break;
                }
            }
        }
    }
    if exact_match {
        keys_found.len() == expected_count && keys_found.len() == all_keys.len()
    } else {
        keys_found.len() == expected_count
    }
}

fn verify_record_field(
    records: &str,
    bucket_name: &str,
    event_type: &str,
    object_key: &str,
    record_type: &str,
    field_name: &str,
    expected_value: &str,
) -> bool {
    for line in records.lines() {
        if line.is_empty() {
            continue;
        }
        if line.contains(bucket_name) && line.contains(event_type) && line.contains(object_key) {
            let value = get_record_field(line, record_type, field_name);
            return value == expected_value;
        }
    }
    false
}

// ── verify helpers ──

async fn verify_access_denied(
    client: &aws_sdk_s3::Client,
    src_bucket: &str,
    log_bucket: &str,
    prefix: &str,
) {
    let result = client
        .put_bucket_logging()
        .bucket(src_bucket)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(
                    LoggingEnabled::builder()
                        .target_bucket(log_bucket)
                        .target_prefix(prefix)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

fn randcontent() -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let len = rng.gen_range(10..1024);
    (0..len).map(|_| rng.gen_range(b'a'..=b'z')).collect()
}

async fn get_keys(client: &aws_sdk_s3::Client, bucket: &str) -> Vec<String> {
    let resp = client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    resp.contents()
        .iter()
        .filter_map(|o| o.key().map(|s| s.to_string()))
        .collect()
}

async fn get_body(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> String {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let bytes = resp.body.collect().await.unwrap().into_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn put_bucket_encryption_s3(client: &aws_sdk_s3::Client, bucket: &str) {
    use aws_sdk_s3::types::{
        ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
        ServerSideEncryptionRule,
    };
    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(
            ServerSideEncryptionByDefault::builder()
                .sse_algorithm(ServerSideEncryption::Aes256)
                .build()
                .unwrap(),
        )
        .build();
    let config = ServerSideEncryptionConfiguration::builder()
        .rules(rule)
        .build()
        .unwrap();
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(config)
        .send()
        .await
        .unwrap();
}

// ══════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_put_bucket_logging() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // minimal configuration
    if has_ext {
        put_bucket_logging_ext(
            &client, &src_bucket, &log_bucket, prefix,
            None, None, Some(EXPECTED_OBJECT_ROLL_TIME), None,
        ).await;
    } else {
        put_bucket_logging_ext(
            &client, &src_bucket, &log_bucket, prefix,
            None, None, None, None,
        ).await;
    }

    let resp = client.get_bucket_logging().bucket(&src_bucket).send().await.unwrap();
    let le = resp.logging_enabled().unwrap();
    assert_eq!(le.target_bucket(), log_bucket);
    assert_eq!(le.target_prefix(), prefix);

    if has_ext {
        let xml = get_bucket_logging_xml(&client, &src_bucket).await;
        assert_eq!(xml_text(&xml, "LoggingType").as_deref(), Some("Standard"));
        assert_eq!(xml_text(&xml, "RecordsBatchSize").as_deref(), Some("0"));
    }

    // with SimplePrefix
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        None,
        if has_ext { Some(EXPECTED_OBJECT_ROLL_TIME) } else { None },
        None,
    ).await;

    let resp = client.get_bucket_logging().bucket(&src_bucket).send().await.unwrap();
    let le = resp.logging_enabled().unwrap();
    assert!(le.target_object_key_format().unwrap().simple_prefix().is_some());

    // with PartitionedPrefix
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .partitioned_prefix(
                PartitionedPrefix::builder()
                    .partition_date_source(PartitionDateSource::DeliveryTime)
                    .build(),
            )
            .build()),
        None,
        if has_ext { Some(EXPECTED_OBJECT_ROLL_TIME) } else { None },
        None,
    ).await;

    let resp = client.get_bucket_logging().bucket(&src_bucket).send().await.unwrap();
    let le = resp.logging_enabled().unwrap();
    assert!(le.target_object_key_format().unwrap().partitioned_prefix().is_some());

    // with TargetGrants (not implemented in RGW — grants are silently dropped)
    let cfg = get_config();
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        None, None,
        if has_ext { Some(EXPECTED_OBJECT_ROLL_TIME) } else { None },
        None,
    ).await;
    let resp = client.get_bucket_logging().bucket(&src_bucket).send().await.unwrap();
    assert!(resp.logging_enabled().is_some());
    let _ = cfg;
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_mtime() {
    let _guard = TestGuard::setup();
    let client = get_client();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        None, None, None, None,
    ).await;

    let capture = ResponseHeaderCapture::new();
    client.get_bucket_logging().bucket(&src_bucket)
        .customize().interceptor(capture.clone()).send().await.unwrap();
    let mtime = capture.header("last-modified").expect("last-modified header");

    // same config again — mtime should not change
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        None, None, None, None,
    ).await;
    let capture = ResponseHeaderCapture::new();
    client.get_bucket_logging().bucket(&src_bucket)
        .customize().interceptor(capture.clone()).send().await.unwrap();
    let mtime2 = capture.header("last-modified").expect("last-modified header");
    assert_eq!(mtime, mtime2);

    // change prefix — mtime should be newer
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let new_prefix = "another-log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[new_prefix]).await;
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, new_prefix,
        None, None, None, None,
    ).await;
    let capture = ResponseHeaderCapture::new();
    client.get_bucket_logging().bucket(&src_bucket)
        .customize().interceptor(capture.clone()).send().await.unwrap();
    let mtime3 = capture.header("last-modified").expect("last-modified header");
    assert!(mtime3 > mtime, "mtime should increase after config change");

    // disable + re-enable — mtime should be newer
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    client
        .put_bucket_logging()
        .bucket(&src_bucket)
        .bucket_logging_status(BucketLoggingStatus::builder().build())
        .send()
        .await
        .unwrap();
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, new_prefix,
        None, None, None, None,
    ).await;
    let capture = ResponseHeaderCapture::new();
    client.get_bucket_logging().bucket(&src_bucket)
        .customize().interceptor(capture.clone()).send().await.unwrap();
    let mtime4 = capture.header("last-modified").expect("last-modified header");
    assert!(mtime4 > mtime3, "mtime should increase after disable/enable");
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_rm_bucket_logging() {
    let _guard = TestGuard::setup();
    let client = get_client();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        None, None, None, None,
    ).await;

    // disable
    client
        .put_bucket_logging()
        .bucket(&src_bucket)
        .bucket_logging_status(BucketLoggingStatus::builder().build())
        .send()
        .await
        .unwrap();

    let resp = client.get_bucket_logging().bucket(&src_bucket).send().await.unwrap();
    assert!(resp.logging_enabled().is_none());
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_owner() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let alt_client = get_alt_client();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // allow PutBucketLogging for everyone
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:PutBucketLogging"],
        }]
    })
    .to_string();
    client
        .put_bucket_policy()
        .bucket(&src_bucket)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    // alt user should still be denied (not bucket owner)
    verify_access_denied(&alt_client, &src_bucket, &log_bucket, prefix).await;

    // owner can set it
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        None, None, None, None,
    ).await;

    // alt user cannot disable it
    let result = alt_client
        .put_bucket_logging()
        .bucket(&src_bucket)
        .bucket_logging_status(BucketLoggingStatus::builder().build())
        .send()
        .await;
    assert_s3_err!(result, 403, "AccessDenied");
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_put_bucket_logging_errors() {
    let _guard = TestGuard::setup();
    let client = get_client();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket1 = get_new_bucket(Some(&client)).await;
    let prefix = "log/";

    // invalid source bucket
    let result = client
        .put_bucket_logging()
        .bucket(&format!("{src_bucket}kaboom"))
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(
                    LoggingEnabled::builder()
                        .target_bucket(&log_bucket1)
                        .target_prefix(prefix)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchBucket");

    // invalid log bucket
    let result = client
        .put_bucket_logging()
        .bucket(&src_bucket)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(
                    LoggingEnabled::builder()
                        .target_bucket(&format!("{log_bucket1}kaboom"))
                        .target_prefix(prefix)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 404, "NoSuchKey");

    // log bucket has logging configured (circular)
    let log_bucket2 = get_new_bucket(Some(&client)).await;
    set_log_bucket_policy(&client, &log_bucket1, &[&log_bucket2], &[prefix]).await;
    put_bucket_logging_ext(
        &client, &log_bucket2, &log_bucket1, prefix,
        None, None, None, None,
    ).await;
    set_log_bucket_policy(&client, &log_bucket2, &[&src_bucket], &[prefix]).await;
    let result = client
        .put_bucket_logging()
        .bucket(&src_bucket)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(
                    LoggingEnabled::builder()
                        .target_bucket(&log_bucket2)
                        .target_prefix(prefix)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidArgument");

    // invalid PartitionDateSource
    let result = client
        .put_bucket_logging()
        .bucket(&src_bucket)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(
                    LoggingEnabled::builder()
                        .target_bucket(&log_bucket1)
                        .target_prefix(prefix)
                        .target_object_key_format(
                            TargetObjectKeyFormat::builder()
                                .partitioned_prefix(
                                    PartitionedPrefix::builder()
                                        .partition_date_source("kaboom".into())
                                        .build(),
                                )
                                .build(),
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 400, "MalformedXML");

    // log bucket same as source bucket
    set_log_bucket_policy(&client, &src_bucket, &[&src_bucket], &[prefix]).await;
    let result = client
        .put_bucket_logging()
        .bucket(&src_bucket)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(
                    LoggingEnabled::builder()
                        .target_bucket(&src_bucket)
                        .target_prefix(prefix)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidArgument");

    // log bucket is encrypted
    put_bucket_encryption_s3(&client, &log_bucket1).await;
    set_log_bucket_policy(&client, &log_bucket1, &[&src_bucket], &[prefix]).await;
    let result = client
        .put_bucket_logging()
        .bucket(&src_bucket)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(
                    LoggingEnabled::builder()
                        .target_bucket(&log_bucket1)
                        .target_prefix(prefix)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidArgument");

    // requester pays on log bucket
    let log_bucket3 = get_new_bucket(Some(&client)).await;
    set_log_bucket_policy(&client, &log_bucket3, &[&src_bucket], &[prefix]).await;
    client
        .put_bucket_request_payment()
        .bucket(&log_bucket3)
        .request_payment_configuration(
            aws_sdk_s3::types::RequestPaymentConfiguration::builder()
                .payer(aws_sdk_s3::types::Payer::Requester)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let result = client
        .put_bucket_logging()
        .bucket(&src_bucket)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(
                    LoggingEnabled::builder()
                        .target_bucket(&log_bucket3)
                        .target_prefix(prefix)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert_s3_err!(result, 400, "InvalidArgument");

    // invalid log type (extension)
    if has_bucket_logging_extension().await {
        set_log_bucket_policy(&client, &log_bucket3, &[&src_bucket], &[prefix]).await;
        let result = client
            .put_bucket_logging()
            .bucket(&src_bucket)
            .bucket_logging_status(
                BucketLoggingStatus::builder()
                    .logging_enabled(
                        LoggingEnabled::builder()
                            .target_bucket(&log_bucket3)
                            .target_prefix(prefix)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .customize()
            .mutate_request(|req| {
                inject_extension_xml(req, Some("kaboom"), None, None);
            })
            .send()
            .await;
        assert_s3_err!(result, 400, "MalformedXML");
    }
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_put_bucket_logging_extensions() {
    let _guard = TestGuard::setup();
    if !has_bucket_logging_extension().await {
        return;
    }
    let client = get_client();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        None,
        Some("Standard"),
        Some(EXPECTED_OBJECT_ROLL_TIME),
        Some(0),
    ).await;

    let xml = get_bucket_logging_xml(&client, &src_bucket).await;
    assert_eq!(xml_text(&xml, "LoggingType").as_deref(), Some("Standard"));
    assert_eq!(
        xml_text(&xml, "ObjectRollTime").as_deref(),
        Some(EXPECTED_OBJECT_ROLL_TIME.to_string().as_str())
    );
    assert_eq!(xml_text(&xml, "RecordsBatchSize").as_deref(), Some("0"));
    assert!(xml.contains("<SimplePrefix"));
}

#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_put_bucket_logging_permissions() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let cfg = get_config();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";

    // missing policy → AccessDenied
    verify_access_denied(&client, &src_bucket, &log_bucket, prefix).await;

    // missing service principal
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "S3ServerAccessLogsPolicy",
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": ["s3:PutObject"],
            "Resource": format!("arn:aws:s3:::{log_bucket}/{prefix}"),
            "Condition": {
                "ArnLike": {"aws:SourceArn": format!("arn:aws:s3:::{src_bucket}")},
                "StringEquals": {"aws:SourceAccount": &cfg.main_user_id}
            }
        }]
    });
    client.put_bucket_policy().bucket(&log_bucket).policy(policy.to_string()).send().await.unwrap();
    verify_access_denied(&client, &src_bucket, &log_bucket, prefix).await;

    // invalid service principal
    let mut policy = policy.clone();
    policy["Statement"][0]["Principal"] = serde_json::json!({"Service": "logging.s3.amazonaws.comkaboom"});
    client.put_bucket_policy().bucket(&log_bucket).policy(policy.to_string()).send().await.unwrap();
    verify_access_denied(&client, &src_bucket, &log_bucket, prefix).await;

    // valid principal, invalid action
    let mut policy2 = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "S3ServerAccessLogsPolicy",
            "Effect": "Allow",
            "Principal": {"Service": "logging.s3.amazonaws.com"},
            "Action": ["s3:GetObject"],
            "Resource": format!("arn:aws:s3:::{log_bucket}/{prefix}"),
            "Condition": {
                "ArnLike": {"aws:SourceArn": format!("arn:aws:s3:::{src_bucket}")},
                "StringEquals": {"aws:SourceAccount": &cfg.main_user_id}
            }
        }]
    });
    client.put_bucket_policy().bucket(&log_bucket).policy(policy2.to_string()).send().await.unwrap();
    verify_access_denied(&client, &src_bucket, &log_bucket, prefix).await;

    // valid action, invalid resource
    policy2["Statement"][0]["Action"] = serde_json::json!(["s3:PutObject"]);
    policy2["Statement"][0]["Resource"] = serde_json::json!(format!("arn:aws:s3:::{log_bucket}/kaboom"));
    client.put_bucket_policy().bucket(&log_bucket).policy(policy2.to_string()).send().await.unwrap();
    verify_access_denied(&client, &src_bucket, &log_bucket, prefix).await;

    // valid resource, invalid source bucket
    policy2["Statement"][0]["Resource"] = serde_json::json!(format!("arn:aws:s3:::{log_bucket}/{prefix}"));
    policy2["Statement"][0]["Condition"]["ArnLike"]["aws:SourceArn"] = serde_json::json!("arn:aws:s3:::kaboom");
    client.put_bucket_policy().bucket(&log_bucket).policy(policy2.to_string()).send().await.unwrap();
    verify_access_denied(&client, &src_bucket, &log_bucket, prefix).await;

    // valid source bucket, invalid source account
    policy2["Statement"][0]["Condition"]["ArnLike"]["aws:SourceArn"] = serde_json::json!(format!("arn:aws:s3:::{src_bucket}"));
    policy2["Statement"][0]["Condition"]["StringEquals"]["aws:SourceAccount"] = serde_json::json!("kaboom");
    client.put_bucket_policy().bucket(&log_bucket).policy(policy2.to_string()).send().await.unwrap();
    verify_access_denied(&client, &src_bucket, &log_bucket, prefix).await;
}

// ── key format verification ──

fn verify_logging_key(
    key_format: &str,
    key: &str,
    expected_prefix: &str,
    expected_src_account: &str,
    expected_src_region: &str,
    expected_src_bucket: &str,
) {
    if key_format == "SimplePrefix" {
        // [DestinationPrefix]YYYY-MM-DD-hh-mm-ss-UniqueString
        let pattern = if expected_prefix.is_empty() {
            regex::Regex::new(r"^(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(.+)$").unwrap()
        } else {
            regex::Regex::new(r"^(.+?)(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(.+)$").unwrap()
        };
        let caps = pattern.captures(key).unwrap_or_else(|| panic!("key {key} does not match SimplePrefix pattern"));
        if !expected_prefix.is_empty() {
            assert_eq!(&caps[1], expected_prefix, "prefix mismatch");
        }
    } else if key_format == "PartitionedPrefix" {
        // [DestinationPrefix]AccountId/Region/Bucket/YYYY/MM/DD/YYYY-MM-DD-hh-mm-ss-UniqueString
        let pattern = if expected_prefix.is_empty() {
            regex::Regex::new(r"^([^/]+)/([^/]+)/([^/]+)/(\d{4})/(\d{2})/(\d{2})/(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(.+)$").unwrap()
        } else {
            regex::Regex::new(r"^(.+?)([^/]+)/([^/]+)/([^/]+)/(\d{4})/(\d{2})/(\d{2})/(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(.+)$").unwrap()
        };
        let caps = pattern.captures(key).unwrap_or_else(|| panic!("key {key} does not match PartitionedPrefix pattern"));
        let offset = if expected_prefix.is_empty() { 0 } else {
            assert_eq!(&caps[1], expected_prefix, "prefix mismatch");
            1
        };
        assert_eq!(&caps[1 + offset], expected_src_account, "source account mismatch");
        assert_eq!(&caps[2 + offset], expected_src_region, "source region mismatch");
        assert_eq!(&caps[3 + offset], expected_src_bucket, "source bucket mismatch");
    } else {
        panic!("unknown key format: {key_format}");
    }
}

async fn bucket_logging_object_name(key_format: &str, prefix: &str) {
    let client = get_client();
    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    let key_fmt = if key_format == "SimplePrefix" {
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build())
    } else {
        Some(TargetObjectKeyFormat::builder()
            .partitioned_prefix(
                PartitionedPrefix::builder()
                    .partition_date_source(PartitionDateSource::DeliveryTime)
                    .build(),
            )
            .build())
    };

    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        key_fmt, None, None, None,
    ).await;

    for j in 0..5 {
        let name = format!("myobject{j}");
        client.put_object().bucket(&src_bucket).key(&name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert_eq!(keys.len(), 1, "expected 1 log object, got {}", keys.len());

    let cfg = get_config();
    verify_logging_key(key_format, &keys[0], prefix, &cfg.main_user_id, "default", &src_bucket);
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_simple_key() {
    let _guard = TestGuard::setup();
    bucket_logging_object_name("SimplePrefix", "log/").await;
    bucket_logging_object_name("SimplePrefix", "").await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_partitioned_key() {
    let _guard = TestGuard::setup();
    bucket_logging_object_name("PartitionedPrefix", "log/").await;
    bucket_logging_object_name("PartitionedPrefix", "").await;
}

// ── ACL-related logging tests ──

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_bucket_acl_required() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let alt_client = get_alt_client();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    let key = "my-test-object";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // enable logging before creating objects (see docs/rgw-bugs.md #1)
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        None, None, None,
    ).await;

    client.put_object().bucket(&src_bucket).key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
        .send().await.unwrap();
    client.put_bucket_acl().bucket(&src_bucket)
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send().await.unwrap();

    // flush the PUT/ACL setup records
    flush_logs(&client, &src_bucket).await;

    // alt user lists bucket via ACL
    alt_client.list_objects_v2().bucket(&src_bucket).send().await.unwrap();
    flush_logs(&client, &src_bucket).await;

    let log_keys = get_keys(&client, &log_bucket).await;
    assert!(!log_keys.is_empty());
    let body = get_body(&client, &log_bucket, log_keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.GET.BUCKET", &["-"], "Standard", 1, false));
    assert!(verify_record_field(&body, &src_bucket, "REST.GET.BUCKET", "-", "Standard", "ACLRequired", "Yes"));

    // set policy allowing ListBucket without ACL
    let resource = format!("arn:aws:s3:::{src_bucket}");
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": [resource]
        }]
    });
    client.put_bucket_policy().bucket(&src_bucket).policy(policy.to_string()).send().await.unwrap();

    alt_client.list_objects_v2().bucket(&src_bucket).send().await.unwrap();
    flush_logs(&client, &src_bucket).await;

    let log_keys = get_keys(&client, &log_bucket).await;
    let body = get_body(&client, &log_bucket, log_keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.GET.BUCKET", &["-"], "Standard", 1, false));
    assert!(verify_record_field(&body, &src_bucket, "REST.GET.BUCKET", "-", "Standard", "ACLRequired", "-"));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_object_acl_required() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let alt_client = get_alt_client();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    let key = "my-test-object";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // enable logging before creating objects (see docs/rgw-bugs.md #1)
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        None, None, None,
    ).await;

    client.put_object().bucket(&src_bucket).key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
        .send().await.unwrap();
    client.put_object_acl().bucket(&src_bucket).key(key)
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .send().await.unwrap();

    // flush the PUT/ACL setup records
    flush_logs(&client, &src_bucket).await;

    // alt user gets object via ACL
    alt_client.get_object().bucket(&src_bucket).key(key).send().await.unwrap();
    flush_logs(&client, &src_bucket).await;

    let log_keys = get_keys(&client, &log_bucket).await;
    assert!(!log_keys.is_empty());
    let body = get_body(&client, &log_bucket, log_keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.GET.OBJECT", &[key], "Standard", 1, false));
    assert!(verify_record_field(&body, &src_bucket, "REST.GET.OBJECT", key, "Standard", "ACLRequired", "Yes"));

    // set policy allowing GetObject without ACL
    let resource1 = format!("arn:aws:s3:::{src_bucket}");
    let resource2 = format!("arn:aws:s3:::{src_bucket}/*");
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:GetObject",
            "Resource": [resource1, resource2]
        }]
    });
    client.put_bucket_policy().bucket(&src_bucket).policy(policy.to_string()).send().await.unwrap();

    alt_client.get_object().bucket(&src_bucket).key(key).send().await.unwrap();
    flush_logs(&client, &src_bucket).await;

    let log_keys = get_keys(&client, &log_bucket).await;
    let body = get_body(&client, &log_bucket, log_keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.GET.OBJECT", &[key], "Standard", 1, false));
    assert!(verify_record_field(&body, &src_bucket, "REST.GET.OBJECT", key, "Standard", "ACLRequired", "-"));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_bucket_auth_type() {
    let _guard = TestGuard::setup();
    let client = get_client();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    let key = "my-test-object";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // enable logging before creating objects (see docs/rgw-bugs.md #1)
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        None, None, None,
    ).await;

    client.put_object().bucket(&src_bucket).key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
        .send().await.unwrap();
    client.put_object_acl().bucket(&src_bucket).key(key)
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .send().await.unwrap();

    // flush the PUT/ACL setup records
    flush_logs(&client, &src_bucket).await;

    // AuthType AuthHeader
    client.get_object().bucket(&src_bucket).key(key).send().await.unwrap();
    flush_logs(&client, &src_bucket).await;
    let log_keys = get_keys(&client, &log_bucket).await;
    assert!(!log_keys.is_empty());
    let body = get_body(&client, &log_bucket, log_keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.GET.OBJECT", &[key], "Standard", 1, false));
    assert!(verify_record_field(&body, &src_bucket, "REST.GET.OBJECT", key, "Standard", "AuthType", "AuthHeader"));

    // AuthType "-" (unauthenticated)
    let unauth_client = get_unauthenticated_client();
    unauth_client.get_object().bucket(&src_bucket).key(key).send().await.unwrap();
    flush_logs(&client, &src_bucket).await;
    let log_keys = get_keys(&client, &log_bucket).await;
    let body = get_body(&client, &log_bucket, log_keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.GET.OBJECT", &[key], "Standard", 1, false));
    assert!(verify_record_field(&body, &src_bucket, "REST.GET.OBJECT", key, "Standard", "AuthType", "-"));

    // AuthType QueryString (presigned)
    let presigned_url = presign_get(
        &client, &src_bucket, key,
        std::time::Duration::from_secs(100000),
    ).await;
    let resp = raw_request(reqwest::Method::GET, &presigned_url, None).await;
    assert_eq!(resp.status, 200);

    flush_logs(&client, &src_bucket).await;
    let log_keys = get_keys(&client, &log_bucket).await;
    let body = get_body(&client, &log_bucket, log_keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.GET.OBJECT", &[key], "Standard", 1, false));
    assert!(verify_record_field(&body, &src_bucket, "REST.GET.OBJECT", key, "Standard", "AuthType", "QueryString"));
}

// ══════════════════════════════════════════════════════════════════
// Flush tests
// ══════════════════════════════════════════════════════════════════

async fn bucket_logging_flush(logging_type: &str, single_prefix: bool, concurrency: bool, num_keys: usize) {
    if !has_bucket_logging_extension().await {
        return;
    }
    let client = get_client();
    let log_bucket = get_new_bucket(Some(&client)).await;

    let num_buckets = 5usize;
    let mut buckets = Vec::new();
    let mut log_prefixes = Vec::new();
    let longer_time = EXPECTED_OBJECT_ROLL_TIME * 10;

    for _ in 0..num_buckets {
        let src = get_new_bucket(Some(&client)).await;
        if single_prefix {
            log_prefixes.push("log/".to_string());
        } else {
            log_prefixes.push(format!("{src}/"));
        }
        buckets.push(src);
    }

    let prefix_refs: Vec<&str> = log_prefixes.iter().map(|s| s.as_str()).collect();
    let bucket_refs: Vec<&str> = buckets.iter().map(|s| s.as_str()).collect();
    set_log_bucket_policy(&client, &log_bucket, &bucket_refs, &prefix_refs).await;

    for j in 0..num_buckets {
        put_bucket_logging_ext(
            &client, &buckets[j], &log_bucket, &log_prefixes[j],
            None, Some(logging_type), Some(longer_time), None,
        ).await;
    }

    let mut src_names: Vec<String> = Vec::new();
    for j in 0..num_keys {
        src_names.push(format!("myobject{j}"));
    }

    for src_bucket in &buckets {
        for name in &src_names {
            client.put_object().bucket(src_bucket).key(name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
            client.delete_object().bucket(src_bucket).key(name)
                .send().await.unwrap();
        }
    }

    let keys = get_keys(&client, &log_bucket).await;
    assert_eq!(keys.len(), 0, "expected no log objects before flush");

    let flushed_objs = std::sync::Arc::new(tokio::sync::Mutex::new(
        std::collections::HashMap::<String, String>::new(),
    ));

    if concurrency {
        let mut set = tokio::task::JoinSet::new();
        for src_bucket in &buckets {
            let bucket = src_bucket.clone();
            let flushed = flushed_objs.clone();
            set.spawn(async move {
                let resp = post_bucket_logging(&bucket).await;
                assert_eq!(resp.status, 200, "concurrent flush failed: {}", resp.body);
                if let Some(obj) = xml_text(&resp.body, "FlushedLoggingObject") {
                    flushed.lock().await.insert(bucket, obj);
                }
            });
            if single_prefix {
                break;
            }
        }
        while let Some(result) = set.join_next().await {
            result.unwrap();
        }
    } else {
        for src_bucket in &buckets {
            let resp = post_bucket_logging(src_bucket).await;
            assert_eq!(resp.status, 200, "serial flush failed: {}", resp.body);
            if let Some(obj) = xml_text(&resp.body, "FlushedLoggingObject") {
                flushed_objs.lock().await.insert(src_bucket.clone(), obj);
            }
            if single_prefix {
                break;
            }
        }
    }

    let keys = get_keys(&client, &log_bucket).await;
    let flushed = flushed_objs.lock().await;

    if single_prefix {
        assert_eq!(keys.len(), 1);
        assert_eq!(flushed.len(), 1);
    } else {
        assert_eq!(keys.len(), num_buckets);
        assert!(flushed.len() >= num_buckets);
    }

    let src_name_refs: Vec<&str> = src_names.iter().map(|s| s.as_str()).collect();
    for key in &keys {
        let body = get_body(&client, &log_bucket, key).await;
        let mut found = false;
        for j in 0..num_buckets {
            if key.starts_with(&log_prefixes[j]) {
                if let Some(flushed_obj) = flushed.get(&buckets[j]) {
                    assert_eq!(key, flushed_obj);
                }
                found = true;
                if num_keys > 0 {
                    assert!(verify_records(&body, &buckets[j], "REST.PUT.OBJECT", &src_name_refs, logging_type, num_keys, false));
                    assert!(verify_records(&body, &buckets[j], "REST.DELETE.OBJECT", &src_name_refs, logging_type, num_keys, false));
                }
            }
        }
        assert!(found, "log key {key} does not match any expected prefix");
    }
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_flush_empty() {
    let _guard = TestGuard::setup();
    bucket_logging_flush("Journal", false, false, 0).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_flush_j() {
    let _guard = TestGuard::setup();
    bucket_logging_flush("Journal", false, false, 10).await;
    bucket_logging_flush("Journal", false, false, 100).await;
    bucket_logging_flush("Journal", false, false, 0).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_flush_s() {
    let _guard = TestGuard::setup();
    bucket_logging_flush("Standard", false, false, 10).await;
    bucket_logging_flush("Standard", false, false, 100).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_flush_j_single() {
    let _guard = TestGuard::setup();
    bucket_logging_flush("Journal", true, false, 10).await;
    bucket_logging_flush("Journal", true, false, 100).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_flush_s_single() {
    let _guard = TestGuard::setup();
    bucket_logging_flush("Standard", true, false, 10).await;
    bucket_logging_flush("Standard", true, false, 100).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_concurrent_flush_j() {
    let _guard = TestGuard::setup();
    bucket_logging_flush("Journal", false, true, 10).await;
    bucket_logging_flush("Journal", false, true, 100).await;
    bucket_logging_flush("Journal", false, true, 0).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_concurrent_flush_s() {
    let _guard = TestGuard::setup();
    bucket_logging_flush("Standard", false, true, 10).await;
    bucket_logging_flush("Standard", false, true, 100).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_concurrent_flush_j_single() {
    let _guard = TestGuard::setup();
    bucket_logging_flush("Journal", true, true, 10).await;
    bucket_logging_flush("Journal", true, true, 100).await;
    bucket_logging_flush("Journal", true, true, 0).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_concurrent_flush_s_single() {
    let _guard = TestGuard::setup();
    bucket_logging_flush("Standard", true, true, 10).await;
    bucket_logging_flush("Standard", true, true, 100).await;
}

// ══════════════════════════════════════════════════════════════════
// Operation logging tests
// ══════════════════════════════════════════════════════════════════

async fn setup_logging_with_journal(
    client: &aws_sdk_s3::Client,
    src_bucket: &str,
    log_bucket: &str,
    prefix: &str,
) {
    let has_ext = has_bucket_logging_extension().await;
    put_bucket_logging_ext(
        client, src_bucket, log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        if has_ext { Some("Journal") } else { None },
        if has_ext { Some(EXPECTED_OBJECT_ROLL_TIME) } else { None },
        None,
    ).await;
}

async fn setup_logging_with_standard(
    client: &aws_sdk_s3::Client,
    src_bucket: &str,
    log_bucket: &str,
    prefix: &str,
) {
    let has_ext = has_bucket_logging_extension().await;
    put_bucket_logging_ext(
        client, src_bucket, log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        if has_ext { Some("Standard") } else { None },
        if has_ext { Some(EXPECTED_OBJECT_ROLL_TIME) } else { None },
        None,
    ).await;
}

fn record_type_for_ext(has_ext: bool, preferred: &str) -> &str {
    if has_ext { preferred } else { "Standard" }
}

async fn bucket_logging_put_objects(versioned: bool) {
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let src_bucket = get_new_bucket(Some(&client)).await;
    if versioned {
        s3_tests_rs::fixtures::check_configure_versioning_retry(&client, &src_bucket, "Enabled", "Enabled").await;
    }
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;
    setup_logging_with_journal(&client, &src_bucket, &log_bucket, prefix).await;

    let num_keys = 5;
    for j in 0..num_keys {
        let name = format!("myobject{j}");
        client.put_object().bucket(&src_bucket).key(&name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
        if versioned {
            client.put_object().bucket(&src_bucket).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        }
    }

    let expected_count = if versioned { 2 * num_keys } else { num_keys };
    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();

    let flushed_obj = flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty(), "expected log objects");

    let record_type = record_type_for_ext(has_ext, "Journal");
    let body = get_body(&client, &log_bucket, keys.last().unwrap()).await;
    if let Some(ref fobj) = flushed_obj {
        assert_eq!(keys.last().unwrap(), fobj);
    }
    assert!(verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &src_keys, record_type, expected_count, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_put_objects() {
    let _guard = TestGuard::setup();
    bucket_logging_put_objects(false).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_put_objects_versioned() {
    let _guard = TestGuard::setup();
    bucket_logging_put_objects(true).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_put_concurrency() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;
    setup_logging_with_journal(&client, &src_bucket, &log_bucket, prefix).await;

    let num_keys = 50usize;
    let mut set = tokio::task::JoinSet::new();
    for i in 0..num_keys {
        let c = client.clone();
        let b = src_bucket.clone();
        set.spawn(async move {
            c.put_object().bucket(&b).key(&format!("myobject{i}"))
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        });
    }
    while let Some(r) = set.join_next().await { r.unwrap(); }

    let src_keys_list: Vec<String> = (0..num_keys).map(|i| format!("myobject{i}")).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();

    if !has_ext {
        tokio::time::sleep(std::time::Duration::from_secs_f64(EXPECTED_OBJECT_ROLL_TIME as f64 * 1.1)).await;
    }
    let mut flush_set = tokio::task::JoinSet::new();
    for _ in 0..num_keys {
        if has_ext {
            let b = src_bucket.clone();
            flush_set.spawn(async move { post_bucket_logging(&b).await; });
        } else {
            let c = client.clone();
            let b = src_bucket.clone();
            flush_set.spawn(async move {
                c.put_object().bucket(&b).key("dummy")
                    .body(aws_sdk_s3::primitives::ByteStream::from_static(b"dummy"))
                    .send().await.unwrap();
            });
        }
    }
    while let Some(r) = flush_set.join_next().await { r.unwrap(); }

    let keys = get_keys(&client, &log_bucket).await;
    let record_type = record_type_for_ext(has_ext, "Journal");
    let mut body = String::new();
    for key in &keys {
        assert!(key.starts_with("log/"));
        body.push_str(&get_body(&client, &log_bucket, key).await);
    }
    assert!(verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &src_keys, record_type, num_keys, false));
}

async fn bucket_logging_delete_objects(versioned: bool) {
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let src_bucket = get_new_bucket(Some(&client)).await;
    if versioned {
        s3_tests_rs::fixtures::check_configure_versioning_retry(&client, &src_bucket, "Enabled", "Enabled").await;
    }
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // enable logging first (RGW bug workaround), then create objects
    setup_logging_with_journal(&client, &src_bucket, &log_bucket, prefix).await;

    let num_keys = 5;
    for j in 0..num_keys {
        let name = format!("myobject{j}");
        client.put_object().bucket(&src_bucket).key(&name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
        if versioned {
            client.put_object().bucket(&src_bucket).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        }
    }

    // flush setup records
    flush_logs(&client, &src_bucket).await;

    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();

    for key in &src_keys {
        if versioned {
            let head = client.head_object().bucket(&src_bucket).key(*key).send().await.unwrap();
            client.delete_object().bucket(&src_bucket).key(*key)
                .version_id(head.version_id().unwrap())
                .send().await.unwrap();
        }
        client.delete_object().bucket(&src_bucket).key(*key).send().await.unwrap();
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty());

    let expected_count = if versioned { 2 * num_keys } else { num_keys };
    let record_type = record_type_for_ext(has_ext, "Journal");
    let body = get_body(&client, &log_bucket, keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.DELETE.OBJECT", &src_keys, record_type, expected_count, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_delete_objects() {
    let _guard = TestGuard::setup();
    bucket_logging_delete_objects(false).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_delete_objects_versioned() {
    let _guard = TestGuard::setup();
    bucket_logging_delete_objects(true).await;
}

async fn bucket_logging_get_objects(versioned: bool) {
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let src_bucket = get_new_bucket(Some(&client)).await;
    if versioned {
        s3_tests_rs::fixtures::check_configure_versioning_retry(&client, &src_bucket, "Enabled", "Enabled").await;
    }
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // enable logging first, then create objects (RGW bug workaround)
    setup_logging_with_standard(&client, &src_bucket, &log_bucket, prefix).await;

    let num_keys = 5;
    for j in 0..num_keys {
        let name = format!("myobject{j}");
        client.put_object().bucket(&src_bucket).key(&name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
        if versioned {
            client.put_object().bucket(&src_bucket).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        }
    }

    // flush setup records
    flush_logs(&client, &src_bucket).await;

    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();

    for key in &src_keys {
        if versioned {
            let head = client.head_object().bucket(&src_bucket).key(*key).send().await.unwrap();
            client.get_object().bucket(&src_bucket).key(*key)
                .version_id(head.version_id().unwrap())
                .send().await.unwrap();
        }
        client.get_object().bucket(&src_bucket).key(*key).send().await.unwrap();
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty());

    let expected_count = if versioned { 2 * num_keys } else { num_keys };
    let body = get_body(&client, &log_bucket, keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.GET.OBJECT", &src_keys, "Standard", expected_count, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_get_objects() {
    let _guard = TestGuard::setup();
    bucket_logging_get_objects(false).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_get_objects_versioned() {
    let _guard = TestGuard::setup();
    bucket_logging_get_objects(true).await;
}

async fn bucket_logging_copy_objects(versioned: bool, another_bucket: bool) {
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let src_bucket = get_new_bucket(Some(&client)).await;
    let dst_bucket = if another_bucket {
        get_new_bucket(Some(&client)).await
    } else {
        src_bucket.clone()
    };
    if versioned {
        s3_tests_rs::fixtures::check_configure_versioning_retry(&client, &src_bucket, "Enabled", "Enabled").await;
    }
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket, &dst_bucket], &[prefix]).await;

    // enable logging first, then create objects (RGW bug workaround)
    setup_logging_with_journal(&client, &src_bucket, &log_bucket, prefix).await;
    if another_bucket {
        setup_logging_with_journal(&client, &dst_bucket, &log_bucket, prefix).await;
    }

    let num_keys = 5;
    for j in 0..num_keys {
        let name = format!("myobject{j}");
        client.put_object().bucket(&src_bucket).key(&name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
        if versioned {
            client.put_object().bucket(&src_bucket).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        }
    }

    // flush setup records
    flush_logs(&client, &src_bucket).await;

    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
    let mut dst_keys_list: Vec<String> = Vec::new();
    for key in &src_keys_list {
        let dst_key = format!("copy_of_{key}");
        dst_keys_list.push(dst_key.clone());
        client.copy_object()
            .bucket(&dst_bucket)
            .key(&dst_key)
            .copy_source(format!("{src_bucket}/{key}"))
            .send().await.unwrap();
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty());

    let dst_keys: Vec<&str> = dst_keys_list.iter().map(|s| s.as_str()).collect();
    let record_type = record_type_for_ext(has_ext, "Journal");
    let body = get_body(&client, &log_bucket, keys.last().unwrap()).await;
    assert!(verify_records(&body, &dst_bucket, "REST.PUT.OBJECT", &dst_keys, record_type, num_keys, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_copy_objects() {
    let _guard = TestGuard::setup();
    bucket_logging_copy_objects(false, false).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_copy_objects_versioned() {
    let _guard = TestGuard::setup();
    bucket_logging_copy_objects(true, false).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_copy_objects_bucket() {
    let _guard = TestGuard::setup();
    bucket_logging_copy_objects(false, true).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_copy_objects_bucket_versioned() {
    let _guard = TestGuard::setup();
    bucket_logging_copy_objects(true, true).await;
}

async fn bucket_logging_head_objects(versioned: bool) {
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let src_bucket = get_new_bucket(Some(&client)).await;
    if versioned {
        s3_tests_rs::fixtures::check_configure_versioning_retry(&client, &src_bucket, "Enabled", "Enabled").await;
    }
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // enable logging first, then create objects (RGW bug workaround)
    setup_logging_with_standard(&client, &src_bucket, &log_bucket, prefix).await;

    let num_keys = 5;
    for j in 0..num_keys {
        let name = format!("myobject{j}");
        client.put_object().bucket(&src_bucket).key(&name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
    }

    // flush setup records
    flush_logs(&client, &src_bucket).await;

    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();

    for key in &src_keys {
        if versioned {
            let head = client.head_object().bucket(&src_bucket).key(*key).send().await.unwrap();
            client.head_object().bucket(&src_bucket).key(*key)
                .version_id(head.version_id().unwrap())
                .send().await.unwrap();
        } else {
            client.head_object().bucket(&src_bucket).key(*key).send().await.unwrap();
        }
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty());

    let expected_count = if versioned { 2 * num_keys } else { num_keys };
    let body = get_body(&client, &log_bucket, keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.HEAD.OBJECT", &src_keys, "Standard", expected_count, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_head_objects() {
    let _guard = TestGuard::setup();
    bucket_logging_head_objects(false).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_head_objects_versioned() {
    let _guard = TestGuard::setup();
    bucket_logging_head_objects(true).await;
}

async fn bucket_logging_mpu(versioned: bool, record_type: &str) {
    let client = get_client();
    let src_bucket = get_new_bucket(Some(&client)).await;
    if versioned {
        s3_tests_rs::fixtures::check_configure_versioning_retry(&client, &src_bucket, "Enabled", "Enabled").await;
    }
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    if record_type == "Journal" {
        setup_logging_with_journal(&client, &src_bucket, &log_bucket, prefix).await;
    } else {
        setup_logging_with_standard(&client, &src_bucket, &log_bucket, prefix).await;
    }

    let src_key = "myobject";
    let objlen = 30 * 1024 * 1024;
    let result = s3_tests_rs::fixtures::multipart_upload(
        &client, &src_bucket, src_key, objlen, None, None, None, None,
    ).await;
    client.complete_multipart_upload()
        .bucket(&src_bucket).key(src_key)
        .upload_id(&result.upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(result.parts))
                .build(),
        )
        .send().await.unwrap();

    if versioned {
        let result2 = s3_tests_rs::fixtures::multipart_upload(
            &client, &src_bucket, src_key, objlen, None, None, None, None,
        ).await;
        client.complete_multipart_upload()
            .bucket(&src_bucket).key(src_key)
            .upload_id(&result2.upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(result2.parts))
                    .build(),
            )
            .send().await.unwrap();
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty());

    let expected_upload_count = if record_type == "Journal" {
        if versioned { 2 } else { 1 }
    } else {
        if versioned { 4 } else { 2 }
    };

    let body = get_body(&client, &log_bucket, keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.POST.UPLOAD", &[src_key], record_type, expected_upload_count, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_mpu_s() {
    let _guard = TestGuard::setup();
    bucket_logging_mpu(false, "Standard").await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_mpu_versioned_s() {
    let _guard = TestGuard::setup();
    bucket_logging_mpu(true, "Standard").await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_mpu_j() {
    let _guard = TestGuard::setup();
    if !has_bucket_logging_extension().await { return; }
    bucket_logging_mpu(false, "Journal").await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_mpu_versioned_j() {
    let _guard = TestGuard::setup();
    if !has_bucket_logging_extension().await { return; }
    bucket_logging_mpu(true, "Journal").await;
}

async fn bucket_logging_mpu_copy(versioned: bool) {
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let src_bucket = get_new_bucket(Some(&client)).await;
    if versioned {
        s3_tests_rs::fixtures::check_configure_versioning_retry(&client, &src_bucket, "Enabled", "Enabled").await;
    }
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // create MPU object first, then enable logging (mpu_copy creates objects before logging in Python,
    // but since we copy AFTER logging, the copy operation is what we're testing)
    let src_key = "myobject";
    let objlen = 30 * 1024 * 1024;
    let result = s3_tests_rs::fixtures::multipart_upload(
        &client, &src_bucket, src_key, objlen, None, None, None, None,
    ).await;
    client.complete_multipart_upload()
        .bucket(&src_bucket).key(src_key)
        .upload_id(&result.upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(result.parts))
                .build(),
        )
        .send().await.unwrap();

    if versioned {
        let result2 = s3_tests_rs::fixtures::multipart_upload(
            &client, &src_bucket, src_key, objlen, None, None, None, None,
        ).await;
        client.complete_multipart_upload()
            .bucket(&src_bucket).key(src_key)
            .upload_id(&result2.upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(result2.parts))
                    .build(),
            )
            .send().await.unwrap();
    }

    // enable logging after MPU objects exist — this triggers the RGW bug,
    // so flush_logs will fall back to sleep if post_bucket_logging fails
    setup_logging_with_journal(&client, &src_bucket, &log_bucket, prefix).await;

    client.copy_object()
        .bucket(&src_bucket)
        .key(&format!("copy_of_{src_key}"))
        .copy_source(format!("{src_bucket}/{src_key}"))
        .send().await.unwrap();

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty());

    let record_type = record_type_for_ext(has_ext, "Journal");
    let body = get_body(&client, &log_bucket, keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &[&format!("copy_of_{src_key}")], record_type, 1, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_mpu_copy() {
    let _guard = TestGuard::setup();
    bucket_logging_mpu_copy(false).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_mpu_copy_versioned() {
    let _guard = TestGuard::setup();
    bucket_logging_mpu_copy(true).await;
}

async fn bucket_logging_multi_delete(versioned: bool) {
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let src_bucket = get_new_bucket(Some(&client)).await;
    if versioned {
        s3_tests_rs::fixtures::check_configure_versioning_retry(&client, &src_bucket, "Enabled", "Enabled").await;
    }
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    // enable logging first (RGW bug workaround)
    setup_logging_with_journal(&client, &src_bucket, &log_bucket, prefix).await;

    let num_keys = 5;
    for j in 0..num_keys {
        let name = format!("myobject{j}");
        client.put_object().bucket(&src_bucket).key(&name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
        if versioned {
            client.put_object().bucket(&src_bucket).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        }
    }

    // flush setup records
    flush_logs(&client, &src_bucket).await;

    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();

    if versioned {
        let versions = client.list_object_versions().bucket(&src_bucket).send().await.unwrap();
        let mut objects = Vec::new();
        for v in versions.versions() {
            objects.push(
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(v.key().unwrap())
                    .version_id(v.version_id().unwrap())
                    .build()
                    .unwrap(),
            );
        }
        client.delete_objects().bucket(&src_bucket)
            .delete(aws_sdk_s3::types::Delete::builder().set_objects(Some(objects)).build().unwrap())
            .send().await.unwrap();
    } else {
        let objects: Vec<_> = src_keys.iter().map(|k|
            aws_sdk_s3::types::ObjectIdentifier::builder().key(*k).build().unwrap()
        ).collect();
        client.delete_objects().bucket(&src_bucket)
            .delete(aws_sdk_s3::types::Delete::builder().set_objects(Some(objects)).build().unwrap())
            .send().await.unwrap();
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty());

    let expected_count = if versioned { 2 * num_keys } else { num_keys };
    let record_type = record_type_for_ext(has_ext, "Journal");
    let body = get_body(&client, &log_bucket, keys.last().unwrap()).await;
    assert!(verify_records(&body, &src_bucket, "REST.POST.DELETE_MULTI_OBJECT", &src_keys, record_type, expected_count, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_multi_delete() {
    let _guard = TestGuard::setup();
    bucket_logging_multi_delete(false).await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_multi_delete_versioned() {
    let _guard = TestGuard::setup();
    bucket_logging_multi_delete(true).await;
}

// ══════════════════════════════════════════════════════════════════
// Event type tests
// ══════════════════════════════════════════════════════════════════

async fn bucket_logging_type(logging_type: &str) {
    if !has_bucket_logging_extension().await { return; }
    let client = get_client();
    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        Some(logging_type), Some(EXPECTED_OBJECT_ROLL_TIME), None,
    ).await;

    let num_keys = 5;
    for j in 0..num_keys {
        let name = format!("myobject{j}");
        client.put_object().bucket(&src_bucket).key(&name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
        client.head_object().bucket(&src_bucket).key(&name).send().await.unwrap();
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty());

    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();

    let body = get_body(&client, &log_bucket, keys.last().unwrap()).await;
    if logging_type == "Journal" {
        assert!(verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &src_keys, "Journal", num_keys, false));
        assert!(!verify_records(&body, &src_bucket, "REST.HEAD.OBJECT", &src_keys, "Journal", num_keys, false));
    } else {
        assert!(verify_records(&body, &src_bucket, "REST.HEAD.OBJECT", &src_keys, "Standard", num_keys, false));
        assert!(verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &src_keys, "Standard", num_keys, false));
    }
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_event_type_j() {
    let _guard = TestGuard::setup();
    bucket_logging_type("Journal").await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_event_type_s() {
    let _guard = TestGuard::setup();
    bucket_logging_type("Standard").await;
}

// ── multi-prefix tests ──

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_multiple_prefixes() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let log_bucket = get_new_bucket(Some(&client)).await;

    let num_buckets = 5;
    let mut buckets = Vec::new();
    let mut log_prefixes = Vec::new();
    for _ in 0..num_buckets {
        let src = get_new_bucket(Some(&client)).await;
        log_prefixes.push(format!("{src}/"));
        buckets.push(src);
    }

    let prefix_refs: Vec<&str> = log_prefixes.iter().map(|s| s.as_str()).collect();
    let bucket_refs: Vec<&str> = buckets.iter().map(|s| s.as_str()).collect();
    set_log_bucket_policy(&client, &log_bucket, &bucket_refs, &prefix_refs).await;

    for j in 0..num_buckets {
        put_bucket_logging_ext(
            &client, &buckets[j], &log_bucket, &log_prefixes[j],
            Some(TargetObjectKeyFormat::builder()
                .simple_prefix(SimplePrefix::builder().build())
                .build()),
            None,
            if has_ext { Some(EXPECTED_OBJECT_ROLL_TIME) } else { None },
            None,
        ).await;
    }

    let num_keys = 5;
    for src_bucket in &buckets {
        for j in 0..num_keys {
            let name = format!("myobject{j}");
            client.put_object().bucket(src_bucket).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        }
    }

    for src_bucket in &buckets {
        flush_logs(&client, src_bucket).await;
    }

    let keys = get_keys(&client, &log_bucket).await;
    assert!(keys.len() >= num_buckets);

    for key in &keys {
        let body = get_body(&client, &log_bucket, key).await;
        let mut found = false;
        for src_bucket in &buckets {
            if key.starts_with(src_bucket.as_str()) {
                found = true;
                let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
                let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();
                assert!(verify_records(&body, src_bucket, "REST.PUT.OBJECT", &src_keys, "Standard", num_keys, false));
            }
        }
        assert!(found, "log key {key} does not match any bucket prefix");
    }
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_single_prefix() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let has_ext = has_bucket_logging_extension().await;
    let log_bucket = get_new_bucket(Some(&client)).await;

    let num_buckets = 5;
    let mut buckets = Vec::new();
    let prefix = "log/";
    for _ in 0..num_buckets {
        let src = get_new_bucket(Some(&client)).await;
        buckets.push(src);
    }

    let bucket_refs: Vec<&str> = buckets.iter().map(|s| s.as_str()).collect();
    set_log_bucket_policy(&client, &log_bucket, &bucket_refs, &[prefix]).await;

    for src_bucket in &buckets {
        put_bucket_logging_ext(
            &client, src_bucket, &log_bucket, prefix,
            Some(TargetObjectKeyFormat::builder()
                .simple_prefix(SimplePrefix::builder().build())
                .build()),
            None,
            if has_ext { Some(EXPECTED_OBJECT_ROLL_TIME) } else { None },
            None,
        ).await;
    }

    let num_keys = 5;
    for (bucket_ind, src_bucket) in buckets.iter().enumerate() {
        for j in 0..num_keys {
            let name = format!("myobject{}{j}", bucket_ind + 1);
            client.put_object().bucket(src_bucket).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        }
    }

    flush_logs(&client, buckets.last().unwrap()).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert_eq!(keys.len(), 1);

    let body = get_body(&client, &log_bucket, &keys[0]).await;
    let last_bucket = buckets.last().unwrap();
    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{}{j}", num_buckets)).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();
    assert!(verify_records(&body, last_bucket, "REST.PUT.OBJECT", &src_keys, "Standard", num_keys, false));
}

// ══════════════════════════════════════════════════════════════════
// Roll time, put_and_flush, object_meta, permission_change
// ══════════════════════════════════════════════════════════════════

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
#[ignore = "VERIFY: automatic rollover timing is unreliable — Python also skips (requires SDK extensions)"]
async fn test_bucket_logging_roll_time() {
    let _guard = TestGuard::setup();
    if !has_bucket_logging_extension().await { return; }
    let client = get_client();
    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    let roll_time = 10u32;
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        None, Some(roll_time), None,
    ).await;

    let num_keys = 5;
    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();
    for name in &src_keys {
        client.put_object().bucket(&src_bucket).key(*name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_secs(roll_time as u64 / 2)).await;
    client.put_object().bucket(&src_bucket).key("myobject")
        .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
        .send().await.unwrap();

    let keys = get_keys(&client, &log_bucket).await;
    assert_eq!(keys.len(), 0, "no log objects expected before roll time");

    tokio::time::sleep(std::time::Duration::from_secs(roll_time as u64 / 2)).await;
    client.put_object().bucket(&src_bucket).key("myobject")
        .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
        .send().await.unwrap();

    // wait for bucket logging manager to detect commit list
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert_eq!(keys.len(), 1);

    let body = get_body(&client, &log_bucket, &keys[0]).await;
    assert!(verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &src_keys, "Standard", num_keys, false));
    client.delete_object().bucket(&log_bucket).key(&keys[0]).send().await.unwrap();

    // second phase: 25 keys with 1s spacing → multiple roll files
    let num_keys2 = 25;
    let src_keys2_list: Vec<String> = (0..num_keys2).map(|j| format!("myobject{j}")).collect();
    let src_keys2: Vec<&str> = src_keys2_list.iter().map(|s| s.as_str()).collect();
    for name in &src_keys2 {
        client.put_object().bucket(&src_bucket).key(*name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    tokio::time::sleep(std::time::Duration::from_secs(roll_time as u64)).await;
    client.put_object().bucket(&src_bucket).key("myobject")
        .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
        .send().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let keys = get_keys(&client, &log_bucket).await;
    assert!(keys.len() > 1, "expected multiple log objects after spaced puts");

    let mut body = String::new();
    for key in &keys {
        assert!(key.starts_with("log/"));
        body.push_str(&get_body(&client, &log_bucket, key).await);
    }
    assert!(verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &src_keys2, "Standard", num_keys2 + 1, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_put_and_flush() {
    let _guard = TestGuard::setup();
    if !has_bucket_logging_extension().await { return; }
    let client = get_client();
    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[prefix]).await;

    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        Some("Journal"), None, None,
    ).await;

    let num_keys = 300usize;
    let flush_rate = 10;
    let mut put_set = tokio::task::JoinSet::new();
    let mut flush_set = tokio::task::JoinSet::new();
    let src_names: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();

    for j in 0..num_keys {
        let c = client.clone();
        let b = src_bucket.clone();
        let name = src_names[j].clone();
        put_set.spawn(async move {
            c.put_object().bucket(&b).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        });
        if j % flush_rate == 0 {
            let b = src_bucket.clone();
            flush_set.spawn(async move { post_bucket_logging(&b).await; });
        }
    }
    while let Some(r) = put_set.join_next().await { r.unwrap(); }
    while let Some(r) = flush_set.join_next().await { r.unwrap(); }

    flush_logs(&client, &src_bucket).await;

    let keys = get_keys(&client, &log_bucket).await;
    let mut body = String::new();
    let mut prev_key = String::new();
    for key in &keys {
        assert!(*key > prev_key, "log keys should be sorted");
        prev_key = key.clone();
        body.push_str(&get_body(&client, &log_bucket, key).await);
    }
    let src_refs: Vec<&str> = src_names.iter().map(|s| s.as_str()).collect();
    assert!(verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &src_refs, "Journal", num_keys, false));
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_object_meta() {
    let _guard = TestGuard::setup();
    if !has_bucket_logging_extension().await { return; }
    let client = get_client();

    let src_bucket_name = get_new_bucket_name();
    client
        .create_bucket()
        .bucket(&src_bucket_name)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
    s3_tests_rs::fixtures::register_bucket_for_cleanup(&client, &src_bucket_name);

    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";
    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket_name], &[prefix]).await;

    put_bucket_logging_ext(
        &client, &src_bucket_name, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        Some("Journal"), None, None,
    ).await;

    let name = "myobject";
    let resp = client.put_object().bucket(&src_bucket_name).key(name)
        .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
        .send().await.unwrap();
    let version_id = resp.version_id().unwrap().to_string();

    client.put_object_acl().bucket(&src_bucket_name).key(name)
        .version_id(&version_id)
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicReadWrite)
        .send().await.unwrap();

    client.put_object_tagging().bucket(&src_bucket_name).key(name)
        .version_id(&version_id)
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(aws_sdk_s3::types::Tag::builder().key("tag1").value("value1").build().unwrap())
                .tag_set(aws_sdk_s3::types::Tag::builder().key("tag2").value("value2").build().unwrap())
                .build().unwrap(),
        )
        .send().await.unwrap();

    client.delete_object_tagging().bucket(&src_bucket_name).key(name)
        .version_id(&version_id)
        .send().await.unwrap();

    client.put_object_legal_hold().bucket(&src_bucket_name).key(name)
        .legal_hold(aws_sdk_s3::types::ObjectLockLegalHold::builder()
            .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On).build())
        .send().await.unwrap();

    let retain_until = aws_smithy_types::DateTime::from_secs(
        (chrono::Utc::now() + chrono::Duration::days(60)).timestamp(),
    );
    client.put_object_retention().bucket(&src_bucket_name).key(name)
        .retention(aws_sdk_s3::types::ObjectLockRetention::builder()
            .mode(aws_sdk_s3::types::ObjectLockRetentionMode::Governance)
            .retain_until_date(retain_until)
            .build())
        .send().await.unwrap();

    flush_logs(&client, &src_bucket_name).await;
    let log_keys = get_keys(&client, &log_bucket).await;
    assert_eq!(log_keys.len(), 1);

    let body = get_body(&client, &log_bucket, &log_keys[0]).await;
    let mut op_list: Vec<String> = Vec::new();
    for line in body.lines() {
        if line.is_empty() { continue; }
        let record = parse_journal_log_record(line);
        op_list.push(record.operation.clone());
        if record.version_id != "-" {
            assert_eq!(record.version_id, version_id);
        }
    }

    let mut expected_ops = vec![
        "REST.PUT.OBJECT", "REST.PUT.ACL", "REST.PUT.LEGAL_HOLD",
        "REST.PUT.RETENTION", "REST.PUT.OBJECT_TAGGING", "REST.DELETE.OBJECT_TAGGING",
    ];
    expected_ops.sort();
    op_list.sort();
    assert_eq!(op_list, expected_ops);

    // allow cleanup
    client.put_object_legal_hold().bucket(&src_bucket_name).key(name)
        .legal_hold(aws_sdk_s3::types::ObjectLockLegalHold::builder()
            .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off).build())
        .send().await.unwrap();
    client.delete_object().bucket(&src_bucket_name).key(name)
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send().await.unwrap();
}

// ── permission change tests ──

async fn bucket_logging_permission_change(logging_type: &str) {
    let client = get_client();
    let cfg = get_config();
    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let prefix = "log/";

    let mut policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "S3ServerAccessLogsPolicy",
            "Effect": "Allow",
            "Principal": {"Service": "logging.s3.amazonaws.com"},
            "Action": ["s3:PutObject"],
            "Resource": format!("arn:aws:s3:::{log_bucket}/{prefix}"),
            "Condition": {
                "ArnLike": {"aws:SourceArn": format!("arn:aws:s3:::{src_bucket}")},
                "StringEquals": {"aws:SourceAccount": &cfg.main_user_id}
            }
        }]
    });
    client.put_bucket_policy().bucket(&log_bucket).policy(policy.to_string()).send().await.unwrap();

    if logging_type == "Journal" {
        put_bucket_logging_ext(
            &client, &src_bucket, &log_bucket, prefix,
            Some(TargetObjectKeyFormat::builder()
                .simple_prefix(SimplePrefix::builder().build())
                .build()),
            Some("Journal"), None, None,
        ).await;
    } else {
        put_bucket_logging_ext(
            &client, &src_bucket, &log_bucket, prefix,
            Some(TargetObjectKeyFormat::builder()
                .simple_prefix(SimplePrefix::builder().build())
                .build()),
            None, None, None,
        ).await;
    }

    // put objects with valid policy
    let num_keys = 5;
    for j in 0..num_keys {
        client.put_object().bucket(&src_bucket).key(&format!("myobject{j}"))
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
    }

    // change policy to invalid source bucket
    policy["Statement"][0]["Condition"]["ArnLike"]["aws:SourceArn"] = serde_json::json!("arn:aws:s3:::kaboom");
    client.put_bucket_policy().bucket(&log_bucket).policy(policy.to_string()).send().await.unwrap();

    // put objects with invalid policy
    for j in num_keys..(num_keys * 2) {
        let name = format!("myobject{j}");
        if logging_type == "Standard" {
            client.put_object().bucket(&src_bucket).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
        } else {
            let result = client.put_object().bucket(&src_bucket).key(&name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await;
            assert_s3_err!(result, 403, "AccessDenied");
        }
    }

    // flush should fail with AccessDenied
    let resp = post_bucket_logging(&src_bucket).await;
    assert_eq!(resp.status, 403);

    let log_keys = get_keys(&client, &log_bucket).await;
    assert_eq!(log_keys.len(), 0);
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_permission_change_s() {
    let _guard = TestGuard::setup();
    bucket_logging_permission_change("Standard").await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_bucket_logging_permission_change_j() {
    let _guard = TestGuard::setup();
    if !has_bucket_logging_extension().await { return; }
    bucket_logging_permission_change("Journal").await;
}

// ── policy wildcard with objects test ──

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_put_bucket_logging_policy_wildcard_objects() {
    let _guard = TestGuard::setup();
    let client = get_client();
    let alt_client = get_alt_client();
    let cfg = get_config();

    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&client)).await;
    let alt_src_bucket = get_new_bucket(Some(&alt_client)).await;
    let prefix = "log/";

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ServerAccessLogsPolicy",
                "Effect": "Allow",
                "Principal": {"Service": "logging.s3.amazonaws.com"},
                "Action": ["s3:PutObject"],
                "Resource": format!("arn:aws:s3:::{log_bucket}/{prefix}"),
                "Condition": {
                    "ArnLike": {"aws:SourceArn": format!("arn:aws:s3:::{src_bucket}")},
                    "StringLike": {"aws:SourceAccount": &cfg.main_user_id}
                }
            },
            {
                "Sid": "S3ServerAccessLogsPolicy",
                "Effect": "Allow",
                "Principal": {"Service": "logging.s3.amazonaws.com"},
                "Action": ["s3:PutObject"],
                "Resource": format!("arn:aws:s3:::{log_bucket}/{prefix}"),
                "Condition": {
                    "ArnLike": {"aws:SourceArn": format!("arn:aws:s3:::{alt_src_bucket}")},
                    "StringLike": {"aws:SourceAccount": &cfg.alt_user_id}
                }
            }
        ]
    });

    client.put_bucket_policy().bucket(&log_bucket).policy(policy.to_string()).send().await.unwrap();

    // both buckets can enable logging
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        None, None, None,
    ).await;
    put_bucket_logging_ext(
        &alt_client, &alt_src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        None, None, None,
    ).await;

    // use wildcards
    let wildcard_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "S3ServerAccessLogsPolicy",
            "Effect": "Allow",
            "Principal": {"Service": "logging.s3.amazonaws.com"},
            "Action": ["s3:PutObject"],
            "Resource": format!("arn:aws:s3:::{log_bucket}/{prefix}"),
            "Condition": {
                "ArnLike": {"aws:SourceArn": "arn:aws:s3:::*"},
                "StringLike": {"aws:SourceAccount": "*"}
            }
        }]
    });
    client.put_bucket_policy().bucket(&log_bucket).policy(wildcard_policy.to_string()).send().await.unwrap();

    // both should still work with wildcards
    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        None, None, None,
    ).await;
    put_bucket_logging_ext(
        &alt_client, &alt_src_bucket, &log_bucket, prefix,
        Some(TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build()),
        None, None, None,
    ).await;

    // put objects and verify logging
    let num_keys = 5;
    for j in 0..num_keys {
        client.put_object().bucket(&src_bucket).key(&format!("myobject_1_{j}"))
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
        alt_client.put_object().bucket(&alt_src_bucket).key(&format!("myobject_2_{j}"))
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert!(!keys.is_empty());

    let mut all_keys: Vec<String> = Vec::new();
    for j in 0..num_keys {
        all_keys.push(format!("myobject_1_{j}"));
        all_keys.push(format!("myobject_2_{j}"));
    }
    let all_refs: Vec<&str> = all_keys.iter().map(|s| s.as_str()).collect();
    let mut body = String::new();
    for key in &keys {
        body.push_str(&get_body(&client, &log_bucket, key).await);
    }
    assert!(verify_records(&body, " ", "REST.PUT.OBJECT", &all_refs, "Standard", num_keys * 2, false));
}

// ── cross-tenant logging tests ──

async fn set_log_bucket_policy_tenant(
    client: &aws_sdk_s3::Client,
    log_tenant: &str,
    log_bucket: &str,
    src_tenant: &str,
    src_user: &str,
    src_buckets: &[&str],
    log_prefixes: &[&str],
) {
    let policy = log_bucket_policy_json(log_tenant, log_bucket, src_tenant, src_user, src_buckets, log_prefixes);
    client.put_bucket_policy().bucket(log_bucket).policy(policy).send().await.unwrap();
}

async fn bucket_logging_objects(
    src_client: &aws_sdk_s3::Client,
    src_bucket: &str,
    log_client: &aws_sdk_s3::Client,
    log_bucket: &str,
    log_type: &str,
    creds: Option<(&str, &str)>,
) {
    let num_keys = 5;
    for j in 0..num_keys {
        src_client.put_object().bucket(src_bucket).key(&format!("myobject{j}"))
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
    }

    let src_keys_list: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();
    let src_keys: Vec<&str> = src_keys_list.iter().map(|s| s.as_str()).collect();

    flush_logs_creds(src_client, src_bucket, "dummy", creds).await;

    let keys = get_keys(log_client, log_bucket).await;
    assert!(!keys.is_empty(), "expected log objects in {log_bucket}");

    let body = get_body(log_client, log_bucket, keys.last().unwrap()).await;
    assert!(verify_records(&body, src_bucket, "REST.PUT.OBJECT", &src_keys, log_type, num_keys, false));
}

async fn put_bucket_logging_tenant(log_type: &str) {
    let client = get_client();
    let tenant_client = get_tenant_client();
    let cfg = get_config();
    let prefix = "log/";

    // case 1: src on default tenant, log on different tenant
    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&tenant_client)).await;
    set_log_bucket_policy_tenant(
        &tenant_client, &cfg.tenant_name, &log_bucket,
        "", &cfg.main_user_id, &[&src_bucket], &[prefix],
    ).await;
    let target = format!("{}:{}", cfg.tenant_name, log_bucket);
    if log_type == "Journal" {
        put_bucket_logging_ext(&client, &src_bucket, &target, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            Some("Journal"), None, None).await;
    } else {
        put_bucket_logging_ext(&client, &src_bucket, &target, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            None, None, None).await;
    }
    bucket_logging_objects(&client, &src_bucket, &tenant_client, &log_bucket, log_type, None).await;

    // case 2: src on default, log on tenant with same bucket name
    let src_bucket2 = get_new_bucket(Some(&client)).await;
    let log_bucket2 = get_new_bucket(Some(&tenant_client)).await;
    set_log_bucket_policy_tenant(
        &tenant_client, &cfg.tenant_name, &log_bucket2,
        "", &cfg.main_user_id, &[&src_bucket2], &[prefix],
    ).await;
    let target2 = format!("{}:{}", cfg.tenant_name, log_bucket2);
    if log_type == "Journal" {
        put_bucket_logging_ext(&client, &src_bucket2, &target2, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            Some("Journal"), None, None).await;
    } else {
        put_bucket_logging_ext(&client, &src_bucket2, &target2, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            None, None, None).await;
    }
    bucket_logging_objects(&client, &src_bucket2, &tenant_client, &log_bucket2, log_type, None).await;

    // case 3: missing tenant prefix → should fail
    let src_bucket3 = get_new_bucket(Some(&client)).await;
    let log_bucket3 = get_new_bucket(Some(&tenant_client)).await;
    set_log_bucket_policy_tenant(
        &tenant_client, &cfg.tenant_name, &log_bucket3,
        "", &cfg.main_user_id, &[&src_bucket3], &[prefix],
    ).await;
    let result = client
        .put_bucket_logging()
        .bucket(&src_bucket3)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(LoggingEnabled::builder()
                    .target_bucket(&log_bucket3)
                    .target_prefix(prefix)
                    .build().unwrap())
                .build(),
        )
        .send().await;
    assert_s3_err!(result, 404, "NoSuchKey");

    // case 4: src and log on same tenant, with tenant prefix
    let src_bucket4 = get_new_bucket(Some(&tenant_client)).await;
    let log_bucket4 = get_new_bucket(Some(&tenant_client)).await;
    set_log_bucket_policy_tenant(
        &tenant_client, &cfg.tenant_name, &log_bucket4,
        &cfg.tenant_name, &cfg.tenant_user_id, &[&src_bucket4], &[prefix],
    ).await;
    let target4 = format!("{}:{}", cfg.tenant_name, log_bucket4);
    if log_type == "Journal" {
        put_bucket_logging_ext(&tenant_client, &src_bucket4, &target4, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            Some("Journal"), None, None).await;
    } else {
        put_bucket_logging_ext(&tenant_client, &src_bucket4, &target4, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            None, None, None).await;
    }
    let tenant_creds = Some((cfg.tenant_access_key.as_str(), cfg.tenant_secret_key.as_str()));
    bucket_logging_objects(&tenant_client, &src_bucket4, &tenant_client, &log_bucket4, log_type, tenant_creds).await;

    // case 5: src and log on same tenant, without tenant prefix
    let src_bucket5 = get_new_bucket(Some(&tenant_client)).await;
    let log_bucket5 = get_new_bucket(Some(&tenant_client)).await;
    set_log_bucket_policy_tenant(
        &tenant_client, &cfg.tenant_name, &log_bucket5,
        &cfg.tenant_name, &cfg.tenant_user_id, &[&src_bucket5], &[prefix],
    ).await;
    if log_type == "Journal" {
        put_bucket_logging_ext(&tenant_client, &src_bucket5, &log_bucket5, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            Some("Journal"), None, None).await;
    } else {
        put_bucket_logging_ext(&tenant_client, &src_bucket5, &log_bucket5, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            None, None, None).await;
    }
    bucket_logging_objects(&tenant_client, &src_bucket5, &tenant_client, &log_bucket5, log_type, tenant_creds).await;

    // case 6: src on tenant, log on default with explicit default tenant
    let src_bucket6 = get_new_bucket(Some(&tenant_client)).await;
    let log_bucket6 = get_new_bucket(Some(&client)).await;
    set_log_bucket_policy_tenant(
        &client, "", &log_bucket6,
        &cfg.tenant_name, &cfg.tenant_user_id, &[&src_bucket6], &[prefix],
    ).await;
    let target6 = format!(":{log_bucket6}");
    if log_type == "Journal" {
        put_bucket_logging_ext(&tenant_client, &src_bucket6, &target6, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            Some("Journal"), None, None).await;
    } else {
        put_bucket_logging_ext(&tenant_client, &src_bucket6, &target6, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            None, None, None).await;
    }
    bucket_logging_objects(&tenant_client, &src_bucket6, &client, &log_bucket6, log_type, tenant_creds).await;

    // case 7: src on tenant, log on default WITHOUT tenant prefix → should fail
    let src_bucket7 = get_new_bucket(Some(&tenant_client)).await;
    let log_bucket7 = get_new_bucket(Some(&client)).await;
    set_log_bucket_policy_tenant(
        &client, "", &log_bucket7,
        &cfg.tenant_name, &cfg.tenant_user_id, &[&src_bucket7], &[prefix],
    ).await;
    let result = tenant_client
        .put_bucket_logging()
        .bucket(&src_bucket7)
        .bucket_logging_status(
            BucketLoggingStatus::builder()
                .logging_enabled(LoggingEnabled::builder()
                    .target_bucket(&log_bucket7)
                    .target_prefix(prefix)
                    .build().unwrap())
                .build(),
        )
        .send().await;
    assert_s3_err!(result, 404, "NoSuchKey");
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_put_bucket_logging_tenant_s() {
    let _guard = TestGuard::setup();
    put_bucket_logging_tenant("Standard").await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_put_bucket_logging_tenant_j() {
    let _guard = TestGuard::setup();
    if !has_bucket_logging_extension().await { return; }
    put_bucket_logging_tenant("Journal").await;
}

// ── cross-account logging tests ──

async fn put_bucket_logging_account(log_type: &str) {
    let client = get_client();
    let iam_s3 = get_iam_root_s3client();
    let cfg = get_config();
    let prefix = "log/";

    // case 1: src is default user, log is IAM account
    let src_bucket = get_new_bucket(Some(&client)).await;
    let log_bucket = get_new_bucket(Some(&iam_s3)).await;
    set_log_bucket_policy_tenant(
        &iam_s3, "", &log_bucket, "", &cfg.main_user_id, &[&src_bucket], &[prefix],
    ).await;
    if log_type == "Journal" {
        put_bucket_logging_ext(&client, &src_bucket, &log_bucket, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            Some("Journal"), None, None).await;
    } else {
        put_bucket_logging_ext(&client, &src_bucket, &log_bucket, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            None, None, None).await;
    }
    bucket_logging_objects(&client, &src_bucket, &iam_s3, &log_bucket, log_type, None).await;

    // case 2: src and log both in IAM account
    let iam_client = get_iam_root_client();
    let iam_user_resp = iam_client.get_user().send().await.unwrap();
    let iam_arn = iam_user_resp.user().unwrap().arn();
    let iam_user = iam_arn.split(':').nth(4).unwrap_or("unknown");

    let src_bucket2 = get_new_bucket(Some(&iam_s3)).await;
    let log_bucket2 = get_new_bucket(Some(&iam_s3)).await;
    set_log_bucket_policy_tenant(
        &iam_s3, "", &log_bucket2, "", iam_user, &[&src_bucket2], &[prefix],
    ).await;
    if log_type == "Journal" {
        put_bucket_logging_ext(&iam_s3, &src_bucket2, &log_bucket2, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            Some("Journal"), None, None).await;
    } else {
        put_bucket_logging_ext(&iam_s3, &src_bucket2, &log_bucket2, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            None, None, None).await;
    }
    let iam_creds = Some((cfg.iam_root_access_key.as_str(), cfg.iam_root_secret_key.as_str()));
    bucket_logging_objects(&iam_s3, &src_bucket2, &iam_s3, &log_bucket2, log_type, iam_creds).await;

    // case 3: src in IAM account, log is default user
    let src_bucket3 = get_new_bucket(Some(&iam_s3)).await;
    let log_bucket3 = get_new_bucket(Some(&client)).await;
    set_log_bucket_policy_tenant(
        &client, "", &log_bucket3, "", iam_user, &[&src_bucket3], &[prefix],
    ).await;
    if log_type == "Journal" {
        put_bucket_logging_ext(&iam_s3, &src_bucket3, &log_bucket3, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            Some("Journal"), None, None).await;
    } else {
        put_bucket_logging_ext(&iam_s3, &src_bucket3, &log_bucket3, prefix,
            Some(TargetObjectKeyFormat::builder().simple_prefix(SimplePrefix::builder().build()).build()),
            None, None, None).await;
    }
    bucket_logging_objects(&iam_s3, &src_bucket3, &client, &log_bucket3, log_type, iam_creds).await;
}

// ══════════════════════════════════════════════════════════════════
// Cleanup tests
// ══════════════════════════════════════════════════════════════════

async fn bucket_logging_cleanup(
    cleanup_type: &str,
    logging_type: &str,
    single_prefix: bool,
    concurrency: bool,
) {
    if !has_bucket_logging_extension().await { return; }
    let client = get_client();
    let log_bucket = get_new_bucket(Some(&client)).await;

    let num_buckets = 5usize;
    let mut buckets = Vec::new();
    let mut log_prefixes = Vec::new();
    let longer_time = EXPECTED_OBJECT_ROLL_TIME * 10;

    for _ in 0..num_buckets {
        let src = get_new_bucket(Some(&client)).await;
        if single_prefix {
            log_prefixes.push("log/".to_string());
        } else {
            log_prefixes.push(format!("{src}/"));
        }
        buckets.push(src);
    }

    let prefix_refs: Vec<&str> = log_prefixes.iter().map(|s| s.as_str()).collect();
    let bucket_refs: Vec<&str> = buckets.iter().map(|s| s.as_str()).collect();
    set_log_bucket_policy(&client, &log_bucket, &bucket_refs, &prefix_refs).await;

    for j in 0..num_buckets {
        put_bucket_logging_ext(
            &client, &buckets[j], &log_bucket, &log_prefixes[j],
            None, Some(logging_type), Some(longer_time), None,
        ).await;
    }

    let num_keys = 10usize;
    let src_names: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();

    for src_bucket in &buckets {
        for name in &src_names {
            client.put_object().bucket(src_bucket).key(name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
            client.delete_object().bucket(src_bucket).key(name).send().await.unwrap();
        }
    }

    let keys = get_keys(&client, &log_bucket).await;
    assert_eq!(keys.len(), 0);

    let updated_longer_time = EXPECTED_OBJECT_ROLL_TIME * 20;
    let mut flushed_obj: Option<String> = None;

    if cleanup_type == "target" {
        // handled after the match block
    } else if concurrency {
        let mut set = tokio::task::JoinSet::new();
        for (idx, src_bucket) in buckets.iter().enumerate() {
            let c = client.clone();
            let b = src_bucket.clone();
            let ct = cleanup_type.to_string();
            let lb = log_bucket.clone();
            let lt = logging_type.to_string();
            let lp = if single_prefix { "log/".to_string() } else { log_prefixes[idx].clone() };
            let ult = updated_longer_time;
            set.spawn(async move {
                match ct.as_str() {
                    "deletion" => { c.delete_bucket().bucket(&b).send().await.ok(); }
                    "disabling" => { c.put_bucket_logging().bucket(&b)
                        .bucket_logging_status(BucketLoggingStatus::builder().build())
                        .send().await.ok(); }
                    "updating" => {
                        put_bucket_logging_ext(
                            &c, &b, &lb, &lp, None, Some(&lt), Some(ult), None,
                        ).await;
                    }
                    _ => panic!("invalid cleanup type: {ct}"),
                }
            });
        }
        while let Some(r) = set.join_next().await { r.unwrap(); }
    } else {
        let mut first_time = true;
        for src_bucket in &buckets {
            match cleanup_type {
                "deletion" => {
                    client.delete_bucket().bucket(src_bucket).send().await.unwrap();
                }
                "disabling" => {
                    let capture = ResponseHeaderCapture::new();
                    client.put_bucket_logging().bucket(src_bucket)
                        .bucket_logging_status(BucketLoggingStatus::builder().build())
                        .customize().interceptor(capture.clone())
                        .send().await.unwrap();
                    if first_time {
                        let body = capture.body();
                        flushed_obj = xml_text(&body, "FlushedLoggingObject");
                        if single_prefix { first_time = false; }
                    }
                }
                "updating" => {
                    let prefix = if single_prefix { "log/" } else { &log_prefixes[buckets.iter().position(|b| b == src_bucket).unwrap()] };
                    let resp_body = put_bucket_logging_ext(
                        &client, src_bucket, &log_bucket, prefix,
                        None, Some(logging_type), Some(updated_longer_time), None,
                    ).await;
                    flushed_obj = xml_text(&resp_body, "FlushedLoggingObject");
                }
                "notupdating" => {
                    let prefix = if single_prefix { "log/" } else { &log_prefixes[buckets.iter().position(|b| b == src_bucket).unwrap()] };
                    put_bucket_logging_ext(
                        &client, src_bucket, &log_bucket, prefix,
                        None, Some(logging_type), Some(longer_time), None,
                    ).await;
                }
                _ => panic!("invalid cleanup type: {cleanup_type}"),
            }
        }
    }

    if cleanup_type == "target" {
        client.delete_bucket().bucket(&log_bucket).send().await.unwrap();
        client.create_bucket().bucket(&log_bucket).send().await.unwrap();
        s3_tests_rs::fixtures::register_bucket_for_cleanup(&client, &log_bucket);
        set_log_bucket_policy(&client, &log_bucket, &bucket_refs, &prefix_refs).await;

        let new_names: Vec<String> = (0..num_keys).map(|j| format!("after_deletion_myobject{j}")).collect();
        for src_bucket in &buckets {
            for name in &new_names {
                client.put_object().bucket(src_bucket).key(name)
                    .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                    .send().await.unwrap();
                client.delete_object().bucket(src_bucket).key(name).send().await.unwrap();
            }
        }
        let keys = get_keys(&client, &log_bucket).await;
        assert_eq!(keys.len(), 0);
        for src_bucket in &buckets {
            post_bucket_logging(src_bucket).await;
        }
    }

    let keys = get_keys(&client, &log_bucket).await;

    if let Some(ref fobj) = flushed_obj {
        assert!(keys.contains(fobj), "flushed object {fobj} not in keys");
    }

    if cleanup_type == "notupdating" {
        assert_eq!(keys.len(), 0, "no cleanup expected for notupdating");
        return;
    }

    if concurrency {
        assert!(keys.len() >= 1 && keys.len() <= num_buckets,
            "concurrent: expected 1..{num_buckets} keys, got {}", keys.len());
    } else if single_prefix && logging_type == "Journal" && cleanup_type != "target" && cleanup_type != "updating" {
        assert_eq!(keys.len(), 1);
    } else {
        assert_eq!(keys.len(), num_buckets);
    }

    let src_name_refs: Vec<&str> = src_names.iter().map(|s| s.as_str()).collect();
    let target_names: Vec<String> = if cleanup_type == "target" {
        (0..num_keys).map(|j| format!("after_deletion_myobject{j}")).collect()
    } else {
        src_names.clone()
    };
    let check_names: Vec<&str> = target_names.iter().map(|s| s.as_str()).collect();

    let mut body = String::new();
    for key in &keys {
        body.push_str(&get_body(&client, &log_bucket, key).await);
        let mut found = false;
        for pfx in &log_prefixes {
            if key.starts_with(pfx.as_str()) { found = true; }
        }
        assert!(found, "log key {key} does not match any expected prefix");
    }

    for src_bucket in &buckets {
        assert!(verify_records(&body, src_bucket, "REST.PUT.OBJECT", &check_names, logging_type, num_keys, true));
        assert!(verify_records(&body, src_bucket, "REST.DELETE.OBJECT", &check_names, logging_type, num_keys, true));
    }
}

// 44 cleanup test functions — parameterized over cleanup_type × logging_type × single_prefix × concurrency

macro_rules! cleanup_test {
    ($name:ident, $ct:expr, $lt:expr, $sp:expr, $cc:expr) => {
        #[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
        #[tokio::test]
        async fn $name() {
            let _guard = TestGuard::setup();
            bucket_logging_cleanup($ct, $lt, $sp, $cc).await;
        }
    };
}

cleanup_test!(test_bucket_logging_cleanup_bucket_deletion_j, "deletion", "Journal", false, false);
cleanup_test!(test_bucket_logging_cleanup_bucket_deletion_j_single, "deletion", "Journal", true, false);
cleanup_test!(test_bucket_logging_cleanup_disabling_j, "disabling", "Journal", false, false);
cleanup_test!(test_bucket_logging_cleanup_disabling_j_single, "disabling", "Journal", true, false);
cleanup_test!(test_bucket_logging_cleanup_updating_j, "updating", "Journal", false, false);
cleanup_test!(test_bucket_logging_cleanup_updating_j_single, "updating", "Journal", true, false);
cleanup_test!(test_bucket_logging_notupdating_j, "notupdating", "Journal", false, false);
cleanup_test!(test_bucket_logging_notupdating_j_single, "notupdating", "Journal", true, false);
cleanup_test!(test_bucket_logging_cleanup_bucket_deletion_s, "deletion", "Standard", false, false);
cleanup_test!(test_bucket_logging_cleanup_bucket_deletion_s_single, "deletion", "Standard", true, false);
cleanup_test!(test_bucket_logging_cleanup_disabling_s, "disabling", "Standard", false, false);
cleanup_test!(test_bucket_logging_cleanup_disabling_s_single, "disabling", "Standard", true, false);
cleanup_test!(test_bucket_logging_cleanup_updating_s, "updating", "Standard", false, false);
cleanup_test!(test_bucket_logging_cleanup_updating_s_single, "updating", "Standard", true, false);
cleanup_test!(test_bucket_logging_notupdating_s, "notupdating", "Standard", false, false);
cleanup_test!(test_bucket_logging_notupdating_s_single, "notupdating", "Standard", true, false);
cleanup_test!(test_bucket_logging_target_cleanup_j, "target", "Journal", false, false);
cleanup_test!(test_bucket_logging_target_cleanup_j_single, "target", "Journal", true, false);
cleanup_test!(test_bucket_logging_target_cleanup_s, "target", "Standard", false, false);
cleanup_test!(test_bucket_logging_target_cleanup_s_single, "target", "Standard", true, false);
cleanup_test!(test_bucket_logging_cleanup_bucket_concurrent_deletion_j, "deletion", "Journal", false, true);
cleanup_test!(test_bucket_logging_cleanup_bucket_concurrent_deletion_j_single, "deletion", "Journal", true, true);
cleanup_test!(test_bucket_logging_cleanup_concurrent_disabling_j, "disabling", "Journal", false, true);
cleanup_test!(test_bucket_logging_cleanup_concurrent_disabling_j_single, "disabling", "Journal", true, true);
cleanup_test!(test_bucket_logging_cleanup_concurrent_updating_j, "updating", "Journal", false, true);
cleanup_test!(test_bucket_logging_cleanup_concurrent_updating_j_single, "updating", "Journal", true, true);
cleanup_test!(test_bucket_logging_cleanup_bucket_concurrent_deletion_s, "deletion", "Standard", false, true);
cleanup_test!(test_bucket_logging_cleanup_bucket_concurrent_deletion_s_single, "deletion", "Standard", true, true);
cleanup_test!(test_bucket_logging_cleanup_concurrent_disabling_s, "disabling", "Standard", false, true);
cleanup_test!(test_bucket_logging_cleanup_concurrent_disabling_s_single, "disabling", "Standard", true, true);
cleanup_test!(test_bucket_logging_cleanup_concurrent_updating_s, "updating", "Standard", false, true);
cleanup_test!(test_bucket_logging_cleanup_concurrent_updating_s_single, "updating", "Standard", true, true);

// ── partial cleanup tests (12) ──

async fn bucket_logging_partial_cleanup(
    cleanup_type: &str,
    logging_type: &str,
    concurrency: bool,
) {
    if !has_bucket_logging_extension().await { return; }
    let client = get_client();
    let log_bucket = get_new_bucket(Some(&client)).await;

    let num_buckets = 3usize;
    let mut buckets = Vec::new();
    let log_prefixes = vec!["log/".to_string(); num_buckets];
    let longer_time = EXPECTED_OBJECT_ROLL_TIME * 10;

    for _ in 0..num_buckets {
        let src = get_new_bucket(Some(&client)).await;
        buckets.push(src);
    }

    let prefix_refs: Vec<&str> = log_prefixes.iter().map(|s| s.as_str()).collect();
    let bucket_refs: Vec<&str> = buckets.iter().map(|s| s.as_str()).collect();
    set_log_bucket_policy(&client, &log_bucket, &bucket_refs, &prefix_refs).await;

    for j in 0..num_buckets {
        put_bucket_logging_ext(
            &client, &buckets[j], &log_bucket, "log/",
            None, Some(logging_type), Some(longer_time), None,
        ).await;
    }

    let num_keys = 3usize;
    let src_names: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();

    for src_bucket in &buckets {
        for name in &src_names {
            client.put_object().bucket(src_bucket).key(name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await.unwrap();
            client.delete_object().bucket(src_bucket).key(name).send().await.unwrap();
        }
    }

    assert_eq!(get_keys(&client, &log_bucket).await.len(), 0);

    let updated_longer_time = EXPECTED_OBJECT_ROLL_TIME * 20;
    let mut flushed_obj: Option<String> = None;

    if concurrency {
        let mut set = tokio::task::JoinSet::new();
        for j in 0..(num_buckets - 1) {
            let c = client.clone();
            let b = buckets[j].clone();
            let ct = cleanup_type.to_string();
            let lb = log_bucket.clone();
            let lt = logging_type.to_string();
            let ult = updated_longer_time;
            set.spawn(async move {
                match ct.as_str() {
                    "deletion" => { c.delete_bucket().bucket(&b).send().await.ok(); }
                    "disabling" => { c.put_bucket_logging().bucket(&b)
                        .bucket_logging_status(BucketLoggingStatus::builder().build())
                        .send().await.ok(); }
                    "updating" => {
                        put_bucket_logging_ext(
                            &c, &b, &lb, "log/", None, Some(&lt), Some(ult), None,
                        ).await;
                    }
                    _ => panic!("invalid cleanup type: {ct}"),
                }
            });
        }
        while let Some(r) = set.join_next().await { r.unwrap(); }
    } else {
        let mut first_time = true;
        for j in 0..(num_buckets - 1) {
            match cleanup_type {
                "deletion" => {
                    client.delete_bucket().bucket(&buckets[j]).send().await.unwrap();
                }
                "disabling" => {
                    let capture = ResponseHeaderCapture::new();
                    client.put_bucket_logging().bucket(&buckets[j])
                        .bucket_logging_status(BucketLoggingStatus::builder().build())
                        .customize().interceptor(capture.clone())
                        .send().await.unwrap();
                    if first_time {
                        flushed_obj = xml_text(&capture.body(), "FlushedLoggingObject");
                        first_time = false;
                    }
                }
                "updating" => {
                    let resp_body = put_bucket_logging_ext(
                        &client, &buckets[j], &log_bucket, "log/",
                        None, Some(logging_type), Some(updated_longer_time), None,
                    ).await;
                    flushed_obj = xml_text(&resp_body, "FlushedLoggingObject");
                }
                _ => panic!("invalid cleanup type: {cleanup_type}"),
            }
        }
    }

    // put more objects in the last bucket
    let last_bucket = &buckets[num_buckets - 1];
    for name in &src_names {
        client.put_object().bucket(last_bucket).key(name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
        client.delete_object().bucket(last_bucket).key(name).send().await.unwrap();
    }

    flush_logs(&client, last_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;

    if let Some(ref fobj) = flushed_obj {
        assert!(keys.contains(fobj));
    }

    if concurrency {
        assert!(keys.len() >= 1 && keys.len() <= num_buckets);
    } else if cleanup_type == "updating" {
        assert_eq!(keys.len(), num_buckets);
    } else if logging_type == "Standard" {
        assert_eq!(keys.len(), num_buckets);
    } else {
        assert_eq!(keys.len(), num_buckets - 1);
    }

    let src_name_refs: Vec<&str> = src_names.iter().map(|s| s.as_str()).collect();
    let mut body = String::new();
    for key in &keys {
        body.push_str(&get_body(&client, &log_bucket, key).await);
        assert!(key.starts_with("log/"), "key {key} doesn't start with log/");
    }

    for src_bucket in &buckets {
        verify_records(&body, src_bucket, "REST.PUT.OBJECT", &src_name_refs, logging_type, num_keys, true);
        verify_records(&body, src_bucket, "REST.DELETE.OBJECT", &src_name_refs, logging_type, num_keys, true);
    }
}

macro_rules! partial_cleanup_test {
    ($name:ident, $ct:expr, $lt:expr, $cc:expr) => {
        #[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
        #[tokio::test]
        async fn $name() {
            let _guard = TestGuard::setup();
            bucket_logging_partial_cleanup($ct, $lt, $cc).await;
        }
    };
}

partial_cleanup_test!(test_bucket_logging_part_cleanup_concurrent_updating_s, "updating", "Standard", true);
partial_cleanup_test!(test_bucket_logging_part_cleanup_concurrent_disabling_s, "disabling", "Standard", true);
partial_cleanup_test!(test_bucket_logging_part_cleanup_concurrent_deletion_s, "deletion", "Standard", true);
partial_cleanup_test!(test_bucket_logging_part_cleanup_concurrent_updating_j, "updating", "Journal", true);
partial_cleanup_test!(test_bucket_logging_part_cleanup_concurrent_disabling_j, "disabling", "Journal", true);
partial_cleanup_test!(test_bucket_logging_part_cleanup_concurrent_deletion_j, "deletion", "Journal", true);
partial_cleanup_test!(test_bucket_logging_part_cleanup_updating_s, "updating", "Standard", false);
partial_cleanup_test!(test_bucket_logging_part_cleanup_disabling_s, "disabling", "Standard", false);
partial_cleanup_test!(test_bucket_logging_part_cleanup_deletion_s, "deletion", "Standard", false);
partial_cleanup_test!(test_bucket_logging_part_cleanup_updating_j, "updating", "Journal", false);
partial_cleanup_test!(test_bucket_logging_part_cleanup_disabling_j, "disabling", "Journal", false);
partial_cleanup_test!(test_bucket_logging_part_cleanup_deletion_j, "deletion", "Journal", false);

// ── config update tests (8) ──

async fn bucket_logging_conf_update(
    logging_type: &str,
    update_value: &str,
    concurrency: bool,
) {
    if !has_bucket_logging_extension().await { return; }
    let client = get_client();
    let log_bucket = get_new_bucket(Some(&client)).await;
    let src_bucket = get_new_bucket(Some(&client)).await;
    let mut prefix = "log/".to_string();
    let longer_time = EXPECTED_OBJECT_ROLL_TIME * 10;

    set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[&prefix]).await;

    put_bucket_logging_ext(
        &client, &src_bucket, &log_bucket, &prefix,
        None, Some(logging_type), Some(longer_time), None,
    ).await;

    // conf update without any log records
    if update_value == "roll_time" {
        put_bucket_logging_ext(
            &client, &src_bucket, &log_bucket, &prefix,
            None, Some(logging_type), Some(longer_time * 2), None,
        ).await;
    } else if update_value == "prefix" {
        prefix = "newlog1/".to_string();
        set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[&prefix]).await;
        put_bucket_logging_ext(
            &client, &src_bucket, &log_bucket, &prefix,
            None, Some(logging_type), Some(longer_time), None,
        ).await;
    }

    let num_keys = 100usize;
    let src_names: Vec<String> = (0..num_keys).map(|j| format!("myobject{j}")).collect();

    if concurrency {
        let mut set = tokio::task::JoinSet::new();
        for name in &src_names {
            let c = client.clone();
            let b = src_bucket.clone();
            let n = name.clone();
            set.spawn(async move {
                let _ = c.put_object().bucket(&b).key(&n)
                    .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                    .send().await;
            });
        }

        // update config mid-flight
        if update_value == "roll_time" {
            put_bucket_logging_ext(
                &client, &src_bucket, &log_bucket, &prefix,
                None, Some(logging_type), Some(longer_time * 3), None,
            ).await;
        } else {
            prefix = "newlog2/".to_string();
            set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[&prefix]).await;
            put_bucket_logging_ext(
                &client, &src_bucket, &log_bucket, &prefix,
                None, Some(logging_type), Some(longer_time), None,
            ).await;
        }

        while let Some(r) = set.join_next().await { r.unwrap(); }
        flush_logs(&client, &src_bucket).await;
    } else {
        for name in &src_names {
            let _ = client.put_object().bucket(&src_bucket).key(name)
                .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
                .send().await;
        }

        if update_value == "roll_time" {
            put_bucket_logging_ext(
                &client, &src_bucket, &log_bucket, &prefix,
                None, Some(logging_type), Some(longer_time * 3), None,
            ).await;
        } else {
            prefix = "newlog2/".to_string();
            set_log_bucket_policy(&client, &log_bucket, &[&src_bucket], &[&prefix]).await;
            put_bucket_logging_ext(
                &client, &src_bucket, &log_bucket, &prefix,
                None, Some(logging_type), Some(longer_time), None,
            ).await;
        }
    }

    let keys = get_keys(&client, &log_bucket).await;
    let expected_log_objs = if concurrency { 3 } else { 2 };
    assert!(keys.len() >= expected_log_objs - 1 && keys.len() <= expected_log_objs + 1,
        "expected ~{expected_log_objs} log objects, got {}", keys.len());

    let mut body = String::new();
    for key in &keys {
        body.push_str(&get_body(&client, &log_bucket, key).await);
        client.delete_object().bucket(&log_bucket).key(key).send().await.unwrap();
    }

    let src_refs: Vec<&str> = src_names.iter().map(|s| s.as_str()).collect();
    let ok = verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &src_refs, logging_type, num_keys, true);
    if !(concurrency && update_value == "prefix" && logging_type == "Standard") {
        assert!(ok, "expected {num_keys} PUT records");
    }

    // verify logging still works after conf change
    let after_names: Vec<String> = (0..num_keys).map(|j| format!("anotherobject{j}")).collect();
    for name in &after_names {
        client.put_object().bucket(&src_bucket).key(name)
            .body(aws_sdk_s3::primitives::ByteStream::from(randcontent()))
            .send().await.unwrap();
    }

    flush_logs(&client, &src_bucket).await;
    let keys = get_keys(&client, &log_bucket).await;
    assert_eq!(keys.len(), 1);

    let body = get_body(&client, &log_bucket, &keys[0]).await;
    let after_refs: Vec<&str> = after_names.iter().map(|s| s.as_str()).collect();
    assert!(verify_records(&body, &src_bucket, "REST.PUT.OBJECT", &after_refs, logging_type, num_keys, true));
}

macro_rules! conf_update_test {
    ($name:ident, $lt:expr, $uv:expr, $cc:expr) => {
        #[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
        #[tokio::test]
        async fn $name() {
            let _guard = TestGuard::setup();
            bucket_logging_conf_update($lt, $uv, $cc).await;
        }
    };
}

conf_update_test!(test_bucket_logging_conf_updating_roll_s, "Standard", "roll_time", false);
conf_update_test!(test_bucket_logging_conf_updating_roll_j, "Journal", "roll_time", false);
conf_update_test!(test_bucket_logging_conf_updating_pfx_s, "Standard", "prefix", false);
conf_update_test!(test_bucket_logging_conf_updating_pfx_j, "Journal", "prefix", false);
conf_update_test!(test_bucket_logging_conf_concurrent_updating_roll_s, "Standard", "roll_time", true);
conf_update_test!(test_bucket_logging_conf_concurrent_updating_roll_j, "Journal", "roll_time", true);
conf_update_test!(test_bucket_logging_conf_concurrent_updating_pfx_s, "Standard", "prefix", true);
conf_update_test!(test_bucket_logging_conf_concurrent_updating_pfx_j, "Journal", "prefix", true);

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_put_bucket_logging_account_s() {
    let _guard = TestGuard::setup();
    put_bucket_logging_account("Standard").await;
}

#[cfg(not(feature = "fails_on_aws"))]
#[cfg_attr(feature = "fails_on_posix", ignore = "posix: bucket logging not implemented")]
#[cfg_attr(feature = "fails_on_nsfs", ignore = "nsfs: bucket logging not implemented")]
#[tokio::test]
async fn test_put_bucket_logging_account_j() {
    let _guard = TestGuard::setup();
    if !has_bucket_logging_extension().await { return; }
    put_bucket_logging_account("Journal").await;
}
