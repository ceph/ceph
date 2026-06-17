use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::cleanup::nuke_bucket;
use crate::client::get_client;
use crate::config::get_config;
use crate::random::{generate_random, DEFAULT_PART_SIZE};

static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(1);

static CREATED_BUCKETS: Mutex<Vec<String>> = Mutex::new(Vec::new());

static SERVER_DEAD: AtomicBool = AtomicBool::new(false);

fn track_bucket(_client: &S3Client, name: &str) {
    CREATED_BUCKETS
        .lock()
        .unwrap()
        .push(name.to_string());
}

/// Register a bucket for cleanup that was created directly (not via get_new_bucket).
pub fn register_bucket_for_cleanup(_client: &S3Client, name: &str) {
    CREATED_BUCKETS.lock().unwrap().push(name.to_string());
}

/// Nuke all tracked buckets using a fresh client.
async fn cleanup_tracked_buckets_inner() {
    let buckets: Vec<String> = {
        let mut guard = CREATED_BUCKETS.lock().unwrap();
        std::mem::take(&mut *guard)
    };

    if buckets.is_empty() {
        return;
    }

    let client = get_client();
    let mut set = tokio::task::JoinSet::new();
    for name in buckets {
        let c = client.clone();
        set.spawn(async move {
            if let Err(e) = nuke_bucket(&c, &name).await {
                eprintln!("cleanup: failed to nuke {name}: {e}");
            }
        });
    }

    while set.join_next().await.is_some() {}
}

/// RAII guard that cleans up all tracked buckets when dropped.
/// Add `let _guard = TestGuard::setup();` as the first line of each test.
pub struct TestGuard;

impl TestGuard {
    pub fn setup() -> Self {
        if std::env::var("S3TEST_FAST_FAIL").is_ok() {
            if SERVER_DEAD.load(Ordering::Relaxed) {
                panic!("s3tests: server unreachable (fast-fail)");
            }
            let cfg = get_config();
            let addr_str = format!("{}:{}", cfg.default_host, cfg.default_port);
            let timeout = std::time::Duration::from_millis(500);
            let reachable = std::net::ToSocketAddrs::to_socket_addrs(&addr_str)
                .ok()
                .and_then(|mut addrs| addrs.next())
                .map(|sa| std::net::TcpStream::connect_timeout(&sa, timeout).is_ok())
                .unwrap_or(false);
            if !reachable {
                SERVER_DEAD.store(true, Ordering::Relaxed);
                panic!("s3tests: server at {addr_str} unreachable — fast-fail engaged");
            }
        }
        Self
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        let has_buckets = CREATED_BUCKETS
            .lock()
            .map(|g| !g.is_empty())
            .unwrap_or(false);

        if !has_buckets {
            return;
        }

        // Spawn a separate thread with its own runtime, since we can't
        // create a nested runtime on the tokio test thread. A fresh
        // client is created inside the new runtime to avoid connection
        // pool issues across runtimes.
        std::thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create cleanup runtime");

            rt.block_on(cleanup_tracked_buckets_inner());
        })
        .join()
        .ok();
    }
}

pub fn get_new_bucket_name() -> String {
    let cfg = get_config();
    let num = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}{}", cfg.bucket_prefix, num)
}

pub async fn get_new_bucket(client: Option<&S3Client>) -> String {
    let name = get_new_bucket_name();
    let default_client;
    let c = match client {
        Some(c) => c,
        None => {
            default_client = get_client();
            &default_client
        }
    };
    c.create_bucket()
        .bucket(&name)
        .send()
        .await
        .expect("Failed to create bucket");
    track_bucket(c, &name);
    name
}

pub async fn create_objects(client: &S3Client, bucket_name: &str, keys: &[&str]) {
    for key in keys {
        client
            .put_object()
            .bucket(bucket_name)
            .key(*key)
            .body(ByteStream::from(key.as_bytes().to_vec()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put object {key}: {e}"));
    }
}

pub async fn create_objects_in_new_bucket(keys: &[&str]) -> String {
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    create_objects(&client, &bucket_name, keys).await;
    bucket_name
}

pub struct MultipartUploadResult {
    pub upload_id: String,
    pub data: Vec<u8>,
    pub parts: Vec<aws_sdk_s3::types::CompletedPart>,
}

pub async fn multipart_upload(
    client: &S3Client,
    bucket_name: &str,
    key: &str,
    size: usize,
    part_size: Option<usize>,
    content_type: Option<&str>,
    metadata: Option<std::collections::HashMap<String, String>>,
    tagging: Option<&str>,
) -> MultipartUploadResult {
    let part_size = part_size.unwrap_or(DEFAULT_PART_SIZE);

    let mut req = client.create_multipart_upload().bucket(bucket_name).key(key);
    if let Some(ct) = content_type {
        req = req.content_type(ct);
    }
    if let Some(md) = metadata {
        for (k, v) in md {
            req = req.metadata(k, v);
        }
    }
    if let Some(t) = tagging {
        req = req.tagging(t);
    }
    let resp = req.send().await.expect("create_multipart_upload failed");
    let upload_id = resp.upload_id().unwrap().to_string();

    let mut all_data = Vec::new();
    let mut parts = Vec::new();

    for (i, chunk) in generate_random(size, part_size).into_iter().enumerate() {
        let part_num = (i + 1) as i32;
        all_data.extend_from_slice(&chunk);
        let resp = client
            .upload_part()
            .bucket(bucket_name)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_num)
            .body(ByteStream::from(chunk))
            .send()
            .await
            .expect("upload_part failed");

        parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(resp.e_tag().unwrap_or_default())
                .part_number(part_num)
                .build(),
        );
    }

    MultipartUploadResult {
        upload_id,
        data: all_data,
        parts,
    }
}

pub struct MultipartCopyResult {
    pub upload_id: String,
    pub parts: Vec<aws_sdk_s3::types::CompletedPart>,
}

pub async fn multipart_copy(
    client: &S3Client,
    src_bucket: &str,
    src_key: &str,
    dest_bucket: &str,
    dest_key: &str,
    size: usize,
    part_size: Option<usize>,
    version_id: Option<&str>,
) -> MultipartCopyResult {
    let part_size = part_size.unwrap_or(DEFAULT_PART_SIZE);

    let resp = client
        .create_multipart_upload()
        .bucket(dest_bucket)
        .key(dest_key)
        .send()
        .await
        .expect("create_multipart_upload failed");
    let upload_id = resp.upload_id().unwrap().to_string();

    let mut copy_source = format!("{}/{}", src_bucket, src_key);
    if let Some(vid) = version_id {
        copy_source = format!("{}?versionId={}", copy_source, vid);
    }

    let mut parts = Vec::new();
    for (i, start_offset) in (0..size).step_by(part_size).enumerate() {
        let end_offset = std::cmp::min(start_offset + part_size - 1, size - 1);
        let part_num = (i + 1) as i32;
        let range = format!("bytes={}-{}", start_offset, end_offset);

        let resp = client
            .upload_part_copy()
            .bucket(dest_bucket)
            .key(dest_key)
            .copy_source(&copy_source)
            .part_number(part_num)
            .upload_id(&upload_id)
            .copy_source_range(&range)
            .customize()
            .mutate_request(|req| {
                req.headers_mut().insert("content-length", "0");
            })
            .send()
            .await
            .expect("upload_part_copy failed");

        let etag = resp
            .copy_part_result()
            .and_then(|r| r.e_tag())
            .unwrap_or_default();
        parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(etag)
                .part_number(part_num)
                .build(),
        );
    }

    MultipartCopyResult { upload_id, parts }
}

pub async fn create_key_with_random_content(
    client: &S3Client,
    key: &str,
    bucket_name: Option<&str>,
    size: Option<usize>,
) -> String {
    let owned_bucket;
    let bname = match bucket_name {
        Some(b) => b,
        None => {
            owned_bucket = get_new_bucket(Some(client)).await;
            &owned_bucket
        }
    };
    let sz = size.unwrap_or(7 * 1024 * 1024);
    let data = generate_random(sz, sz);
    let body = data.into_iter().next().unwrap_or_default();
    client
        .put_object()
        .bucket(bname)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await
        .expect("put_object failed for random content");
    bname.to_string()
}

pub async fn check_configure_versioning_retry(
    client: &S3Client,
    bucket_name: &str,
    status: &str,
    expected: &str,
) {
    client
        .put_bucket_versioning()
        .bucket(bucket_name)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(status.parse::<aws_sdk_s3::types::BucketVersioningStatus>().unwrap())
                .build(),
        )
        .send()
        .await
        .expect("put_bucket_versioning failed");

    for _ in 0..5 {
        let resp = client
            .get_bucket_versioning()
            .bucket(bucket_name)
            .send()
            .await
            .expect("get_bucket_versioning failed");
        let read_status = resp
            .status()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        if read_status == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    panic!("versioning status never reached {expected}");
}
