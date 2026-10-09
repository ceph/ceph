use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::get_new_bucket;
use std::sync::Arc;
use std::time::Instant;

fn env_or(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

struct MpuBenchOpts {
    concurrent: usize,
    uploads_per_thread: usize,
    parts_per_upload: usize,
    part_size: usize,
}

impl MpuBenchOpts {
    fn from_env() -> Self {
        Self {
            concurrent: env_or("MPU_THREADS", "16").parse().unwrap_or(16),
            uploads_per_thread: env_or("MPU_UPLOADS", "10").parse().unwrap_or(10),
            parts_per_upload: env_or("MPU_PARTS", "10").parse().unwrap_or(10),
            part_size: env_or("MPU_PART_SIZE", "5242880").parse().unwrap_or(5 * 1024 * 1024),
        }
    }
}

async fn do_one_mpu(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    num_parts: usize,
    part_data: &[u8],
) -> Result<std::time::Duration, String> {
    let t0 = Instant::now();

    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("create: {e}"))?;
    let upload_id = create.upload_id().unwrap().to_string();

    let mut parts = Vec::new();
    for n in 1..=num_parts {
        let resp = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(n as i32)
            .body(ByteStream::from(part_data.to_vec()))
            .send()
            .await
            .map_err(|e| format!("upload_part {n}: {e}"))?;
        parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(resp.e_tag().unwrap_or_default())
                .part_number(n as i32)
                .build(),
        );
    }

    let mp = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(mp)
        .send()
        .await
        .map_err(|e| format!("complete: {e}"))?;

    Ok(t0.elapsed())
}

#[ignore = "bench: run explicitly"]
#[tokio::test(flavor = "multi_thread")]
async fn bench_multipart_throughput() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let opts = MpuBenchOpts::from_env();
    let client = Arc::new(get_client());
    let bucket = get_new_bucket(Some(&client)).await;

    let part_data: Vec<u8> = (0..opts.part_size).map(|i| (i % 251) as u8).collect();
    let part_data = Arc::new(part_data);

    let total_uploads = opts.concurrent * opts.uploads_per_thread;
    let total_bytes = total_uploads * opts.parts_per_upload * opts.part_size;

    eprintln!(
        "bench_multipart_throughput: {} threads x {} uploads x {} parts x {} bytes = {} total MiB",
        opts.concurrent,
        opts.uploads_per_thread,
        opts.parts_per_upload,
        opts.part_size,
        total_bytes / (1024 * 1024),
    );

    let wall_start = Instant::now();

    let mut handles = Vec::new();
    for tid in 0..opts.concurrent {
        let client = Arc::clone(&client);
        let bucket = bucket.clone();
        let part_data = Arc::clone(&part_data);
        let uploads = opts.uploads_per_thread;
        let parts = opts.parts_per_upload;

        handles.push(tokio::spawn(async move {
            let mut durations = Vec::new();
            for u in 0..uploads {
                let key = format!("mpu-t{tid:03}-u{u:04}");
                match do_one_mpu(&client, &bucket, &key, parts, &part_data).await {
                    Ok(d) => durations.push(d),
                    Err(e) => eprintln!("ERROR: thread {tid} upload {u}: {e}"),
                }
            }
            durations
        }));
    }

    let mut all_durations = Vec::new();
    for h in handles {
        all_durations.extend(h.await.unwrap());
    }

    let wall_elapsed = wall_start.elapsed();
    let throughput_mib = total_bytes as f64 / (1024.0 * 1024.0) / wall_elapsed.as_secs_f64();

    all_durations.sort();
    let p50 = all_durations[all_durations.len() / 2];
    let p99 = all_durations[all_durations.len() * 99 / 100];

    eprintln!("--- bench_multipart_throughput results ---");
    eprintln!("  wall time:   {:.2}s", wall_elapsed.as_secs_f64());
    eprintln!("  throughput:  {:.1} MiB/s", throughput_mib);
    eprintln!("  completed:   {}/{} uploads", all_durations.len(), total_uploads);
    eprintln!("  per-upload p50: {:.3}s  p99: {:.3}s", p50.as_secs_f64(), p99.as_secs_f64());
    eprintln!(
        "  config: {}t x {}u x {}p x {}B",
        opts.concurrent, opts.uploads_per_thread, opts.parts_per_upload, opts.part_size,
    );

    // cleanup
    let _ = client.delete_bucket().bucket(&bucket).send().await;
}
