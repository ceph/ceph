/*
 * Stress tests for BucketCache LRU recycling and versioning concurrency.
 * These reproduce a pre-existing crash — see src/rgw/driver/nsfs/TODO.md
 * "Known issues" for full analysis with backtraces.
 *
 * Marked #[ignore]; run explicitly with:
 *   cargo nextest run -E 'test(/^stress::/)' --run-ignored=all --test-threads=1
 */

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::get_client;
use s3_tests_rs::fixtures::{
    check_configure_versioning_retry, get_new_bucket,
};

/// Stress the bucket cache by creating many versioned buckets and
/// concurrently listing + writing across them.  Forces cache eviction,
/// reinsertion, and exercises the LMDB comparator under contention.
///
/// Known to trigger the cohort_lru race described above.
#[tokio::test]
#[ignore]
async fn test_versioned_bucket_cache_stress() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();

    let num_buckets = 20;
    let mut bucket_names = Vec::new();

    for _ in 0..num_buckets {
        let bn = get_new_bucket(Some(&client)).await;
        check_configure_versioning_retry(&client, &bn, "Enabled", "Enabled").await;
        bucket_names.push(bn);
    }

    for bn in &bucket_names {
        for i in 0..3 {
            client
                .put_object()
                .bucket(bn)
                .key("stress-key")
                .body(ByteStream::from(format!("data-{i}").into_bytes()))
                .send()
                .await
                .unwrap();
        }
    }

    let mut tasks = tokio::task::JoinSet::new();
    for bn in bucket_names.clone() {
        let c = client.clone();
        tasks.spawn(async move {
            let resp = c
                .list_object_versions()
                .bucket(&bn)
                .send()
                .await
                .unwrap();
            assert!(
                resp.versions().len() >= 3,
                "bucket {} has {} versions, expected >= 3",
                bn,
                resp.versions().len()
            );
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let mut tasks2 = tokio::task::JoinSet::new();
    for (idx, bn) in bucket_names.iter().enumerate() {
        let c = client.clone();
        let bn = bn.clone();
        tasks2.spawn(async move {
            c.put_object()
                .bucket(&bn)
                .key("stress-key")
                .body(ByteStream::from(format!("pass2-{idx}").into_bytes()))
                .send()
                .await
                .unwrap();
            let resp = c
                .list_object_versions()
                .bucket(&bn)
                .send()
                .await
                .unwrap();
            assert!(
                resp.versions().len() >= 4,
                "bucket {} has {} versions after second pass, expected >= 4",
                bn,
                resp.versions().len()
            );
        });
    }
    while let Some(result) = tasks2.join_next().await {
        result.unwrap();
    }
}

/// Rapid concurrent versioned PUT + DELETE + LIST on a single bucket.
/// Exercises safe-link/safe-unlink CAS, delete marker creation, and
/// version-aware listing under contention.
#[tokio::test]
#[ignore]
async fn test_versioned_concurrent_put_delete_list() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let client = get_client();
    let bucket_name = get_new_bucket(Some(&client)).await;
    check_configure_versioning_retry(&client, &bucket_name, "Enabled", "Enabled").await;

    let num_rounds = 5;
    let keys_per_round = 4;

    for round in 0..num_rounds {
        let mut tasks = tokio::task::JoinSet::new();

        for k in 0..keys_per_round {
            let c = client.clone();
            let bn = bucket_name.clone();
            tasks.spawn(async move {
                let key = format!("obj-{k}");
                c.put_object()
                    .bucket(&bn)
                    .key(&key)
                    .body(ByteStream::from(
                        format!("r{round}-k{k}").into_bytes(),
                    ))
                    .send()
                    .await
                    .unwrap();
            });
        }

        {
            let c = client.clone();
            let bn = bucket_name.clone();
            tasks.spawn(async move {
                c.list_object_versions()
                    .bucket(&bn)
                    .send()
                    .await
                    .unwrap();
            });
        }

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }

        let del_key = format!("obj-{}", round % keys_per_round);
        client
            .delete_object()
            .bucket(&bucket_name)
            .key(&del_key)
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .list_object_versions()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(
        !resp.versions().is_empty(),
        "final version listing should not be empty"
    );
}
