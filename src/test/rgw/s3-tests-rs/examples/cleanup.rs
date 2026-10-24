use aws_sdk_s3::Client as S3Client;
use s3_tests_rs::client::{get_alt_client, get_client, get_tenant_client};
use s3_tests_rs::config::get_config;

const MAX_CONCURRENT_NUKES: usize = 16;

async fn nuke_all_buckets(name: &str, client: &S3Client) {
    let response = match client.list_buckets().send().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("  {name}: failed to list buckets: {e}");
            return;
        }
    };

    let buckets: Vec<String> = response
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(|s| s.to_string()))
        .collect();

    eprintln!("  {name}: found {} buckets total", buckets.len());

    let mut set = tokio::task::JoinSet::new();
    let mut nuked = 0u32;
    let mut errors = 0u32;

    for bucket_name in buckets {
        let c = client.clone();
        if set.len() >= MAX_CONCURRENT_NUKES {
            if let Some(result) = set.join_next().await {
                match result {
                    Ok(Ok(())) => nuked += 1,
                    Ok(Err(e)) => {
                        errors += 1;
                        eprintln!("  {name}: service error: {e:?}");
                    }
                    Err(e) => {
                        errors += 1;
                        eprintln!("  {name}: join error: {e}");
                    }
                }
            }
        }
        set.spawn(async move { s3_tests_rs::cleanup::nuke_bucket(&c, &bucket_name).await });
    }

    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(())) => nuked += 1,
            Ok(Err(e)) => {
                errors += 1;
                eprintln!("  {name}: {e}");
            }
            Err(e) => {
                errors += 1;
                eprintln!("  {name}: join error: {e}");
            }
        }
    }

    if errors > 0 {
        eprintln!("  {name}: cleaned up {nuked} buckets ({errors} errors)");
    } else {
        eprintln!("  {name}: cleaned up {nuked} buckets");
    }
}

#[tokio::main]
async fn main() {
    let _cfg = get_config();

    eprintln!("Cleaning up test buckets (all users)...");

    let client = get_client();
    let alt_client = get_alt_client();
    let tenant_client = get_tenant_client();

    tokio::join!(
        nuke_all_buckets("main", &client),
        nuke_all_buckets("alt", &alt_client),
        nuke_all_buckets("tenant", &tenant_client),
    );

    eprintln!("Done.");
}
