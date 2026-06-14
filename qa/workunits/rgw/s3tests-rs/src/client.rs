use aws_credential_types::Credentials;
use aws_sdk_iam::Client as IamClient;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sns::Client as SnsClient;
use aws_sdk_sts::Client as StsClient;
use once_cell::sync::OnceCell;
use std::sync::Arc;

use crate::config::get_config;

static AWS_SDK_CONFIG: OnceCell<Arc<aws_config::SdkConfig>> = OnceCell::new();

/// Load the standard AWS SDK config chain (~/.aws/config, ~/.aws/credentials,
/// env vars, instance metadata, etc.). Cached after first call.
async fn get_aws_sdk_config() -> Arc<aws_config::SdkConfig> {
    // OnceCell doesn't have async init, so we use a tokio OnceCell pattern
    // via blocking check + init
    if let Some(cfg) = AWS_SDK_CONFIG.get() {
        return cfg.clone();
    }
    let sdk_config = aws_config::from_env()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .load()
        .await;
    let arc = Arc::new(sdk_config);
    let _ = AWS_SDK_CONFIG.set(arc.clone());
    arc
}

/// Build an S3 client from the standard AWS SDK config chain
/// (~/.aws/credentials, ~/.aws/config, env vars).
/// Respects endpoint_url, region, addressing_style, etc. from those files.
pub async fn build_s3_client_from_sdk_config() -> S3Client {
    let sdk_config = get_aws_sdk_config().await;
    let mut builder = aws_sdk_s3::config::Builder::from(&*sdk_config);

    // If the s3tests config is available, apply force_path_style from it;
    // otherwise the SDK config chain handles addressing_style
    if std::env::var("S3TEST_CONF").is_ok() {
        let cfg = get_config();
        builder = builder.force_path_style(cfg.force_path_style);
    }

    S3Client::from_conf(builder.build())
}

pub fn build_s3_client(access_key: &str, secret_key: &str) -> S3Client {
    let cfg = get_config();
    let creds = Credentials::new(access_key, secret_key, None, None, "s3tests");
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .endpoint_url(&cfg.default_endpoint)
        .force_path_style(cfg.force_path_style)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .build();
    S3Client::from_conf(config)
}

pub fn build_s3_client_with_session_token(
    access_key: &str,
    secret_key: &str,
    session_token: &str,
) -> S3Client {
    let cfg = get_config();
    let creds = Credentials::new(
        access_key,
        secret_key,
        Some(session_token.to_string()),
        None,
        "s3tests-sts",
    );
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .endpoint_url(&cfg.default_endpoint)
        .force_path_style(cfg.force_path_style)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .build();
    S3Client::from_conf(config)
}

fn build_iam_client(access_key: &str, secret_key: &str) -> IamClient {
    let cfg = get_config();
    let creds = Credentials::new(access_key, secret_key, None, None, "s3tests");
    let config = aws_sdk_iam::Config::builder()
        .behavior_version(aws_sdk_iam::config::BehaviorVersion::latest())
        .endpoint_url(&cfg.default_endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .build();
    IamClient::from_conf(config)
}

pub fn build_iam_client_with_session_token(
    access_key: &str,
    secret_key: &str,
    session_token: &str,
) -> IamClient {
    let cfg = get_config();
    let creds = Credentials::new(
        access_key, secret_key,
        Some(session_token.to_string()),
        None, "s3tests",
    );
    let config = aws_sdk_iam::Config::builder()
        .behavior_version(aws_sdk_iam::config::BehaviorVersion::latest())
        .endpoint_url(&cfg.default_endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .build();
    IamClient::from_conf(config)
}

pub fn build_sts_client(access_key: &str, secret_key: &str) -> StsClient {
    let cfg = get_config();
    let creds = Credentials::new(access_key, secret_key, None, None, "s3tests");
    let config = aws_sdk_sts::Config::builder()
        .behavior_version(aws_sdk_sts::config::BehaviorVersion::latest())
        .endpoint_url(&cfg.default_endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .build();
    StsClient::from_conf(config)
}

pub fn get_client() -> S3Client {
    let cfg = get_config();
    build_s3_client(&cfg.main_access_key, &cfg.main_secret_key)
}

pub fn get_alt_client() -> S3Client {
    let cfg = get_config();
    build_s3_client(&cfg.alt_access_key, &cfg.alt_secret_key)
}

pub fn get_tenant_client() -> S3Client {
    let cfg = get_config();
    build_s3_client(&cfg.tenant_access_key, &cfg.tenant_secret_key)
}

pub fn get_iam_client() -> IamClient {
    let cfg = get_config();
    build_iam_client(&cfg.iam_access_key, &cfg.iam_secret_key)
}

pub fn get_iam_s3client() -> S3Client {
    let cfg = get_config();
    build_s3_client(&cfg.iam_access_key, &cfg.iam_secret_key)
}

pub fn get_iam_root_client() -> IamClient {
    let cfg = get_config();
    build_iam_client(&cfg.iam_root_access_key, &cfg.iam_root_secret_key)
}

pub fn get_iam_root_s3client() -> S3Client {
    let cfg = get_config();
    build_s3_client(&cfg.iam_root_access_key, &cfg.iam_root_secret_key)
}

pub fn get_iam_alt_root_client() -> IamClient {
    let cfg = get_config();
    build_iam_client(&cfg.iam_alt_root_access_key, &cfg.iam_alt_root_secret_key)
}

pub fn get_alt_iam_client() -> IamClient {
    let cfg = get_config();
    build_iam_client(&cfg.alt_access_key, &cfg.alt_secret_key)
}

pub fn get_tenant_iam_client() -> IamClient {
    let cfg = get_config();
    build_iam_client(&cfg.tenant_access_key, &cfg.tenant_secret_key)
}

pub fn get_sts_client() -> StsClient {
    let cfg = get_config();
    build_sts_client(&cfg.alt_access_key, &cfg.alt_secret_key)
}

pub fn get_client_checksum_when_required() -> S3Client {
    let cfg = get_config();
    let creds = Credentials::new(&cfg.main_access_key, &cfg.main_secret_key, None, None, "s3tests");
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .endpoint_url(&cfg.default_endpoint)
        .force_path_style(cfg.force_path_style)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .request_checksum_calculation(
            aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
        )
        .build();
    S3Client::from_conf(config)
}

pub fn get_unauthenticated_client() -> S3Client {
    let cfg = get_config();
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .endpoint_url(&cfg.default_endpoint)
        .force_path_style(cfg.force_path_style)
        .region(Region::new("us-east-1"))
        .build();
    S3Client::from_conf(config)
}

pub fn get_bad_auth_client() -> S3Client {
    build_s3_client("badauth", "roflmao")
}

pub fn get_svc_client() -> S3Client {
    get_client()
}

fn build_sns_client(access_key: &str, secret_key: &str) -> SnsClient {
    let cfg = get_config();
    let creds = Credentials::new(access_key, secret_key, None, None, "s3tests");
    let config = aws_sdk_sns::Config::builder()
        .behavior_version(aws_sdk_sns::config::BehaviorVersion::latest())
        .endpoint_url(&cfg.default_endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .build();
    SnsClient::from_conf(config)
}

pub fn get_sns_root_client() -> SnsClient {
    let cfg = get_config();
    build_sns_client(&cfg.iam_root_access_key, &cfg.iam_root_secret_key)
}

pub fn get_sns_alt_root_client() -> SnsClient {
    let cfg = get_config();
    build_sns_client(&cfg.iam_alt_root_access_key, &cfg.iam_alt_root_secret_key)
}

pub fn get_iam_alt_root_s3client() -> S3Client {
    let cfg = get_config();
    build_s3_client(&cfg.iam_alt_root_access_key, &cfg.iam_alt_root_secret_key)
}

pub fn get_cloud_client() -> Option<S3Client> {
    let cfg = get_config();
    let cloud = cfg.cloud.as_ref()?;
    let creds = Credentials::new(&cloud.access_key, &cloud.secret_key, None, None, "s3tests-cloud");
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .endpoint_url(&cloud.endpoint)
        .force_path_style(true)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .build();
    Some(S3Client::from_conf(config))
}
