use configparser::ini::Ini;
use once_cell::sync::OnceCell;
use std::sync::Arc;

static CONFIG: OnceCell<Arc<S3TestConfig>> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct CloudConfig {
    pub host: String,
    pub port: u16,
    pub is_secure: bool,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub storage_class: Option<String>,
    pub retain_head_object: Option<String>,
    pub allow_read_through: bool,
    pub target_path: Option<String>,
    pub target_storage_class: String,
    pub regular_storage_class: Option<String>,
    pub read_through_restore_days: u64,
}

#[derive(Debug, Clone)]
pub struct S3TestConfig {
    // DEFAULT
    pub default_host: String,
    pub default_port: u16,
    pub default_is_secure: bool,
    pub default_ssl_verify: bool,
    pub default_endpoint: String,
    pub force_path_style: bool,

    // s3 main
    pub main_access_key: String,
    pub main_secret_key: String,
    pub main_display_name: String,
    pub main_user_id: String,
    pub main_email: String,
    pub main_kms_keyid: String,
    pub main_kms_keyid2: String,
    pub main_api_name: String,
    pub storage_classes: String,
    pub lc_debug_interval: u64,
    pub rgw_restore_debug_interval: u64,
    pub rgw_restore_processor_period: u64,

    // s3 alt
    pub alt_access_key: String,
    pub alt_secret_key: String,
    pub alt_display_name: String,
    pub alt_user_id: String,
    pub alt_email: String,

    // s3 tenant
    pub tenant_access_key: String,
    pub tenant_secret_key: String,
    pub tenant_display_name: String,
    pub tenant_user_id: String,
    pub tenant_email: String,
    pub tenant_name: String,

    // iam
    pub iam_access_key: String,
    pub iam_secret_key: String,
    pub iam_display_name: String,
    pub iam_user_id: String,
    pub iam_email: String,

    // iam root
    pub iam_root_access_key: String,
    pub iam_root_secret_key: String,
    pub iam_root_user_id: String,
    pub iam_root_email: String,

    // iam alt root
    pub iam_alt_root_access_key: String,
    pub iam_alt_root_secret_key: String,
    pub iam_alt_root_user_id: String,
    pub iam_alt_root_email: String,

    // fixtures
    pub bucket_prefix: String,
    pub iam_name_prefix: String,
    pub iam_path_prefix: String,

    // webidentity (optional — present when keycloak-vstart.sh has run)
    pub webidentity_token: Option<String>,
    pub webidentity_user_token: Option<String>,
    pub webidentity_thumbprint: Option<String>,
    pub webidentity_aud: Option<String>,
    pub webidentity_sub: Option<String>,
    pub webidentity_azp: Option<String>,
    pub webidentity_realm: Option<String>,

    // kafka (optional — present when kafka-vstart.sh has run)
    pub kafka_broker: Option<String>,

    // optional
    pub cloud: Option<CloudConfig>,
}

pub fn get_config() -> Arc<S3TestConfig> {
    CONFIG
        .get_or_init(|| Arc::new(load_config().expect("Failed to load S3TEST_CONF")))
        .clone()
}

fn get_str(ini: &Ini, section: &str, key: &str) -> Result<String, String> {
    ini.get(section, key)
        .ok_or_else(|| format!("missing [{section}] {key}"))
}

fn get_str_or(ini: &Ini, section: &str, key: &str, default: &str) -> String {
    ini.get(section, key).unwrap_or_else(|| default.to_string())
}

fn get_bool(ini: &Ini, section: &str, key: &str, default: bool) -> bool {
    ini.getbool(section, key)
        .ok()
        .flatten()
        .unwrap_or(default)
}

fn get_u64(ini: &Ini, section: &str, key: &str, default: u64) -> u64 {
    ini.getuint(section, key)
        .ok()
        .flatten()
        .unwrap_or(default)
}

pub fn choose_bucket_prefix(template: &str, max_len: usize) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let rand_str: String = (0..255)
        .map(|_| {
            let idx = rng.gen_range(0..36u8);
            if idx < 10 {
                (b'0' + idx) as char
            } else {
                (b'a' + idx - 10) as char
            }
        })
        .collect();

    for len in (0..=rand_str.len()).rev() {
        let s = template.replace("{random}", &rand_str[..len]);
        if s.len() <= max_len {
            return s;
        }
    }
    panic!("Bucket prefix template is impossible to fulfill: {template}");
}

fn load_cloud_config(ini: &Ini) -> Option<CloudConfig> {
    let host = ini.get("s3 cloud", "host")?;
    let port = get_u64(ini, "s3 cloud", "port", 8001) as u16;
    let is_secure = get_bool(ini, "s3 cloud", "is_secure", false);
    let proto = if is_secure { "https" } else { "http" };
    let endpoint = format!("{proto}://{host}:{port}");

    Some(CloudConfig {
        host,
        port,
        is_secure,
        endpoint,
        access_key: get_str_or(ini, "s3 cloud", "access_key", ""),
        secret_key: get_str_or(ini, "s3 cloud", "secret_key", ""),
        storage_class: ini.get("s3 cloud", "cloud_storage_class"),
        retain_head_object: ini.get("s3 cloud", "retain_head_object"),
        allow_read_through: get_bool(ini, "s3 cloud", "allow_read_through", false),
        target_path: ini.get("s3 cloud", "target_path"),
        target_storage_class: get_str_or(ini, "s3 cloud", "target_storage_class", "STANDARD"),
        regular_storage_class: ini.get("s3 cloud", "storage_class"),
        read_through_restore_days: get_u64(ini, "s3 cloud", "read_through_restore_days", 10),
    })
}

fn load_config() -> Result<S3TestConfig, String> {
    let path =
        std::env::var("S3TEST_CONF").map_err(|_| "S3TEST_CONF environment variable not set")?;

    let mut ini = Ini::new_cs(); // case-sensitive keys
    ini.load(&path)
        .map_err(|e| format!("Failed to load config: {e}"))?;

    // validate required sections
    if ini.get("default", "host").is_none() && ini.get("DEFAULT", "host").is_none() {
        return Err("Config missing DEFAULT section or host key".into());
    }

    // DEFAULT section — configparser lowercases section names by default,
    // but we use Ini::new_cs() so try both casings
    let def = if ini.get("DEFAULT", "host").is_some() {
        "DEFAULT"
    } else {
        "default"
    };

    let host = get_str_or(&ini, def, "host", "localhost");
    let port = get_u64(&ini, def, "port", 8000) as u16;
    let is_secure = get_bool(&ini, def, "is_secure", false);
    let ssl_verify = get_bool(&ini, def, "ssl_verify", false);
    let force_path_style = get_bool(&ini, def, "path_style", true);
    let proto = if is_secure { "https" } else { "http" };
    let endpoint = format!("{proto}://{host}:{port}");

    let bucket_template = get_str_or(&ini, "fixtures", "bucket prefix", "test-{random}-");
    let bucket_prefix = choose_bucket_prefix(&bucket_template, 30);
    let iam_name_template = get_str_or(&ini, "fixtures", "iam name prefix", "s3-tests-");
    let iam_name_prefix = choose_bucket_prefix(&iam_name_template, 30);
    let iam_path_template = get_str_or(&ini, "fixtures", "iam path prefix", "/s3-tests/");
    let iam_path_prefix = choose_bucket_prefix(&iam_path_template, 30);

    Ok(S3TestConfig {
        default_host: host,
        default_port: port,
        default_is_secure: is_secure,
        default_ssl_verify: ssl_verify,
        default_endpoint: endpoint,
        force_path_style,

        main_access_key: get_str(&ini, "s3 main", "access_key")?,
        main_secret_key: get_str(&ini, "s3 main", "secret_key")?,
        main_display_name: get_str(&ini, "s3 main", "display_name")?,
        main_user_id: get_str(&ini, "s3 main", "user_id")?,
        main_email: get_str(&ini, "s3 main", "email")?,
        main_kms_keyid: get_str_or(&ini, "s3 main", "kms_keyid", "testkey-1"),
        main_kms_keyid2: get_str_or(&ini, "s3 main", "kms_keyid2", "testkey-2"),
        main_api_name: get_str_or(&ini, "s3 main", "api_name", ""),
        storage_classes: get_str_or(&ini, "s3 main", "storage_classes", ""),
        lc_debug_interval: get_u64(&ini, "s3 main", "lc_debug_interval", 10),
        rgw_restore_debug_interval: get_u64(&ini, "s3 main", "rgw_restore_debug_interval", 100),
        rgw_restore_processor_period: get_u64(&ini, "s3 main", "rgw_restore_processor_period", 100),

        alt_access_key: get_str(&ini, "s3 alt", "access_key")?,
        alt_secret_key: get_str(&ini, "s3 alt", "secret_key")?,
        alt_display_name: get_str(&ini, "s3 alt", "display_name")?,
        alt_user_id: get_str(&ini, "s3 alt", "user_id")?,
        alt_email: get_str(&ini, "s3 alt", "email")?,

        tenant_access_key: get_str(&ini, "s3 tenant", "access_key")?,
        tenant_secret_key: get_str(&ini, "s3 tenant", "secret_key")?,
        tenant_display_name: get_str(&ini, "s3 tenant", "display_name")?,
        tenant_user_id: get_str(&ini, "s3 tenant", "user_id")?,
        tenant_email: get_str(&ini, "s3 tenant", "email")?,
        tenant_name: get_str(&ini, "s3 tenant", "tenant")?,

        iam_access_key: get_str(&ini, "iam", "access_key")?,
        iam_secret_key: get_str(&ini, "iam", "secret_key")?,
        iam_display_name: get_str(&ini, "iam", "display_name")?,
        iam_user_id: get_str(&ini, "iam", "user_id")?,
        iam_email: get_str(&ini, "iam", "email")?,

        iam_root_access_key: get_str(&ini, "iam root", "access_key")?,
        iam_root_secret_key: get_str(&ini, "iam root", "secret_key")?,
        iam_root_user_id: get_str(&ini, "iam root", "user_id")?,
        iam_root_email: get_str(&ini, "iam root", "email")?,

        iam_alt_root_access_key: get_str(&ini, "iam alt root", "access_key")?,
        iam_alt_root_secret_key: get_str(&ini, "iam alt root", "secret_key")?,
        iam_alt_root_user_id: get_str(&ini, "iam alt root", "user_id")?,
        iam_alt_root_email: get_str(&ini, "iam alt root", "email")?,

        bucket_prefix,
        iam_name_prefix,
        iam_path_prefix,

        webidentity_token: ini.get("webidentity", "token").map(|s| s.to_string()),
        webidentity_user_token: ini.get("webidentity", "user_token").map(|s| s.to_string()),
        webidentity_thumbprint: ini.get("webidentity", "thumbprint").map(|s| s.to_string()),
        webidentity_aud: ini.get("webidentity", "aud").map(|s| s.to_string()),
        webidentity_sub: ini.get("webidentity", "sub").map(|s| s.to_string()),
        webidentity_azp: ini.get("webidentity", "azp").map(|s| s.to_string()),
        webidentity_realm: ini.get("webidentity", "KC_REALM").map(|s| s.to_string()),

        kafka_broker: ini.get("kafka", "broker").map(|s| s.to_string()),

        cloud: load_cloud_config(&ini),
    })
}

pub fn configured_storage_classes() -> Vec<String> {
    let cfg = get_config();
    let mut sc = vec!["STANDARD".to_string()];
    for item in cfg.storage_classes.split(|c: char| !c.is_alphanumeric() && c != '_') {
        let trimmed = item.trim();
        if !trimmed.is_empty() && trimmed != "STANDARD" {
            sc.push(trimmed.to_string());
        }
    }
    sc
}
