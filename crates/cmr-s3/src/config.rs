use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use clap::builder::TypedValueParser as _;
use s3s::auth::SecretKey;
use serde::Deserialize;

use crate::{Error, Result};

/// A bucket and the S3 credentials that grant access to it.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketConfig {
    /// The S3 bucket name.
    pub name: String,
    /// The unique access key assigned to this bucket.
    pub access_key: String,
    /// The unique secret key used to sign requests for this bucket.
    pub secret_key: SecretKey,
}

/// Optional server settings from TOML, environment variables, or command-line arguments.
#[derive(clap::Args, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfiguration {
    /// Cloud Mail.ru account login.
    #[arg(long, env = "CMR_LOGIN")]
    pub login: Option<String>,
    /// Cloud Mail.ru application password.
    #[arg(long, env = "CMR_PASSWORD", hide_env_values = true,
        value_parser = clap::builder::StringValueParser::new().map(SecretKey::from))]
    pub password: Option<SecretKey>,
    /// Path to the existing master encryption key file.
    #[arg(long, env = "CMR_S3_KEY_FILE")]
    pub key_file: Option<PathBuf>,
    /// Exclusively owned cloud folder (default: /cmr-s3).
    #[arg(long, env = "CMR_S3_CLOUD_ROOT")]
    pub cloud_root: Option<String>,
    /// Private local state directory (default: .cmr-s3).
    #[arg(long, env = "CMR_S3_STATE_DIR")]
    pub state_dir: Option<PathBuf>,
    /// Listen address (default: 127.0.0.1:9000).
    #[arg(long, env = "CMR_S3_BIND")]
    pub bind: Option<SocketAddr>,
    /// Maximum concurrent cloud transfers (default: 2).
    #[arg(long)]
    pub cloud_concurrency: Option<usize>,
    /// Local incoming-upload allowance in bytes (default: 21474836480).
    #[arg(long)]
    pub staging_bytes: Option<u64>,
    /// Maximum concurrent requests (default: 8).
    #[arg(long)]
    pub max_requests: Option<usize>,
    /// Inactive multipart upload expiry in seconds (default: 604800).
    #[arg(long)]
    pub upload_expiry_seconds: Option<u64>,
    /// Unused content grace period in seconds (default: 86400).
    #[arg(long)]
    pub orphan_grace_seconds: Option<u64>,
    /// Rebuild local state on startup (default: false).
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true",
        action = clap::ArgAction::Set)]
    pub rebuild_index: Option<bool>,
}

/// Server settings and the buckets available through the S3 server.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketConfiguration {
    /// Optional startup settings, overridden by environment variables and command-line arguments.
    #[serde(default)]
    pub server: ServerConfiguration,
    /// The buckets available after the server starts.
    pub buckets: Vec<BucketConfig>,
}

impl BucketConfiguration {
    /// Read a TOML configuration without including its contents in error messages.
    pub fn read(path: impl AsRef<Path>) -> Result<Self> {
        let source = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&source)
            .map_err(|_| Error::Configuration("invalid bucket configuration file".into()))?;
        validate(&config.buckets)?;
        Ok(config)
    }
}

pub(crate) fn validate(buckets: &[BucketConfig]) -> Result<()> {
    let mut names = HashSet::new();
    let mut access_keys = HashSet::new();
    let mut secret_keys = HashSet::new();
    for bucket in buckets {
        if !s3s::path::check_bucket_name(&bucket.name) {
            return Err(Error::Configuration(
                "Bucket names must use valid S3 names".into(),
            ));
        }
        if bucket.access_key.is_empty() || bucket.secret_key.expose().is_empty() {
            return Err(Error::Configuration(
                "S3 credentials must not be empty".into(),
            ));
        }
        if !names.insert(&bucket.name) {
            return Err(Error::Configuration("Bucket names must be unique".into()));
        }
        if !access_keys.insert(&bucket.access_key)
            || !secret_keys.insert(bucket.secret_key.expose())
        {
            return Err(Error::Configuration(
                "Each bucket must use a separate S3 access key and secret key".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_configuration_without_disclosing_secrets() {
        let file = tempfile::NamedTempFile::new().unwrap();
        for source in [
            r#"buckets = [{ name = "bucket", access_key = "access", secret_key = "private-value" }, { name = "bucket", access_key = "different", secret_key = "other" }]"#,
            r#"buckets = [{ name = "bucket", access_key = "access", secret_key = "private-value" }, { name = "other", access_key = "access", secret_key = "other" }]"#,
            r#"buckets = [{ name = "bucket", access_key = "access", secret_key = "private-value" }, { name = "other", access_key = "different", secret_key = "private-value" }]"#,
            r#"buckets = [{ name = "bad/name", access_key = "access", secret_key = "private-value" }]"#,
            r#"buckets = [{ name = "bucket", access_key = "", secret_key = "private-value" }]"#,
            r#"buckets = [{ name = "bucket", access_key = "access", secret_key = "" }]"#,
            r#"buckets = [{ name = "bucket", access_key = "access", secret_key = "private-value", unexpected = "private-value" }]"#,
            "buckets = []\nunexpected = \"private-value\"",
            r#"buckets = "private-value""#,
            r#"buckets = [{ secret_key = "private-value" "#,
            "buckets = []\n[server]\npassword = \"private-value\"\nunexpected = true",
            "buckets = []\n[server]\npassword = \"private-value\"\nbind = \"private-value\"",
            "buckets = []\n[server]\npassword = \"private-value\"\nstaging_bytes = -1",
        ] {
            std::fs::write(file.path(), source).unwrap();
            let error = BucketConfiguration::read(file.path())
                .unwrap_err()
                .to_string();
            assert!(!error.contains("private-value"));
        }
        std::fs::write(
            file.path(),
            "buckets = [{ name = \"bucket\", access_key = \"access\", secret_key = \"private-value\" }]\n[server]\npassword = \"private-password\"",
        )
        .unwrap();
        let config = BucketConfiguration::read(file.path()).unwrap();
        assert!(!format!("{config:?}").contains("private-value"));
        assert!(!format!("{config:?}").contains("private-password"));
    }
}
