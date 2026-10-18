use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use clap::{Parser, Subcommand};
use cmr_s3::{
    BucketConfiguration, EncryptionKey, S3Backend, ServerConfiguration, StorageConfig, serve,
};

#[derive(Parser)]
#[command(version, about = "Encrypted S3 storage on Cloud Mail.ru")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a private 256-bit encryption key in JWK format without replacing an existing file.
    Keygen { path: PathBuf },
    /// Open storage and serve signed S3 requests.
    Serve(Box<ServeArgs>),
}

#[derive(clap::Args)]
struct ServeArgs {
    /// TOML file containing server settings and bucket credentials.
    #[arg(long, env = "CMR_S3_CONFIG")]
    config: PathBuf,
    #[command(flatten)]
    server: ServerConfiguration,
}

impl ServeArgs {
    fn read(self) -> cmr_s3::Result<BucketConfiguration> {
        let file = BucketConfiguration::read(self.config)?;
        let overrides = self.server;
        let settings = file.server;
        Ok(BucketConfiguration {
            buckets: file.buckets,
            server: ServerConfiguration {
                login: overrides.login.or(settings.login),
                password: overrides.password.or(settings.password),
                key_file: overrides.key_file.or(settings.key_file),
                cloud_root: overrides.cloud_root.or(settings.cloud_root),
                state_dir: overrides.state_dir.or(settings.state_dir),
                bind: overrides.bind.or(settings.bind),
                cloud_concurrency: overrides.cloud_concurrency.or(settings.cloud_concurrency),
                staging_bytes: overrides.staging_bytes.or(settings.staging_bytes),
                max_requests: overrides.max_requests.or(settings.max_requests),
                upload_expiry_seconds: overrides
                    .upload_expiry_seconds
                    .or(settings.upload_expiry_seconds),
                orphan_grace_seconds: overrides
                    .orphan_grace_seconds
                    .or(settings.orphan_grace_seconds),
                rebuild_index: overrides.rebuild_index.or(settings.rebuild_index),
            },
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cmr_s3=info".into()),
        )
        .init();
    match Cli::parse().command {
        Command::Keygen { path } => EncryptionKey::generate(path)?,
        Command::Serve(args) => run(*args).await?,
    }
    Ok(())
}

async fn run(args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let BucketConfiguration { buckets, server } = args.read()?;
    let login = server.login.ok_or_else(|| {
        cmr_s3::Error::Configuration("set server.login, --login, or CMR_LOGIN".into())
    })?;
    let password = server.password.ok_or_else(|| {
        cmr_s3::Error::Configuration("set server.password, --password, or CMR_PASSWORD".into())
    })?;
    let key_file = server.key_file.ok_or_else(|| {
        cmr_s3::Error::Configuration("set server.key_file, --key-file, or CMR_S3_KEY_FILE".into())
    })?;
    let max_requests = server.max_requests.unwrap_or(8);
    if max_requests == 0 || max_requests > tokio::sync::Semaphore::MAX_PERMITS / 2 {
        return Err(cmr_s3::Error::Configuration(
            "request concurrency must be positive and within the supported limit".into(),
        )
        .into());
    }
    let listener = tokio::net::TcpListener::bind(
        server
            .bind
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 9000))),
    )
    .await?;
    let cloud = Arc::new(cmr_api::Client::new(login, password.expose())?);
    let defaults = StorageConfig::default();
    let config = StorageConfig {
        cloud_root: server.cloud_root.unwrap_or(defaults.cloud_root),
        state_dir: server.state_dir.unwrap_or(defaults.state_dir),
        cloud_concurrency: server
            .cloud_concurrency
            .unwrap_or(defaults.cloud_concurrency),
        staging_bytes: server.staging_bytes.unwrap_or(defaults.staging_bytes),
        upload_expiry: server
            .upload_expiry_seconds
            .map(Duration::from_secs)
            .unwrap_or(defaults.upload_expiry),
        orphan_grace: server
            .orphan_grace_seconds
            .map(Duration::from_secs)
            .unwrap_or(defaults.orphan_grace),
        rebuild_catalog: server.rebuild_index.unwrap_or(defaults.rebuild_catalog),
        ..defaults
    };
    let backend = S3Backend::open(cloud, config, EncryptionKey::read(key_file)?).await?;
    serve(listener, backend, &buckets, max_requests, shutdown()).await?;
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_overrides_preserve_file_settings_and_false_or_zero_values() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"
buckets = []
[server]
login = "file-login"
password = "file-password"
key_file = "file.key"
bind = "127.0.0.1:9001"
cloud_root = "/configured"
max_requests = 4
rebuild_index = true
"#,
        )
        .unwrap();
        let settings = ServeArgs {
            config: file.path().into(),
            server: ServerConfiguration {
                key_file: Some("override.key".into()),
                bind: Some("127.0.0.1:0".parse().unwrap()),
                max_requests: Some(0),
                rebuild_index: Some(false),
                ..Default::default()
            },
        }
        .read()
        .unwrap()
        .server;
        assert_eq!(
            settings.key_file.as_deref(),
            Some(std::path::Path::new("override.key"))
        );
        assert_eq!(settings.bind, Some("127.0.0.1:0".parse().unwrap()));
        assert_eq!(settings.max_requests, Some(0));
        assert_eq!(settings.rebuild_index, Some(false));
        assert_eq!(settings.cloud_root.as_deref(), Some("/configured"));
        assert_eq!(settings.login.as_deref(), Some("file-login"));
        assert_eq!(settings.password.unwrap().expose(), "file-password");
    }
}
