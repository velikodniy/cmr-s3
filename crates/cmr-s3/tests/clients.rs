mod support;

use std::{path::Path, sync::Arc};

use cmr_s3::{BucketConfig, EncryptionKey, S3Backend, StorageConfig, serve};
use tokio::process::Command;

async fn rclone(endpoint: &str, arguments: &[&str]) {
    let result = Command::new("rclone")
        .args(arguments)
        .args([
            "--s3-upload-cutoff",
            "5M",
            "--s3-chunk-size",
            "5M",
            "--s3-no-check-bucket",
            "--s3-disable-http2",
        ])
        .env("RCLONE_CONFIG_TEST_TYPE", "s3")
        .env("RCLONE_CONFIG_TEST_PROVIDER", "Other")
        .env("RCLONE_CONFIG_TEST_ACCESS_KEY_ID", "access")
        .env("RCLONE_CONFIG_TEST_SECRET_ACCESS_KEY", "secret")
        .env("RCLONE_CONFIG_TEST_ENDPOINT", endpoint)
        .env("RCLONE_CONFIG_TEST_REGION", "us-east-1")
        .env("RCLONE_CONFIG_TEST_FORCE_PATH_STYLE", "true")
        .env("RCLONE_CONFIG_TEST_SERVER_SIDE_ENCRYPTION", "AES256")
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "rclone {:?}: {}",
        arguments,
        String::from_utf8_lossy(&result.stderr)
    );
}

async fn fixture(path: &Path) -> Vec<u8> {
    let mut bytes = vec![0; 6 * 1024 * 1024 + 11];
    getrandom::fill(&mut bytes).unwrap();
    tokio::fs::write(path, &bytes).await.unwrap();
    bytes
}

async fn docker(arguments: &[&str]) -> String {
    let result = Command::new("docker")
        .args(arguments)
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "docker {:?}: {}",
        arguments,
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap()
}

struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .output();
    }
}

struct Image(String);
impl Drop for Image {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["image", "rm", "-f", &self.0])
            .output();
    }
}

async fn ready(client: &reqwest::Client, endpoint: &str, name: &str) {
    for _ in 0..60 {
        if client
            .get(format!("{endpoint}/v2/"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let logs = Command::new("docker")
        .args(["logs", name])
        .output()
        .await
        .unwrap();
    panic!(
        "the registry did not become ready: {} {}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
}

async fn registry_request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    bytes: Vec<u8>,
) -> reqwest::Response {
    let response = client
        .request(method, url)
        .header("content-type", "application/octet-stream")
        .body(bytes)
        .send()
        .await
        .unwrap();
    let status = response.status();
    if !status.is_success() {
        panic!(
            "registry {url}: {status}: {}",
            response.text().await.unwrap()
        );
    }
    response
}

#[tokio::test]
#[ignore = "requires Docker Desktop, registry:3.1.2, and local socket permission"]
async fn real_registry_resume_large_layers_docker_push_pull_delete_and_gc() {
    use sha2::{Digest, Sha256};
    let state = tempfile::tempdir().unwrap();
    let files = tempfile::tempdir().unwrap();
    let backend = S3Backend::open(
        Arc::new(support::MemoryCloud::default()),
        StorageConfig {
            state_dir: state.path().into(),
            chunk_bytes: 4 * 1024 * 1024,
            ..Default::default()
        },
        EncryptionKey::from_bytes([7; 32]),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve(
            listener,
            backend,
            &[BucketConfig {
                name: "registry".into(),
                access_key: "access".into(),
                secret_key: "secret".into(),
            }],
            8,
            async {
                let _ = stopped.await;
            },
        )
        .await
        .unwrap();
    });
    let configuration = files.path().join("registry.yml");
    tokio::fs::write(&configuration, format!("version: 0.1\nlog:\n  level: error\nhttp:\n  addr: :5000\n  secret: stable-test-secret\nstorage:\n  s3:\n    accesskey: access\n    secretkey: secret\n    region: us-east-1\n    regionendpoint: http://host.docker.internal:{port}\n    bucket: registry\n    secure: false\n    forcepathstyle: true\n    encrypt: true\n    chunksize: 5242880\n    multipartcopychunksize: 5242880\n    multipartcopymaxconcurrency: 2\n    multipartcopythresholdsize: 33554432\n  redirect:\n    disable: true\n  delete:\n    enabled: true\n")).await.unwrap();
    let name = format!("cmr-s3-test-{}", uuid::Uuid::new_v4());
    let port_reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let registry_port = port_reservation.local_addr().unwrap().port();
    drop(port_reservation);
    let publication = format!("127.0.0.1:{registry_port}:5000");
    let mount = format!(
        "{}:/etc/distribution/config.yml:ro",
        configuration.display()
    );
    docker(&[
        "run",
        "-d",
        "--name",
        &name,
        "-p",
        &publication,
        "-v",
        &mount,
        "registry:3.1.2",
    ])
    .await;
    let _container = Container(name.clone());
    let endpoint = format!("http://{}", docker(&["port", &name, "5000"]).await.trim());
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .unwrap();
    ready(&client, &endpoint, &name).await;
    let mut layer = vec![0; 40 * 1024 * 1024 + 37];
    getrandom::fill(&mut layer).unwrap();
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&layer)));
    let begun = registry_request(
        &client,
        reqwest::Method::POST,
        &format!("{endpoint}/v2/resume/blobs/uploads/"),
        vec![],
    )
    .await;
    let location = begun.headers()["location"].to_str().unwrap().to_owned();
    let written = registry_request(
        &client,
        reqwest::Method::PATCH,
        &location,
        layer[..35 * 1024 * 1024].to_vec(),
    )
    .await;
    let location = written.headers()["location"].to_str().unwrap().to_owned();
    docker(&["restart", &name]).await;
    ready(&client, &endpoint, &name).await;
    let resumed = registry_request(&client, reqwest::Method::GET, &location, vec![]).await;
    let location = resumed.headers()["location"].to_str().unwrap().to_owned();
    let tail = registry_request(
        &client,
        reqwest::Method::PATCH,
        &location,
        layer[35 * 1024 * 1024..].to_vec(),
    )
    .await;
    let location = tail.headers()["location"].to_str().unwrap();
    let completed = registry_request(
        &client,
        reqwest::Method::PUT,
        &format!("{location}&digest={digest}"),
        vec![],
    )
    .await;
    assert_eq!(completed.status(), reqwest::StatusCode::CREATED);
    let downloaded = registry_request(
        &client,
        reqwest::Method::GET,
        &format!("{endpoint}/v2/resume/blobs/{digest}"),
        vec![],
    )
    .await;
    assert_eq!(downloaded.bytes().await.unwrap(), layer);
    let range = client
        .get(format!("{endpoint}/v2/resume/blobs/{digest}"))
        .header("range", "bytes=1048573-1048583")
        .send()
        .await
        .unwrap();
    assert_eq!(range.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(range.bytes().await.unwrap(), &layer[1048573..=1048583]);

    // Import a valid scratch image without fetching a base image or leaving a build cache.
    fixture(&files.path().join("payload")).await;
    let archive = files.path().join("image.tar");
    assert!(
        Command::new("tar")
            .args([
                "-cf",
                archive.to_str().unwrap(),
                "-C",
                files.path().to_str().unwrap(),
                "payload"
            ])
            .status()
            .await
            .unwrap()
            .success()
    );
    let tag = format!("{}/pet:test", endpoint.trim_start_matches("http://"));
    let _image = Image(tag.clone());
    docker(&["image", "import", archive.to_str().unwrap(), &tag]).await;
    docker(&["push", &tag]).await;
    docker(&["image", "rm", &tag]).await;
    docker(&["pull", &tag]).await;
    let manifest = client.head(format!("{endpoint}/v2/pet/manifests/test")).header("accept", "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json").send().await.unwrap();
    assert!(manifest.status().is_success());
    let manifest_digest = manifest.headers()["docker-content-digest"]
        .to_str()
        .unwrap();
    assert_eq!(
        client
            .delete(format!("{endpoint}/v2/pet/manifests/{manifest_digest}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    docker(&["stop", &name]).await;
    docker(&[
        "run",
        "--rm",
        "-v",
        &mount,
        "registry:3.1.2",
        "garbage-collect",
        "--delete-untagged",
        "/etc/distribution/config.yml",
    ])
    .await;
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "requires installed rclone and permission to bind a loopback socket"]
async fn real_rclone_sync_metadata_multipart_and_deletion() {
    let state = tempfile::tempdir().unwrap();
    let files = tempfile::tempdir().unwrap();
    let backend = S3Backend::open(
        Arc::new(support::MemoryCloud::default()),
        StorageConfig {
            state_dir: state.path().into(),
            chunk_bytes: 1024 * 1024,
            ..Default::default()
        },
        EncryptionKey::from_bytes([7; 32]),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve(
            listener,
            backend,
            &[BucketConfig {
                name: "bucket".into(),
                access_key: "access".into(),
                secret_key: "secret".into(),
            }],
            8,
            async {
                let _ = stopped.await;
            },
        )
        .await
        .unwrap();
    });
    // Listing uses the bucket that the server provisions from configuration.
    let result = Command::new("rclone")
        .args(["lsd", "test:"])
        .env("RCLONE_CONFIG_TEST_TYPE", "s3")
        .env("RCLONE_CONFIG_TEST_PROVIDER", "Other")
        .env("RCLONE_CONFIG_TEST_ACCESS_KEY_ID", "access")
        .env("RCLONE_CONFIG_TEST_SECRET_ACCESS_KEY", "secret")
        .env("RCLONE_CONFIG_TEST_ENDPOINT", &endpoint)
        .env("RCLONE_CONFIG_TEST_REGION", "us-east-1")
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("bucket"));
    let source = files.path().join("source");
    let target = files.path().join("download");
    tokio::fs::create_dir(&source).await.unwrap();
    tokio::fs::create_dir(&target).await.unwrap();
    let original = fixture(&source.join("layer")).await;
    tokio::fs::write(source.join("A"), "UPPER").await.unwrap();
    tokio::fs::write(source.join("b"), "lower").await.unwrap();
    rclone(
        &endpoint,
        &["sync", source.to_str().unwrap(), "test:bucket/files"],
    )
    .await;
    rclone(
        &endpoint,
        &["sync", "test:bucket/files", target.to_str().unwrap()],
    )
    .await;
    assert_eq!(
        tokio::fs::read(target.join("layer")).await.unwrap(),
        original
    );
    assert_eq!(tokio::fs::read(target.join("A")).await.unwrap(), b"UPPER");
    rclone(
        &endpoint,
        &["copyto", "test:bucket/files/layer", "test:bucket/copied"],
    )
    .await;
    rclone(&endpoint, &["touch", "test:bucket/copied"]).await;
    rclone(
        &endpoint,
        &[
            "check",
            source.to_str().unwrap(),
            "test:bucket/files",
            "--download",
        ],
    )
    .await;
    rclone(&endpoint, &["deletefile", "test:bucket/copied"]).await;
    rclone(&endpoint, &["delete", "test:bucket"]).await;
    let _ = stop.send(());
    server.await.unwrap();
}
