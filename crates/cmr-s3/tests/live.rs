// Load CMR_LOGIN and CMR_TOKEN into the environment before running this ignored test.
#[path = "support/signed.rs"]
mod signature;

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use bytes::Bytes;
use cmr_api::{ByteRange, Client, CloudApi, CloudObject, Download, ObjectStream, UploadedContent};
use cmr_s3::{BucketConfig, EncryptionKey, S3Backend, StorageConfig, serve};
use futures_util::TryStreamExt;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn require(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

struct Server {
    endpoint: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<cmr_s3::Result<()>>>,
}

impl Server {
    async fn start(cloud: Arc<dyn CloudApi>, root: &str, state: &Path, key: &Path) -> Result<Self> {
        let backend = S3Backend::open(
            cloud,
            StorageConfig {
                cloud_root: root.into(),
                state_dir: state.into(),
                ..Default::default()
            },
            EncryptionKey::read(key)?,
        )
        .await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
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
        });
        Ok(Self {
            endpoint,
            stop: Some(stop),
            task: Some(task),
        })
    }

    async fn stop(mut self) -> Result<()> {
        let _ = self.stop.take().expect("server stop sender").send(());
        self.task.take().expect("server task").await??;
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct Response {
    headers: http::HeaderMap,
    body: Bytes,
}

impl Response {
    fn header(&self, name: &str) -> Result<&str> {
        Ok(self
            .headers
            .get(name)
            .ok_or("missing S3 header")?
            .to_str()?)
    }

    fn xml(&self) -> Result<&str> {
        Ok(std::str::from_utf8(&self.body)?)
    }

    fn element(&self, name: &str) -> Result<String> {
        let start = format!("<{name}>");
        let end = format!("</{name}>");
        Ok(self
            .xml()?
            .split_once(&start)
            .and_then(|(_, tail)| tail.split_once(&end))
            .ok_or("missing S3 XML element")?
            .0
            .replace("&quot;", "\""))
    }
}

async fn call(
    client: &reqwest::Client,
    server: &Server,
    method: &str,
    target: &str,
    body: impl Into<Bytes>,
    headers: &[(&str, &str)],
    expected_status: u16,
) -> Result<Response> {
    let body = body.into();
    let (parts, _) = signature::signed(method, target, body.clone(), headers).into_parts();
    let response = client
        .request(parts.method, format!("{}{target}", server.endpoint))
        .headers(parts.headers)
        .body(body)
        .send()
        .await?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await?;
    if status.as_u16() != expected_status {
        return Err(format!(
            "S3 {method} {target}: expected {expected_status}, received {status}: {}",
            String::from_utf8_lossy(&body)
        )
        .into());
    }
    Ok(Response { headers, body })
}

async fn exercise(
    cloud: Arc<Client>,
    root: &str,
    directory: &Path,
    key: &Path,
    running: &mut Option<Server>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(90))
        .build()?;
    let state = directory.join("state");
    *running = Some(Server::start(cloud.clone(), root, &state, key).await?);
    let server = running.as_ref().expect("running server");
    call(&client, server, "HEAD", "/bucket", "", &[], 200).await?;
    call(&client, server, "PUT", "/bucket", "", &[], 403).await?;
    let buckets = call(&client, server, "GET", "/", "", &[], 200).await?;
    require(
        buckets.xml()?.contains("<Name>bucket</Name>"),
        "bucket listing differs",
    )?;
    let anonymous = client
        .get(format!("{}/bucket", server.endpoint))
        .send()
        .await?;
    require(anonymous.status() == 403, "anonymous access was accepted")?;
    drop(anonymous);
    eprintln!("Live S3 bucket management and signature authentication passed");

    let put = call(
        &client,
        server,
        "PUT",
        "/bucket/a//b",
        "hello world",
        &[
            ("content-type", "text/plain"),
            ("x-amz-meta-private", "live-private-metadata"),
            ("x-amz-server-side-encryption", "AES256"),
            ("x-amz-checksum-crc32", "DUoRhQ=="),
        ],
        200,
    )
    .await?;
    let etag = put.header("etag")?.to_owned();
    require(
        put.header("x-amz-server-side-encryption")? == "AES256",
        "encryption header differs",
    )?;
    let head = call(
        &client,
        server,
        "HEAD",
        "/bucket/a//b",
        "",
        &[("x-amz-checksum-mode", "ENABLED")],
        200,
    )
    .await?;
    require(
        head.header("content-length")? == "11",
        "object size differs",
    )?;
    require(
        head.header("x-amz-meta-private")? == "live-private-metadata",
        "object metadata differs",
    )?;
    require(
        head.header("x-amz-checksum-crc32")? == "DUoRhQ==",
        "plaintext checksum differs",
    )?;
    let range = call(
        &client,
        server,
        "GET",
        "/bucket/a//b",
        "",
        &[("range", "bytes=3-8")],
        206,
    )
    .await?;
    require(range.body == "lo wor", "object range differs")?;
    call(
        &client,
        server,
        "GET",
        "/bucket/a//b",
        "",
        &[("if-none-match", &etag)],
        304,
    )
    .await?;
    call(
        &client,
        server,
        "PUT",
        "/bucket/a//b",
        "rejected",
        &[("if-match", "\"wrong\"")],
        412,
    )
    .await?;
    call(
        &client,
        server,
        "PUT",
        "/bucket/copied",
        "",
        &[("x-amz-copy-source", "/bucket/a//b")],
        200,
    )
    .await?;
    let copied = call(&client, server, "GET", "/bucket/copied", "", &[], 200).await?;
    require(copied.body == "hello world", "copied content differs")?;
    let listing = call(
        &client,
        server,
        "GET",
        "/bucket?list-type=2&delimiter=%2F",
        "",
        &[],
        200,
    )
    .await?;
    require(
        listing.xml()?.contains("<Prefix>a/</Prefix>"),
        "delimiter listing differs",
    )?;
    call(&client, server, "DELETE", "/bucket", "", &[], 403).await?;
    eprintln!(
        "Live S3 object upload, metadata, checksum, range, conditions, copy, and listing passed"
    );

    // Inspect provider bytes directly to establish that the live manifest is encrypted.
    let records = cloud
        .list(&format!("{root}/objects"))
        .try_collect::<Vec<_>>()
        .await?;
    let record = records.first().ok_or("no encrypted object manifest")?;
    require(
        !record.name.contains("copied"),
        "cloud path exposes an object key",
    )?;
    let encrypted = cloud
        .download(&record.home, None)
        .await?
        .stream
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await?;
    require(
        encrypted.starts_with(b"CMRS3\0\0\x01"),
        "cloud manifest format differs",
    )?;
    require(
        !encrypted
            .windows(b"live-private-metadata".len())
            .any(|w| w == b"live-private-metadata"),
        "cloud manifest exposes metadata",
    )?;
    eprintln!("Live cloud manifest encryption passed");

    let initiated = call(
        &client,
        server,
        "POST",
        "/bucket/large?uploads",
        "",
        &[],
        200,
    )
    .await?;
    let upload = initiated.element("UploadId")?;
    let mut first = vec![0; 5 * 1024 * 1024];
    getrandom::fill(&mut first)?;
    let first = Bytes::from(first);
    let part = call(
        &client,
        server,
        "PUT",
        &format!("/bucket/large?partNumber=1&uploadId={upload}"),
        first.clone(),
        &[],
        200,
    )
    .await?;
    let first_etag = part.header("etag")?.to_owned();
    let uploads = call(&client, server, "GET", "/bucket?uploads", "", &[], 200).await?;
    require(
        uploads.xml()?.contains(&upload),
        "active upload listing differs",
    )?;
    eprintln!("Live S3 first multipart part passed. Restarting with the same local state");

    running.take().expect("running server").stop().await?;
    *running = Some(Server::start(cloud.clone(), root, &state, key).await?);
    let server = running.as_ref().expect("running server");
    let parts = call(
        &client,
        server,
        "GET",
        &format!("/bucket/large?uploadId={upload}"),
        "",
        &[],
        200,
    )
    .await?;
    require(
        parts.xml()?.contains("<PartNumber>1</PartNumber>"),
        "multipart part did not survive restart",
    )?;
    let tail = Bytes::from_static(b"tail after the multipart restart");
    let part = call(
        &client,
        server,
        "PUT",
        &format!("/bucket/large?partNumber=2&uploadId={upload}"),
        tail.clone(),
        &[],
        200,
    )
    .await?;
    let second_etag = part.header("etag")?;
    let completion = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{first_etag}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{second_etag}</ETag></Part></CompleteMultipartUpload>"
    );
    call(
        &client,
        server,
        "POST",
        &format!("/bucket/large?uploadId={upload}"),
        completion,
        &[("content-type", "application/xml")],
        200,
    )
    .await?;
    let mut expected = first.to_vec();
    expected.extend_from_slice(&tail);
    let downloaded = call(&client, server, "GET", "/bucket/large", "", &[], 200).await?;
    require(
        downloaded.body == expected,
        "completed multipart content differs",
    )?;
    let range_start = first.len() - 8;
    let range_header = format!("bytes={range_start}-{}", first.len() + 7);
    let range = call(
        &client,
        server,
        "GET",
        "/bucket/large",
        "",
        &[("range", &range_header)],
        206,
    )
    .await?;
    require(
        range.body == expected[range_start..first.len() + 8],
        "range across multipart parts differs",
    )?;
    let aborted = call(
        &client,
        server,
        "POST",
        "/bucket/aborted?uploads",
        "",
        &[],
        200,
    )
    .await?
    .element("UploadId")?;
    call(
        &client,
        server,
        "PUT",
        &format!("/bucket/aborted?partNumber=1&uploadId={aborted}"),
        "",
        &[("x-amz-copy-source", "/bucket/copied")],
        200,
    )
    .await?;
    call(
        &client,
        server,
        "DELETE",
        &format!("/bucket/aborted?uploadId={aborted}"),
        "",
        &[],
        204,
    )
    .await?;
    eprintln!(
        "Live S3 multipart restart, completion, cross-part range, copy part, and abort passed"
    );

    running.take().expect("running server").stop().await?;
    *running = Some(Server::start(cloud.clone(), root, &directory.join("fresh-state"), key).await?);
    let server = running.as_ref().expect("running server");
    let recovered = call(&client, server, "GET", "/bucket/large", "", &[], 200).await?;
    require(
        recovered.body == expected,
        "cloud-only recovery content differs",
    )?;
    let recovered = call(&client, server, "HEAD", "/bucket/a//b", "", &[], 200).await?;
    require(
        recovered.header("x-amz-meta-private")? == "live-private-metadata",
        "cloud-only recovery metadata differs",
    )?;
    eprintln!("Live cloud recovery with a fresh local index and the original key passed");

    for path in ["/bucket/a//b", "/bucket/copied", "/bucket/large"] {
        call(&client, server, "DELETE", path, "", &[], 204).await?;
    }
    call(&client, server, "DELETE", "/bucket", "", &[], 403).await?;
    call(&client, server, "HEAD", "/bucket", "", &[], 200).await?;
    eprintln!("Live S3 object deletion and configured bucket preservation passed");
    Ok(())
}

struct CloudTiming {
    operation: &'static str,
    bytes: usize,
    started: Instant,
    elapsed: Duration,
}

struct TimedCloud {
    client: Arc<Client>,
    timings: Mutex<Vec<CloudTiming>>,
}

impl TimedCloud {
    fn record(&self, operation: &'static str, bytes: usize, started: Instant) {
        self.timings.lock().unwrap().push(CloudTiming {
            operation,
            bytes,
            started,
            elapsed: started.elapsed(),
        });
    }

    fn take(&self) -> Vec<CloudTiming> {
        std::mem::take(&mut *self.timings.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl CloudApi for TimedCloud {
    async fn stat(&self, path: &str) -> cmr_api::Result<CloudObject> {
        self.client.stat(path).await
    }

    fn list<'a>(&'a self, path: &'a str) -> ObjectStream<'a> {
        self.client.list(path)
    }

    async fn download(&self, path: &str, range: Option<ByteRange>) -> cmr_api::Result<Download> {
        self.client.download(path, range).await
    }

    async fn upload(&self, bytes: Bytes) -> cmr_api::Result<UploadedContent> {
        let started = Instant::now();
        let size = bytes.len();
        let result = self.client.upload(bytes).await;
        self.record("upload", size, started);
        result
    }

    async fn bind(
        &self,
        path: &str,
        content: UploadedContent,
        mtime: SystemTime,
    ) -> cmr_api::Result<()> {
        let started = Instant::now();
        let result = self.client.bind(path, content, mtime).await;
        self.record("bind", 0, started);
        result
    }

    async fn mkdir(&self, path: &str) -> cmr_api::Result<()> {
        let started = Instant::now();
        let result = self.client.mkdir(path).await;
        self.record("mkdir", 0, started);
        result
    }

    async fn remove(&self, path: &str) -> cmr_api::Result<()> {
        self.client.remove(path).await
    }
}

async fn benchmark_upload(
    cloud: Arc<Client>,
    root: &str,
    directory: &Path,
    key: &Path,
    running: &mut Option<Server>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(300))
        .build()?;
    let timed = Arc::new(TimedCloud {
        client: cloud.clone(),
        timings: Mutex::default(),
    });
    *running = Some(Server::start(timed.clone(), root, &directory.join("state"), key).await?);
    let server = running.as_ref().expect("running server");
    call(&client, server, "PUT", "/benchmark", "", &[], 200).await?;
    let mut bytes = vec![0; 50 * 1024 * 1024];
    getrandom::fill(&mut bytes)?;
    let bytes = Bytes::from(bytes);
    let mut seconds = Vec::new();
    let mut direct_seconds = Vec::new();
    let direct_path = format!("{root}/direct/50-mib");
    cloud.mkdir(&format!("{root}/direct")).await?;
    let mut direct_bytes = Bytes::new();
    for sample in 1..=3 {
        timed.take();
        let started = Instant::now();
        call(
            &client,
            server,
            "PUT",
            "/benchmark/50-mib",
            bytes.clone(),
            &[("x-amz-server-side-encryption", "AES256")],
            200,
        )
        .await?;
        let elapsed = started.elapsed().as_secs_f64();
        seconds.push(elapsed);
        let mut operations = timed.take();
        let content: f64 = operations
            .iter()
            .filter(|operation| operation.operation == "upload" && operation.bytes >= bytes.len())
            .map(|operation| operation.elapsed.as_secs_f64())
            .sum();
        let other: f64 = operations
            .iter()
            .filter(|operation| operation.operation != "upload" || operation.bytes < bytes.len())
            .map(|operation| operation.elapsed.as_secs_f64())
            .sum();
        operations.sort_by_key(|operation| operation.started);
        let mut end = None;
        let mut cloud_seconds = 0.0;
        for operation in &operations {
            let finished = operation.started + operation.elapsed;
            let uncovered = end.map_or(operation.started, |previous: Instant| {
                previous.max(operation.started)
            });
            if finished > uncovered {
                cloud_seconds += (finished - uncovered).as_secs_f64();
            }
            end = Some(end.map_or(finished, |previous| previous.max(finished)));
        }
        eprintln!(
            "Live 50 MiB PutObject sample {sample}: {elapsed:.3} seconds, {:.2} MiB/s",
            50.0 / elapsed
        );
        eprintln!(
            "Live S3 phase sample {sample}: content upload {content:.3}s, other cloud calls {other:.3}s, overlap {:.3}s, outside cloud calls {:.3}s, {} cloud operations",
            content + other - cloud_seconds,
            elapsed - cloud_seconds,
            operations.len(),
        );
        // Match ciphertext size and change every payload to avoid provider deduplication.
        let mut payload = vec![0; bytes.len() + 64];
        getrandom::fill(&mut payload)?;
        direct_bytes = Bytes::from(payload);
        let started = Instant::now();
        let uploaded = cloud.upload(direct_bytes.clone()).await?;
        let upload_seconds = started.elapsed().as_secs_f64();
        cloud
            .bind(&direct_path, uploaded, SystemTime::now())
            .await?;
        let elapsed = started.elapsed().as_secs_f64();
        direct_seconds.push(elapsed);
        eprintln!(
            "Live direct Cloud Mail.ru sample {sample}: upload {upload_seconds:.3}s, upload plus binding {elapsed:.3}s, {:.2} MiB/s",
            50.0 / elapsed,
        );
    }
    seconds.sort_by(f64::total_cmp);
    eprintln!(
        "Live 50 MiB PutObject median: {:.3} seconds, {:.2} MiB/s; min {:.3}, max {:.3}",
        seconds[1],
        50.0 / seconds[1],
        seconds[0],
        seconds[2],
    );
    direct_seconds.sort_by(f64::total_cmp);
    eprintln!(
        "Live direct Cloud Mail.ru median: {:.3}s; S3 median: {:.3}s; difference {:.3}s",
        direct_seconds[1],
        seconds[1],
        seconds[1] - direct_seconds[1],
    );
    let downloaded = call(&client, server, "GET", "/benchmark/50-mib", "", &[], 200).await?;
    require(downloaded.body == bytes, "50 MiB benchmark content differs")?;
    let downloaded = cloud
        .download(&direct_path, None)
        .await?
        .stream
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await?;
    require(
        downloaded == direct_bytes,
        "direct benchmark content differs",
    )?;
    eprintln!("Live 50 MiB download integrity passed");
    Ok(())
}

enum LiveScenario {
    RoundTrip,
    UploadBenchmark,
}

async fn run_live(scenario: LiveScenario) -> Result<()> {
    let cloud = Arc::new(Client::new(
        std::env::var("CMR_LOGIN")?,
        std::env::var("CMR_TOKEN")?,
    )?);
    let root = format!("/cmr-s3-live-{}", uuid::Uuid::new_v4());
    match cloud.stat(&root).await {
        Err(cmr_api::Error::NotFound(_)) => {}
        Ok(_) => return Err("the isolated live root already exists".into()),
        Err(error) => return Err(error.into()),
    }
    let directory = tempfile::tempdir()?;
    let key = directory.path().join("master.key");
    EncryptionKey::generate(&key)?;
    std::fs::write(directory.path().join("cloud-root.txt"), &root)?;
    let mut running = None;
    eprintln!("Authenticated live account. Isolated cloud root: {root}");
    let outcome = tokio::time::timeout(Duration::from_secs(900), async {
        match scenario {
            LiveScenario::RoundTrip => {
                exercise(cloud.clone(), &root, directory.path(), &key, &mut running).await
            }
            LiveScenario::UploadBenchmark => {
                benchmark_upload(cloud.clone(), &root, directory.path(), &key, &mut running).await
            }
        }
    })
    .await
    .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> { "live S3 test timed out".into() })
    .and_then(|result| result);
    if let Err(error) = &outcome {
        eprintln!("Live S3 operation failed: {error}");
    }
    if let Some(server) = running.take()
        && let Err(error) = server.stop().await
    {
        eprintln!("Live server shutdown failed: {error}");
    }
    let cleanup = async {
        match cloud.remove(&root).await {
            Ok(()) | Err(cmr_api::Error::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        match cloud.stat(&root).await {
            Err(cmr_api::Error::NotFound(_)) => Ok(()),
            Ok(_) => Err("the temporary cloud root still exists".into()),
            Err(error) => Err(error.into()),
        }
    }
    .await;
    if let Err(error) = cleanup {
        let retained = directory.keep();
        eprintln!(
            "Live cloud cleanup failed: {error}. Key and state retained at {}",
            retained.display()
        );
        return Err(error);
    }
    eprintln!("Live temporary cloud root removal passed");
    outcome
}

#[tokio::test]
#[ignore = "writes an isolated Cloud Mail.ru folder using CMR_LOGIN and CMR_TOKEN"]
async fn live_signed_s3_round_trip_and_recovery() -> Result<()> {
    run_live(LiveScenario::RoundTrip).await
}

#[tokio::test]
#[ignore = "uploads 300 MiB to an isolated Cloud Mail.ru folder using CMR_LOGIN and CMR_TOKEN"]
async fn live_50_mib_upload_benchmark() -> Result<()> {
    run_live(LiveScenario::UploadBenchmark).await
}
