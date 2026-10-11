use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared, try_join},
};
use reqwest::{RequestBuilder, Response, header};
use serde::{
    Deserialize,
    de::{DeserializeOwned, IgnoredAny},
};
use tokio::{sync::Semaphore, time::Instant};
use url::Url;

use crate::{
    ByteRange, CloudApi, CloudObject, ContentHash, Download, Error, ObjectStream, ObjectType,
    Result, UploadedContent,
    auth::{Authentication, Token},
    defaults, download,
    hash::INLINE_CONTENT_BYTES,
    protocol::{self, WriteOutcome},
    transport,
};

/// A client whose clones share HTTP connections, authentication, and service discovery.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    download_http: reqwest::Client,
    api: Url,
    auth: Authentication,
    metadata: Service,
    upload: Service,
    download: Service,
}

type ServiceLookup = Shared<BoxFuture<'static, Result<Arc<ServiceUrl>>>>;

struct Service {
    http: reqwest::Client,
    endpoint: Url,
    lookup: Mutex<Option<ServiceLookup>>,
}

struct ServiceUrl {
    url: Url,
    selected_at: Instant,
}

enum Replay<'a> {
    Safe,
    Reconcile {
        operation: &'static str,
        path: &'a str,
    },
}

pub(crate) struct RequestContext<'a> {
    token: Arc<Token>,
    service: Option<(&'a Service, Arc<ServiceUrl>)>,
}

pub(crate) struct Attempt<'a, T> {
    pub(crate) response: Result<T>,
    pub(crate) context: RequestContext<'a>,
    pub(crate) retry_after: Option<Duration>,
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Client {
    /// Create a client with default timeouts and limits.
    /// The first operation starts authentication.
    pub fn new(login: impl Into<String>, application_password: impl Into<String>) -> Result<Self> {
        let http = http_builder()
            .build()
            .map_err(|_| Error::ClientInitialization)?;
        let download_http = download_http_builder()
            .build()
            .map_err(|_| Error::ClientInitialization)?;
        let auth = Authentication::new(
            login.into(),
            application_password.into(),
            http.clone(),
            Url::parse(protocol::TOKEN_URL).expect("the token URL is fixed"),
        )?;
        let dispatcher = Url::parse(protocol::DISPATCHER_URL).expect("the dispatcher URL is fixed");
        Ok(Self {
            inner: Arc::new(Inner {
                metadata: Service::new(&http, &dispatcher, "m"),
                upload: Service::new(&http, &dispatcher, "u"),
                download: Service::new(&http, &dispatcher, "d"),
                http,
                download_http,
                auth,
                api: Url::parse(protocol::API_URL).expect("the API URL is fixed"),
            }),
        })
    }
}

#[async_trait]
impl CloudApi for Client {
    async fn stat(&self, path: &str) -> Result<CloudObject> {
        protocol::path(path)?;
        let request = self.inner.http.get(self.api_url("file")).query(&[
            ("home", path),
            ("offset", "0"),
            ("limit", protocol::METADATA_LIMIT),
        ]);
        self.json(request, Replay::Safe, "the object metadata is invalid")
            .await
            .map_err(|error| match error {
                Error::Api(404) => Error::NotFound(path.to_owned()),
                other => other,
            })
    }

    fn list<'a>(&'a self, path: &'a str) -> ObjectStream<'a> {
        Box::pin(async_stream::try_stream! {
            let mut offset = 0u64;
            loop {
                let objects = self.list_page(path, offset).await?;
                if objects.is_empty() {
                    break;
                }
                offset = offset.checked_add(objects.len() as u64)
                    .ok_or(Error::Protocol("the folder offset exceeds the integer limit"))?;
                for object in objects {
                    yield object;
                }
            }
        })
    }

    async fn upload(&self, bytes: Bytes) -> Result<UploadedContent> {
        let size = bytes.len() as u64;
        if bytes.len() <= INLINE_CONTENT_BYTES {
            return Ok(UploadedContent {
                hash: ContentHash::compute(&bytes),
                size,
            });
        }
        let (hash, reply) = try_join(hash_upload(bytes.clone()), async {
            let reply = self
                .transfer(&self.inner.upload, Replay::Safe, |url| {
                    self.inner
                        .http
                        .put(url)
                        .header(header::ACCEPT, "*/*")
                        .body(bytes.clone())
                        .timeout(defaults::UPLOAD_TIMEOUT)
                })
                .await?;
            protocol::upload_hash(&reply)
        })
        .await?;
        if reply != hash {
            return Err(Error::Integrity("the upload hash differs"));
        }
        Ok(UploadedContent { hash, size })
    }

    async fn bind(&self, path: &str, content: UploadedContent, mtime: SystemTime) -> Result<()> {
        protocol::path(path)?;
        match self
            .write_metadata("bind", path, protocol::bind(path, content, mtime)?)
            .await
            .map(protocol::bind_outcome)
        {
            Ok(WriteOutcome::Applied) => Ok(()),
            Ok(WriteOutcome::Rejected(code)) => Err(Error::Binary {
                operation: "bind",
                code,
            }),
            Ok(_) | Err(Error::UnresolvedWrite { .. }) => {
                self.resolve_bind(path, content, mtime).await
            }
            Err(error) => Err(error),
        }
    }

    async fn download(&self, path: &str, range: Option<ByteRange>) -> Result<Download> {
        protocol::path(path)?;
        let metadata = self.stat(path).await?;
        let ObjectType::File(file) = &metadata.object_type else {
            return Err(Error::NotFile);
        };
        let (size, hash) = (file.size, file.hash);
        if range.is_some_and(|range| range.end() >= size) {
            return Err(Error::InvalidRange);
        }
        Ok(Download {
            metadata,
            range,
            stream: download::stream(self.clone(), path.to_owned(), size, hash, range),
        })
    }

    async fn mkdir(&self, path: &str) -> Result<()> {
        protocol::path(path)?;
        match self
            .write_metadata("mkdir", path, protocol::mkdir(path))
            .await
            .map(protocol::mkdir_outcome)
        {
            Ok(WriteOutcome::Applied) => Ok(()),
            Ok(WriteOutcome::Rejected(code)) => Err(Error::Binary {
                operation: "mkdir",
                code,
            }),
            Ok(WriteOutcome::AlreadyExists) => self.resolve_mkdir(path, true).await,
            Ok(_) | Err(Error::UnresolvedWrite { .. }) => self.resolve_mkdir(path, false).await,
            Err(error) => Err(error),
        }
    }

    async fn remove(&self, path: &str) -> Result<()> {
        protocol::path(path)?;
        let request = self
            .inner
            .http
            .post(self.api_url("file/remove"))
            .form(&[("home", path)]);
        match self
            .json::<IgnoredAny>(
                request,
                Replay::Reconcile {
                    operation: "remove",
                    path,
                },
                "the removal response is invalid",
            )
            .await
        {
            Ok(_) | Err(Error::Api(404)) => Ok(()),
            Err(Error::UnresolvedWrite { .. }) => {
                if matches!(self.stat(path).await, Err(Error::NotFound(_))) {
                    Ok(())
                } else {
                    Err(Self::unresolved("remove", path))
                }
            }
            Err(error) => Err(error),
        }
    }
}

impl Client {
    fn api_url(&self, path: &str) -> Url {
        self.inner.api.join(path).expect("API paths are fixed")
    }

    async fn list_page(&self, path: &str, offset: u64) -> Result<Vec<CloudObject>> {
        protocol::path(path)?;
        let request = self
            .inner
            .http
            .post(self.api_url("folder"))
            .query(&[
                ("offset", offset),
                ("limit", defaults::FOLDER_PAGE_SIZE as u64),
            ])
            .form(&[("home", path)]);
        #[derive(Deserialize)]
        struct Page {
            list: Vec<CloudObject>,
        }
        let page: Page = self
            .json(request, Replay::Safe, "the folder page is invalid")
            .await
            .map_err(|error| match error {
                Error::Api(404) => Error::NotFound(path.to_owned()),
                other => other,
            })?;
        Ok(page.list)
    }

    fn unresolved(operation: &'static str, path: &str) -> Error {
        Error::UnresolvedWrite {
            operation,
            path: path.to_owned(),
        }
    }

    async fn resolve_bind(
        &self,
        path: &str,
        content: UploadedContent,
        mtime: SystemTime,
    ) -> Result<()> {
        let mtime = protocol::unix_seconds(mtime)?;
        if let Ok(item) = self.stat(path).await
            && bound_file_matches(&item, content, mtime)
        {
            return Ok(());
        }
        Err(Self::unresolved("bind", path))
    }

    async fn resolve_mkdir(&self, path: &str, known_exists: bool) -> Result<()> {
        match self.stat(path).await {
            Ok(item) if matches!(item.object_type, ObjectType::Folder(_)) => Ok(()),
            Ok(_) if known_exists => Err(Error::NotFolder),
            _ => Err(Self::unresolved("mkdir", path)),
        }
    }

    async fn json<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        replay: Replay<'_>,
        invalid_body: &'static str,
    ) -> Result<T> {
        self.retry(replay, || async {
            let token = self.inner.auth.token().await?;
            let request = request
                .try_clone()
                .expect("JSON request bodies are replayable")
                .query(&[("access_token", &token.access)])
                .timeout(defaults::CONTROL_TIMEOUT);
            let (response, retry_after) = transport::send(request).await;
            let response = match response {
                Ok(response) => read_json(response, invalid_body).await,
                Err(error) => Err(error),
            };
            Ok(Attempt {
                response,
                context: RequestContext {
                    token,
                    service: None,
                },
                retry_after,
            })
        })
        .await
    }

    async fn write_metadata(
        &self,
        operation: &'static str,
        path: &str,
        body: Bytes,
    ) -> Result<Bytes> {
        self.transfer(
            &self.inner.metadata,
            Replay::Reconcile { operation, path },
            |url| {
                self.inner
                    .http
                    .post(url)
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(body.clone())
                    .timeout(defaults::CONTROL_TIMEOUT)
            },
        )
        .await
    }

    async fn transfer(
        &self,
        service: &Service,
        replay: Replay<'_>,
        request: impl Fn(Url) -> RequestBuilder,
    ) -> Result<Bytes> {
        self.retry(replay, || async {
            let (token, selected) = try_join(self.inner.auth.token(), service.get()).await?;
            let request = request(selected.url.clone()).query(&[
                ("token", token.access.as_str()),
                ("client_id", protocol::CLIENT_ID),
            ]);
            let (response, retry_after) =
                transport::buffered(request, defaults::MAX_CONTROL_RESPONSE_BYTES).await;
            Ok(Attempt {
                response,
                context: RequestContext {
                    token,
                    service: Some((service, selected)),
                },
                retry_after,
            })
        })
        .await
    }

    async fn retry<'a, T, F>(&self, replay: Replay<'_>, mut send: impl FnMut() -> F) -> Result<T>
    where
        F: Future<Output = Result<Attempt<'a, T>>>,
    {
        for attempt in 0..defaults::MAX_ATTEMPTS {
            // Authentication and discovery own their budgets. A failure before sending is terminal.
            let Attempt {
                response,
                context,
                retry_after,
            } = send().await?;
            match response {
                Ok(reply) => return Ok(reply),
                Err(error) => {
                    let retry = self.recover(&context, &error);
                    if let Replay::Reconcile { operation, path } = replay
                        && error.uncertain()
                    {
                        return Err(Self::unresolved(operation, path));
                    }
                    if !retry || attempt + 1 == defaults::MAX_ATTEMPTS {
                        return Err(error);
                    }
                    transport::delay(attempt, retry_after).await;
                }
            }
        }
        unreachable!("the attempt budget is positive")
    }

    pub(crate) async fn download_attempt(
        &self,
        path: &str,
        range: Option<ByteRange>,
    ) -> Result<Attempt<'_, Response>> {
        let service = &self.inner.download;
        let (token, selected) = try_join(self.inner.auth.token(), service.get()).await?;
        let url = protocol::download_url(&selected.url, path)?;
        let mut request = self
            .inner
            .download_http
            .get(url)
            .query(&[
                ("token", token.access.as_str()),
                ("client_id", protocol::CLIENT_ID),
            ])
            .header(header::ACCEPT, "*/*")
            .header(header::ACCEPT_ENCODING, "identity");
        if let Some(range) = range {
            request = request.header(
                header::RANGE,
                format!("bytes={}-{}", range.start(), range.end()),
            );
        }
        let (response, retry_after) =
            tokio::time::timeout(defaults::DOWNLOAD_HEADER_TIMEOUT, transport::send(request))
                .await
                .unwrap_or((Err(Error::Timeout), None));
        let response = response.and_then(|response| {
            if response.status().is_success() {
                Ok(response)
            } else {
                Err(Error::Http(response.status().as_u16()))
            }
        });
        Ok(Attempt {
            response,
            context: RequestContext {
                token,
                service: Some((service, selected)),
            },
            retry_after,
        })
    }

    pub(crate) fn recover(&self, context: &RequestContext<'_>, error: &Error) -> bool {
        if error.is_auth() {
            self.inner.auth.reject(&context.token);
        }
        if matches!(
            error,
            Error::Transport | Error::Timeout | Error::Http(404 | 500..=599)
        ) && let Some((service, selected)) = &context.service
        {
            service.reject(selected);
        }
        error.retryable()
            || error.is_auth()
            || (context.service.is_some() && matches!(error, Error::Http(404)))
    }
}

impl Service {
    fn new(http: &reqwest::Client, dispatcher: &Url, path: &str) -> Self {
        Self {
            http: http.clone(),
            endpoint: dispatcher.join(path).expect("dispatcher paths are fixed"),
            lookup: Mutex::default(),
        }
    }

    fn reject(&self, selected: &Arc<ServiceUrl>) {
        let mut lookup = self.lookup.lock().unwrap();
        if let Some(Ok(current)) = lookup.as_ref().and_then(Shared::peek)
            && Arc::ptr_eq(current, selected)
        {
            *lookup = None;
        }
    }

    async fn get(&self) -> Result<Arc<ServiceUrl>> {
        let pending = {
            let mut lookup = self.lookup.lock().unwrap();
            // Shared keeps both the in-flight request and its completed result.
            match lookup.as_ref().and_then(Shared::peek) {
                Some(Ok(selected)) if selected.selected_at.elapsed() < defaults::SERVICE_TTL => {
                    return Ok(selected.clone());
                }
                Some(_) => *lookup = None,
                None => {}
            }
            lookup
                .get_or_insert_with(|| {
                    let request = self
                        .http
                        .get(self.endpoint.clone())
                        .timeout(defaults::CONTROL_TIMEOUT);
                    async move {
                        let bytes = transport::control_request(request).await?;
                        Ok(Arc::new(ServiceUrl {
                            url: protocol::service_url(&bytes)?,
                            selected_at: Instant::now(),
                        }))
                    }
                    .boxed()
                    .shared()
                })
                .clone()
        };
        pending.await
    }
}

async fn read_json<T: DeserializeOwned>(
    response: Response,
    invalid_body: &'static str,
) -> Result<T> {
    if response.status().as_u16() == 404 {
        let body = transport::read_limited(response, defaults::MAX_JSON_RESPONSE_BYTES).await?;
        return Err(protocol::json_http_error(404, &body));
    }
    protocol::json_body(
        transport::read_success(response, defaults::MAX_JSON_RESPONSE_BYTES).await?,
        invalid_body,
    )
}

fn http_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .https_only(true)
        .connect_timeout(defaults::CONNECT_TIMEOUT)
        .pool_idle_timeout(defaults::POOL_IDLE_TIMEOUT)
        .pool_max_idle_per_host(defaults::POOL_MAX_IDLE_PER_HOST)
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .gzip(true)
}

fn download_http_builder() -> reqwest::ClientBuilder {
    // Preserve byte offsets and content-encoding headers for download integrity checks.
    http_builder().no_gzip().no_brotli().no_deflate().no_zstd()
}

static HASH_JOBS: Semaphore = Semaphore::const_new(defaults::MAX_CONCURRENT_HASHES);

fn bound_file_matches(object: &CloudObject, content: UploadedContent, mtime: i64) -> bool {
    matches!(&object.object_type, ObjectType::File(file)
        if file.size == content.size
            && file.hash == content.hash
            && file.mtime.and_then(|time| protocol::unix_seconds(time).ok()) == Some(mtime))
}

async fn hash_upload(bytes: Bytes) -> Result<ContentHash> {
    if bytes.len() < defaults::BLOCKING_HASH_THRESHOLD {
        return Ok(ContentHash::compute(&bytes));
    }
    let permit = HASH_JOBS
        .acquire()
        .await
        .expect("the hashing semaphore stays open");
    tokio::task::spawn_blocking(move || {
        // Keep the permit until hashing finishes, even if the caller cancels its future.
        let _permit = permit;
        ContentHash::compute(&bytes)
    })
    .await
    .map_err(|_| Error::Hashing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{future::join_all, poll};
    use httpmock::{HttpMockResponse, Mock, MockServer};
    use proptest::prelude::*;
    use serde_json::json;
    use std::{
        io::Write,
        sync::atomic::{AtomicUsize, Ordering},
    };

    fn client(server: &MockServer) -> Client {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut client = Client::new("login", "password").unwrap();
        let inner = Arc::get_mut(&mut client.inner).unwrap();
        let http = |builder: reqwest::ClientBuilder| {
            builder
                .no_proxy()
                .https_only(false)
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap()
        };
        inner.http = http(http_builder());
        inner.download_http = http(download_http_builder());
        let base = server.url("/").parse().unwrap();
        inner.metadata = Service::new(&inner.http, &base, "m");
        inner.upload = Service::new(&inner.http, &base, "u");
        inner.download = Service::new(&inner.http, &base, "d");
        inner.api = base;
        inner.auth = Authentication::new(
            "login".into(),
            "password".into(),
            inner.http.clone(),
            server.url("/token").parse().unwrap(),
        )
        .unwrap();
        client
    }

    fn mock_token(server: &MockServer) -> Mock<'_> {
        server.mock(|when, then| {
            when.path("/token");
            then.status(200).json_body(json!({
                "access_token": "access", "refresh_token": "refresh",
                "token_type": "Bearer", "expires_in": 3600,
            }));
        })
    }

    #[tokio::test]
    async fn folder_stream_fetches_pages_lazily_and_preserves_later_errors() {
        use futures_util::{StreamExt, TryStreamExt};

        for final_status in [200, 400] {
            let server = MockServer::start_async().await;
            let token = mock_token(&server);
            let object = |name: &str| {
                json!({
                    "home": format!("/folder/{name}"), "name": name,
                    "kind": "folder", "type": "folder",
                })
            };
            let first = server.mock(|when, then| {
                when.method("POST")
                    .path("/folder")
                    .query_param("access_token", "access")
                    .query_param("offset", "0")
                    .query_param("limit", defaults::FOLDER_PAGE_SIZE.to_string())
                    .form_urlencoded_tuple("home", "/folder");
                then.status(200).json_body(json!({
                    "status": 200, "body": {"list": [object("a"), object("b")]},
                }));
            });
            let second = server.mock(|when, then| {
                when.method("POST")
                    .path("/folder")
                    .query_param("offset", "2");
                then.status(200).json_body(json!({
                    "status": 200, "body": {"list": [object("c")]},
                }));
            });
            let last = server.mock(|when, then| {
                when.method("POST")
                    .path("/folder")
                    .query_param("offset", "3");
                then.status(200).json_body(json!({
                    "status": final_status, "body": {"list": []},
                }));
            });
            let client = client(&server);
            let mut objects = client.list("/folder");
            token.assert_calls(0);
            first.assert_calls(0);

            for name in ["a", "b"] {
                assert_eq!(objects.try_next().await.unwrap().unwrap().name, name);
                second.assert_calls(0);
            }
            assert_eq!(objects.try_next().await.unwrap().unwrap().name, "c");
            last.assert_calls(0);
            match objects.try_next().await {
                Ok(None) => assert_eq!(final_status, 200),
                Err(Error::Api(400)) => assert_eq!(final_status, 400),
                other => panic!("unexpected folder stream result: {other:?}"),
            }
            assert!(objects.next().await.is_none());
            token.assert();
            first.assert();
            second.assert();
            last.assert();
        }
    }

    #[tokio::test]
    async fn uncertain_removal_inspects_metadata_without_repeating_the_write() {
        for metadata_status in [404, 200, 400] {
            let server = MockServer::start_async().await;
            mock_token(&server);
            let remove = server.mock(|when, then| {
                when.method("POST")
                    .path("/file/remove")
                    .query_param("access_token", "access")
                    .form_urlencoded_tuple("home", "/target");
                then.status(503);
            });
            let metadata = server.mock(|when, then| {
                when.method("GET")
                    .path("/file")
                    .query_param("home", "/target");
                then.status(metadata_status).json_body(json!({
                    "status": metadata_status, "body": {
                        "home": "/target", "name": "target", "kind": "folder", "type": "folder",
                    },
                }));
            });
            let result = client(&server).remove("/target").await;
            if metadata_status == 404 {
                result.unwrap();
            } else {
                assert!(
                    matches!(result, Err(Error::UnresolvedWrite { operation: "remove", path }) if path == "/target")
                );
            }
            remove.assert();
            metadata.assert();
        }
    }

    #[tokio::test]
    async fn upload_starts_while_hash_workers_are_busy() {
        let permits = HASH_JOBS
            .acquire_many(defaults::MAX_CONCURRENT_HASHES as u32)
            .await
            .unwrap();
        let bytes = Bytes::from(vec![42; defaults::BLOCKING_HASH_THRESHOLD]);
        let expected = UploadedContent {
            hash: ContentHash::compute(&bytes),
            size: bytes.len() as u64,
        };
        let received = Arc::new(tokio::sync::Notify::new());
        let signal = received.clone();
        let server = MockServer::start_async().await;
        mock_token(&server);
        let discovery = server.mock(|when, then| {
            when.path("/u");
            then.status(200).body(server.url("/upload"));
        });
        let expected_body = bytes.clone();
        let upload = server.mock(|when, then| {
            when.method("PUT").path("/upload");
            then.respond_with(move |request| {
                assert_eq!(request.body_ref(), expected_body.as_ref());
                signal.notify_one();
                HttpMockResponse::builder()
                    .status(200)
                    .body(expected.hash.to_string())
                    .build()
            });
        });
        let client = client(&server);
        let release = async {
            let sent = tokio::time::timeout(Duration::from_secs(2), received.notified()).await;
            drop(permits);
            sent.expect("upload must start before hashing acquires a worker");
        };
        let (actual, ()) = futures_util::future::join(client.upload(bytes), release).await;
        assert_eq!(actual.unwrap(), expected);
        discovery.assert();
        upload.assert();
    }

    #[tokio::test]
    async fn uncertain_metadata_writes_reconcile_without_repeating_the_write() {
        for operation in ["bind", "mkdir"] {
            let content = UploadedContent {
                hash: ContentHash::compute(b"a"),
                size: 1,
            };
            let server = MockServer::start_async().await;
            mock_token(&server);
            let discovery = server.mock(|when, then| {
                when.path("/m");
                then.status(200).body(server.url("/write"));
            });
            let write = server.mock(|when, then| {
                when.method("POST").path("/write");
                then.status(503);
            });
            let metadata = server.mock(|when, then| {
                when.method("GET").path("/file").query_param("home", "/target");
                let body = if operation == "bind" {
                    json!({"home":"/target", "name":"target", "kind":"file", "type":"file", "size":1, "hash":content.hash, "mtime":0})
                } else {
                    json!({"home":"/target", "name":"target", "kind":"folder", "type":"folder"})
                };
                then.status(200).json_body(json!({"status":200, "body":body}));
            });
            let client = client(&server);
            if operation == "bind" {
                client
                    .bind("/target", content, SystemTime::UNIX_EPOCH)
                    .await
                    .unwrap();
            } else {
                client.mkdir("/target").await.unwrap();
            }
            write.assert();
            metadata.assert();
            discovery.assert();
        }
    }

    #[tokio::test]
    async fn preparation_failures_do_not_repeat_their_budget_or_reconcile_a_write() {
        for (operation, failing_path) in
            [("mkdir", "/token"), ("mkdir", "/m"), ("remove", "/token")]
        {
            let server = MockServer::start_async().await;
            let token = if failing_path == "/token" {
                server.mock(|when, then| {
                    when.path("/token");
                    then.status(503);
                })
            } else {
                mock_token(&server)
            };
            let discovery = server.mock(|when, then| {
                when.path("/m");
                if failing_path == "/m" {
                    then.status(503);
                } else {
                    then.status(200).body(server.url("/write"));
                }
            });
            let metadata = server.mock(|when, then| {
                when.path("/file");
                then.status(503);
            });
            let write = server.mock(|when, then| {
                when.path(if operation == "mkdir" {
                    "/write"
                } else {
                    "/file/remove"
                });
                then.status(503);
            });
            let client = client(&server);
            let result = if operation == "mkdir" {
                client.mkdir("/target").await
            } else {
                client.remove("/target").await
            };
            assert!(matches!(result, Err(Error::Http(503))));
            if failing_path == "/token" {
                token.assert_calls(defaults::MAX_ATTEMPTS);
            } else {
                discovery.assert_calls(defaults::MAX_ATTEMPTS);
            }
            metadata.assert_calls(0);
            write.assert_calls(0);
        }
    }

    #[tokio::test]
    async fn rejected_requests_repeat_with_the_replacement_token() {
        for (operation, rejected_status) in [("stat", 200), ("stat", 401), ("mkdir", 401)] {
            let server = MockServer::start_async().await;
            let calls = AtomicUsize::new(0);
            let token = server.mock(|when, then| {
                when.path("/token");
                then.respond_with(move |_| {
                    let access = if calls.fetch_add(1, Ordering::SeqCst) == 0 { "old" } else { "new" };
                    HttpMockResponse::builder().status(200).body(json!({
                        "access_token":access, "refresh_token":"refresh", "token_type":"Bearer", "expires_in":3600
                    }).to_string()).build()
                });
            });
            server.mock(|when, then| {
                when.path("/m");
                then.status(200).body(server.url("/write"));
            });
            let (path, token_key) = if operation == "stat" {
                ("/file", "access_token")
            } else {
                ("/write", "token")
            };
            let rejected = server.mock(|when, then| {
                when.path(path).query_param(token_key, "old");
                then.status(rejected_status)
                    .json_body(json!({"status":401, "body":{}}));
            });
            let accepted = server.mock(|when, then| {
                when.path(path).query_param(token_key, "new");
                if operation == "stat" {
                    then.status(200).json_body(json!({"status":200, "body":{
                        "home":"/target", "name":"target", "kind":"folder", "type":"folder"
                    }}));
                } else {
                    then.status(200).body([0]);
                }
            });
            let client = client(&server);
            if operation == "stat" {
                client.stat("/target").await.unwrap();
            } else {
                client.mkdir("/target").await.unwrap();
            }
            token.assert_calls(2);
            rejected.assert();
            accepted.assert();
        }
    }

    #[tokio::test]
    async fn metadata_writes_rediscover_and_reach_the_replacement_within_the_budget() {
        for operation in ["bind", "mkdir"] {
            for replacement_status in [200, 404] {
                let attempts = if replacement_status == 200 {
                    2
                } else {
                    defaults::MAX_ATTEMPTS
                };
                let server = MockServer::start_async().await;
                mock_token(&server);
                let stale = server.url("/stale");
                let replacement = server.url("/replacement");
                let calls = std::sync::atomic::AtomicUsize::new(0);
                let discovery = server.mock(|when, then| {
                    when.path("/m");
                    then.respond_with(move |_| {
                        let first = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                        HttpMockResponse::builder()
                            .status(200)
                            .body(if first {
                                stale.as_str()
                            } else {
                                replacement.as_str()
                            })
                            .build()
                    });
                });
                let first = server.mock(|when, then| {
                    when.method("POST").path("/stale");
                    then.status(404);
                });
                let second = server.mock(|when, then| {
                    when.method("POST").path("/replacement");
                    then.status(replacement_status).body([0]);
                });
                let client = client(&server);
                let result = if operation == "bind" {
                    client
                        .bind(
                            "/target",
                            UploadedContent {
                                hash: ContentHash::compute(b"a"),
                                size: 1,
                            },
                            SystemTime::UNIX_EPOCH,
                        )
                        .await
                } else {
                    client.mkdir("/target").await
                };
                if replacement_status == 200 {
                    result.unwrap();
                } else {
                    assert!(matches!(result, Err(Error::Http(404))));
                }
                discovery.assert_calls(attempts);
                first.assert();
                second.assert_calls(attempts - 1);
            }
        }
    }

    proptest! {
        #[test]
        fn bind_reconciliation_requires_content_identity_and_the_requested_whole_second(
            hash in any::<[u8; 20]>(), size in any::<u64>(), nanos in any::<i64>(), difference in 0..6u8,
        ) {
            let seconds = nanos.div_euclid(1_000_000_000);
            let duration = Duration::from_nanos(nanos.unsigned_abs());
            let time = if nanos < 0 { SystemTime::UNIX_EPOCH - duration } else { SystemTime::UNIX_EPOCH + duration };
            let hash = ContentHash::from_bytes(hash);
            let mut file = crate::FileMetadata { size, hash, mtime: Some(time) };
            match difference {
                1 => file.size ^= 1,
                2 => {
                    let mut bytes = *hash.as_bytes();
                    bytes[0] ^= 1;
                    file.hash = ContentHash::from_bytes(bytes);
                }
                3 => file.mtime = Some(time + Duration::from_secs(1)),
                4 => file.mtime = None,
                _ => {}
            }
            let object_type = if difference == 5 {
                ObjectType::Folder(crate::FolderMetadata { size: Some(size), mtime: Some(time), count: None })
            } else { ObjectType::File(file) };
            let object = CloudObject { home: "/a".into(), name: "a".into(), kind: "file".into(), object_type };
            prop_assert_eq!(bound_file_matches(&object, UploadedContent { size, hash }, seconds), difference == 0);
        }

    }

    #[tokio::test]
    async fn gzip_control_responses_obey_the_decoded_size_limit() {
        let body = vec![b'x'; 8192];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&body).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < body.len() - 1);
        let server = MockServer::start_async().await;
        let reply = server.mock(|when, then| {
            when.path("/").header_includes("accept-encoding", "gzip");
            then.status(200)
                .header("content-encoding", "gzip")
                .body(compressed);
        });
        let client = client(&server);
        let request = || client.inner.http.get(server.url("/"));
        assert_eq!(
            transport::buffered(request(), body.len())
                .await
                .0
                .unwrap()
                .as_ref(),
            body
        );
        assert!(matches!(
            transport::buffered(request(), body.len() - 1).await.0,
            Err(Error::ResponseTooLarge)
        ));
        reply.assert_calls(2);
    }

    #[tokio::test]
    async fn authentication_and_discovery_are_both_started_before_either_finishes() {
        let server = MockServer::start_async().await;
        // Each request stays in flight long enough to observe both calls without timing a speedup.
        let token = server.mock(|when, then| {
            when.path("/token");
            then.status(200).delay(Duration::from_secs(10)).json_body(json!({
                "access_token":"access", "refresh_token":"refresh", "token_type":"Bearer", "expires_in":3600
            }));
        });
        let discovery = server.mock(|when, then| {
            when.path("/u");
            then.status(200)
                .delay(Duration::from_secs(10))
                .body(server.url("/upload"));
        });
        let client = client(&server);
        let preparing = client.upload(Bytes::from_static(b"content that exceeds twenty bytes"));
        tokio::pin!(preparing);
        let both_started = async {
            while token.calls_async().await == 0 || discovery.calls_async().await == 0 {
                tokio::task::yield_now().await;
            }
        };
        tokio::select! {
            result = &mut preparing => panic!("preparation finished too early: {}", result.is_ok()),
            result = tokio::time::timeout(Duration::from_secs(2), both_started) => result.unwrap(),
        }
        token.assert();
        discovery.assert();
    }

    #[tokio::test]
    async fn discovery_shares_failures_and_caches_success_until_expiry() {
        let server = MockServer::start_async().await;
        let mut failure = server.mock(|when, then| {
            when.path("/u");
            then.status(503).delay(Duration::from_millis(25));
        });
        let client = client(&server);
        let results = join_all((0..8).map(|_| client.inner.upload.get())).await;
        assert!(
            results
                .iter()
                .all(|result| matches!(result, Err(Error::Http(503))))
        );
        failure.assert_calls(defaults::MAX_ATTEMPTS);
        failure.delete();
        let success = server.mock(|when, then| {
            when.path("/u");
            then.status(200).body(server.url("/upload"));
        });
        let first = client.inner.upload.get().await.unwrap();
        assert!(Arc::ptr_eq(
            &first,
            &client.inner.upload.get().await.unwrap()
        ));
        success.assert();
        tokio::time::pause();
        tokio::time::advance(defaults::SERVICE_TTL).await;
        tokio::time::resume();
        let renewed = client.inner.upload.get().await.unwrap();
        assert!(!Arc::ptr_eq(&first, &renewed));
        success.assert_calls(2);
    }

    #[tokio::test]
    async fn cancelled_discovery_callers_do_not_strand_the_remaining_or_next_caller() {
        for keep_waiter in [false, true] {
            let server = MockServer::start_async().await;
            let reply = server.mock(|when, then| {
                when.path("/u");
                then.status(200).body(server.url("/upload"));
            });
            let client = client(&server);
            let mut first = client.inner.upload.get().boxed();
            assert!(poll!(&mut first).is_pending());
            let mut second = client.inner.upload.get().boxed();
            if keep_waiter {
                assert!(poll!(&mut second).is_pending());
            }
            drop(first);
            second.await.unwrap();
            reply.assert();
        }
    }

    #[tokio::test]
    async fn late_discovery_waiters_and_failures_cannot_restore_or_evict_an_endpoint() {
        let server = MockServer::start_async().await;
        mock_token(&server);
        let calls = AtomicUsize::new(0);
        let base = server.url("/");
        let discovery = server.mock(|when, then| {
            when.path("/u");
            then.respond_with(move |_| {
                let generation = calls.fetch_add(1, Ordering::SeqCst);
                HttpMockResponse::builder()
                    .status(200)
                    .body(format!("{base}upload-{generation}"))
                    .build()
            });
        });
        let client = client(&server);
        let mut first = client.inner.upload.get().boxed();
        let mut late = client.inner.upload.get().boxed();
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut late).is_pending());
        let old = first.await.unwrap();
        let context = RequestContext {
            token: client.inner.auth.token().await.unwrap(),
            service: Some((&client.inner.upload, old.clone())),
        };
        assert!(client.recover(&context, &Error::Http(404)));
        assert!(Arc::ptr_eq(&old, &late.await.unwrap()));
        assert!(client.inner.upload.lookup.lock().unwrap().is_none());
        let current = client.inner.upload.get().await.unwrap();
        assert!(!Arc::ptr_eq(&old, &current));
        client.recover(&context, &Error::Transport);
        assert!(Arc::ptr_eq(
            &current,
            &client.inner.upload.get().await.unwrap()
        ));
        discovery.assert_calls(2);
    }

    #[tokio::test]
    async fn full_and_range_downloads_reject_unrequested_compression() {
        use futures_util::TryStreamExt;
        let bytes = b"the original file bytes";
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        let compressed = encoder.finish().unwrap();
        let server = MockServer::start_async().await;
        mock_token(&server);
        let discovery = server.mock(|when, then| {
            when.path("/d");
            then.status(200).body(server.url("/download"));
        });
        let response = server.mock(|when, then| {
            when.path("/download/file")
                .header("accept-encoding", "identity");
            then.status(200)
                .header("content-encoding", "gzip")
                .body(compressed);
        });
        let client = client(&server);
        for range in [None, Some(ByteRange::new(1, 5).unwrap())] {
            let result = crate::download::stream(
                client.clone(),
                "/file".into(),
                bytes.len() as u64,
                crate::ContentHash::compute(bytes),
                range,
            )
            .try_collect::<Vec<_>>()
            .await;
            assert!(matches!(
                result,
                Err(Error::Protocol("the download uses content encoding"))
            ));
        }
        response.assert_calls(2);
        discovery.assert();
    }

    #[tokio::test]
    async fn interrupted_downloads_validate_metadata_before_resuming_with_one_attempt_budget() {
        use futures_util::{StreamExt, TryStreamExt};
        use std::{
            io::{BufRead, BufReader},
            net::TcpListener,
            sync::{atomic::AtomicBool, mpsc},
            thread,
        };
        let bytes = b"abcdefghijklmnopqrstuvwxyz0123456789";
        for (range, changed, response_status) in [
            (None, false, 206),
            (Some(ByteRange::new(2, 20).unwrap()), false, 206),
            (None, false, 200),
            (None, true, 206),
            (None, false, 503),
        ] {
            let start = range.map_or(0, |range| range.start() as usize);
            let end = range.map_or(bytes.len(), |range| range.end() as usize + 1);
            let cut = start + 5;
            let hash = ContentHash::compute(bytes);
            let server = MockServer::start_async().await;
            mock_token(&server);
            let inspected = Arc::new(AtomicBool::new(false));
            let inspected_metadata = inspected.clone();
            let metadata = server.mock(|when, then| {
                when.method("GET").path("/file").query_param("home", "/file");
                then.respond_with(move |_| {
                    inspected_metadata.store(true, Ordering::SeqCst);
                    let hash = if changed { ContentHash::compute(b"another version") } else { hash };
                    HttpMockResponse::builder().status(200).body(json!({"status":200, "body":{
                        "home":"/file", "name":"file", "kind":"file", "type":"file", "size":bytes.len(), "hash":hash
                    }}).to_string()).build()
                });
            });
            let discovery = server.mock(|when, then| {
                when.path("/d");
                then.status(200).body(server.url("/second"));
            });
            let resumed = server.mock(|when, then| {
                when.method("GET")
                    .path("/second/file")
                    .header("range", format!("bytes={cut}-{}", end - 1));
                then.respond_with(move |_| {
                    assert!(
                        inspected.load(Ordering::SeqCst),
                        "metadata must precede the resumed GET"
                    );
                    let reply = HttpMockResponse::builder().status(response_status);
                    if response_status == 206 {
                        reply
                            .header(
                                "content-range",
                                format!("bytes {cut}-{}/{}", end - 1, bytes.len()),
                            )
                            .body(&bytes[cut..end])
                            .build()
                    } else {
                        reply.body(bytes.as_slice()).build()
                    }
                });
            });

            // A raw socket is necessary here: mock libraries emit only complete HTTP bodies.
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let first_url =
                Url::parse(&format!("http://{}/first", listener.local_addr().unwrap())).unwrap();
            let (close, wait) = mpsc::channel();
            let worker = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert!(line.starts_with("GET /first/file?"));
                loop {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                let status = if range.is_some() { 206 } else { 200 };
                write!(
                    socket,
                    "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\n",
                    end - start
                )
                .unwrap();
                if range.is_some() {
                    write!(
                        socket,
                        "Content-Range: bytes {start}-{}/{}\r\n",
                        end - 1,
                        bytes.len()
                    )
                    .unwrap();
                }
                write!(socket, "Connection: close\r\n\r\n").unwrap();
                socket.write_all(&bytes[start..cut]).unwrap();
                socket.flush().unwrap();
                let _ = wait.recv_timeout(Duration::from_secs(5));
                // Drop the connection before the declared body ends.
            });
            let client = client(&server);
            let selected = Arc::new(ServiceUrl {
                url: first_url,
                selected_at: Instant::now(),
            });
            *client.inner.download.lookup.lock().unwrap() =
                Some(futures_util::future::ready(Ok(selected)).boxed().shared());
            let mut stream =
                crate::download::stream(client, "/file".into(), bytes.len() as u64, hash, range);
            let first = stream.next().await.unwrap().unwrap();
            assert_eq!(first.as_ref(), &bytes[start..cut]);
            close.send(()).unwrap();
            let remaining = stream.try_collect::<Vec<_>>().await;
            if changed {
                assert!(matches!(
                    remaining,
                    Err(Error::Integrity(
                        "the file metadata changed during download"
                    ))
                ));
            } else if response_status == 503 {
                assert!(matches!(remaining, Err(Error::Http(503))));
            } else {
                let actual: Vec<_> = first
                    .iter()
                    .copied()
                    .chain(remaining.unwrap().iter().flatten().copied())
                    .collect();
                assert_eq!(actual, &bytes[start..end]);
            }
            worker.join().unwrap();
            let retries = if response_status == 503 {
                defaults::MAX_ATTEMPTS - 1
            } else {
                1
            };
            metadata.assert_calls(retries);
            let resumed_requests = if changed { 0 } else { retries };
            discovery.assert_calls(resumed_requests);
            resumed.assert_calls(resumed_requests);
        }
    }

    #[tokio::test]
    async fn json_404_preserves_body_failures_and_limits() {
        let body = reqwest::Body::wrap_stream(futures_util::stream::iter([
            Ok(Bytes::from_static(b"{\"status\":404,\"body\":")),
            Err(std::io::Error::other("connection lost")),
        ]));
        let response = http::Response::builder()
            .status(404)
            .body(body)
            .unwrap()
            .into();
        let error = read_json::<IgnoredAny>(response, "invalid metadata")
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Transport));
        let response = http::Response::builder()
            .status(404)
            .body(vec![0; defaults::MAX_JSON_RESPONSE_BYTES + 1])
            .unwrap()
            .into();
        assert!(matches!(
            read_json::<IgnoredAny>(response, "invalid metadata").await,
            Err(Error::ResponseTooLarge)
        ));
    }
}
