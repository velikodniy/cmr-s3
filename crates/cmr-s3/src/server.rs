use std::{future::Future, sync::Arc, time::Duration};

use http::{
    HeaderValue,
    header::{CONTENT_LENGTH, TRAILER, TRANSFER_ENCODING},
};
use hyper::body::Body as _;

use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use s3s::{
    auth::SimpleAuth,
    config::{S3Config, StaticConfigProvider},
    service::{S3Service, S3ServiceBuilder},
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
};

use crate::{BucketConfig, Error, Result, S3Backend, access::BucketAccess};

/// Create missing configured buckets and build a signed service with separate access for each bucket.
/// The service denies S3 bucket creation, bucket deletion, and copies between different buckets.
pub async fn service(backend: S3Backend, buckets: &[BucketConfig]) -> Result<S3Service> {
    crate::config::validate(buckets)?;
    backend.provision_buckets(buckets).await?;
    let mut auth = SimpleAuth::new();
    for bucket in buckets {
        auth.register(bucket.access_key.clone(), bucket.secret_key.clone());
    }
    let mut config = S3Config::default();
    config.normalize_forward_slash_path = false;
    config.aws_chunked_stream_max_chunk_size = 8 * 1024 * 1024;
    config.xml_max_body_size = 8 * 1024 * 1024;
    let mut builder = S3ServiceBuilder::new(backend);
    builder.set_auth(auth);
    builder.set_access(BucketAccess::new(buckets));
    builder.set_config(Arc::new(StaticConfigProvider::new(Arc::new(config))));
    Ok(builder.build())
}

/// Serve bounded concurrent requests until shutdown, then drain connections for up to 30 seconds.
pub async fn serve(
    listener: TcpListener,
    backend: S3Backend,
    buckets: &[BucketConfig],
    max_requests: usize,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<()> {
    if max_requests == 0 || max_requests > Semaphore::MAX_PERMITS / 2 {
        return Err(Error::Configuration(
            "request concurrency must be positive".into(),
        ));
    }
    let service = service(backend.clone(), buckets).await?;
    tracing::info!(address = %listener.local_addr()?, "S3 server listening");
    let admission = Arc::new(Semaphore::new(max_requests));
    let connections = Arc::new(Semaphore::new(max_requests.saturating_mul(2)));
    let (stop, receiver) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut cleanup = tokio::time::interval(Duration::from_secs(3600));
    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Startup recovery already runs before the listener is served.
    cleanup.tick().await;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result { tracing::error!(%error, "connection task failed"); }
            }
            _ = cleanup.tick() => {
                let backend = backend.clone();
                tasks.spawn(async move {
                    if let Err(error) = backend.cleanup().await { tracing::error!(%error, "storage cleanup failed"); }
                });
            }
            connection = listener.accept(), if connections.available_permits() > 0 => {
                let (stream, _) = connection?;
                let connection_permit = connections.clone().acquire_owned().await.expect("open connection semaphore");
                let service = service.clone(); let admission = admission.clone(); let mut stopped = receiver.clone();
                tasks.spawn(async move {
                    let _connection_permit = connection_permit;
                    let handler = hyper::service::service_fn(move |mut request: hyper::Request<hyper::body::Incoming>| {
                        let service = service.clone(); let admission = admission.clone();
                        async move {
                            let _permit = admission.acquire_owned().await.expect("open request semaphore");
                            // Go proxies can omit a signed zero length. Restore only proven-empty
                            // framing; s3s still verifies the original signed header and signature.
                            if !request.headers().contains_key(CONTENT_LENGTH)
                                && !request.headers().contains_key(TRANSFER_ENCODING)
                                && !request.headers().contains_key(TRAILER)
                                && request.body().is_end_stream()
                                && request.body().size_hint().exact() == Some(0)
                            {
                                request.headers_mut().insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
                            }
                            service.call(request.map(s3s::Body::from)).await
                        }
                    });
                    let mut builder = Builder::new(TokioExecutor::new());
                    builder.http2().max_concurrent_streams(max_requests.min(u32::MAX as usize) as u32);
                    let connection = builder.serve_connection(TokioIo::new(stream), handler);
                    tokio::pin!(connection);
                    tokio::select! {
                        result = &mut connection => { if let Err(error) = result { tracing::debug!(%error, "connection closed"); } }
                        _ = stopped.changed() => {
                            connection.as_mut().graceful_shutdown();
                            let _ = tokio::time::timeout(Duration::from_secs(30), connection).await;
                        }
                    }
                });
            }
        }
    }
    let _ = stop.send(true);
    if tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error, "connection task failed");
            }
        }
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    Ok(())
}
