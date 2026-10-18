use std::{
    collections::HashSet,
    fs::File,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use cmr_api::CloudApi;
use futures_core::Stream;
use futures_util::StreamExt;
use s3s::{S3Error, S3ErrorCode, TrailingHeaders, dto::StreamingBlob, s3_error};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{OwnedRwLockReadGuard, RwLock, Semaphore},
};

use crate::{
    EncryptionKey, Error, Result, checksum,
    crypto::{Cipher, OVERHEAD},
    index::Index,
    metadata::{Checksums, Extent, Layout, Object, Part, Upload, now},
};

const MAX_RECORD: usize = 8 * 1024 * 1024;
const MIN_CHUNK_BYTES: usize = 1024;
const MAX_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const LAYOUT_FANOUT: usize = 10_000;
pub(crate) const MAX_PART: u64 = 5 * 1024 * 1024 * 1024;

/// Construction-time storage limits and paths.
#[derive(Clone, Debug)]
pub struct StorageConfig {
    /// Exclusively owned absolute Cloud Mail.ru folder.
    pub cloud_root: String,
    /// Private local directory for the index and incoming bodies.
    pub state_dir: PathBuf,
    /// Maximum cloud file size, including encryption overhead.
    pub chunk_bytes: usize,
    /// Maximum concurrent cloud content transfers.
    pub cloud_concurrency: usize,
    /// Aggregate incoming body allowance on local disk.
    pub staging_bytes: u64,
    /// Maximum idle time for an incomplete multipart upload.
    pub upload_expiry: Duration,
    /// Minimum age before unused cloud content is deleted.
    pub orphan_grace: Duration,
    /// Rebuild the committed catalog from cloud records instead of reusing the local index.
    pub rebuild_catalog: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            cloud_root: "/cmr-s3".into(),
            state_dir: PathBuf::from(".cmr-s3"),
            chunk_bytes: MAX_CHUNK_BYTES,
            cloud_concurrency: 2,
            staging_bytes: 20 * 1024 * 1024 * 1024,
            upload_expiry: Duration::from_secs(7 * 86400),
            orphan_grace: Duration::from_secs(86400),
            rebuild_catalog: false,
        }
    }
}

/// An encrypted S3 adapter with a recoverable catalog and persistent multipart uploads.
#[derive(Clone)]
pub struct S3Backend(pub(crate) Arc<Store>);

pub(crate) struct Store {
    pub cloud: Arc<dyn CloudApi>,
    pub config: StorageConfig,
    pub cipher: Cipher,
    pub index: Index,
    pub mutation: tokio::sync::Mutex<()>,
    pub activity: Arc<RwLock<()>>,
    cleanup: tokio::sync::Mutex<()>,
    pub transfers: Arc<Semaphore>,
    readers: Semaphore,
    staged: AtomicU64,
    poisoned: AtomicBool,
    _lock: File,
}

impl S3Backend {
    /// Create missing configured buckets without changing existing bucket identities or content.
    pub(crate) async fn provision_buckets(&self, buckets: &[crate::BucketConfig]) -> Result<()> {
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        for bucket in buckets {
            if self.0.index.bucket(&bucket.name)?.is_none() {
                self.0
                    .create_bucket(&crate::metadata::Bucket {
                        name: bucket.name.clone(),
                        id: uuid::Uuid::new_v4().to_string(),
                        created: now(),
                    })
                    .await?;
            }
        }
        Ok(())
    }

    /// Open owned cloud storage and rebuild its committed catalog.
    pub async fn open(
        cloud: Arc<dyn CloudApi>,
        config: StorageConfig,
        key: EncryptionKey,
    ) -> Result<Self> {
        if !config.cloud_root.starts_with('/')
            || config.cloud_root == "/"
            || config.cloud_root.ends_with('/')
            || config.cloud_root.contains('\0')
            || config
                .cloud_root
                .split('/')
                .any(|s| matches!(s, "." | ".."))
            || !(MIN_CHUNK_BYTES..=MAX_CHUNK_BYTES).contains(&config.chunk_bytes)
            || config.cloud_concurrency == 0
            || config.cloud_concurrency > Semaphore::MAX_PERMITS
            || config.staging_bytes == 0
        {
            return Err(Error::Configuration(
                "invalid storage path or resource limits".into(),
            ));
        }
        std::fs::create_dir_all(&config.state_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config.state_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let lock = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.state_dir.join("lock"))?;
        lock.try_lock()
            .map_err(|_| Error::Configuration("another server owns this state directory".into()))?;
        let cipher = Cipher::new(key);
        let marker_path = format!("{}/format", config.cloud_root);
        let marker = match cloud.download(&marker_path, None).await {
            Ok(download) => {
                let bytes = collect(download.stream, MAX_RECORD).await?;
                let plaintext = cipher.open("format", bytes)?;
                let value: serde_json::Value = serde_json::from_slice(&plaintext)?;
                if value["version"] != 1 || !value["id"].is_string() {
                    return Err(Error::Encryption);
                }
                value["id"].as_str().unwrap().to_owned()
            }
            Err(cmr_api::Error::NotFound(_)) => {
                match cloud.stat(&config.cloud_root).await {
                    Ok(_) => {
                        let mut children = cloud.list(&config.cloud_root);
                        if children.next().await.transpose()?.is_some() {
                            return Err(Error::Configuration(
                                "the cloud root contains an unknown storage format".into(),
                            ));
                        }
                    }
                    Err(cmr_api::Error::NotFound(_)) => cloud.mkdir(&config.cloud_root).await?,
                    Err(error) => return Err(error.into()),
                }
                let id = uuid::Uuid::new_v4().to_string();
                let bytes = cipher.seal(
                    "format",
                    serde_json::to_vec(&serde_json::json!({"version":1,"id":id}))?,
                )?;
                let content = cloud.upload(bytes.into()).await?;
                cloud.bind(&marker_path, content, SystemTime::now()).await?;
                id
            }
            Err(error) => return Err(error.into()),
        };
        for folder in ["buckets", "objects", "layouts", "data"] {
            cloud
                .mkdir(&format!("{}/{folder}", config.cloud_root))
                .await?;
        }
        let identity = cipher.name(&["state", &config.cloud_root, &marker]);
        let index = Index::open(&config.state_dir.join("index.sqlite3"), &identity)?;
        let stage_dir = config.state_dir.join("stage");
        std::fs::create_dir_all(&stage_dir)?;
        // Incoming bodies never count as completed parts. A process lock makes these leftovers safe to remove.
        for file in std::fs::read_dir(&stage_dir)? {
            std::fs::remove_file(file?.path())?;
        }
        let store = Arc::new(Store {
            cloud,
            readers: Semaphore::new(config.cloud_concurrency),
            transfers: Arc::new(Semaphore::new(config.cloud_concurrency)),
            config,
            cipher,
            index,
            mutation: tokio::sync::Mutex::new(()),
            activity: Arc::new(RwLock::new(())),
            cleanup: tokio::sync::Mutex::new(()),
            staged: AtomicU64::new(0),
            poisoned: AtomicBool::new(false),
            _lock: lock,
        });
        if store.config.rebuild_catalog || !store.index.ready()? {
            store.index.set_ready(false)?;
            store.rebuild().await?;
            store.index.set_ready(true)?;
        } else {
            store.reconcile().await?;
        }
        store.reconcile_uploads()?;
        Ok(Self(store))
    }

    /// Expire inactive uploads and remove old content without live references.
    /// Active operations and response streams delay cleanup until their content is no longer in use.
    pub async fn cleanup(&self) -> Result<()> {
        let store = &self.0;
        let _cleanup = store.cleanup.lock().await;
        let _activity = store.activity.write().await;
        let _mutation = store.mutation.lock().await;
        store.ready()?;
        // Provider times have second precision. Never sweep content written during this collection.
        let cutoff = store
            .config
            .orphan_grace
            .checked_add(Duration::from_secs(1))
            .and_then(|grace| SystemTime::now().checked_sub(grace));
        let mut roots = store.index.roots()?;
        for upload in store.index.uploads()? {
            if now().saturating_sub(upload.updated) >= store.config.upload_expiry.as_secs() {
                store.index.remove_upload(&upload.id)?;
            } else {
                roots.extend(upload.parts.values().map(|part| part.content.clone()));
            }
        }
        // New operations can only reference this snapshot or content newer than the cutoff.
        drop(_mutation);
        drop(_activity);
        let mut layouts = HashSet::new();
        let mut groups = HashSet::new();
        while let Some(id) = roots.pop() {
            if !layouts.insert(id.clone()) {
                continue;
            }
            match store.layout(&id).await? {
                Layout::Group { .. } => {
                    groups.insert(id);
                }
                Layout::Composite { extents, .. } => {
                    roots.extend(extents.into_iter().map(|e| e.content))
                }
            }
        }
        for (folder, live) in [("layouts", &layouts), ("data", &groups)] {
            let path = store.path(folder);
            // Finish pagination before changing this folder.
            let entries: Vec<_> = store.cloud.list(&path).collect().await;
            for entry in entries {
                let entry = entry?;
                let id = entry.name.trim_end_matches(".bin");
                let mut modified = match entry.object_type {
                    cmr_api::ObjectType::File(file) => file.mtime,
                    cmr_api::ObjectType::Folder(folder) => folder.mtime,
                    _ => None,
                };
                if folder == "data" && !live.contains(id) {
                    let mut children = store.cloud.list(&entry.home);
                    let mut youngest = None;
                    let mut unknown = false;
                    while let Some(child) = children.next().await {
                        match child?.object_type {
                            cmr_api::ObjectType::File(file) => match file.mtime {
                                Some(time) => {
                                    youngest =
                                        Some(youngest.map_or(time, |old: SystemTime| old.max(time)))
                                }
                                None => unknown = true,
                            },
                            _ => unknown = true,
                        }
                    }
                    // Empty groups left by interrupted ingestion retain their folder timestamp.
                    modified = if unknown { None } else { youngest.or(modified) };
                }
                // Unknown timestamps are not safe evidence of age.
                if !live.contains(id)
                    && modified
                        .zip(cutoff)
                        .is_some_and(|(time, cutoff)| time < cutoff)
                {
                    store.cloud.remove(&entry.home).await?;
                    if folder == "layouts" {
                        store.index.remove_layout(id)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Store {
    pub async fn lock(&self) -> Result<tokio::sync::MutexGuard<'_, ()>> {
        let guard = self.mutation.lock().await;
        self.ready()?;
        Ok(guard)
    }

    pub fn ready(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Error::Configuration(
                "restart the server to reconcile an uncertain catalog change".into(),
            ));
        }
        Ok(())
    }

    pub async fn active(&self) -> Result<Arc<OwnedRwLockReadGuard<()>>> {
        let guard = self.activity.clone().read_owned().await;
        Ok(Arc::new(guard))
    }

    pub fn path(&self, relative: &str) -> String {
        format!("{}/{relative}", self.config.cloud_root)
    }
    pub fn bucket_path(&self, bucket: &str) -> String {
        format!("buckets/{}.bin", self.cipher.name(&["bucket", bucket]))
    }
    pub fn object_path(&self, bucket: &str, key: &str) -> String {
        format!("objects/{}.bin", self.cipher.name(&["object", bucket, key]))
    }

    pub async fn record<T: DeserializeOwned>(&self, relative: &str) -> Result<T> {
        let _permit = self
            .transfers
            .acquire()
            .await
            .expect("open transfer semaphore");
        let download = self.cloud.download(&self.path(relative), None).await?;
        let bytes = collect(download.stream, MAX_RECORD + OVERHEAD).await?;
        Ok(serde_json::from_slice(&self.cipher.open(relative, bytes)?)?)
    }

    pub async fn write_record<T: Serialize>(
        &self,
        relative: &str,
        value: &T,
        mutable: bool,
    ) -> Result<()> {
        let plaintext = serde_json::to_vec(value)?;
        if plaintext.len() > MAX_RECORD {
            return Err(s3_error!(
                EntityTooLarge,
                "The metadata record exceeds the storage limit"
            )
            .into());
        }
        let _permit = self
            .transfers
            .acquire()
            .await
            .expect("open transfer semaphore");
        let bytes = Bytes::from(self.cipher.seal(relative, plaintext)?);
        let uploaded = self.cloud.upload(bytes.clone()).await?;
        // The mutation lock hides this fence during a healthy write. Cancellation keeps it set.
        if mutable {
            self.poisoned.store(true, Ordering::Release);
        }
        if let Err(error) = self
            .cloud
            .bind(&self.path(relative), uploaded, SystemTime::now())
            .await
        {
            // Reconcile a possibly successful publication with the exact prepared ciphertext.
            let matches = match self.cloud.download(&self.path(relative), None).await {
                Ok(download) => collect(download.stream, MAX_RECORD + OVERHEAD)
                    .await
                    .is_ok_and(|actual| actual == bytes),
                Err(_) => false,
            };
            if !matches {
                if mutable {
                    self.poisoned.store(true, Ordering::Release);
                }
                return Err(error.into());
            }
        }
        Ok(())
    }

    pub fn indexed<T>(&self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    pub async fn remove_record(&self, relative: &str) -> Result<()> {
        self.poisoned.store(true, Ordering::Release);
        if let Err(error) = self.cloud.remove(&self.path(relative)).await
            && !matches!(
                self.cloud.stat(&self.path(relative)).await,
                Err(cmr_api::Error::NotFound(_))
            )
        {
            self.poisoned.store(true, Ordering::Release);
            return Err(error.into());
        }
        Ok(())
    }

    async fn rebuild(&self) -> Result<()> {
        self.index.clear_catalog()?;
        let bucket_path = self.path("buckets");
        let mut buckets = self.cloud.list(&bucket_path);
        while let Some(entry) = buckets.next().await {
            let entry = entry?;
            let relative = format!("buckets/{}", entry.name);
            let bucket: crate::metadata::Bucket = self.record(&relative).await?;
            if self.bucket_path(&bucket.name) != relative {
                return Err(Error::Encryption);
            }
            self.index.save_bucket(&bucket)?;
        }
        let object_path = self.path("objects");
        let mut objects = self.cloud.list(&object_path);
        while let Some(entry) = objects.next().await {
            let entry = entry?;
            let relative = format!("objects/{}", entry.name);
            let object: Object = self.record(&relative).await?;
            if self.object_path(&object.bucket, &object.key) != relative
                || self.index.bucket(&object.bucket)?.is_none()
            {
                return Err(Error::Encryption);
            }
            self.index.save_object(&object)?;
        }
        Ok(())
    }

    async fn reconcile(&self) -> Result<()> {
        for (relative, bucket, key) in self.index.pending()? {
            if let Some(key) = key {
                match self.record::<Object>(&relative).await {
                    Ok(object) => {
                        if object.bucket != bucket
                            || object.key != key
                            || self.object_path(&bucket, &key) != relative
                        {
                            return Err(Error::Encryption);
                        }
                        self.index.save_object(&object)?;
                    }
                    Err(Error::Cloud(cmr_api::Error::NotFound(_))) => {
                        self.index.remove_object(&bucket, &key)?
                    }
                    Err(error) => return Err(error),
                }
            } else {
                match self.record::<crate::metadata::Bucket>(&relative).await {
                    Ok(record) => {
                        if record.name != bucket || self.bucket_path(&bucket) != relative {
                            return Err(Error::Encryption);
                        }
                        self.index.save_bucket(&record)?;
                    }
                    Err(Error::Cloud(cmr_api::Error::NotFound(_))) => {
                        self.index.remove_bucket(&bucket)?
                    }
                    Err(error) => return Err(error),
                }
            }
            self.index.finish(&relative)?;
        }
        Ok(())
    }

    fn reconcile_uploads(&self) -> Result<()> {
        // A cloud commit can precede the final local upload deletion during a crash.
        for upload in self.index.uploads()? {
            if self
                .index
                .object(&upload.bucket, &upload.key)?
                .is_some_and(|o| o.upload_id.as_deref() == Some(&upload.id))
            {
                self.index.remove_upload(&upload.id)?;
            }
        }
        Ok(())
    }

    pub async fn layout(&self, id: &str) -> Result<Layout> {
        if uuid::Uuid::parse_str(id).is_err() {
            return Err(Error::Encryption);
        }
        if let Some(layout) = self.index.layout(id)? {
            return Ok(layout);
        }
        let layout = self.record(&format!("layouts/{id}.bin")).await?;
        self.index.save_layout(id, &layout)?;
        Ok(layout)
    }

    async fn persist_layout(&self, layout: &Layout) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        self.write_record(&format!("layouts/{id}.bin"), layout, false)
            .await?;
        self.index.save_layout(&id, layout)?;
        Ok(id)
    }

    /// Resolve copied ranges into data groups and write a shallow tree of bounded records.
    pub async fn compose(self: &Arc<Self>, extents: Vec<Extent>) -> Result<String> {
        let mut builder = LayoutBuilder::default();
        for extent in extents {
            let end = extent
                .offset
                .checked_add(extent.size)
                .ok_or(Error::Encryption)?;
            let slices = self.slices(extent.content, extent.offset..end);
            futures_util::pin_mut!(slices);
            while let Some(slice) = slices.next().await {
                builder.push(self, slice?.extent, 0).await?;
            }
        }
        builder.finish(self).await
    }

    fn slices(
        self: &Arc<Self>,
        content: String,
        range: std::ops::Range<u64>,
    ) -> impl Stream<Item = Result<ContentSlice>> + Send + 'static {
        let store = self.clone();
        async_stream::try_stream! {
            let mut stack = vec![Visit::Enter(content, range)];
            let mut ancestors = HashSet::new();
            while let Some(visit) = stack.pop() {
                let (id, range) = match visit {
                    Visit::Leave(id) => { ancestors.remove(&id); continue; }
                    Visit::Enter(id, range) => (id, range),
                };
                if !ancestors.insert(id.clone()) { Err(Error::Encryption)?; }
                let layout = store.layout(&id).await?;
                if range.start > range.end || range.end > layout.size() { Err(Error::Encryption)?; }
                stack.push(Visit::Leave(id.clone()));
                match layout {
                    Layout::Group { size, chunk_size } => {
                        if chunk_size == 0 || chunk_size > (MAX_CHUNK_BYTES - OVERHEAD) as u64 { Err(Error::Encryption)?; }
                        yield ContentSlice {
                            extent: Extent { content: id, offset: range.start, size: range.end - range.start },
                            group_size: size,
                            chunk_size,
                        };
                    }
                    Layout::Composite { size, extents } => {
                        let mut offset = 0u64;
                        let mut selected = Vec::new();
                        for extent in extents {
                            let end = offset.checked_add(extent.size).ok_or(Error::Encryption)?;
                            if range.start < end && range.end > offset {
                                let start = range.start.saturating_sub(offset).checked_add(extent.offset).ok_or(Error::Encryption)?;
                                let end = (range.end - offset).min(extent.size).checked_add(extent.offset).ok_or(Error::Encryption)?;
                                selected.push(Visit::Enter(extent.content, start..end));
                            }
                            offset = end;
                        }
                        if offset != size { Err(Error::Encryption)?; }
                        stack.extend(selected.into_iter().rev());
                    }
                }
            }
        }
    }

    pub fn require_bucket(&self, bucket: &str) -> Result<crate::metadata::Bucket> {
        self.index
            .bucket(bucket)?
            .ok_or_else(|| s3_error!(NoSuchBucket).into())
    }

    pub fn require_object(&self, bucket: &str, key: &str) -> Result<Object> {
        self.require_bucket(bucket)?;
        self.index
            .object(bucket, key)?
            .ok_or_else(|| s3_error!(NoSuchKey).into())
    }

    pub fn require_upload(
        &self,
        bucket: &str,
        key: &str,
        id: &str,
    ) -> Result<crate::metadata::Upload> {
        self.require_bucket(bucket)?;
        self.index
            .upload(id)?
            .filter(|u| u.bucket == bucket && u.key == key)
            .ok_or_else(|| s3_error!(NoSuchUpload).into())
    }

    /// Record against current upload state, including concurrent changes to its other parts.
    pub async fn record_part(&self, upload: &Upload, mut part: Part) -> Result<Part> {
        let _mutation = self.lock().await?;
        let mut current = self.require_upload(&upload.bucket, &upload.key, &upload.id)?;
        part.modified = now();
        current.parts.insert(part.number, part.clone());
        current.updated = part.modified;
        self.index.save_upload(&current)?;
        Ok(part)
    }

    pub async fn hash_content(
        self: &Arc<Self>,
        content: String,
        range: std::ops::Range<u64>,
        algorithm: &str,
        guard: Arc<OwnedRwLockReadGuard<()>>,
    ) -> Result<(String, Checksums)> {
        let mut hasher = checksum::hasher(["MD5", algorithm])?;
        let mut stream = self.stream(content, range, guard);
        while let Some(bytes) = stream.next().await {
            hasher.update(&bytes.map_err(|_| s3_error!(InternalError))?);
        }
        let mut checksums = checksum::finish(hasher);
        let etag = checksum::etag(&checksums)?;
        checksums.values.retain(|name, _| name == algorithm);
        Ok((etag, checksums))
    }

    pub async fn create_bucket(&self, bucket: &crate::metadata::Bucket) -> Result<()> {
        let path = self.bucket_path(&bucket.name);
        self.index.begin(&path, &bucket.name, None)?;
        self.write_record(&path, bucket, true).await?;
        self.indexed(self.index.save_bucket(bucket))?;
        self.finish_publication(&path)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        let path = self.bucket_path(bucket);
        self.index.begin(&path, bucket, None)?;
        self.remove_record(&path).await?;
        self.indexed(self.index.remove_bucket(bucket))?;
        self.finish_publication(&path)
    }

    pub(super) fn finish_publication(&self, path: &str) -> Result<()> {
        self.indexed(self.index.finish(path))?;
        self.poisoned.store(false, Ordering::Release);
        Ok(())
    }

    pub async fn stage(
        self: &Arc<Self>,
        mut body: Option<StreamingBlob>,
        length: Option<i64>,
        expected: Checksums,
        algorithm: &str,
        content_md5: Option<&str>,
        trailers: Option<(TrailingHeaders, Vec<String>)>,
    ) -> Result<Staged> {
        if length.is_some_and(|length| length < 0 || length as u64 > MAX_PART) {
            return Err(s3_error!(EntityTooLarge).into());
        }
        // Trailer checksums arrive after the body. Compute the advertised set plus MD5 and the selected algorithm.
        let mut names: HashSet<&str> = expected.values.keys().map(String::as_str).collect();
        names.insert("MD5");
        names.insert(algorithm);
        if let Some((_, algorithms)) = &trailers {
            names.extend(algorithms.iter().map(String::as_str));
        }
        let mut hasher = checksum::hasher(names)?;
        let temp = tempfile::NamedTempFile::new_in(self.config.state_dir.join("stage"))?;
        let mut staged = Staged {
            file: temp,
            size: 0,
            checksums: Checksums::default(),
            etag: String::new(),
            store: self.clone(),
        };
        let mut file = tokio::fs::File::from_std(staged.file.reopen()?);
        if let Some(body) = &mut body {
            while let Some(bytes) = body.next().await {
                let bytes = bytes.map_err(|_| s3_error!(IncompleteBody))?;
                let size = bytes.len() as u64;
                if staged
                    .size
                    .checked_add(size)
                    .is_none_or(|total| total > MAX_PART)
                {
                    return Err(s3_error!(EntityTooLarge).into());
                }
                self.staged
                    .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                        used.checked_add(size)
                            .filter(|total| *total <= self.config.staging_bytes)
                    })
                    .map_err(|_| {
                        S3Error::with_message(
                            S3ErrorCode::SlowDown,
                            "The staging allowance is full",
                        )
                    })?;
                staged.size += size;
                hasher.update(&bytes);
                file.write_all(&bytes).await?;
            }
        }
        if length.is_some_and(|length| length as u64 != staged.size) {
            return Err(s3_error!(IncompleteBody).into());
        }
        file.sync_all().await?;
        let mut actual = checksum::finish(hasher);
        checksum::validate(&actual, &expected)?;
        if content_md5
            .is_some_and(|expected| actual.values.get("MD5").map(String::as_str) != Some(expected))
        {
            return Err(s3_error!(BadDigest).into());
        }
        if let Some((handle, algorithms)) = trailers {
            let headers = handle.take().ok_or_else(|| {
                s3_error!(InvalidRequest, "The declared checksum trailer is missing")
            })?;
            let mut expected = Checksums::default();
            for algorithm in algorithms {
                let suffix = checksum::ALGORITHMS
                    .iter()
                    .find(|(name, _)| *name == algorithm)
                    .expect("supported trailer algorithm")
                    .1;
                let value = headers
                    .get(format!("x-amz-checksum-{suffix}"))
                    .ok_or_else(|| {
                        s3_error!(InvalidRequest, "The declared checksum trailer is missing")
                    })?;
                expected.values.insert(
                    algorithm,
                    value.to_str().map_err(|_| s3_error!(InvalidDigest))?.into(),
                );
            }
            checksum::validate(&actual, &expected)?;
        }
        staged.etag = checksum::etag(&actual)?;
        actual.values.retain(|name, _| name == algorithm);
        staged.checksums = actual;
        Ok(staged)
    }

    pub async fn ingest(&self, staged: &Staged) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        self.cloud.mkdir(&self.path(&format!("data/{id}"))).await?;
        let chunk_size = self.config.chunk_bytes - OVERHEAD;
        let layout = Layout::Group {
            size: staged.size,
            chunk_size: chunk_size as u64,
        };
        // Immutable descriptors can transfer alongside content. Publication still waits for all writes.
        let content = async {
            let mut file = tokio::fs::File::from_std(staged.file.reopen()?);
            let mut remaining = staged.size;
            let mut number = 0;
            while remaining != 0 {
                let _permit = self
                    .transfers
                    .acquire()
                    .await
                    .expect("open transfer semaphore");
                let size = remaining.min(chunk_size as u64) as usize;
                let mut plaintext = Vec::with_capacity(size + OVERHEAD);
                plaintext.resize(size, 0);
                file.read_exact(&mut plaintext).await?;
                let relative = format!("data/{id}/{number:08}.bin");
                let encrypted = self.cipher.seal(&relative, plaintext)?;
                let content = self.cloud.upload(encrypted.into()).await?;
                self.cloud
                    .bind(&self.path(&relative), content, SystemTime::now())
                    .await?;
                remaining -= size as u64;
                number += 1;
            }
            Ok::<_, Error>(())
        };
        let descriptors = async {
            let created = now();
            let marker_path = format!("data/{id}/created.bin");
            let layout_path = format!("layouts/{id}.bin");
            tokio::try_join!(
                self.write_record(&marker_path, &created, false),
                self.write_record(&layout_path, &layout, false),
            )?;
            Ok::<_, Error>(())
        };
        tokio::try_join!(content, descriptors)?;
        self.index.save_layout(&id, &layout)?;
        Ok(id)
    }

    pub fn stream(
        self: &Arc<Self>,
        content: String,
        range: std::ops::Range<u64>,
        guard: Arc<OwnedRwLockReadGuard<()>>,
    ) -> StreamingBlob {
        let store = self.clone();
        let stream = async_stream::try_stream! {
            let _guard = guard;
            let _reader = store.readers.acquire().await.expect("open read semaphore");
            let slices = store.slices(content, range);
            futures_util::pin_mut!(slices);
            while let Some(slice) = slices.next().await {
                let ContentSlice { extent, group_size: size, chunk_size } = slice?;
                let end = extent.offset.checked_add(extent.size).ok_or(Error::Encryption)?;
                let range = extent.offset..end;
                if range.is_empty() { continue; }
                for number in range.start / chunk_size..=(range.end - 1) / chunk_size {
                    let _permit = store.transfers.acquire().await.expect("open transfer semaphore");
                    let relative = format!("data/{}/{number:08}.bin", extent.content);
                    let download = store.cloud.download(&store.path(&relative), None).await?;
                    let begin = number * chunk_size;
                    let expected = (size - begin).min(chunk_size);
                    let encrypted_size = expected as usize + OVERHEAD;
                    let bytes = collect_into(download.stream, encrypted_size, Vec::with_capacity(encrypted_size)).await?;
                    let plaintext = store.cipher.open(&relative, bytes)?;
                    if plaintext.len() as u64 != expected { Err(Error::Encryption)?; }
                    let start = range.start.saturating_sub(begin) as usize;
                    let end = (range.end - begin).min(expected) as usize;
                    let bytes = plaintext.slice(start..end);
                    // Release the network slot before waiting for a slow S3 consumer.
                    drop(_permit);
                    yield bytes;
                }
            }
        };
        StreamingBlob::wrap(SyncStream(Mutex::new(Box::pin(stream))))
    }
}

enum Visit {
    Enter(String, std::ops::Range<u64>),
    Leave(String),
}

struct ContentSlice {
    extent: Extent,
    group_size: u64,
    chunk_size: u64,
}

#[derive(Default)]
struct LayoutBuilder {
    levels: Vec<Vec<Extent>>,
}

impl LayoutBuilder {
    async fn page(store: &Store, extents: Vec<Extent>) -> Result<Extent> {
        let size = extents.iter().try_fold(0u64, |size, extent| {
            size.checked_add(extent.size).ok_or(Error::Encryption)
        })?;
        let content = store
            .persist_layout(&Layout::Composite { size, extents })
            .await?;
        Ok(Extent {
            content,
            offset: 0,
            size,
        })
    }

    async fn push(&mut self, store: &Store, mut extent: Extent, mut level: usize) -> Result<()> {
        if extent.size == 0 {
            return Ok(());
        }
        loop {
            if self.levels.len() <= level {
                self.levels.push(Vec::new());
            }
            let entries = &mut self.levels[level];
            if let Some(last) = entries.last_mut()
                && last.content == extent.content
                && last.offset.checked_add(last.size) == Some(extent.offset)
            {
                last.size = last
                    .size
                    .checked_add(extent.size)
                    .ok_or(Error::Encryption)?;
                return Ok(());
            }
            entries.push(extent);
            if entries.len() < LAYOUT_FANOUT {
                return Ok(());
            }
            extent = Self::page(store, std::mem::take(entries)).await?;
            level += 1;
        }
    }

    async fn finish(mut self, store: &Store) -> Result<String> {
        loop {
            let Some(level) = self.levels.iter().position(|entries| !entries.is_empty()) else {
                return store
                    .persist_layout(&Layout::Composite {
                        size: 0,
                        extents: vec![],
                    })
                    .await;
            };
            let entries = std::mem::take(&mut self.levels[level]);
            if self.levels.iter().skip(level + 1).all(Vec::is_empty) {
                if let [extent] = entries.as_slice()
                    && extent.offset == 0
                    && extent.size == store.layout(&extent.content).await?.size()
                {
                    return Ok(extent.content.clone());
                }
                return Ok(Self::page(store, entries).await?.content);
            }
            let extent = Self::page(store, entries).await?;
            self.push(store, extent, level + 1).await?;
        }
    }
}

pub(crate) struct Staged {
    file: tempfile::NamedTempFile,
    pub size: u64,
    pub checksums: Checksums,
    pub etag: String,
    store: Arc<Store>,
}

impl Drop for Staged {
    fn drop(&mut self) {
        self.store.staged.fetch_sub(self.size, Ordering::AcqRel);
    }
}

// s3s requires Sync streams. Polling behind a mutex safely adapts CloudApi's Send streams.
struct SyncStream(Mutex<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>);
impl Stream for SyncStream {
    type Item = Result<Bytes>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.lock().unwrap().as_mut().poll_next(cx)
    }
}

async fn collect(
    stream: Pin<Box<dyn Stream<Item = cmr_api::Result<Bytes>> + Send + '_>>,
    limit: usize,
) -> Result<Vec<u8>> {
    collect_into(stream, limit, Vec::new()).await
}

async fn collect_into(
    mut stream: Pin<Box<dyn Stream<Item = cmr_api::Result<Bytes>> + Send + '_>>,
    limit: usize,
    mut bytes: Vec<u8>,
) -> Result<Vec<u8>> {
    while let Some(part) = stream.next().await {
        let part = part?;
        if bytes
            .len()
            .checked_add(part.len())
            .is_none_or(|size| size > limit)
        {
            return Err(Error::Encryption);
        }
        bytes.extend_from_slice(&part);
    }
    Ok(bytes)
}

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod test_support;

#[cfg(test)]
mod tests {
    use super::test_support::request;
    use super::*;
    use s3s::{S3, dto::*};

    #[tokio::test]
    async fn restart_reconciles_cloud_publication_after_index_failure() {
        let state = tempfile::tempdir().unwrap();
        let cloud = Arc::new(test_support::MemoryCloud::default());
        let config = StorageConfig {
            state_dir: state.path().into(),
            ..Default::default()
        };
        let backend = S3Backend::open(
            cloud.clone(),
            config.clone(),
            EncryptionKey::from_bytes([7; 32]),
        )
        .await
        .unwrap();
        backend
            .create_bucket(request(CreateBucketInput {
                bucket: "bucket".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        backend
            .put_object(request(PutObjectInput {
                bucket: "bucket".into(),
                key: "recover".into(),
                body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"old"))),
                ..Default::default()
            }))
            .await
            .unwrap();
        let old = backend
            .0
            .index
            .object("bucket", "recover")
            .unwrap()
            .unwrap();
        backend.0.index.fail_object_writes().unwrap();
        assert!(
            backend
                .put_object(request(PutObjectInput {
                    bucket: "bucket".into(),
                    key: "recover".into(),
                    body: Some(StreamingBlob::from_bytes(Bytes::from_static(
                        b"cloud committed",
                    ))),
                    ..Default::default()
                }))
                .await
                .is_err()
        );
        // The cloud committed the replacement, but SQLite still holds the old object.
        let published: super::Object = backend
            .0
            .record(&backend.0.object_path("bucket", "recover"))
            .await
            .unwrap();
        assert_ne!(published.content, old.content);
        assert_eq!(
            backend
                .0
                .index
                .object("bucket", "recover")
                .unwrap()
                .unwrap()
                .content,
            old.content
        );
        assert_eq!(backend.0.index.pending().unwrap().len(), 1);
        assert!(
            backend
                .head_object(request(HeadObjectInput {
                    bucket: "bucket".into(),
                    key: "recover".into(),
                    ..Default::default()
                }))
                .await
                .is_err()
        );
        assert!(backend.cleanup().await.is_err());
        drop(backend);
        let backend = S3Backend::open(cloud, config, EncryptionKey::from_bytes([7; 32]))
            .await
            .unwrap();
        assert!(backend.0.index.pending().unwrap().is_empty());
        let mut body = backend
            .get_object(request(GetObjectInput {
                bucket: "bucket".into(),
                key: "recover".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output
            .body
            .unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(bytes, b"cloud committed");
    }

    #[tokio::test]
    async fn composition_preserves_order_across_bounded_metadata_records() {
        let state = tempfile::tempdir().unwrap();
        let backend = S3Backend::open(
            Arc::new(test_support::MemoryCloud::default()),
            StorageConfig {
                state_dir: state.path().into(),
                ..Default::default()
            },
            EncryptionKey::from_bytes([7; 32]),
        )
        .await
        .unwrap();
        backend
            .create_bucket(request(CreateBucketInput {
                bucket: "bucket".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        backend
            .put_object(request(PutObjectInput {
                bucket: "bucket".into(),
                key: "source".into(),
                body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"abc"))),
                ..Default::default()
            }))
            .await
            .unwrap();
        let source = backend.0.require_object("bucket", "source").unwrap();
        let mut extents: Vec<_> = (0..10_000)
            .map(|index| Extent {
                content: source.content.clone(),
                offset: if index % 2 == 0 { 2 } else { 0 },
                size: 1,
            })
            .collect();
        extents.extend((0..2).map(|_| Extent {
            content: source.content.clone(),
            offset: 1,
            size: 1,
        }));
        let content = backend.0.compose(extents).await.unwrap();
        let mut body = backend
            .0
            .stream(content, 0..10_002, backend.0.active().await.unwrap());
        let mut actual = Vec::new();
        while let Some(bytes) = body.next().await {
            actual.extend_from_slice(&bytes.unwrap());
        }
        let mut expected = b"ca".repeat(5_000);
        expected.extend_from_slice(b"bb");
        assert_eq!(actual, expected);
    }
}
