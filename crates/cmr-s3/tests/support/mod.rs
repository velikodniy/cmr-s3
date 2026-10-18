#![allow(dead_code)]

pub mod signed;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use cmr_api::{
    ByteRange, CloudApi, CloudObject, ContentHash, Download, FileMetadata, FolderMetadata,
    ObjectStream, ObjectType, UploadedContent,
};
use s3s::S3Request;

struct Entry {
    bytes: Option<Bytes>,
    modified: SystemTime,
}

/// An external CloudApi implementation. It keeps encrypted bytes and provider-style paths.
pub struct MemoryCloud {
    entries: Mutex<BTreeMap<String, Entry>>,
    uploads: Mutex<BTreeMap<String, Bytes>>,
    fail_bind: Mutex<Option<(String, bool)>>,
    pause_bind: Mutex<Option<Pause>>,
    pause_download: Mutex<Option<Pause>>,
    lists: std::sync::atomic::AtomicUsize,
}

type Pause = (String, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);

impl Default for MemoryCloud {
    fn default() -> Self {
        Self {
            entries: Mutex::new(
                [(
                    String::from("/"),
                    Entry {
                        bytes: None,
                        modified: SystemTime::now(),
                    },
                )]
                .into(),
            ),
            uploads: Mutex::new(BTreeMap::new()),
            fail_bind: Mutex::new(None),
            pause_bind: Mutex::new(None),
            pause_download: Mutex::new(None),
            lists: Default::default(),
        }
    }
}

impl MemoryCloud {
    pub fn list_calls(&self) -> usize {
        self.lists.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn metadata(path: &str, entry: &Entry) -> CloudObject {
        let name = path.rsplit('/').next().unwrap_or("").into();
        let object_type = match &entry.bytes {
            Some(bytes) => ObjectType::File(FileMetadata {
                size: bytes.len() as u64,
                hash: ContentHash::compute(bytes),
                mtime: Some(entry.modified),
            }),
            None => ObjectType::Folder(FolderMetadata {
                size: None,
                mtime: Some(entry.modified),
                count: None,
            }),
        };
        CloudObject {
            home: path.into(),
            name,
            kind: if entry.bytes.is_some() {
                "file"
            } else {
                "folder"
            }
            .into(),
            object_type,
        }
    }

    pub fn bytes(&self) -> Vec<(String, Bytes)> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(path, entry)| entry.bytes.clone().map(|bytes| (path.clone(), bytes)))
            .collect()
    }

    pub fn corrupt(&self, path: &str) {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.get_mut(path).unwrap();
        let mut bytes = entry.bytes.as_ref().unwrap().to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        entry.bytes = Some(bytes.into());
    }

    pub fn age_all(&self, time: SystemTime) {
        for entry in self.entries.lock().unwrap().values_mut() {
            entry.modified = time;
        }
    }

    pub fn fail_next_bind(&self, matching: &str, after_write: bool) {
        *self.fail_bind.lock().unwrap() = Some((matching.into(), after_write));
    }

    pub fn pause_next_bind(
        &self,
        matching: &str,
        written: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) {
        *self.pause_bind.lock().unwrap() = Some((matching.into(), written, resume));
    }

    pub fn pause_next_download(
        &self,
        matching: &str,
        started: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) {
        *self.pause_download.lock().unwrap() = Some((matching.into(), started, resume));
    }
}

#[async_trait]
impl CloudApi for MemoryCloud {
    async fn stat(&self, path: &str) -> cmr_api::Result<CloudObject> {
        let entries = self.entries.lock().unwrap();
        entries
            .get(path)
            .map(|entry| Self::metadata(path, entry))
            .ok_or_else(|| cmr_api::Error::NotFound(path.into()))
    }

    fn list<'a>(&'a self, path: &'a str) -> ObjectStream<'a> {
        self.lists
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let prefix = format!("{}/", path.trim_end_matches('/'));
        let entries = self.entries.lock().unwrap();
        let items: Vec<_> = entries
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix) && !key[prefix.len()..].contains('/'))
            .map(|(key, value)| Ok(Self::metadata(key, value)))
            .collect();
        Box::pin(futures_util::stream::iter(items))
    }

    async fn upload(&self, bytes: Bytes) -> cmr_api::Result<UploadedContent> {
        let content = UploadedContent {
            hash: ContentHash::compute(&bytes),
            size: bytes.len() as u64,
        };
        self.uploads
            .lock()
            .unwrap()
            .insert(content.hash.to_string(), bytes);
        Ok(content)
    }

    async fn bind(
        &self,
        path: &str,
        content: UploadedContent,
        modified: SystemTime,
    ) -> cmr_api::Result<()> {
        let failure = {
            let mut failure = self.fail_bind.lock().unwrap();
            if failure
                .as_ref()
                .is_some_and(|(matching, _)| path.contains(matching))
            {
                failure.take().map(|(_, after)| after)
            } else {
                None
            }
        };
        if failure == Some(false) {
            return Err(cmr_api::Error::Transport);
        }
        let bytes = self.uploads.lock().unwrap()[&content.hash.to_string()].clone();
        self.entries.lock().unwrap().insert(
            path.into(),
            Entry {
                bytes: Some(bytes),
                modified,
            },
        );
        let pause = {
            let mut pause = self.pause_bind.lock().unwrap();
            if pause
                .as_ref()
                .is_some_and(|(matching, _, _)| path.contains(matching))
            {
                pause.take()
            } else {
                None
            }
        };
        if let Some((_, written, resume)) = pause {
            written.notify_one();
            resume.notified().await;
        }
        if failure == Some(true) {
            return Err(cmr_api::Error::Transport);
        }
        Ok(())
    }

    async fn download(&self, path: &str, range: Option<ByteRange>) -> cmr_api::Result<Download> {
        let pause = {
            let mut pause = self.pause_download.lock().unwrap();
            if pause
                .as_ref()
                .is_some_and(|(matching, _, _)| path.contains(matching))
            {
                pause.take()
            } else {
                None
            }
        };
        if let Some((_, started, resume)) = pause {
            started.notify_one();
            resume.notified().await;
        }
        let entries = self.entries.lock().unwrap();
        let entry = entries
            .get(path)
            .ok_or_else(|| cmr_api::Error::NotFound(path.into()))?;
        let bytes = entry.bytes.clone().ok_or(cmr_api::Error::NotFile)?;
        assert!(
            range.is_none(),
            "the adapter must authenticate whole encrypted chunks"
        );
        let metadata = Self::metadata(path, entry);
        Ok(Download {
            metadata,
            range,
            stream: Box::pin(futures_util::stream::once(async move { Ok(bytes) })),
        })
    }

    async fn mkdir(&self, path: &str) -> cmr_api::Result<()> {
        self.entries
            .lock()
            .unwrap()
            .entry(path.into())
            .or_insert(Entry {
                bytes: None,
                modified: SystemTime::now(),
            });
        Ok(())
    }

    async fn remove(&self, path: &str) -> cmr_api::Result<()> {
        let prefix = format!("{path}/");
        self.entries
            .lock()
            .unwrap()
            .retain(|key, _| key != path && !key.starts_with(&prefix));
        Ok(())
    }
}

pub fn request<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}
