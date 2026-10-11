use std::{pin::Pin, time::SystemTime};

use bytes::Bytes;
use futures_core::Stream;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use crate::{ContentHash, Error, Result};

/// File, folder, or unrecognized provider metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ObjectType {
    /// A file with byte content.
    File(FileMetadata),
    /// A folder with child objects.
    Folder(FolderMetadata),
    /// An unrecognized provider type, preserved as received.
    #[serde(untagged)]
    Other(OtherMetadata),
}

/// Metadata for a file or folder in a Cloud Mail.ru account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudObject {
    /// The absolute account path.
    pub home: String,
    /// The file or folder name.
    pub name: String,
    /// The provider classification.
    pub kind: String,
    /// The object type and its associated metadata.
    #[serde(flatten)]
    pub object_type: ObjectType,
}

/// Metadata required to identify and download a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMetadata {
    /// The complete file size in bytes.
    pub size: u64,
    /// The Mail.ru hash of the complete file.
    pub hash: ContentHash,
    /// The modification time, with whole-second precision from the provider.
    #[serde(default, with = "crate::protocol::optional_time")]
    pub mtime: Option<SystemTime>,
}

/// Metadata specific to a folder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderMetadata {
    /// The folder size in bytes, if the provider supplies it.
    pub size: Option<u64>,
    /// The modification time, if available.
    #[serde(default, with = "crate::protocol::optional_time")]
    pub mtime: Option<SystemTime>,
    /// The numbers of direct child folders and files, if available.
    pub count: Option<FolderCount>,
}

/// Metadata whose provider type the client does not recognize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtherMetadata {
    /// The original provider type.
    #[serde(rename = "type")]
    pub object_type: String,
    /// The size in bytes, if available.
    pub size: Option<u64>,
    /// The Mail.ru hash, if available.
    pub hash: Option<ContentHash>,
    /// The modification time, if available.
    #[serde(default, with = "crate::protocol::optional_time")]
    pub mtime: Option<SystemTime>,
    /// The numbers of direct child folders and files, if available.
    pub count: Option<FolderCount>,
}

impl<'de> Deserialize<'de> for ObjectType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "lowercase")]
        enum Metadata {
            File(FileMetadata),
            Folder(FolderMetadata),
            #[serde(untagged)]
            Other(OtherMetadata),
        }

        match Metadata::deserialize(deserializer)? {
            Metadata::File(file) => Ok(Self::File(file)),
            Metadata::Folder(folder) => Ok(Self::Folder(folder)),
            // A malformed known type must not fall back to optional metadata.
            Metadata::Other(fields) if matches!(fields.object_type.as_str(), "file" | "folder") => {
                Err(D::Error::custom("invalid metadata for a known object type"))
            }
            Metadata::Other(fields) => Ok(Self::Other(fields)),
        }
    }
}

/// The number of direct child folders and files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderCount {
    /// The number of direct child folders.
    pub folders: u64,
    /// The number of direct child files.
    pub files: u64,
}

/// Uploaded bytes that the client can bind to an account path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadedContent {
    /// The Mail.ru hash of the complete content.
    pub hash: ContentHash,
    /// The content size in bytes.
    pub size: u64,
}

/// A byte range with inclusive offsets, starting at zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// The first byte offset, starting at zero.
    start: u64,
    /// The last byte offset, included in the range.
    end: u64,
}

impl ByteRange {
    /// Create a nonempty range with inclusive offsets.
    /// Return [`Error::InvalidRange`] if `start > end` or `end == u64::MAX`.
    /// The limit leaves room for the offset after the last byte.
    pub fn new(start: u64, end: u64) -> Result<Self> {
        if start > end || end == u64::MAX {
            return Err(Error::InvalidRange);
        }
        Ok(Self { start, end })
    }

    /// Get the first byte offset, starting at zero.
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Get the last byte offset, included in the range.
    pub fn end(&self) -> u64 {
        self.end
    }
}

/// A stream of downloaded byte chunks.
pub type DownloadStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;
/// A stream of cloud objects from a folder.
pub type ObjectStream<'a> = Pin<Box<dyn Stream<Item = Result<CloudObject>> + Send + 'a>>;

/// A lazy body stream and the metadata that the client uses for integrity validation.
///
/// Consume the stream to completion and handle its errors.
/// The client holds the final chunk until it accepts the size and full-file hash.
/// Earlier chunks can arrive before an integrity error.
/// Partial ranges support length validation but cannot establish full-file integrity.
pub struct Download {
    /// The file metadata captured before the download.
    pub metadata: CloudObject,
    /// The requested range, or `None` for the complete file.
    pub range: Option<ByteRange>,
    /// The byte stream. Read it to completion to receive all errors.
    pub stream: DownloadStream,
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;
    use std::time::{Duration, UNIX_EPOCH};

    proptest! {
        #[test]
        fn metadata_round_trips_preserve_types_and_optional_fields(
            tag in 0..3u8,
            extension in "[a-z-]{0,30}",
            name in ".{0,40}",
            size in any::<u64>(),
            hash in any::<[u8; 20]>(),
            seconds in prop::option::of(any::<i32>()),
            count in prop::option::of((any::<u64>(), any::<u64>())),
            optional_size in any::<bool>(),
            optional_hash in any::<bool>(),
        ) {
            let mtime = seconds.map(|seconds| {
                let duration = Duration::from_secs(u64::from(seconds.unsigned_abs()));
                if seconds < 0 { UNIX_EPOCH - duration } else { UNIX_EPOCH + duration }
            });
            let hash = ContentHash::from_bytes(hash);
            let count = count.map(|(folders, files)| FolderCount { folders, files });
            let wire_type = match tag {
                0 => "file".to_owned(),
                1 => "folder".to_owned(),
                _ => format!("extension-{extension}"),
            };
            let object_type = match tag {
                0 => ObjectType::File(FileMetadata { size, hash, mtime }),
                1 => ObjectType::Folder(FolderMetadata {
                    size: optional_size.then_some(size), mtime, count,
                }),
                _ => ObjectType::Other(OtherMetadata {
                    object_type: wire_type.clone(),
                    size: optional_size.then_some(size),
                    hash: optional_hash.then_some(hash),
                    mtime, count,
                }),
            };
            let object = CloudObject {
                home: format!("/{name}"), name, kind: "provider-class".into(), object_type,
            };
            let mut wire = serde_json::to_value(&object).unwrap();
            prop_assert_eq!(&wire["type"], &serde_json::json!(wire_type));
            prop_assert_eq!(&wire["mtime"], &serde_json::json!(seconds));
            prop_assert_eq!(&wire["home"], &serde_json::json!(object.home));
            if seconds.is_none() {
                wire.as_object_mut().unwrap().remove("mtime");
            }
            prop_assert_eq!(serde_json::from_value::<CloudObject>(wire).unwrap(), object);
        }
    }

    #[test]
    fn byte_ranges_reject_reversed_or_overflowing_bounds() {
        for (start, end) in [(0, 0), (0, u64::MAX - 1), (u64::MAX - 1, u64::MAX - 1)] {
            let range = ByteRange::new(start, end).unwrap();
            assert_eq!((range.start(), range.end()), (start, end));
        }
        for (start, end) in [(1, 0), (0, u64::MAX), (u64::MAX, u64::MAX)] {
            assert!(matches!(
                ByteRange::new(start, end),
                Err(Error::InvalidRange)
            ));
        }
    }

    #[test]
    fn known_types_ignore_fields_that_they_do_not_use() {
        let folder: CloudObject = serde_json::from_value(serde_json::json!({
            "home": "/folder", "name": "folder", "kind": "folder", "type": "folder",
            "hash": "",
        }))
        .unwrap();
        assert!(matches!(folder.object_type, ObjectType::Folder(_)));

        let file: CloudObject = serde_json::from_value(serde_json::json!({
            "home": "/file", "name": "file", "kind": "file", "type": "file",
            "size": 0, "hash": "0000000000000000000000000000000000000000", "count": {},
        }))
        .unwrap();
        assert!(matches!(file.object_type, ObjectType::File(_)));
    }

    #[test]
    fn known_files_require_size_and_hash_instead_of_falling_back_to_other() {
        for field in ["size", "hash"] {
            for invalid in [
                None,
                Some(serde_json::Value::Null),
                Some(serde_json::json!("invalid")),
            ] {
                let mut value = serde_json::json!({
                    "home": "/a", "name": "a", "kind": "file", "type": "file",
                    "size": 0, "hash": "0000000000000000000000000000000000000000",
                });
                match &invalid {
                    Some(invalid) => value[field] = invalid.clone(),
                    None => {
                        value.as_object_mut().unwrap().remove(field);
                    }
                }
                assert!(
                    serde_json::from_value::<CloudObject>(value).is_err(),
                    "{field}, invalid={invalid:?}"
                );
            }
        }
    }
}
