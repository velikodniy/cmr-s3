//! Persisted bucket, object, content-layout, and multipart-upload metadata.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3s::dto::Timestamp;
use serde::{Deserialize, Serialize};

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn timestamp(seconds: u64) -> Timestamp {
    Timestamp::from(UNIX_EPOCH + Duration::from_secs(seconds))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Bucket {
    pub name: String,
    pub id: String,
    pub created: u64,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub(crate) struct Attributes {
    pub metadata: HashMap<String, String>,
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub(crate) struct Checksums {
    pub values: BTreeMap<String, String>,
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Object {
    pub bucket: String,
    pub key: String,
    pub content: String,
    pub size: u64,
    pub etag: String,
    pub modified: u64,
    pub attributes: Attributes,
    pub checksums: Checksums,
    pub parts: Vec<Part>,
    // Identifies a completed upload after a crash between cloud publication and local deletion.
    pub upload_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Extent {
    pub content: String,
    pub offset: u64,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Layout {
    Group { size: u64, chunk_size: u64 },
    Composite { size: u64, extents: Vec<Extent> },
}

impl Layout {
    pub fn size(&self) -> u64 {
        match self {
            Self::Group { size, .. } | Self::Composite { size, .. } => *size,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Part {
    pub number: i32,
    pub content: String,
    pub size: u64,
    pub etag: String,
    pub modified: u64,
    pub checksums: Checksums,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Upload {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub created: u64,
    pub updated: u64,
    pub attributes: Attributes,
    pub algorithm: String,
    pub checksum_type: String,
    pub parts: BTreeMap<i32, Part>,
}

pub(crate) fn content_encoding(value: Option<&str>) -> Option<String> {
    let encodings: Vec<_> = value?
        .split(',')
        .map(str::trim)
        .filter(|encoding| !encoding.is_empty() && !encoding.eq_ignore_ascii_case("aws-chunked"))
        .collect();
    (!encodings.is_empty()).then(|| encodings.join(","))
}

macro_rules! attributes {
    ($input:expr) => {
        $crate::metadata::Attributes {
            metadata: $input.metadata.clone().unwrap_or_default(),
            content_type: $input.content_type.clone(),
            cache_control: $input.cache_control.clone(),
            content_disposition: $input.content_disposition.clone(),
            content_encoding: $crate::metadata::content_encoding(
                $input.content_encoding.as_deref(),
            ),
            content_language: $input.content_language.clone(),
            expires: $input.expires.clone(),
        }
    };
}
pub(crate) use attributes;
