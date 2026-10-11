//! Cloud Mail.ru operations for the S3 backend.

mod api;
mod auth;
mod client;
mod defaults;
mod download;
mod error;
mod hash;
mod protocol;
mod transport;
mod types;

pub use api::CloudApi;
pub use client::Client;
pub use error::{Error, Result};
pub use hash::ContentHash;
pub use types::{
    ByteRange, CloudObject, Download, DownloadStream, FileMetadata, FolderCount, FolderMetadata,
    ObjectStream, ObjectType, OtherMetadata, UploadedContent,
};
