//! Encrypted S3 storage backed by a Cloud Mail.ru account.

mod access;
mod checksum;
mod config;
mod crypto;
mod error;
mod index;
mod metadata;
mod objects;
mod s3;
mod server;
mod storage;

pub use config::{BucketConfig, BucketConfiguration, ServerConfiguration};
pub use crypto::EncryptionKey;
pub use error::{Error, Result};
pub use server::{serve, service};
pub use storage::{S3Backend, StorageConfig};
