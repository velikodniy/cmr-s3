use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{ByteRange, CloudObject, Download, ObjectStream, Result, UploadedContent};

/// Cloud operations with automatic authentication and bounded recovery.
///
/// The caller must exclusively own the account folders that it mutates.
/// Operations can run concurrently. Conflicting mutations have no promised order.
/// To order mutations of the same path or overlapping folders, await each operation before starting the next.
/// Recoverable write failures repeat within the attempt budget, with the same content, path, and modification time.
/// If a write outcome remains uncertain, the client inspects metadata before reporting an unresolved write.
#[async_trait]
pub trait CloudApi: Send + Sync {
    /// Get file or folder metadata.
    async fn stat(&self, path: &str) -> Result<CloudObject>;

    /// Stream a folder's direct children, with automatic pagination.
    /// Requests start when the stream is polled. Errors arrive through the stream.
    /// Folder changes between requests can cause missing or repeated objects.
    fn list<'a>(&'a self, path: &'a str) -> ObjectStream<'a>;

    /// Upload bytes from memory and return their hash and size.
    /// The client retains the complete buffer until the upload finishes, for automatic retries.
    /// Call `bind` to give the content a path.
    /// Content of up to 20 bytes travels in the hash during binding.
    async fn upload(&self, bytes: Bytes) -> Result<UploadedContent>;

    /// Bind uploaded bytes to a path and overwrite any existing file.
    /// The parent folder must exist.
    /// The protocol rounds the modification time down to a whole second.
    async fn bind(&self, path: &str, content: UploadedContent, mtime: SystemTime) -> Result<()>;

    /// Download the complete file or an inclusive byte range as a lazy stream.
    async fn download(&self, path: &str, range: Option<ByteRange>) -> Result<Download>;

    /// Create one folder. The parent folder must exist.
    /// An existing folder counts as success.
    async fn mkdir(&self, path: &str) -> Result<()>;

    /// Remove a file or folder. An absent path counts as success.
    async fn remove(&self, path: &str) -> Result<()>;
}
