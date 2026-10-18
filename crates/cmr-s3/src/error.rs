use s3s::{S3Error, S3ErrorCode};

/// A storage, encryption, or local state failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The Cloud Mail.ru operation failed.
    #[error("cloud operation failed: {0}")]
    Cloud(#[from] cmr_api::Error),
    /// A local file operation failed.
    #[error("local file operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// The local index failed.
    #[error("local index operation failed: {0}")]
    Index(#[from] rusqlite::Error),
    /// Stored metadata is invalid.
    #[error("stored metadata is invalid: {0}")]
    Json(#[from] serde_json::Error),
    /// The key, encrypted record, or format is invalid.
    #[error("encrypted storage authentication failed")]
    Encryption,
    /// The configuration cannot support the storage operation.
    #[error("invalid configuration: {0}")]
    Configuration(String),
    /// The random source failed.
    #[error("secure random generation failed")]
    Random,
    /// The incoming request cannot be accepted.
    #[error("S3 request failed: {0}")]
    S3(#[from] S3Error),
}

/// Result of a storage operation.
pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for S3Error {
    fn from(error: Error) -> Self {
        match error {
            Error::S3(error) => error,
            error => {
                tracing::error!(error = %error, "S3 storage operation failed");
                S3Error::with_message(S3ErrorCode::InternalError, "The storage operation failed")
            }
        }
    }
}
