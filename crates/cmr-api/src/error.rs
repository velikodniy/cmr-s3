use thiserror::Error;

/// A client failure without credentials, request URLs, or server response bodies.
#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("The account path must start with / and contain no zero byte")]
    InvalidPath,
    #[error("The byte range is invalid or outside the file")]
    InvalidRange,
    #[error("The modification time is outside the supported range")]
    InvalidTimestamp,
    /// The caller supplied a malformed content hash.
    #[error("The content hash must contain 40 hexadecimal digits")]
    InvalidHash,
    #[error("The credentials must contain a login and an application password")]
    InvalidCredentials,
    #[error("The HTTP client initialization failed")]
    ClientInitialization,
    #[error("The content hash calculation failed")]
    Hashing,
    #[error("The HTTP transfer failed")]
    Transport,
    #[error("The request timed out")]
    Timeout,
    #[error("Authentication failed")]
    Authentication,
    /// An HTTP failure, distinct from a status in the response body.
    #[error("The server returned HTTP status {0}")]
    Http(u16),
    /// A failure status from a JSON response body.
    #[error("The API returned status {0}")]
    Api(u16),
    #[error("The path does not exist: {0}")]
    NotFound(String),
    #[error("The cloud object is not a file")]
    NotFile,
    #[error("The cloud object is not a folder")]
    NotFolder,
    #[error("The response exceeds the size limit")]
    ResponseTooLarge,
    /// The response does not follow the expected protocol.
    #[error("The server response is invalid: {0}")]
    Protocol(&'static str),
    /// The content or its metadata differs from the expected value.
    #[error("The content failed integrity validation: {0}")]
    Integrity(&'static str),
    /// A binary result code whose meaning depends on the operation.
    #[error("The {operation} operation returned result code {code}")]
    Binary { operation: &'static str, code: u8 },
    /// The write can have completed, but the client cannot establish its outcome from metadata.
    /// Inspect the remote state before you repeat the operation.
    #[error("The {operation} outcome is unresolved for {path}")]
    UnresolvedWrite {
        operation: &'static str,
        path: String,
    },
}

impl Error {
    pub(crate) fn from_http(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else {
            Self::Transport
        }
    }

    /// True for response statuses that can require a token refresh.
    pub(crate) fn is_auth(&self) -> bool {
        matches!(self, Self::Http(401 | 403) | Self::Api(401 | 403))
    }

    /// True for failures that allow another read or repeatable upload.
    /// Writes require a separate decision based on their outcome.
    pub(crate) fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Transport
                | Self::Timeout
                | Self::Http(408 | 429 | 500..=599)
                | Self::Api(408 | 429 | 500..=599)
        )
    }

    /// True when a submitted write can have changed remote state.
    pub(crate) fn uncertain(&self) -> bool {
        matches!(
            self,
            Self::Transport
                | Self::Timeout
                | Self::Http(408 | 500..=599)
                | Self::Api(408 | 500..=599)
                | Self::Protocol(_)
                | Self::ResponseTooLarge
        )
    }
}

pub type Result<T> = std::result::Result<T, Error>;
