//! Operational defaults for the client.
//!
//! Each request allows four attempts, including its first attempt.
//! Reads, memory uploads, binding, folder creation, and removal repeat after recoverable failures.
//! These retries cover timeouts, HTTP or JSON status 408, status 429, and server errors from 500 through 599.
//! A service response with HTTP status 404 triggers discovery and a retry, including metadata writes.
//!
//! Authentication and service discovery have separate, bounded request budgets.
//! A download shares one attempt budget across header failures and body recovery.
//!
//! Backoff is a delay between attempts.
//! The delay doubles after each failure, up to five seconds.
//! Jitter chooses a random delay between zero and that limit.
//! A valid `Retry-After` header sets a minimum delay, capped at 30 seconds.
//! Writes also repeat after an authentication rejection or a service HTTP 404.
//! After write retries end, the client inspects metadata if any submitted attempt remains uncertain.
//! An uncertain reply that prevents further retries also triggers metadata inspection.
//! The caller owns its working folders and controls the order of conflicting mutations.
//!
//! Downloads have no total deadline because file sizes vary.
//! The client limits the wait for headers and each body read.
//! Caller delays between stream polls do not consume the body read timeout.
//! Control responses accept gzip. Response limits apply after decompression.
//! Downloads request unencoded bytes so that byte ranges and hashes refer to the stored file.
//! HTTPS connections negotiate HTTP/2 when the server supports it, with HTTP/1.1 as a fallback.

use std::time::Duration;

/// Maximum time to establish a connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time for a complete control request, including its response body.
pub const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum time to receive download headers.
pub const DOWNLOAD_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum time to wait for the next download body chunk.
pub const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum time for a memory upload, including its response body.
pub const UPLOAD_TIMEOUT: Duration = Duration::from_secs(300);
/// Maximum attempts for one request or download, including the first attempt.
pub const MAX_ATTEMPTS: usize = 4;
/// Initial upper limit for the random retry delay.
pub const RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
/// Maximum upper limit for the random retry delay.
pub const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
/// Maximum delay from a `Retry-After` header.
pub const RETRY_AFTER_MAX: Duration = Duration::from_secs(30);
/// Refresh tokens this long before expiry, or halfway through shorter lifetimes.
pub const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(60);
/// Maximum time to reuse a service URL without another dispatcher request.
pub const SERVICE_TTL: Duration = Duration::from_secs(600);
/// Maximum time to keep an unused HTTP connection.
pub const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// Maximum unused connections for each host in the shared HTTP pool.
pub const POOL_MAX_IDLE_PER_HOST: usize = 8;
/// Page size for the folder stream.
pub const FOLDER_PAGE_SIZE: u32 = 1_000;
/// Maximum JSON response size, including a folder page.
pub const MAX_JSON_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum response size for authentication, discovery, upload, and binary operations.
pub const MAX_CONTROL_RESPONSE_BYTES: usize = 64 * 1024;
/// Minimum upload size for hashing on a blocking worker, in bytes.
pub const BLOCKING_HASH_THRESHOLD: usize = 1024 * 1024;
/// Maximum concurrent upload hashes on blocking workers, across all clients.
pub const MAX_CONCURRENT_HASHES: usize = 2;
