use std::time::{Duration, SystemTime};

use bytes::{Bytes, BytesMut};
use reqwest::{RequestBuilder, Response, header};

use crate::{Error, Result, defaults};

pub(crate) async fn control_request(request: RequestBuilder) -> Result<Bytes> {
    for attempt in 0..defaults::MAX_ATTEMPTS {
        let cloned = request
            .try_clone()
            .ok_or(Error::Protocol("the request cannot repeat"))?;
        let (result, retry_after) = buffered(cloned, defaults::MAX_CONTROL_RESPONSE_BYTES).await;
        match result {
            Ok(bytes) => return Ok(bytes),
            Err(error) => {
                if !error.retryable() || attempt + 1 == defaults::MAX_ATTEMPTS {
                    return Err(error);
                }
                delay(attempt, retry_after).await;
            }
        }
    }
    unreachable!("the attempt budget is positive")
}

pub(crate) async fn delay(attempt: usize, retry_after: Option<Duration>) {
    tokio::time::sleep(retry_delay(attempt, retry_after)).await;
}

fn retry_delay(attempt: usize, retry_after: Option<Duration>) -> Duration {
    let multiplier = 2u32.saturating_pow(attempt.try_into().unwrap_or(u32::MAX));
    let cap = defaults::RETRY_BASE_DELAY
        .saturating_mul(multiplier)
        .min(defaults::RETRY_MAX_DELAY);
    let jitter = rand::random_range(Duration::ZERO..=cap);
    jitter.max(
        retry_after
            .unwrap_or_default()
            .min(defaults::RETRY_AFTER_MAX),
    )
}

pub(crate) async fn send(request: RequestBuilder) -> (Result<Response>, Option<Duration>) {
    match request.send().await {
        Ok(response) => {
            let hint = retry_after(&response);
            (Ok(response), hint)
        }
        Err(error) => (Err(Error::from_http(error)), None),
    }
}

pub(crate) async fn buffered(
    request: RequestBuilder,
    limit: usize,
) -> (Result<Bytes>, Option<Duration>) {
    let (response, hint) = send(request).await;
    let result = match response {
        Ok(response) => read_success(response, limit).await,
        Err(error) => Err(error),
    };
    (result, hint)
}

pub(crate) async fn read_success(response: Response, limit: usize) -> Result<Bytes> {
    if !response.status().is_success() {
        return Err(Error::Http(response.status().as_u16()));
    }
    if !matches!(response.status().as_u16(), 200 | 201) {
        return Err(Error::Protocol("the success status is unexpected"));
    }
    read_limited(response, limit).await
}

pub(crate) async fn read_limited(mut response: Response, limit: usize) -> Result<Bytes> {
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        return Err(Error::ResponseTooLarge);
    }
    let mut bytes = BytesMut::new();
    while let Some(chunk) = response.chunk().await.map_err(Error::from_http)? {
        if chunk.len() > limit - bytes.len() {
            return Err(Error::ResponseTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes.freeze())
}

pub(crate) fn retry_after(response: &Response) -> Option<Duration> {
    let value = response.headers().get(header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let time = httpdate::parse_http_date(value).ok()?;
    Some(time.duration_since(SystemTime::now()).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn retry_delays_respect_the_server_hint_and_the_delay_caps(
            attempt in 0usize..defaults::MAX_ATTEMPTS,
            hint in prop::option::of(any::<u64>().prop_map(Duration::from_secs)),
        ) {
            let minimum = hint.unwrap_or_default().min(defaults::RETRY_AFTER_MAX);
            let delay = retry_delay(attempt, hint);
            prop_assert!(delay >= minimum);
            prop_assert!(delay <= defaults::RETRY_MAX_DELAY.max(minimum));
            if attempt == 0 {
                prop_assert!(delay <= defaults::RETRY_BASE_DELAY.max(minimum));
            }
        }

        #[test]
        fn response_limits_apply_to_the_complete_body_regardless_of_chunking(
            bytes in prop::collection::vec(any::<u8>(), 0..4096),
            allowance in -5i32..=5,
            chunk_size in 1..128usize,
            streamed in any::<bool>(),
        ) {
            let limit = (bytes.len() as i32 + allowance).max(0) as usize;
            let body = if streamed {
                let chunks: Vec<_> = bytes.chunks(chunk_size)
                    .map(|chunk| Ok::<_, std::io::Error>(Bytes::copy_from_slice(chunk))).collect();
                reqwest::Body::wrap_stream(stream::iter(chunks))
            } else {
                reqwest::Body::from(bytes.clone())
            };
            let response = http::Response::new(body).into();
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let result = runtime.block_on(read_success(response, limit));
            if bytes.len() <= limit {
                let actual = result.unwrap();
                prop_assert_eq!(actual.as_ref(), bytes.as_slice());
            } else {
                prop_assert!(matches!(result, Err(Error::ResponseTooLarge)));
            }
        }
    }

    #[tokio::test]
    async fn body_failures_do_not_return_partial_control_responses() {
        let body = reqwest::Body::wrap_stream(stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(std::io::Error::other("server-secret")),
        ]));
        let response = http::Response::new(body).into();
        let error = read_success(response, 1024).await.unwrap_err();
        assert!(matches!(error, Error::Transport));
        assert!(!format!("{error} {error:?}").contains("secret"));
    }

    #[test]
    fn retry_after_dates_and_invalid_values_do_not_overflow() {
        for (value, expected) in [
            ("0", Some(Duration::ZERO)),
            ("30", Some(Duration::from_secs(30))),
            ("18446744073709551615", Some(Duration::from_secs(u64::MAX))),
            ("Thu, 01 Jan 1970 00:00:00 GMT", Some(Duration::ZERO)),
            ("not a date", None),
            ("-1", None),
            ("18446744073709551616", None),
        ] {
            let response = http::Response::builder()
                .header("retry-after", value)
                .body("")
                .unwrap()
                .into();
            assert_eq!(retry_after(&response), expected);
        }
    }
}
