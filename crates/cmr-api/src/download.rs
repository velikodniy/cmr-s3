use bytes::Bytes;
use reqwest::{Response, header};

use crate::{
    ByteRange, Client, CloudApi, ContentHash, DownloadStream, Error, ObjectType, Result,
    client::Attempt, defaults, hash::ContentHasher, transport,
};

pub(crate) fn stream(
    client: Client,
    path: String,
    size: u64,
    hash: ContentHash,
    range: Option<ByteRange>,
) -> DownloadStream {
    Box::pin(async_stream::try_stream! {
        let mut state = DownloadState::new(size, hash, range);
        for attempt in 0..defaults::MAX_ATTEMPTS {
            if attempt > 0 {
                let metadata = client.stat(&path).await?;
                validate_metadata(metadata, size, hash)?;
            }
            let requested = if attempt == 0 { range } else { state.remaining_range()? };
            let Attempt { response, context, retry_after } = client.download_attempt(&path, requested).await?;
            let mut response = match response {
                Ok(response) => response,
                Err(error) => {
                    if !client.recover(&context, &error)
                        || attempt + 1 == defaults::MAX_ATTEMPTS
                    {
                        Err(error)?;
                    }
                    transport::delay(attempt, retry_after).await;
                    continue;
                }
            };
            let (mut position, response_end) = layout(&response, requested, size)?;
            // The file hasher covers the whole response when no bytes precede this attempt.
            // Partial downloads and resumed full responses need a separate response hash.
            let mut response_hasher = (response.status().as_u16() == 200
                && (state.file_hasher.is_none() || state.next != 0)).then(ContentHasher::new);
            let failure = loop {
                let chunk = tokio::time::timeout(defaults::READ_IDLE_TIMEOUT, response.chunk()).await;
                let chunk = match chunk {
                    Ok(Ok(Some(chunk))) => chunk,
                    Ok(Ok(None)) => {
                        break (position != response_end).then_some(Error::Transport);
                    }
                    Ok(Err(error)) => break Some(Error::from_http(error)),
                    Err(_) => break Some(Error::Timeout),
                };
                let chunk_end = position.checked_add(chunk.len() as u64)
                    .ok_or(Error::Integrity("the body length exceeds the integer limit"))?;
                if chunk_end > response_end {
                    Err(Error::Integrity("the body is longer than its declared range"))?;
                }
                if let Some(hasher) = &mut response_hasher { hasher.update(&chunk); }
                if let Some(selected) = state.select(chunk, position, chunk_end) {
                    yield selected;
                }
                position = chunk_end;
            };
            drop(response);
            if let Some(error) = failure {
                if !client.recover(&context, &error)
                    || state.complete() || attempt + 1 == defaults::MAX_ATTEMPTS {
                    Err(error)?;
                }
                transport::delay(attempt, None).await;
                continue;
            }
            if let Some(chunk) = state.finish(response_hasher)? { yield chunk; }
            break;
        }
    })
}

fn validate_metadata(metadata: crate::CloudObject, size: u64, hash: ContentHash) -> Result<()> {
    if matches!(metadata.object_type, ObjectType::File(file) if file.size == size && file.hash == hash)
    {
        Ok(())
    } else {
        Err(Error::Integrity(
            "the file metadata changed during download",
        ))
    }
}

struct DownloadState {
    next: u64,
    end: u64,
    hash: ContentHash,
    file_hasher: Option<ContentHasher>,
    final_chunk: Option<Bytes>,
}

impl DownloadState {
    fn new(size: u64, hash: ContentHash, range: Option<ByteRange>) -> Self {
        let next = range.map_or(0, |range| range.start());
        let end = range.map_or(size, |range| range.end() + 1);
        Self {
            next,
            end,
            hash,
            file_hasher: (next == 0 && end == size).then(ContentHasher::new),
            final_chunk: None,
        }
    }

    fn complete(&self) -> bool {
        self.next == self.end
    }

    fn remaining_range(&self) -> Result<Option<ByteRange>> {
        (self.next < self.end)
            .then(|| ByteRange::new(self.next, self.end - 1))
            .transpose()
    }

    // The caller checks the response bounds before selecting bytes.
    fn select(&mut self, chunk: Bytes, position: u64, chunk_end: u64) -> Option<Bytes> {
        let from = self.next.max(position);
        let to = self.end.min(chunk_end);
        if from >= to {
            return None;
        }
        let selected = if from == position && to == chunk_end {
            chunk
        } else {
            chunk.slice((from - position) as usize..(to - position) as usize)
        };
        if let Some(hasher) = &mut self.file_hasher {
            hasher.update(&selected);
        }
        self.next = to;
        if self.complete() {
            self.final_chunk = Some(selected);
            None
        } else {
            Some(selected)
        }
    }

    fn finish(self, response_hasher: Option<ContentHasher>) -> Result<Option<Bytes>> {
        if !self.complete() {
            return Err(Error::Integrity("the requested bytes are incomplete"));
        }
        if response_hasher.is_some_and(|hasher| hasher.finish() != self.hash) {
            return Err(Error::Integrity(
                "the full response hash differs from the metadata",
            ));
        }
        if self
            .file_hasher
            .is_some_and(|hasher| hasher.finish() != self.hash)
        {
            return Err(Error::Integrity("the file hash differs from the metadata"));
        }
        Ok(self.final_chunk)
    }
}

fn layout(response: &Response, requested: Option<ByteRange>, size: u64) -> Result<(u64, u64)> {
    if response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|value| value.as_bytes() != b"identity")
    {
        return Err(Error::Protocol("the download uses content encoding"));
    }
    let (start, end) = match response.status().as_u16() {
        200 => {
            if response.headers().contains_key(header::CONTENT_RANGE) {
                return Err(Error::Protocol(
                    "the complete response contains a range header",
                ));
            }
            (0, size)
        }
        206 => {
            let requested =
                requested.ok_or(Error::Protocol("the server returned an unsolicited range"))?;
            let value = response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .ok_or(Error::Protocol("the range header is absent"))?;
            let (start, end, total) = parse_content_range(value)?;
            if start != requested.start() || end != requested.end() || total != size {
                return Err(Error::Integrity(
                    "the response range differs from the request",
                ));
            }
            (start, end + 1)
        }
        _ => return Err(Error::Protocol("the download status is unexpected")),
    };
    if response
        .content_length()
        .is_some_and(|length| length != end - start)
    {
        return Err(Error::Integrity(
            "the response length differs from the expected length",
        ));
    }
    Ok((start, end))
}

fn parse_content_range(value: &str) -> Result<(u64, u64, u64)> {
    let error = || Error::Protocol("the range header is invalid");
    let value = value.strip_prefix("bytes ").ok_or_else(error)?;
    let (range, total) = value.split_once('/').ok_or_else(error)?;
    let (start, end) = range.split_once('-').ok_or_else(error)?;
    let parse = |value: &str| {
        // Integer parsing accepts a leading plus sign, but range fields do not.
        if value.starts_with('+') {
            return Err(error());
        }
        value.parse::<u64>().map_err(|_| error())
    };
    let (start, end, total) = (parse(start)?, parse(end)?, parse(total)?);
    if start > end || end >= total {
        return Err(error());
    }
    Ok((start, end, total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #[test]
        fn resuming_requires_the_original_file_identity(
            size in any::<u64>(), hash in any::<[u8; 20]>(), difference in 0..4u8,
        ) {
            let hash = ContentHash::from_bytes(hash);
            let mut file = crate::FileMetadata { size, hash, mtime: None };
            match difference {
                1 => file.size ^= 1,
                2 => {
                    let mut bytes = *hash.as_bytes();
                    bytes[0] ^= 1;
                    file.hash = ContentHash::from_bytes(bytes);
                }
                _ => {}
            }
            let object_type = if difference == 3 {
                ObjectType::Folder(crate::FolderMetadata { size: Some(size), mtime: None, count: None })
            } else {
                ObjectType::File(file)
            };
            let metadata = crate::CloudObject { home: "/a".into(), name: "a".into(), kind: "file".into(), object_type };
            let result = validate_metadata(metadata, size, hash);
            if difference == 0 {
                prop_assert!(result.is_ok());
            } else {
                prop_assert!(matches!(result, Err(Error::Integrity(_))));
            }
        }

        #[test]
        fn content_ranges_require_ordered_bounds_inside_the_file(
            start in any::<u64>(), end in any::<u64>(), total in any::<u64>(), padding in 0..4usize,
        ) {
            let header = format!("bytes {start:0padding$}-{end:0padding$}/{total:0padding$}");
            match parse_content_range(&header) {
                Ok(actual) => {
                    prop_assert!(start <= end && end < total);
                    prop_assert_eq!(actual, (start, end, total));
                }
                Err(Error::Protocol(_)) => prop_assert!(start > end || end >= total),
                Err(error) => prop_assert!(false, "unexpected error: {error}"),
            }
        }

        #[test]
        fn resumed_downloads_deliver_each_requested_byte_once(
            bytes in prop::collection::vec(any::<u8>(), 1..2048),
            bounds in any::<[usize; 2]>(),
            full in any::<bool>(),
            cut in any::<usize>(),
            chunk_size in 1..128usize,
            server_ignores_range in any::<bool>(),
        ) {
            let size = bytes.len();
            let mut bounds = bounds.map(|value| value % size);
            bounds.sort_unstable();
            let (start, end) = if full { (0, size) } else { (bounds[0], bounds[1] + 1) };
            let range = (!full).then(|| ByteRange::new(start as u64, end as u64 - 1).unwrap());
            let mut state = DownloadState::new(size as u64, ContentHash::compute(&bytes), range);
            let cut = start + cut % (end - start);
            let mut delivered = Vec::new();
            let mut position = start;
            for chunk in bytes[start..cut].chunks(chunk_size) {
                let chunk_end = position + chunk.len();
                if let Some(selected) = state.select(Bytes::copy_from_slice(chunk), position as u64, chunk_end as u64) {
                    delivered.extend_from_slice(&selected);
                }
                position = chunk_end;
            }
            let remaining = state.remaining_range().unwrap().unwrap();
            prop_assert_eq!((remaining.start(), remaining.end()), (cut as u64, end as u64 - 1));
            let (response_start, response_end) = if server_ignores_range { (0, size) } else { (cut, end) };
            let mut response_hasher = server_ignores_range.then(ContentHasher::new);
            position = response_start;
            for chunk in bytes[response_start..response_end].chunks(chunk_size) {
                if let Some(hasher) = &mut response_hasher {
                    hasher.update(chunk);
                }
                let chunk_end = position + chunk.len();
                if let Some(selected) = state.select(Bytes::copy_from_slice(chunk), position as u64, chunk_end as u64) {
                    delivered.extend_from_slice(&selected);
                }
                position = chunk_end;
            }
            // The final bytes remain withheld until length and hash validation succeeds.
            prop_assert!(delivered.len() < end - start);
            delivered.extend_from_slice(&state.finish(response_hasher).unwrap().unwrap());
            prop_assert_eq!(delivered, &bytes[start..end]);
        }

        #[test]
        fn corrupt_or_incomplete_files_never_release_the_final_chunk(
            bytes in prop::collection::vec(any::<u8>(), 1..2048), index in any::<usize>(),
        ) {
            let hash = ContentHash::compute(&bytes);
            let size = bytes.len();
            let mut corrupted = bytes.clone();
            corrupted[index % size] ^= 1;
            let mut state = DownloadState::new(size as u64, hash, None);
            prop_assert!(state.select(corrupted.into(), 0, size as u64).is_none());
            prop_assert!(matches!(state.finish(None), Err(Error::Integrity(_))));

            let mut incomplete = DownloadState::new(size as u64, hash, None);
            incomplete.select(Bytes::copy_from_slice(&bytes[..size - 1]), 0, size as u64 - 1);
            prop_assert!(matches!(incomplete.finish(None), Err(Error::Integrity(_))));
        }

        #[test]
        fn range_responses_must_match_all_requested_bounds(
            total in 1..=u64::MAX, a in any::<u64>(), b in any::<u64>(), mismatch in 0..4u8,
        ) {
            let end = a % total;
            let start = b % (end + 1);
            let requested = ByteRange::new(start, end).unwrap();
            let (actual_start, actual_end, actual_total) = match mismatch {
                0 => (start, end, total),
                1 => (start ^ 1, end, total),
                2 => (start, end ^ 1, total),
                _ => (start, end, total ^ 1),
            };
            let response = http::Response::builder()
                .status(206)
                .header("content-range", format!("bytes {actual_start}-{actual_end}/{actual_total}"))
                .body(reqwest::Body::wrap_stream(futures_util::stream::pending::<Result<Bytes>>()))
                .unwrap().into();
            if mismatch == 0 {
                prop_assert_eq!(layout(&response, Some(requested), total).unwrap(), (start, end + 1));
            } else {
                prop_assert!(layout(&response, Some(requested), total).is_err());
            }
        }
    }

    #[test]
    fn empty_downloads_still_require_the_empty_content_hash() {
        let state = DownloadState::new(0, ContentHash::compute(b""), None);
        assert!(state.finish(None).unwrap().is_none());
        let state = DownloadState::new(0, ContentHash::compute(b"a"), None);
        assert!(matches!(state.finish(None), Err(Error::Integrity(_))));
    }

    #[test]
    fn full_response_integrity_covers_bytes_outside_the_requested_range() {
        for corrupt_delivered in [false, true] {
            let bytes = b"abcdefghijklmnopqrstuvwxyz0123456789";
            let mut state =
                DownloadState::new(bytes.len() as u64, ContentHash::compute(bytes), None);
            let mut prefix = bytes[..10].to_vec();
            if corrupt_delivered {
                prefix[0] ^= 1;
            }
            assert!(state.select(prefix.into(), 0, 10).is_some());
            let mut response = bytes.to_vec();
            if !corrupt_delivered {
                response[0] ^= 1;
            }
            let mut hasher = ContentHasher::new();
            hasher.update(&response);
            assert!(
                state
                    .select(response.into(), 0, bytes.len() as u64)
                    .is_none()
            );
            assert!(matches!(
                state.finish(Some(hasher)),
                Err(Error::Integrity(_))
            ));
        }
    }

    #[test]
    fn partial_downloads_validate_full_response_bytes_outside_the_range() {
        let mut state = DownloadState::new(
            11,
            ContentHash::compute(b"hello world"),
            Some(ByteRange::new(0, 4).unwrap()),
        );
        let mut response_hasher = ContentHasher::new();
        response_hasher.update(b"hello wrong");
        assert!(
            state
                .select(Bytes::from_static(b"hello wrong"), 0, 11)
                .is_none()
        );
        assert!(matches!(
            state.finish(Some(response_hasher)),
            Err(Error::Integrity(_))
        ));
    }

    #[test]
    fn ambiguous_headers_and_lengths_cannot_establish_a_download_layout() {
        for (status, headers, range) in [
            (200, vec![("content-range", "bytes 0-0/1")], None),
            (200, vec![("content-encoding", "gzip")], None),
            (206, vec![("content-range", "bytes 0-0/1")], None),
            (206, vec![], Some(ByteRange::new(0, 0).unwrap())),
            (204, vec![], None),
        ] {
            let mut response = http::Response::builder().status(status);
            for (name, value) in headers {
                response = response.header(name, value);
            }
            let response = response.body("a").unwrap().into();
            assert!(matches!(
                layout(&response, range, 1),
                Err(Error::Protocol(_))
            ));
        }
        for length in [0, 2] {
            let response = http::Response::new(vec![b'a'; length]).into();
            assert!(matches!(
                layout(&response, None, 1),
                Err(Error::Integrity(_))
            ));
        }
    }

    #[test]
    fn content_ranges_reject_invalid_syntax_and_bounds() {
        for field in [
            "",
            "+1",
            "-1",
            " 1",
            "1 ",
            "1x",
            "١",
            "18446744073709551616",
        ] {
            for header in [
                format!("bytes {field}-2/3"),
                format!("bytes 0-{field}/3"),
                format!("bytes 0-0/{field}"),
            ] {
                assert!(
                    matches!(parse_content_range(&header), Err(Error::Protocol(_))),
                    "{header}"
                );
            }
        }
        for header in [
            "",
            "0-1/2",
            "bytes0-1/2",
            "bytes 0/2",
            "bytes 0-1",
            "bytes 0-1/*",
            "bytes */2",
            "bytes 0-1/2/3",
            "bytes 0-1-2/3",
            "bytes 2-1/3",
            "bytes 0-2/2",
            "bytes 0-0/0",
            "bytes 0-1/2\n",
        ] {
            assert!(
                matches!(parse_content_range(header), Err(Error::Protocol(_))),
                "{header}"
            );
        }
    }
}
