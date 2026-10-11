use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use url::Url;

use crate::{ContentHash, Error, Result, UploadedContent};

pub(crate) const TOKEN_URL: &str = "https://o2.mail.ru/token";
pub(crate) const API_URL: &str = "https://cloud.mail.ru/api/m1/";
pub(crate) const DISPATCHER_URL: &str = "https://dispatcher.cloud.mail.ru/";
pub(crate) const CLIENT_ID: &str = "cloud-win";
pub(crate) const METADATA_LIMIT: &str = "2147483647";
const BIND_OPERATION: u64 = 103;
const MKDIR_OPERATION: u64 = 106;
const OVERWRITE: u64 = 1;

pub(crate) fn path(path: &str) -> Result<()> {
    if !path.starts_with('/') || path.contains('\0') {
        return Err(Error::InvalidPath);
    }
    Ok(())
}

fn integer(mut value: u64, output: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        output.push(byte | if value == 0 { 0 } else { 0x80 });
        if value == 0 {
            break;
        }
    }
}

fn string(value: &str, output: &mut Vec<u8>) {
    integer(value.len() as u64 + 1, output);
    output.extend_from_slice(value.as_bytes());
    output.push(0);
}

pub(crate) fn unix_seconds(time: SystemTime) -> Result<i64> {
    let seconds = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::from(duration.as_secs()),
        Err(error) => {
            // Round negative fractional timestamps down: -1.2 seconds becomes -2.
            let seconds_before_epoch = error.duration().as_nanos().div_ceil(1_000_000_000);
            // Timestamps before the Unix epoch are negative.
            -(seconds_before_epoch as i128)
        }
    };
    i64::try_from(seconds).map_err(|_| Error::InvalidTimestamp)
}

fn system_time(seconds: i64) -> Result<SystemTime> {
    let duration = Duration::from_secs(seconds.unsigned_abs());
    if seconds < 0 {
        UNIX_EPOCH.checked_sub(duration)
    } else {
        UNIX_EPOCH.checked_add(duration)
    }
    .ok_or(Error::InvalidTimestamp)
}

pub(crate) mod optional_time {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        time: &Option<SystemTime>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        time.map(unix_seconds)
            .transpose()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Option<SystemTime>, D::Error> {
        Option::<i64>::deserialize(deserializer)?
            .map(system_time)
            .transpose()
            .map_err(serde::de::Error::custom)
    }
}

pub(crate) fn bind(path: &str, content: UploadedContent, mtime: SystemTime) -> Result<Bytes> {
    let mtime = unix_seconds(mtime)?;
    let mut output = Vec::new();
    integer(BIND_OPERATION, &mut output);
    integer(0, &mut output);
    string(path, &mut output);
    integer(content.size, &mut output);
    integer(mtime as u64, &mut output);
    integer(0, &mut output);
    output.extend_from_slice(content.hash.as_bytes());
    integer(OVERWRITE, &mut output);
    Ok(output.into())
}

pub(crate) fn mkdir(path: &str) -> Bytes {
    let mut output = Vec::new();
    integer(MKDIR_OPERATION, &mut output);
    integer(0, &mut output);
    string(path, &mut output);
    integer(0, &mut output);
    output.into()
}

pub(crate) fn download_url(base: &Url, path: &str) -> Result<Url> {
    // Encode every byte except RFC 3986 unreserved characters.
    let mut encoded = String::new();
    for byte in path[1..].bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(encoded, "%{byte:02X}").expect("writing to a string cannot fail");
        }
    }
    // URL parsers normalize dot-only segments, so reject paths that lose their identity.
    if encoded == "." || encoded == ".." {
        return Err(Error::InvalidPath);
    }
    Url::parse(&format!(
        "{}/{encoded}",
        base.as_str().trim_end_matches('/')
    ))
    .map_err(|_| Error::InvalidPath)
}

pub(crate) fn service_url(bytes: &[u8]) -> Result<Url> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| Error::Protocol("the dispatcher response is not UTF-8"))?;
    let url = text
        .split_whitespace()
        .next()
        .ok_or(Error::Protocol("the dispatcher response is empty"))?;
    let url = Url::parse(url).map_err(|_| Error::Protocol("the service URL is invalid"))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Protocol("the service URL is not allowed"));
    }
    Ok(url)
}

pub(crate) fn upload_hash(bytes: &[u8]) -> Result<ContentHash> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| Error::Protocol("the upload response is not UTF-8"))?;
    let hash: ContentHash = text
        .split_whitespace()
        .next()
        .ok_or(Error::Protocol("the upload response is empty"))?
        .parse()
        .map_err(|_| Error::Protocol("the upload hash is invalid"))?;
    Ok(hash)
}

#[derive(Deserialize)]
struct Envelope<'a> {
    status: u16,
    #[serde(borrow)]
    body: &'a RawValue,
}

pub(crate) fn json_body<T: DeserializeOwned>(
    bytes: Bytes,
    invalid_body: &'static str,
) -> Result<T> {
    let envelope: Envelope = serde_json::from_slice(&bytes)
        .map_err(|_| Error::Protocol("the JSON response is invalid"))?;
    if envelope.status != 200 {
        return Err(Error::Api(envelope.status));
    }
    serde_json::from_str(envelope.body.get()).map_err(|_| Error::Protocol(invalid_body))
}

pub(crate) fn json_http_error(status: u16, bytes: &[u8]) -> Error {
    if status == 404
        && serde_json::from_slice::<Envelope>(bytes).is_ok_and(|envelope| envelope.status == 404)
    {
        Error::Api(404)
    } else {
        Error::Http(status)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WriteOutcome {
    Applied,
    AlreadyExists,
    Rejected(u8),
    Unknown,
}

pub(crate) fn bind_outcome(bytes: Bytes) -> WriteOutcome {
    match bytes.first().copied() {
        Some(0) => WriteOutcome::Applied,
        Some(code @ (1 | 5 | 7 | 10 | 253 | 254)) => WriteOutcome::Rejected(code),
        _ => WriteOutcome::Unknown,
    }
}

pub(crate) fn mkdir_outcome(bytes: Bytes) -> WriteOutcome {
    match bytes.first().copied() {
        Some(0) => WriteOutcome::Applied,
        Some(4 | 9) => WriteOutcome::AlreadyExists,
        Some(code @ (1 | 10 | 254)) => WriteOutcome::Rejected(code),
        _ => WriteOutcome::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CloudObject;
    use proptest::prelude::*;
    use serde::{Deserialize, Serialize, de::IgnoredAny};
    use serde_json::json;

    #[test]
    fn http_and_api_statuses_must_agree_before_a_path_is_missing() {
        for (http, api) in [(404, 404), (404, 400), (404, 200), (400, 404), (503, 404)] {
            let body = serde_json::to_vec(&json!({"status": api, "body": {}})).unwrap();
            let error = json_http_error(http, &body);
            if http == 404 && api == 404 {
                assert!(matches!(error, Error::Api(404)));
            } else {
                assert!(matches!(error, Error::Http(actual) if actual == http));
            }
        }
        for body in ["not JSON", "{\"status\":404}", "{\"body\":{}}"] {
            assert!(matches!(
                json_http_error(404, body.as_bytes()),
                Error::Http(404)
            ));
        }
    }

    #[test]
    fn binary_outcomes_distinguish_rejection_from_uncertainty_and_existing_folders() {
        for (bytes, bind, mkdir) in [
            (vec![0], WriteOutcome::Applied, WriteOutcome::Applied),
            (
                vec![1],
                WriteOutcome::Rejected(1),
                WriteOutcome::Rejected(1),
            ),
            (vec![4], WriteOutcome::Unknown, WriteOutcome::AlreadyExists),
            (vec![9], WriteOutcome::Unknown, WriteOutcome::AlreadyExists),
            (vec![5], WriteOutcome::Rejected(5), WriteOutcome::Unknown),
            (vec![7], WriteOutcome::Rejected(7), WriteOutcome::Unknown),
            (
                vec![10],
                WriteOutcome::Rejected(10),
                WriteOutcome::Rejected(10),
            ),
            (
                vec![253],
                WriteOutcome::Rejected(253),
                WriteOutcome::Unknown,
            ),
            (
                vec![254],
                WriteOutcome::Rejected(254),
                WriteOutcome::Rejected(254),
            ),
            (vec![255], WriteOutcome::Unknown, WriteOutcome::Unknown),
            (vec![], WriteOutcome::Unknown, WriteOutcome::Unknown),
        ] {
            assert_eq!(bind_outcome(bytes.clone().into()), bind);
            assert_eq!(mkdir_outcome(bytes.into()), mkdir);
        }
    }

    #[test]
    fn json_errors_do_not_require_a_success_body_shape() {
        for body in [json!(null), json!("failure"), json!({"error":"failure"})] {
            for status in [404, 429, 503] {
                let bytes = serde_json::to_vec(&json!({"status":status, "body":body})).unwrap();
                assert!(
                    matches!(json_body::<CloudObject>(bytes.into(), "invalid metadata"), Err(Error::Api(actual)) if actual == status)
                );
            }
        }
        assert!(matches!(
            json_body::<CloudObject>(
                Bytes::from_static(br#"{"status":200,"body":{}}"#),
                "invalid metadata"
            ),
            Err(Error::Protocol("invalid metadata"))
        ));
    }

    #[test]
    fn malformed_json_envelopes_are_protocol_errors() {
        for body in ["not JSON", "{\"status\":404}", "{\"body\":{}}"] {
            assert!(matches!(
                json_body::<IgnoredAny>(Bytes::from_static(body.as_bytes()), "invalid body"),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[test]
    fn wire_encoding() {
        let mut output = Vec::new();
        integer(300, &mut output);
        string("/a", &mut output);
        assert_eq!(output, [0xac, 0x02, 0x03, 0x2f, 0x61, 0]);
        let content = UploadedContent {
            hash: crate::ContentHash::from_bytes([0xaa; 20]),
            size: 300,
        };
        let encoded = bind("/a", content, UNIX_EPOCH - Duration::from_secs(1)).unwrap();
        let mut expected = vec![103, 0, 3, b'/', b'a', 0, 0xac, 2];
        expected.extend_from_slice(&[0xff; 9]);
        expected.extend_from_slice(&[1, 0]);
        expected.extend_from_slice(&[0xaa; 20]);
        expected.push(1);
        assert_eq!(encoded.as_ref(), expected);
        assert_eq!(mkdir("/é").as_ref(), &[106, 0, 4, b'/', 0xc3, 0xa9, 0, 0]);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Timestamp {
        #[serde(default, with = "optional_time")]
        mtime: Option<SystemTime>,
    }

    fn read_integer(input: &mut &[u8]) -> u64 {
        let length = input.iter().position(|byte| byte & 0x80 == 0).unwrap() + 1;
        let (encoded, rest) = input.split_at(length);
        assert!(length <= 10);
        let value: u128 = encoded
            .iter()
            .enumerate()
            .map(|(index, byte)| u128::from(byte & 0x7f) << (7 * index))
            .sum();
        let value = u64::try_from(value).unwrap();
        assert_eq!(
            length,
            (64 - value.leading_zeros()).div_ceil(7).max(1) as usize
        );
        *input = rest;
        value
    }

    fn read_path(input: &mut &[u8]) -> String {
        let length = usize::try_from(read_integer(input)).unwrap();
        let (encoded, rest) = input.split_at(length);
        assert_eq!(encoded.last(), Some(&0));
        *input = rest;
        std::str::from_utf8(&encoded[..length - 1])
            .unwrap()
            .to_owned()
    }

    proptest! {
        #[test]
        fn mutation_packets_preserve_paths_and_full_width_numbers(
            suffix in "[^\x00]{0,128}", size in any::<u64>(), hash in any::<[u8; 20]>(), nanos in any::<i64>(),
        ) {
            let path = format!("/{suffix}");
            let duration = Duration::from_nanos(nanos.unsigned_abs());
            let time = if nanos < 0 { UNIX_EPOCH - duration } else { UNIX_EPOCH + duration };
            let content = UploadedContent { size, hash: ContentHash::from_bytes(hash) };
            let encoded = bind(&path, content, time).unwrap();
            let mut wire = encoded.as_ref();
            prop_assert_eq!(read_integer(&mut wire), 103);
            prop_assert_eq!(read_integer(&mut wire), 0);
            prop_assert_eq!(read_path(&mut wire), path.as_str());
            prop_assert_eq!(read_integer(&mut wire), size);
            prop_assert_eq!(read_integer(&mut wire), nanos.div_euclid(1_000_000_000) as u64);
            prop_assert_eq!(read_integer(&mut wire), 0);
            prop_assert_eq!(&wire[..20], hash.as_slice());
            wire = &wire[20..];
            prop_assert_eq!(read_integer(&mut wire), 1);
            prop_assert!(wire.is_empty());

            let encoded = mkdir(&path);
            let mut wire = encoded.as_ref();
            prop_assert_eq!(read_integer(&mut wire), 106);
            prop_assert_eq!(read_integer(&mut wire), 0);
            prop_assert_eq!(read_path(&mut wire), path.as_str());
            prop_assert_eq!(read_integer(&mut wire), 0);
            prop_assert!(wire.is_empty());
        }

        #[test]
        fn timestamps_round_toward_the_previous_whole_second(nanos in any::<i64>()) {
            let duration = Duration::from_nanos(nanos.unsigned_abs());
            let time = if nanos < 0 { UNIX_EPOCH - duration } else { UNIX_EPOCH + duration };
            let seconds = unix_seconds(time).unwrap();
            prop_assert_eq!(seconds, nanos.div_euclid(1_000_000_000));
            let restored = system_time(seconds).unwrap();
            prop_assert!(restored <= time);
            prop_assert!(time.duration_since(restored).unwrap() < Duration::from_secs(1));
        }

        #[test]
        fn optional_timestamps_preserve_signed_seconds(seconds in prop::option::of(any::<i32>())) {
            let wire = serde_json::json!({"mtime": seconds});
            let value: Timestamp = serde_json::from_value(wire.clone()).unwrap();
            let expected = seconds.map(|seconds| {
                let duration = Duration::from_secs(u64::from(seconds.unsigned_abs()));
                if seconds < 0 { UNIX_EPOCH - duration } else { UNIX_EPOCH + duration }
            });
            prop_assert_eq!(value.mtime, expected);
            prop_assert_eq!(serde_json::to_value(value).unwrap(), wire);
        }

        #[test]
        fn download_paths_preserve_identity_without_changing_the_url(
            suffix in prop::collection::vec(any::<char>().prop_filter("no zero byte", |&c| c != '\0'), 0..80),
        ) {
            let path = format!("/{}", suffix.into_iter().collect::<String>());
            let base = Url::parse("https://download.example.test/prefix/").unwrap();
            let result = download_url(&base, &path);
            if path == "/." || path == "/.." {
                prop_assert!(matches!(result, Err(Error::InvalidPath)));
            } else {
                let url = result.unwrap();
                prop_assert_eq!(url.origin(), base.origin());
                prop_assert_eq!(url.query(), None);
                prop_assert_eq!(url.fragment(), None);
                let encoded = url.path().strip_prefix("/prefix/").unwrap();
                prop_assert!(!encoded.contains('/'));
                // Form decoding also accepts percent escapes. A literal plus must be escaped.
                prop_assert!(!encoded.contains('+'));
                let query = format!("path={encoded}");
                let decoded = url::form_urlencoded::parse(query.as_bytes()).next().unwrap().1;
                prop_assert_eq!(decoded.as_ref(), &path[1..]);
            }
        }
    }

    #[test]
    fn dot_only_download_paths_cannot_change_the_base_url() {
        let base = Url::parse("https://download.example.test/prefix/").unwrap();
        for path in ["/.", "/.."] {
            assert!(matches!(download_url(&base, path), Err(Error::InvalidPath)));
        }
    }

    #[test]
    fn service_addresses_cannot_add_credentials_or_insecure_transports() {
        for address in [
            "",
            "http://download.example.test/",
            "https://user:secret@download.example.test/",
            "https://download.example.test/?token=secret",
            "https://download.example.test/#secret",
            "file:///secret",
            "https://",
        ] {
            let error = service_url(address.as_bytes()).unwrap_err();
            assert!(matches!(error, Error::Protocol(_)));
            assert!(!format!("{error} {error:?}").contains("secret"));
        }
        assert!(matches!(service_url(&[0xff]), Err(Error::Protocol(_))));
        let url = service_url(b" https://download.example.test/shard/ 0 1\n").unwrap();
        assert_eq!(url.as_str(), "https://download.example.test/shard/");
    }

    #[test]
    fn malformed_upload_hashes_do_not_expose_response_details() {
        for body in [
            b"".as_slice(),
            b" \r\n\t",
            b"\xffserver-secret",
            b"server-secret",
            b"gggggggggggggggggggggggggggggggggggggggg server-secret",
            b"00000000000000000000000000000000000000000 server-secret",
        ] {
            let error = upload_hash(body).unwrap_err();
            assert!(matches!(error, Error::Protocol(_)));
            assert!(!format!("{error} {error:?}").contains("secret"));
        }
    }

    proptest! {
        #[test]
        fn upload_responses_allow_case_and_trailing_provider_fields(
            bytes in any::<[u8; 20]>(), suffix in ".{0,80}", uppercase in any::<bool>(),
        ) {
            let hash = ContentHash::from_bytes(bytes);
            let text = hash.to_string();
            let text = if uppercase { text.to_uppercase() } else { text };
            let response = format!("\r\n{text}\t{suffix}");
            prop_assert_eq!(upload_hash(response.as_bytes()).unwrap(), hash);
        }
    }
}
