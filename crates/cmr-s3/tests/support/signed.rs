use std::{collections::BTreeMap, time::SystemTime};

use bytes::Bytes;
use hmac::{Hmac, Mac};
use s3s::{
    Body,
    dto::{Timestamp, TimestampFormat},
};
use sha2::{Digest, Sha256};

fn hmac(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha256> as hmac::KeyInit>::new_from_slice(key).unwrap();
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

pub fn signed(
    method: &str,
    target: &str,
    body: Bytes,
    extra: &[(&str, &str)],
) -> http::Request<Body> {
    signed_with_credentials(method, target, body, extra, "access", "secret")
}

pub fn signed_with_credentials(
    method: &str,
    target: &str,
    body: Bytes,
    extra: &[(&str, &str)],
    access_key: &str,
    secret_key: &str,
) -> http::Request<Body> {
    let mut time = Vec::new();
    Timestamp::from(SystemTime::now())
        .format(TimestampFormat::DateTime, &mut time)
        .unwrap();
    let date: String = std::str::from_utf8(&time).unwrap()[..19]
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();
    let date = format!("{date}Z");
    let day = &date[..8];
    let mut headers: BTreeMap<String, String> = [
        ("host".into(), "localhost".into()),
        ("x-amz-date".into(), date.clone()),
        (
            "x-amz-content-sha256".into(),
            hex::encode(Sha256::digest(&body)),
        ),
    ]
    .into();
    for (key, value) in extra {
        headers.insert((*key).into(), (*value).into());
    }
    let names = headers.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_headers = headers
        .iter()
        .map(|(key, value)| format!("{key}:{}\n", value.trim()))
        .collect::<String>();
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut parameters = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            if p.contains('=') {
                p.to_owned()
            } else {
                format!("{p}=")
            }
        })
        .collect::<Vec<_>>();
    parameters.sort();
    let canonical = format!(
        "{method}\n{path}\n{}\n{canonical_headers}\n{names}\n{}",
        parameters.join("&"),
        headers["x-amz-content-sha256"]
    );
    let scope = format!("{day}/us-east-1/s3/aws4_request");
    let string = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical))
    );
    let signing_key = hmac(
        &hmac(
            &hmac(
                &hmac(format!("AWS4{secret_key}").as_bytes(), day.as_bytes()),
                b"us-east-1",
            ),
            b"s3",
        ),
        b"aws4_request",
    );
    let signature = hex::encode(hmac(&signing_key, string.as_bytes()));
    let mut request = http::Request::builder().method(method).uri(target);
    for (key, value) in headers {
        request = request.header(key, value);
    }
    request.header("authorization", format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={names}, Signature={signature}"))
        .header("content-length", body.len()).body(Body::from(body)).unwrap()
}
