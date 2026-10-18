mod support;

use std::sync::Arc;

use bytes::Bytes;
use cmr_s3::{BucketConfig, EncryptionKey, S3Backend, StorageConfig, service};
use s3s::{Body, service::S3Service};
use support::signed::signed;

fn bucket_config(name: &str, access_key: &str, secret_key: &str) -> BucketConfig {
    BucketConfig {
        name: name.into(),
        access_key: access_key.into(),
        secret_key: secret_key.into(),
    }
}

async fn call_as(
    service: &S3Service,
    access_key: &str,
    secret_key: &str,
    method: &str,
    target: &str,
    body: &str,
    headers: &[(&str, &str)],
) -> (http::StatusCode, String) {
    let response = service
        .call(support::signed::signed_with_credentials(
            method,
            target,
            Bytes::copy_from_slice(body.as_bytes()),
            headers,
            access_key,
            secret_key,
        ))
        .await
        .unwrap();
    let (parts, mut body) = response.into_parts();
    let bytes = body.store_all_limited(1024 * 1024).await.unwrap();
    (parts.status, String::from_utf8(bytes.into()).unwrap())
}

#[tokio::test]
async fn configured_credentials_isolate_buckets_and_copy_sources() {
    let state = tempfile::tempdir().unwrap();
    let backend = S3Backend::open(
        Arc::new(support::MemoryCloud::default()),
        StorageConfig {
            state_dir: state.path().into(),
            ..Default::default()
        },
        EncryptionKey::from_bytes([7; 32]),
    )
    .await
    .unwrap();
    let service = service(
        backend,
        &[
            bucket_config("bucket", "access", "secret"),
            bucket_config("other", "other-access", "other-secret"),
        ],
    )
    .await
    .unwrap();
    for (name, access, secret, forbidden) in [
        ("bucket", "access", "secret", "other"),
        ("other", "other-access", "other-secret", "bucket"),
    ] {
        assert_eq!(
            call_as(
                &service,
                access,
                secret,
                "PUT",
                &format!("/{name}/object"),
                "private",
                &[]
            )
            .await
            .0,
            http::StatusCode::OK
        );
        assert_eq!(
            call_as(
                &service,
                access,
                secret,
                "GET",
                &format!("/{name}/object"),
                "",
                &[]
            )
            .await
            .1,
            "private"
        );
        for target in ["/".to_owned(), "/?max-buckets=1".to_owned()] {
            let listed = call_as(&service, access, secret, "GET", &target, "", &[]).await;
            assert_eq!(listed.0, http::StatusCode::OK);
            assert!(listed.1.contains(&format!("<Name>{name}</Name>")));
            assert!(!listed.1.contains(&format!("<Name>{forbidden}</Name>")));
            assert!(!listed.1.contains("<ContinuationToken>"));
        }
        for (method, suffix) in [
            ("HEAD", ""),
            ("GET", "?list-type=2"),
            ("GET", "?uploads"),
            ("GET", "/object"),
            ("HEAD", "/object"),
            ("PUT", "/object"),
            ("DELETE", "/object"),
            ("POST", "/object?uploads"),
            ("PUT", "/object?uploadId=unknown&partNumber=1"),
            ("POST", "?delete"),
            ("GET", "?encryption"),
        ] {
            let denied = call_as(
                &service,
                access,
                secret,
                method,
                &format!("/{forbidden}{suffix}"),
                "",
                &[],
            )
            .await;
            assert_eq!(
                denied.0,
                http::StatusCode::FORBIDDEN,
                "{method} {suffix}: {}",
                denied.1
            );
        }
        for (method, target) in [
            ("PUT", format!("/{name}")),
            ("DELETE", format!("/{name}")),
            ("PUT", "/unconfigured".into()),
        ] {
            assert_eq!(
                call_as(&service, access, secret, method, &target, "", &[])
                    .await
                    .0,
                http::StatusCode::FORBIDDEN
            );
        }
        let source = format!("/{forbidden}/object");
        for target in [
            format!("/{name}/copy"),
            format!("/{name}/copy?partNumber=1&uploadId=unknown"),
        ] {
            let denied = call_as(
                &service,
                access,
                secret,
                "PUT",
                &target,
                "",
                &[("x-amz-copy-source", &source)],
            )
            .await;
            assert_eq!(denied.0, http::StatusCode::FORBIDDEN, "{}", denied.1);
        }
    }
    let wrong = call_as(
        &service,
        "access",
        "other-secret",
        "GET",
        "/bucket/object",
        "",
        &[],
    )
    .await;
    assert_eq!(wrong.0, http::StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn configuration_changes_preserve_data_and_revoke_old_credentials() {
    let cloud = Arc::new(support::MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        state_dir: state.path().into(),
        ..Default::default()
    };
    let backend = S3Backend::open(
        cloud.clone(),
        config.clone(),
        EncryptionKey::from_bytes([7; 32]),
    )
    .await
    .unwrap();
    let initial = service(backend, &[bucket_config("bucket", "access", "secret")])
        .await
        .unwrap();
    assert_eq!(
        call(&initial, "PUT", "/bucket/saved", "preserved", &[])
            .await
            .0,
        http::StatusCode::OK
    );
    let upload = call(&initial, "POST", "/bucket/pending?uploads", "", &[]).await;
    let upload_id = upload
        .2
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_owned();
    drop(initial);

    // A normal restart keeps bucket identity, objects, and unfinished uploads.
    let backend = S3Backend::open(
        cloud.clone(),
        config.clone(),
        EncryptionKey::from_bytes([7; 32]),
    )
    .await
    .unwrap();
    let rotated = service(
        backend,
        &[bucket_config("bucket", "new-access", "new-secret")],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&rotated, "GET", "/bucket/saved", "", &[]).await.0,
        http::StatusCode::FORBIDDEN
    );
    assert_eq!(
        call_as(
            &rotated,
            "new-access",
            "new-secret",
            "GET",
            "/bucket/saved",
            "",
            &[]
        )
        .await
        .1,
        "preserved"
    );
    let uploads = call_as(
        &rotated,
        "new-access",
        "new-secret",
        "GET",
        "/bucket?uploads",
        "",
        &[],
    )
    .await;
    assert!(uploads.1.contains(&upload_id));
    drop(rotated);

    let backend = S3Backend::open(cloud.clone(), config, EncryptionKey::from_bytes([7; 32]))
        .await
        .unwrap();
    let removed = service(backend, &[bucket_config("other", "access", "secret")])
        .await
        .unwrap();
    assert_eq!(
        call(&removed, "GET", "/bucket/saved", "", &[]).await.0,
        http::StatusCode::FORBIDDEN
    );
    assert!(
        !call(&removed, "GET", "/", "", &[])
            .await
            .2
            .contains("<Name>bucket</Name>")
    );
    drop(removed);

    // A new local index recovers the hidden bucket before it becomes available again.
    let recovered_state = tempfile::tempdir().unwrap();
    let backend = S3Backend::open(
        cloud,
        StorageConfig {
            state_dir: recovered_state.path().into(),
            ..Default::default()
        },
        EncryptionKey::from_bytes([7; 32]),
    )
    .await
    .unwrap();
    let restored = service(backend, &[bucket_config("bucket", "access", "secret")])
        .await
        .unwrap();
    assert_eq!(
        call(&restored, "GET", "/bucket/saved", "", &[]).await.2,
        "preserved"
    );
}

async fn call(
    service: &S3Service,
    method: &str,
    target: &str,
    body: impl Into<Bytes>,
    headers: &[(&str, &str)],
) -> (http::StatusCode, http::HeaderMap, String) {
    let response = service
        .call(signed(method, target, body.into(), headers))
        .await
        .unwrap();
    let (parts, mut body) = response.into_parts();
    let bytes = body.store_all_limited(20 * 1024 * 1024).await.unwrap();
    (
        parts.status,
        parts.headers,
        String::from_utf8(bytes.into()).unwrap(),
    )
}

async fn setup() -> (tempfile::TempDir, S3Service, Arc<support::MemoryCloud>) {
    let state = tempfile::tempdir().unwrap();
    let cloud = Arc::new(support::MemoryCloud::default());
    let backend = S3Backend::open(
        cloud.clone(),
        StorageConfig {
            state_dir: state.path().into(),
            chunk_bytes: 1024 * 1024,
            ..Default::default()
        },
        EncryptionKey::from_bytes([7; 32]),
    )
    .await
    .unwrap();
    let service = service(
        backend,
        &[BucketConfig {
            name: "bucket".into(),
            access_key: "access".into(),
            secret_key: "secret".into(),
        }],
    )
    .await
    .unwrap();
    (state, service, cloud)
}

#[tokio::test]
async fn signed_protocol_preserves_metadata_checksums_ranges_and_copy() {
    let (_state, service, _cloud) = setup().await;
    let put = call(
        &service,
        "PUT",
        "/bucket/a//b",
        "hello world",
        &[
            ("x-amz-meta-private", "value"),
            ("content-type", "text/plain"),
            ("x-amz-server-side-encryption", "AES256"),
            ("x-amz-checksum-crc32", "DUoRhQ=="),
        ],
    )
    .await;
    assert_eq!(put.0, http::StatusCode::OK, "{}", put.2);
    assert_eq!(put.1["x-amz-server-side-encryption"], "AES256");
    assert_eq!(put.1["x-amz-checksum-crc32"], "DUoRhQ==");
    let head = call(
        &service,
        "HEAD",
        "/bucket/a//b",
        "",
        &[("x-amz-checksum-mode", "ENABLED")],
    )
    .await;
    assert_eq!(head.0, http::StatusCode::OK);
    assert_eq!(head.1["content-length"], "11");
    assert_eq!(head.1["x-amz-meta-private"], "value");
    assert_eq!(head.1["x-amz-checksum-crc32"], "DUoRhQ==");
    let range = call(
        &service,
        "GET",
        "/bucket/a//b",
        "",
        &[("range", "bytes=3-8")],
    )
    .await;
    assert_eq!(range.0, http::StatusCode::PARTIAL_CONTENT);
    assert_eq!(range.1["content-range"], "bytes 3-8/11");
    assert_eq!(range.2, "lo wor");
    let copy = call(
        &service,
        "PUT",
        "/bucket/copied",
        "",
        &[
            ("x-amz-copy-source", "/bucket/a//b"),
            ("x-amz-metadata-directive", "REPLACE"),
            ("x-amz-meta-private", "changed"),
        ],
    )
    .await;
    assert_eq!(copy.0, http::StatusCode::OK, "{}", copy.2);
    let copied = call(&service, "GET", "/bucket/copied", "", &[]).await;
    assert_eq!(copied.2, "hello world");
    assert_eq!(copied.1["x-amz-meta-private"], "changed");
    let copied_head = call(
        &service,
        "HEAD",
        "/bucket/copied",
        "",
        &[("x-amz-checksum-mode", "ENABLED")],
    )
    .await;
    assert_eq!(copied_head.1["x-amz-checksum-crc32"], "DUoRhQ==");
    let rejected = call(
        &service,
        "PUT",
        "/bucket/bad",
        "hello world",
        &[("x-amz-checksum-crc32", "AAAAAA==")],
    )
    .await;
    assert_eq!(rejected.0, http::StatusCode::BAD_REQUEST);
    assert!(rejected.2.contains("BadDigest"));
    let listing = call(
        &service,
        "GET",
        "/bucket?list-type=2&delimiter=%2F",
        "",
        &[],
    )
    .await;
    assert_eq!(listing.0, http::StatusCode::OK, "{}", listing.2);
    assert!(listing.2.contains("<Prefix>a/</Prefix>"));
}

#[tokio::test]
async fn multipart_listing_url_encoding_preserves_pagination() {
    let (_state, service, _cloud) = setup().await;
    for key in ["a%252Fb", "a%252Fc"] {
        let created = call(&service, "POST", &format!("/bucket/{key}?uploads"), "", &[]).await;
        assert_eq!(created.0, http::StatusCode::OK, "{}", created.2);
    }
    let element = |body: &str, name: &str| {
        body.split_once(&format!("<{name}>"))
            .unwrap()
            .1
            .split_once(&format!("</{name}>"))
            .unwrap()
            .0
            .to_owned()
    };
    let target = "/bucket?uploads&encoding-type=url&max-uploads=1&prefix=a%25&delimiter=%2F";
    let first = call(&service, "GET", target, "", &[]).await;
    assert_eq!(first.0, http::StatusCode::OK, "{}", first.2);
    assert_eq!(element(&first.2, "Key"), "a%252Fb");
    assert_eq!(element(&first.2, "Prefix"), "a%25");
    assert_eq!(element(&first.2, "Delimiter"), "%2F");
    assert_eq!(element(&first.2, "IsTruncated"), "true");
    let marker = element(&first.2, "NextKeyMarker");
    assert_eq!(marker, "a%252Fb");
    let upload_marker = element(&first.2, "NextUploadIdMarker");
    // Decoding a response value and encoding it for the next URL preserves this spelling.
    let next = call(
        &service,
        "GET",
        &format!("{target}&key-marker={marker}&upload-id-marker={upload_marker}"),
        "",
        &[],
    )
    .await;
    assert_eq!(next.0, http::StatusCode::OK, "{}", next.2);
    assert_eq!(element(&next.2, "KeyMarker"), "a%252Fb");
    assert_eq!(element(&next.2, "Key"), "a%252Fc");
    assert_eq!(element(&next.2, "IsTruncated"), "false");
}

#[tokio::test]
async fn signed_batch_deletes_evaluate_each_etag_and_keep_replacements() {
    for quiet in [false, true] {
        let (_state, service, _cloud) = setup().await;
        let old = call(&service, "PUT", "/bucket/kept", "old", &[]).await;
        assert_eq!(old.0, http::StatusCode::OK);
        let old_etag = old.1["etag"].to_str().unwrap();
        let replacement = call(&service, "PUT", "/bucket/kept", "new", &[]).await;
        assert_eq!(replacement.0, http::StatusCode::OK);
        let matched = call(&service, "PUT", "/bucket/matched", "match", &[]).await;
        assert_eq!(matched.0, http::StatusCode::OK);
        let matched_etag = matched.1["etag"].to_str().unwrap();
        for key in ["unconditional", "exists"] {
            let put = call(&service, "PUT", &format!("/bucket/{key}"), "body", &[]).await;
            assert_eq!(put.0, http::StatusCode::OK);
        }
        let body = format!(
            "<Delete>\
             <Object><Key>kept</Key><ETag>{old_etag}</ETag></Object>\
             <Object><Key>matched</Key><ETag>{matched_etag}</ETag></Object>\
             <Object><Key>unconditional</Key></Object>\
             <Object><Key>exists</Key><ETag>*</ETag></Object>\
             <Object><Key>missing-conditional</Key><ETag>*</ETag></Object>\
             <Object><Key>missing</Key></Object>\
             <Quiet>{quiet}</Quiet></Delete>"
        );
        let deleted = call(&service, "POST", "/bucket?delete", body, &[]).await;
        assert_eq!(deleted.0, http::StatusCode::OK, "{}", deleted.2);
        let errors: Vec<_> = deleted
            .2
            .split("<Error>")
            .skip(1)
            .map(|error| error.split_once("</Error>").unwrap().0)
            .collect();
        assert_eq!(errors.len(), 2, "{}", deleted.2);
        for (key, code) in [
            ("kept", "PreconditionFailed"),
            ("missing-conditional", "NoSuchKey"),
        ] {
            assert!(
                errors.iter().any(|error| {
                    error.contains(&format!("<Key>{key}</Key>"))
                        && error.contains(&format!("<Code>{code}</Code>"))
                }),
                "{}",
                deleted.2
            );
        }
        assert_eq!(
            deleted.2.matches("<Deleted>").count(),
            if quiet { 0 } else { 4 }
        );
        let kept = call(&service, "GET", "/bucket/kept", "", &[]).await;
        assert_eq!(kept.0, http::StatusCode::OK);
        assert_eq!(kept.2, "new");
        for key in ["matched", "unconditional", "exists"] {
            let head = call(&service, "HEAD", &format!("/bucket/{key}"), "", &[]).await;
            assert_eq!(head.0, http::StatusCode::NOT_FOUND);
        }
    }
}

#[tokio::test]
async fn signed_streaming_uploads_keep_only_the_content_encoding() {
    let (_state, service, _cloud) = setup().await;
    for (encoding, content, checksum, expected) in [
        ("aws-chunked", b"hello world".as_slice(), "DUoRhQ==", None),
        (
            "gzip, aws-chunked",
            b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\xcb\x48\xcd\xc9\xc9\x57\x28\xcf\x2f\xca\x49\x01\x00\x85\x11\x4a\x0d\x0b\x00\x00\x00".as_slice(),
            "xU5Jig==",
            Some("gzip"),
        ),
    ] {
        let length = content.len().to_string();
        let mut body = format!("{:x}\r\n", content.len()).into_bytes();
        body.extend_from_slice(content);
        body.extend_from_slice(format!("\r\n0\r\nx-amz-checksum-crc32:{checksum}\r\n\r\n").as_bytes());
        let put = call(
            &service,
            "PUT",
            "/bucket/encoded",
            body,
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("content-encoding", encoding),
                ("x-amz-decoded-content-length", &length),
                ("x-amz-trailer", "x-amz-checksum-crc32"),
            ],
        )
        .await;
        assert_eq!(put.0, http::StatusCode::OK, "{}", put.2);
        for method in ["GET", "HEAD"] {
            let response = service
                .call(signed(method, "/bucket/encoded", Bytes::new(), &[]))
                .await
                .unwrap();
            assert_eq!(response.status(), http::StatusCode::OK);
            assert_eq!(
                response.headers().get("content-encoding").map(|v| v.to_str().unwrap()),
                expected,
            );
            if method == "GET" {
                let mut body = response.into_body();
                assert_eq!(body.store_all_limited(1024).await.unwrap().as_ref(), content);
            }
        }
    }
}

#[tokio::test]
async fn signed_streaming_trailers_are_compared_and_private_access_is_required() {
    let (_state, service, _cloud) = setup().await;
    for (checksum, expected) in [
        ("DUoRhQ==", http::StatusCode::OK),
        ("AAAAAA==", http::StatusCode::BAD_REQUEST),
    ] {
        let body = format!("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:{checksum}\r\n\r\n");
        let result = call(
            &service,
            "PUT",
            "/bucket/trailer",
            body,
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "11"),
                ("x-amz-trailer", "x-amz-checksum-crc32"),
                ("x-amz-sdk-checksum-algorithm", "CRC32"),
            ],
        )
        .await;
        assert_eq!(result.0, expected, "{}", result.2);
    }
    let missing = call(
        &service,
        "PUT",
        "/bucket/missing-trailer",
        "b\r\nhello world\r\n0\r\n\r\n",
        &[
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", "11"),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
        ],
    )
    .await;
    assert_eq!(missing.0, http::StatusCode::BAD_REQUEST);
    assert!(missing.2.contains("InvalidRequest"));
    let unsigned = service
        .call(
            http::Request::builder()
                .uri("/bucket/trailer")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unsigned.status(), http::StatusCode::FORBIDDEN);
    let mut wrong = signed("GET", "/bucket/trailer", Bytes::new(), &[]);
    wrong
        .headers_mut()
        .insert("x-amz-date", "20200101T000000Z".parse().unwrap());
    assert_eq!(
        service.call(wrong).await.unwrap().status(),
        http::StatusCode::FORBIDDEN
    );
    for header in [
        ("x-amz-server-side-encryption", "aws:kms"),
        ("x-amz-acl", "public-read"),
        ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        ("x-amz-checksum-blake3", "unsupported"),
    ] {
        let rejected = call(&service, "PUT", "/bucket/unsupported", "body", &[header]).await;
        assert_eq!(
            rejected.0,
            http::StatusCode::NOT_IMPLEMENTED,
            "{}",
            rejected.2
        );
    }
    let encryption = call(&service, "GET", "/bucket?encryption", "", &[]).await;
    assert_eq!(encryption.0, http::StatusCode::OK);
    assert!(encryption.2.contains("AES256"));
    let reset = call(&service, "DELETE", "/bucket?encryption", "", &[]).await;
    assert!(reset.0.is_success());
    assert!(
        call(&service, "GET", "/bucket?encryption", "", &[])
            .await
            .2
            .contains("AES256")
    );
}
