mod support;

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use cmr_s3::{EncryptionKey, S3Backend, StorageConfig};
use futures_util::StreamExt;
use s3s::{S3, S3ErrorCode, dto::*};
use support::{MemoryCloud, request};

fn configuration(path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        state_dir: path.into(),
        chunk_bytes: 1024 * 1024,
        ..Default::default()
    }
}

async fn open(cloud: Arc<MemoryCloud>, config: StorageConfig) -> S3Backend {
    S3Backend::open(cloud, config, EncryptionKey::from_bytes([7; 32]))
        .await
        .unwrap()
}

async fn bucket(backend: &S3Backend) {
    backend
        .create_bucket(request(CreateBucketInput {
            bucket: "private-bucket".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
}

async fn put(backend: &S3Backend, key: &str, bytes: impl Into<Bytes>) -> PutObjectOutput {
    let bytes = bytes.into();
    backend
        .put_object(request(PutObjectInput {
            bucket: "private-bucket".into(),
            key: key.into(),
            content_length: Some(bytes.len() as i64),
            body: Some(StreamingBlob::from_bytes(bytes)),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
}

async fn get(backend: &S3Backend, key: &str, range: Option<Range>) -> Vec<u8> {
    let mut body = backend
        .get_object(request(GetObjectInput {
            bucket: "private-bucket".into(),
            key: key.into(),
            range,
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .body
        .unwrap();
    let mut bytes = vec![];
    while let Some(part) = body.next().await {
        bytes.extend_from_slice(&part.unwrap());
    }
    bytes
}

async fn start(backend: &S3Backend, key: &str) -> String {
    backend
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: "private-bucket".into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap()
}

async fn part(backend: &S3Backend, key: &str, upload: &str, number: i32, bytes: Bytes) -> ETag {
    backend
        .upload_part(request(UploadPartInput {
            bucket: "private-bucket".into(),
            key: key.into(),
            upload_id: upload.into(),
            part_number: number,
            content_length: Some(bytes.len() as i64),
            body: Some(StreamingBlob::from_bytes(bytes)),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .e_tag
        .unwrap()
}

#[tokio::test]
async fn crc_multipart_defaults_to_composite_and_accepts_its_checksum() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let backend = open(cloud, configuration(state.path())).await;
    bucket(&backend).await;
    // Independent CRC vectors for "hello world" and the checksum of its part checksum.
    for (algorithm, part_checksum, composite) in [
        ("CRC32", "DUoRhQ==", "YChbaA==-1"),
        ("CRC32C", "yZRlqg==", "6yb7oQ==-1"),
    ] {
        let created = backend
            .create_multipart_upload(request(CreateMultipartUploadInput {
                bucket: "private-bucket".into(),
                key: algorithm.into(),
                checksum_algorithm: Some(ChecksumAlgorithm::from_static(algorithm)),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(created.checksum_type.unwrap().as_str(), "COMPOSITE");
        let id = created.upload_id.unwrap();
        let tag = part(
            &backend,
            algorithm,
            &id,
            1,
            Bytes::from_static(b"hello world"),
        )
        .await;
        let mut completed_part = CompletedPart {
            part_number: Some(1),
            e_tag: Some(tag),
            ..Default::default()
        };
        let mut input = CompleteMultipartUploadInput {
            bucket: "private-bucket".into(),
            key: algorithm.into(),
            upload_id: id,
            checksum_type: Some(ChecksumType::from_static("COMPOSITE")),
            ..Default::default()
        };
        if algorithm == "CRC32" {
            completed_part.checksum_crc32 = Some(part_checksum.into());
            input.checksum_crc32 = Some(composite.into());
        } else {
            completed_part.checksum_crc32c = Some(part_checksum.into());
            input.checksum_crc32c = Some(composite.into());
        }
        input.multipart_upload = Some(CompletedMultipartUpload {
            parts: Some(vec![completed_part]),
        });
        let completed = backend
            .complete_multipart_upload(request(input))
            .await
            .unwrap()
            .output;
        assert_eq!(completed.checksum_type.unwrap().as_str(), "COMPOSITE");
        assert_eq!(
            completed
                .checksum_crc32
                .or(completed.checksum_crc32c)
                .as_deref(),
            Some(composite)
        );
        assert_eq!(get(&backend, algorithm, None).await, b"hello world");
        let copied_key = format!("{algorithm}-copy");
        let copied = backend
            .copy_object(request(
                CopyObjectInput::builder()
                    .bucket("private-bucket".into())
                    .key(copied_key.clone())
                    .copy_source(CopySource::parse(&format!("private-bucket/{algorithm}")).unwrap())
                    .build()
                    .unwrap(),
            ))
            .await
            .unwrap()
            .output
            .copy_object_result
            .unwrap();
        assert_eq!(copied.checksum_type.unwrap().as_str(), "FULL_OBJECT");
        assert_eq!(
            copied.checksum_crc32.or(copied.checksum_crc32c).as_deref(),
            Some(part_checksum)
        );
        let downloaded = backend
            .get_object(request(GetObjectInput {
                bucket: "private-bucket".into(),
                key: copied_key.clone(),
                checksum_mode: Some(ChecksumMode::from_static("ENABLED")),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(downloaded.checksum_type.unwrap().as_str(), "FULL_OBJECT");
        assert_eq!(
            downloaded
                .checksum_crc32
                .or(downloaded.checksum_crc32c)
                .as_deref(),
            Some(part_checksum)
        );
        let mut body = downloaded.body.unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(bytes, b"hello world");
        let head = backend
            .head_object(request(HeadObjectInput {
                bucket: "private-bucket".into(),
                key: copied_key,
                checksum_mode: Some(ChecksumMode::from_static("ENABLED")),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(head.checksum_type.unwrap().as_str(), "FULL_OBJECT");
        assert_eq!(
            head.checksum_crc32.or(head.checksum_crc32c).as_deref(),
            Some(part_checksum)
        );
    }
}

#[tokio::test]
async fn multipart_reads_report_the_selected_parts_checksum() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let backend = open(cloud, configuration(state.path())).await;
    bucket(&backend).await;
    let id = start(&backend, "parts").await;
    let size = 5 * 1024 * 1024;
    let first = part(&backend, "parts", &id, 1, Bytes::from(vec![1; size])).await;
    let second = part(&backend, "parts", &id, 2, Bytes::from_static(b"x")).await;
    backend
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: "private-bucket".into(),
            key: "parts".into(),
            upload_id: id,
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![
                    CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(first),
                        ..Default::default()
                    },
                    CompletedPart {
                        part_number: Some(2),
                        e_tag: Some(second),
                        ..Default::default()
                    },
                ]),
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    for (number, range) in [
        (Some(2), None),
        (
            None,
            Some(Range::Int {
                first: size as u64,
                last: Some(size as u64),
            }),
        ),
        (None, Some(Range::Suffix { length: 1 })),
    ] {
        let got = backend
            .get_object(request(GetObjectInput {
                bucket: "private-bucket".into(),
                key: "parts".into(),
                part_number: number,
                range,
                checksum_mode: Some(ChecksumMode::from_static("ENABLED")),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(got.checksum_crc64nvme.as_deref(), Some("Lb1nAmRU5LE="));
        let head = backend
            .head_object(request(HeadObjectInput {
                bucket: "private-bucket".into(),
                key: "parts".into(),
                part_number: number,
                range,
                checksum_mode: Some(ChecksumMode::from_static("ENABLED")),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(head.checksum_crc64nvme, got.checksum_crc64nvme);
        if number.is_some() {
            assert_eq!(got.parts_count, Some(2));
            assert_eq!(head.parts_count, Some(2));
        }
        let mut body = got.body.unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(bytes, b"x");
    }
    let fragment = backend
        .get_object(request(GetObjectInput {
            bucket: "private-bucket".into(),
            key: "parts".into(),
            range: Some(Range::Int {
                first: 0,
                last: Some(0),
            }),
            checksum_mode: Some(ChecksumMode::from_static("ENABLED")),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(fragment.checksum_crc64nvme.is_none());
}

#[tokio::test]
async fn repeated_multipart_copies_remain_readable_after_recovery_and_cleanup() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        orphan_grace: Duration::ZERO,
        ..configuration(state.path())
    };
    let backend = open(cloud.clone(), config).await;
    bucket(&backend).await;
    put(&backend, "original", "prefix/x/suffix").await;
    for generation in 0..100 {
        let id = start(&backend, "copy").await;
        let source = if generation == 0 {
            "private-bucket/original"
        } else {
            "private-bucket/copy"
        };
        let copied = backend
            .upload_part_copy(request(
                UploadPartCopyInput::builder()
                    .bucket("private-bucket".into())
                    .key("copy".into())
                    .upload_id(id.clone())
                    .part_number(1)
                    .copy_source(CopySource::parse(source).unwrap())
                    .copy_source_range((generation == 0).then(|| "bytes=7-7".into()))
                    .build()
                    .unwrap(),
            ))
            .await
            .unwrap()
            .output
            .copy_part_result
            .unwrap();
        backend
            .complete_multipart_upload(request(CompleteMultipartUploadInput {
                bucket: "private-bucket".into(),
                key: "copy".into(),
                upload_id: id,
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(vec![CompletedPart {
                        part_number: Some(1),
                        e_tag: copied.e_tag,
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            get(&backend, "copy", None).await,
            b"x",
            "generation {generation}"
        );
    }
    backend
        .delete_object(request(DeleteObjectInput {
            bucket: "private-bucket".into(),
            key: "original".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    drop(backend);
    let recovered_state = tempfile::tempdir().unwrap();
    let backend = open(
        cloud,
        StorageConfig {
            orphan_grace: Duration::ZERO,
            ..configuration(recovered_state.path())
        },
    )
    .await;
    assert_eq!(get(&backend, "copy", None).await, b"x");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    backend.cleanup().await.unwrap();
    assert_eq!(get(&backend, "copy", None).await, b"x");
}

#[tokio::test]
async fn cancelled_content_transfer_preserves_the_committed_object() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let config = configuration(state.path());
    let backend = open(cloud.clone(), config.clone()).await;
    bucket(&backend).await;
    put(&backend, "replace", "original").await;
    let written = Arc::new(tokio::sync::Notify::new());
    cloud.pause_next_bind(
        "/00000000.bin",
        written.clone(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let writing = backend.clone();
    let task = tokio::spawn(async move {
        writing
            .put_object(request(PutObjectInput {
                bucket: "private-bucket".into(),
                key: "replace".into(),
                body: Some(StreamingBlob::from_bytes(Bytes::from_static(
                    b"replacement",
                ))),
                ..Default::default()
            }))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), written.notified())
        .await
        .unwrap();
    assert_eq!(get(&backend, "replace", None).await, b"original");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(backend);
    let backend = open(cloud, config).await;
    assert_eq!(get(&backend, "replace", None).await, b"original");
}

#[tokio::test]
async fn exact_keys_ranges_conditions_encryption_and_recovery() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let backend = open(cloud.clone(), configuration(state.path())).await;
    bucket(&backend).await;
    let bytes = Bytes::from(vec![42; 3 * 1024 * 1024 + 11]);
    let tag = put(&backend, "private-key/a", bytes.clone())
        .await
        .e_tag
        .unwrap();
    for key in ["a", "a/b", "A", "double//slash", "snow/☃"] {
        put(&backend, key, key.to_owned()).await;
    }
    assert_eq!(get(&backend, "A", None).await, b"A");
    assert_eq!(get(&backend, "a/b", None).await, b"a/b");
    assert_eq!(get(&backend, "double//slash", None).await, b"double//slash");
    assert_eq!(
        get(
            &backend,
            "private-key/a",
            Some(Range::Int {
                first: 1024 * 1024 - 100,
                last: Some(2 * 1024 * 1024 + 100)
            })
        )
        .await,
        bytes[(1024 * 1024 - 100)..=(2 * 1024 * 1024 + 100)]
    );
    let failed = backend
        .put_object(request(PutObjectInput {
            bucket: "private-bucket".into(),
            key: "private-key/a".into(),
            if_none_match: Some(ETagCondition::Any),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"new"))),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(failed.code(), &S3ErrorCode::PreconditionFailed);
    let unchanged = backend
        .get_object(request(GetObjectInput {
            bucket: "private-bucket".into(),
            key: "private-key/a".into(),
            if_none_match: Some(ETagCondition::ETag(tag)),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(unchanged.code(), &S3ErrorCode::NotModified);
    for (path, ciphertext) in cloud.bytes() {
        assert!(!path.contains("private-key") && !path.contains("private-bucket"));
        assert!(
            !ciphertext
                .windows(b"private-bucket".len())
                .any(|bytes| bytes == b"private-bucket")
        );
        assert!(
            ciphertext.len() <= 1024 * 1024
                || !path.contains("/data/") && !path.ends_with("/created.bin")
        );
    }
    drop(backend);
    let recovered_state = tempfile::tempdir().unwrap();
    let recovered = open(cloud.clone(), configuration(recovered_state.path())).await;
    assert_eq!(get(&recovered, "private-key/a", None).await, bytes);
    assert_eq!(get(&recovered, "a", None).await, b"a");
    drop(recovered);
    assert!(
        S3Backend::open(
            cloud.clone(),
            configuration(recovered_state.path()),
            EncryptionKey::from_bytes([8; 32])
        )
        .await
        .is_err()
    );
    let path = cloud
        .bytes()
        .into_iter()
        .find(|(path, _)| path.contains("/objects/"))
        .unwrap()
        .0;
    cloud.corrupt(&path);
    assert!(
        S3Backend::open(
            cloud,
            StorageConfig {
                rebuild_catalog: true,
                ..configuration(recovered_state.path())
            },
            EncryptionKey::from_bytes([7; 32])
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn multipart_survives_restart_copies_ranges_and_completes_without_uploading_content() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let config = configuration(state.path());
    let backend = open(cloud.clone(), config.clone()).await;
    bucket(&backend).await;
    let first = Bytes::from(vec![11; 5 * 1024 * 1024]);
    let id = start(&backend, "layer").await;
    let first_tag = part(&backend, "layer", &id, 1, first.clone()).await;
    let scans = cloud.list_calls();
    drop(backend);
    let backend = open(cloud.clone(), config).await;
    assert_eq!(
        cloud.list_calls(),
        scans,
        "a normal restart must reuse the disk catalog"
    );
    let listed = backend
        .list_parts(request(ListPartsInput {
            bucket: "private-bucket".into(),
            key: "layer".into(),
            upload_id: id.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(listed.parts.unwrap()[0].e_tag, Some(first_tag.clone()));
    let second_tag = part(&backend, "layer", &id, 2, Bytes::from_static(b"last part")).await;
    let before = cloud
        .bytes()
        .into_iter()
        .filter(|(path, _)| path.contains("/data/") && !path.ends_with("/created.bin"))
        .count();
    let completed = backend
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: "private-bucket".into(),
            key: "layer".into(),
            upload_id: id,
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![
                    CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(first_tag),
                        ..Default::default()
                    },
                    CompletedPart {
                        part_number: Some(2),
                        e_tag: Some(second_tag),
                        ..Default::default()
                    },
                ]),
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(completed.e_tag.unwrap().value().ends_with("-2"));
    assert_eq!(
        before,
        cloud
            .bytes()
            .into_iter()
            .filter(|(path, _)| path.contains("/data/") && !path.ends_with("/created.bin"))
            .count()
    );
    assert_eq!(
        get(&backend, "layer", Some(Range::Suffix { length: 9 })).await,
        b"last part"
    );
    let copied_id = start(&backend, "range-copy").await;
    let copied = backend
        .upload_part_copy(request(
            UploadPartCopyInput::builder()
                .bucket("private-bucket".into())
                .key("range-copy".into())
                .upload_id(copied_id.clone())
                .part_number(1)
                .copy_source(CopySource::parse("private-bucket/layer").unwrap())
                .copy_source_range(Some(format!(
                    "bytes={}-{}",
                    first.len() - 5,
                    first.len() + 8
                )))
                .build()
                .unwrap(),
        ))
        .await
        .unwrap()
        .output
        .copy_part_result
        .unwrap();
    backend
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: "private-bucket".into(),
            key: "range-copy".into(),
            upload_id: copied_id,
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: copied.e_tag,
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        get(&backend, "range-copy", None).await,
        [vec![11; 5], b"last part".to_vec()].concat()
    );
    assert_eq!(
        before,
        cloud
            .bytes()
            .into_iter()
            .filter(|(path, _)| path.contains("/data/") && !path.ends_with("/created.bin"))
            .count()
    );
    let empty_id = start(&backend, "empty").await;
    let empty_tag = part(&backend, "empty", &empty_id, 1, Bytes::new()).await;
    backend
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: "private-bucket".into(),
            key: "empty".into(),
            upload_id: empty_id,
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(empty_tag),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(get(&backend, "empty", None).await.is_empty());
}

#[tokio::test]
async fn ordered_pages_count_prefixes_and_bind_tokens_to_the_query() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let backend = open(cloud, configuration(state.path())).await;
    bucket(&backend).await;
    for key in ["a", "b/1", "b/2", "c", "d/1", "é"] {
        put(&backend, key, Bytes::new()).await;
    }
    let first = backend
        .list_objects_v2(request(ListObjectsV2Input {
            bucket: "private-bucket".into(),
            delimiter: Some("/".into()),
            max_keys: Some(2),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(first.contents.unwrap()[0].key.as_deref(), Some("a"));
    assert_eq!(
        first.common_prefixes.unwrap()[0].prefix.as_deref(),
        Some("b/")
    );
    assert_eq!(first.key_count, Some(2));
    assert_eq!(first.is_truncated, Some(true));
    let token = first.next_continuation_token.unwrap();
    let second = backend
        .list_objects_v2(request(ListObjectsV2Input {
            bucket: "private-bucket".into(),
            delimiter: Some("/".into()),
            max_keys: Some(2),
            continuation_token: Some(token.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(second.contents.unwrap()[0].key.as_deref(), Some("c"));
    assert_eq!(
        second.common_prefixes.unwrap()[0].prefix.as_deref(),
        Some("d/")
    );
    assert!(
        backend
            .list_objects_v2(request(ListObjectsV2Input {
                bucket: "private-bucket".into(),
                continuation_token: Some(token),
                ..Default::default()
            }))
            .await
            .is_err()
    );
    let v1 = backend
        .list_objects(request(ListObjectsInput {
            bucket: "private-bucket".into(),
            marker: Some("c".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        v1.contents
            .unwrap()
            .iter()
            .map(|o| o.key.as_deref().unwrap())
            .collect::<Vec<_>>(),
        ["d/1", "é"]
    );
}

#[tokio::test]
async fn cleanup_collects_empty_failed_ingestions_after_the_grace_period() {
    use cmr_api::CloudApi;
    use futures_util::TryStreamExt;

    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let backend = open(
        cloud.clone(),
        StorageConfig {
            cloud_concurrency: 1,
            orphan_grace: Duration::from_secs(60),
            ..configuration(state.path())
        },
    )
    .await;
    bucket(&backend).await;
    // Fail before any data or creation marker is bound in the new group.
    cloud.fail_next_bind("data/", false);
    let error = backend
        .put_object(request(PutObjectInput {
            bucket: "private-bucket".into(),
            key: "failed".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"body"))),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), &S3ErrorCode::InternalError);
    let groups = cloud
        .list("/cmr-s3/data")
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let [group] = groups.as_slice() else {
        panic!("failed ingestion must leave one data group: {groups:?}");
    };
    assert!(
        cloud
            .list(&group.home)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    backend.cleanup().await.unwrap();
    assert!(cloud.stat(&group.home).await.is_ok());
    let unknown = format!("/cmr-s3/data/{}", uuid::Uuid::new_v4());
    cloud.mkdir(&unknown).await.unwrap();
    cloud.mkdir(&format!("{unknown}/nested")).await.unwrap();
    cloud.age_all(SystemTime::now() - Duration::from_secs(120));
    backend.cleanup().await.unwrap();
    assert!(matches!(
        cloud.stat(&group.home).await,
        Err(cmr_api::Error::NotFound(_))
    ));
    // An unrecognized child cannot establish a safe age, even when its parent is old.
    assert!(cloud.stat(&unknown).await.is_ok());
}

#[tokio::test]
async fn rejected_bodies_tampering_and_cleanup_keep_referenced_content() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        staging_bytes: 100,
        orphan_grace: Duration::ZERO,
        upload_expiry: Duration::ZERO,
        ..configuration(state.path())
    };
    let backend = open(cloud.clone(), config).await;
    bucket(&backend).await;
    let bad = backend
        .put_object(request(PutObjectInput {
            bucket: "private-bucket".into(),
            key: "bad".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"body"))),
            checksum_crc32: Some("AAAAAA==".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(bad.code(), &S3ErrorCode::BadDigest);
    let quota = backend
        .put_object(request(PutObjectInput {
            bucket: "private-bucket".into(),
            key: "too-large".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from(vec![0; 101]))),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(quota.code(), &S3ErrorCode::SlowDown);
    put(&backend, "keep", Bytes::from_static(b"original")).await;
    put(&backend, "replace", Bytes::from_static(b"previous")).await;
    put(&backend, "replace", Bytes::from_static(b"replacement")).await;
    let id = start(&backend, "expire").await;
    part(&backend, "expire", &id, 1, Bytes::from_static(b"temporary")).await;
    cloud.age_all(SystemTime::now() - Duration::from_secs(100));
    backend.cleanup().await.unwrap();
    assert!(
        backend
            .list_parts(request(ListPartsInput {
                bucket: "private-bucket".into(),
                key: "expire".into(),
                upload_id: id,
                ..Default::default()
            }))
            .await
            .is_err()
    );
    assert_eq!(get(&backend, "replace", None).await, b"replacement");
    assert_eq!(
        cloud
            .bytes()
            .iter()
            .filter(|(path, _)| path.contains("/data/") && !path.ends_with("/created.bin"))
            .count(),
        2
    );
    let path = cloud
        .bytes()
        .into_iter()
        .find(|(path, _)| path.contains("/data/") && !path.ends_with("/created.bin"))
        .unwrap()
        .0;
    cloud.corrupt(&path);
    let mut body = backend
        .get_object(request(GetObjectInput {
            bucket: "private-bucket".into(),
            key: "keep".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .body
        .unwrap();
    // Either live object's chunk was selected. No plaintext from the corrupt chunk can be returned.
    let first = body.next().await.unwrap();
    if first.is_ok() {
        drop(body);
        let mut body = backend
            .get_object(request(GetObjectInput {
                bucket: "private-bucket".into(),
                key: "replace".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output
            .body
            .unwrap();
        assert!(body.next().await.unwrap().is_err());
    }
}

#[tokio::test]
async fn put_rejects_a_recreated_destination_bucket_after_content_transfer() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let backend = open(cloud.clone(), configuration(state.path())).await;
    bucket(&backend).await;
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    cloud.pause_next_bind("/data/", started.clone(), resume.clone());
    let writing = backend.clone();
    let task = tokio::spawn(async move {
        writing
            .put_object(request(PutObjectInput {
                bucket: "private-bucket".into(),
                key: "pending".into(),
                body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"pending"))),
                ..Default::default()
            }))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    backend
        .delete_bucket(request(DeleteBucketInput {
            bucket: "private-bucket".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    bucket(&backend).await;
    resume.notify_one();
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.code(), &S3ErrorCode::NoSuchBucket);
    let error = backend
        .head_object(request(HeadObjectInput {
            bucket: "private-bucket".into(),
            key: "pending".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), &S3ErrorCode::NoSuchKey);
}

#[tokio::test]
async fn copy_rechecks_destination_conditions_and_bucket_identity_after_hashing() {
    for recreate_bucket in [false, true] {
        let cloud = Arc::new(MemoryCloud::default());
        let state = tempfile::tempdir().unwrap();
        let backend = open(cloud.clone(), configuration(state.path())).await;
        bucket(&backend).await;
        put(&backend, "source", Bytes::from_static(b"source")).await;
        backend
            .create_bucket(request(CreateBucketInput {
                bucket: "destination".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        cloud.pause_next_download("/data/", started.clone(), resume.clone());
        let copying = backend.clone();
        let task = tokio::spawn(async move {
            copying
                .copy_object(request(
                    CopyObjectInput::builder()
                        .bucket("destination".into())
                        .key("copy".into())
                        .copy_source(CopySource::parse("private-bucket/source").unwrap())
                        .checksum_algorithm(Some(ChecksumAlgorithm::from_static("SHA256")))
                        .if_none_match(Some(ETagCondition::Any))
                        .build()
                        .unwrap(),
                ))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .unwrap();
        let replacement = if recreate_bucket {
            backend
                .delete_bucket(request(DeleteBucketInput {
                    bucket: "destination".into(),
                    ..Default::default()
                }))
                .await
                .unwrap();
            backend
                .create_bucket(request(CreateBucketInput {
                    bucket: "destination".into(),
                    ..Default::default()
                }))
                .await
                .unwrap();
            None
        } else {
            Some(
                backend
                    .put_object(request(PutObjectInput {
                        bucket: "destination".into(),
                        key: "copy".into(),
                        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"newer"))),
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .output,
            )
        };
        resume.notify_one();
        let error = task.await.unwrap().unwrap_err();
        let current = backend
            .head_object(request(HeadObjectInput {
                bucket: "destination".into(),
                key: "copy".into(),
                ..Default::default()
            }))
            .await;
        if let Some(replacement) = replacement {
            assert_eq!(error.code(), &S3ErrorCode::PreconditionFailed);
            assert_eq!(current.unwrap().output.e_tag, replacement.e_tag);
        } else {
            assert_eq!(error.code(), &S3ErrorCode::NoSuchBucket);
            assert_eq!(current.unwrap_err().code(), &S3ErrorCode::NoSuchKey);
        }
    }
}

#[tokio::test]
async fn concurrent_conditions_read_snapshots_and_uncertain_publication() {
    let cloud = Arc::new(MemoryCloud::default());
    let state = tempfile::tempdir().unwrap();
    let config = configuration(state.path());
    let backend = open(cloud.clone(), config.clone()).await;
    bucket(&backend).await;
    let write = |body| {
        request(PutObjectInput {
            bucket: "private-bucket".into(),
            key: "race".into(),
            if_none_match: Some(ETagCondition::Any),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
            ..Default::default()
        })
    };
    let (first, second) = tokio::join!(
        backend.put_object(write(b"first")),
        backend.put_object(write(b"second"))
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let old = get(&backend, "race", None).await;
    let mut snapshot = backend
        .get_object(request(GetObjectInput {
            bucket: "private-bucket".into(),
            key: "race".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .body
        .unwrap();
    put(&backend, "race", Bytes::from_static(b"replacement")).await;
    let mut snapshot_bytes = Vec::new();
    while let Some(chunk) = snapshot.next().await {
        snapshot_bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(snapshot_bytes, old);
    drop(snapshot);
    cloud.fail_next_bind("/objects/", true);
    put(&backend, "race", Bytes::from_static(b"reconciled")).await;
    assert_eq!(get(&backend, "race", None).await, b"reconciled");
    cloud.fail_next_bind("/objects/", false);
    assert!(
        backend
            .put_object(request(PutObjectInput {
                bucket: "private-bucket".into(),
                key: "race".into(),
                body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"uncertain"))),
                ..Default::default()
            }))
            .await
            .is_err()
    );
    assert!(
        backend
            .head_object(request(HeadObjectInput {
                bucket: "private-bucket".into(),
                key: "race".into(),
                ..Default::default()
            }))
            .await
            .is_err()
    );
    drop(backend);
    let reopened = open(cloud, config).await;
    assert_eq!(get(&reopened, "race", None).await, b"reconciled");
}

#[tokio::test]
async fn concurrent_bodies_share_the_staging_allowance_and_release_failed_reservations() {
    let state = tempfile::tempdir().unwrap();
    let cloud = Arc::new(MemoryCloud::default());
    let backend = open(
        cloud,
        StorageConfig {
            staging_bytes: 100,
            ..configuration(state.path())
        },
    )
    .await;
    bucket(&backend).await;
    let staged = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let signal = staged.clone();
    let resume = release.clone();
    let body = futures_util::stream::iter([Ok::<_, std::io::Error>(Bytes::from(vec![1; 60]))])
        .chain(futures_util::stream::once(async move {
            signal.notify_one();
            resume.notified().await;
            Ok(Bytes::new())
        }));
    let writing = backend.clone();
    let first = tokio::spawn(async move {
        writing
            .put_object(request(PutObjectInput {
                bucket: "private-bucket".into(),
                key: "first".into(),
                body: Some(StreamingBlob::wrap(body)),
                ..Default::default()
            }))
            .await
    });
    staged.notified().await;
    let second = backend
        .put_object(request(PutObjectInput {
            bucket: "private-bucket".into(),
            key: "second".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from(vec![2; 60]))),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(second.code(), &S3ErrorCode::SlowDown);
    release.notify_one();
    first.await.unwrap().unwrap();
    put(&backend, "second", Bytes::from(vec![2; 60])).await;
    assert_eq!(get(&backend, "first", None).await, vec![1; 60]);
}

#[tokio::test]
async fn restart_reconciles_cloud_publication_after_cancellation() {
    let state = tempfile::tempdir().unwrap();
    let cloud = Arc::new(MemoryCloud::default());
    let config = configuration(state.path());
    let backend = open(cloud.clone(), config.clone()).await;
    bucket(&backend).await;
    put(&backend, "recover", Bytes::from_static(b"old")).await;
    let written = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    cloud.pause_next_bind("/objects/", written.clone(), resume);
    let writing = backend.clone();
    let task = tokio::spawn(async move {
        writing
            .put_object(request(PutObjectInput {
                bucket: "private-bucket".into(),
                key: "recover".into(),
                body: Some(StreamingBlob::from_bytes(Bytes::from_static(
                    b"cancelled publication",
                ))),
                ..Default::default()
            }))
            .await
    });
    written.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        backend
            .head_object(request(HeadObjectInput {
                bucket: "private-bucket".into(),
                key: "recover".into(),
                ..Default::default()
            }))
            .await
            .is_err()
    );
    assert!(backend.cleanup().await.is_err());
    drop(backend);
    let backend = open(cloud, config).await;
    assert_eq!(
        get(&backend, "recover", None).await,
        b"cancelled publication"
    );
}
