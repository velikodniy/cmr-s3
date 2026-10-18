//! Committed object mutations own guard lifetimes, conditions, and publication.

use std::sync::Arc;

use s3s::{S3Result, TrailingHeaders, dto::*, s3_error};
use tokio::sync::OwnedRwLockReadGuard;

use crate::{
    checksum::{self, checksum_fields, collect_field, selected_algorithm, upload_trailers},
    metadata::{self, Checksums, Extent, attributes, now, timestamp},
    s3::MAX_MULTIPART_PARTS,
    storage::Store,
};

const MAX_DELETE_OBJECTS: usize = 1000;
const MIN_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024;

pub(crate) fn version(version: Option<&str>) -> S3Result<()> {
    if version.is_some_and(|v| v != "null") {
        return Err(s3_error!(NotImplemented, "Object versions are unsupported"));
    }
    Ok(())
}

pub(crate) fn source(source: &CopySource) -> S3Result<(&str, &str)> {
    match source {
        CopySource::Bucket {
            bucket,
            key,
            version_id,
        } => {
            version(version_id.as_deref())?;
            Ok((bucket, key))
        }
        _ => Err(s3_error!(NotImplemented)),
    }
}

fn matches(condition: &ETagCondition, object: Option<&metadata::Object>, strong: bool) -> bool {
    match (condition, object) {
        (_, None) => false,
        (ETagCondition::Any, Some(_)) => true,
        (ETagCondition::ETag(tag), Some(object)) => {
            if strong {
                tag.as_strong() == Some(&object.etag)
            } else {
                tag.value() == object.etag
            }
        }
    }
}

pub(crate) fn conditions(
    object: Option<&metadata::Object>,
    equal: Option<&ETagCondition>,
    different: Option<&ETagCondition>,
    modified: Option<&Timestamp>,
    unmodified: Option<&Timestamp>,
    read: bool,
) -> S3Result<()> {
    if equal.is_some_and(|c| !matches(c, object, true)) {
        return Err(s3_error!(PreconditionFailed));
    }
    if different.is_some_and(|c| matches(c, object, false)) {
        return Err(if read {
            s3_error!(NotModified)
        } else {
            s3_error!(PreconditionFailed)
        });
    }
    if let Some(object) = object {
        let time = timestamp(object.modified);
        if equal.is_none() && unmodified.is_some_and(|bound| time > *bound) {
            return Err(s3_error!(PreconditionFailed));
        }
        if different.is_none() && modified.is_some_and(|bound| time <= *bound) {
            return Err(if read {
                s3_error!(NotModified)
            } else {
                s3_error!(PreconditionFailed)
            });
        }
    }
    Ok(())
}

// A prepared change retains its bucket identity and keeps its content live until publication.
struct ObjectChange<'a> {
    store: &'a Store,
    destination: metadata::Bucket,
    key: &'a str,
    equal: Option<&'a ETagCondition>,
    different: Option<&'a ETagCondition>,
    _activity: Arc<OwnedRwLockReadGuard<()>>,
}

impl ObjectChange<'_> {
    async fn publish(
        self,
        prepare: impl FnOnce() -> metadata::Object,
    ) -> S3Result<metadata::Object> {
        let _mutation = self.store.lock().await?;
        if self.store.require_bucket(&self.destination.name)?.id != self.destination.id {
            return Err(s3_error!(NoSuchBucket));
        }
        conditions(
            self.store
                .index
                .object(&self.destination.name, self.key)?
                .as_ref(),
            self.equal,
            self.different,
            None,
            None,
            false,
        )?;
        let object = prepare();
        self.store.publish(&object).await?;
        Ok(object)
    }
}

impl Store {
    pub(crate) async fn put_object(
        self: &Arc<Self>,
        mut input: PutObjectInput,
        headers: &http::HeaderMap,
        trailing_headers: Option<TrailingHeaders>,
    ) -> S3Result<metadata::Object> {
        let _activity = self.active().await?;
        let destination = {
            let _mutation = self.lock().await?;
            self.require_bucket(&input.bucket)?
        };
        let mut expected = Checksums::default();
        checksum_fields!(collect_field, input, expected);
        let trailers = upload_trailers(headers, trailing_headers)?;
        let algorithm = selected_algorithm(
            input.checksum_algorithm.as_ref(),
            &expected,
            trailers
                .as_ref()
                .and_then(|(_, names)| names.first().map(String::as_str)),
        )?;
        let staged = self
            .stage(
                input.body.take(),
                input.content_length,
                expected,
                &algorithm,
                input.content_md5.as_deref(),
                trailers,
            )
            .await?;
        let content = self.ingest(&staged).await?;
        let change = ObjectChange {
            store: self,
            destination,
            key: &input.key,
            equal: input.if_match.as_ref(),
            different: input.if_none_match.as_ref(),
            _activity,
        };
        let attrs = attributes!(input);
        change
            .publish(|| metadata::Object {
                bucket: input.bucket.clone(),
                key: input.key.clone(),
                content,
                size: staged.size,
                etag: staged.etag.clone(),
                modified: now(),
                attributes: attrs,
                checksums: staged.checksums.clone(),
                parts: vec![],
                upload_id: None,
            })
            .await
    }

    pub(crate) async fn copy_object(
        self: &Arc<Self>,
        input: CopyObjectInput,
    ) -> S3Result<metadata::Object> {
        let activity = self.active().await?;
        let _mutation = self.lock().await?;
        let (bucket, key) = source(&input.copy_source)?;
        let source = self.require_object(bucket, key)?;
        conditions(
            Some(&source),
            input.copy_source_if_match.as_ref(),
            input.copy_source_if_none_match.as_ref(),
            input.copy_source_if_modified_since.as_ref(),
            input.copy_source_if_unmodified_since.as_ref(),
            false,
        )?;
        let destination = self.require_bucket(&input.bucket)?;
        conditions(
            self.index.object(&input.bucket, &input.key)?.as_ref(),
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            None,
            None,
            false,
        )?;
        let attrs = match input
            .metadata_directive
            .as_ref()
            .map(|d| d.as_str())
            .unwrap_or("COPY")
        {
            "COPY" => source.attributes.clone(),
            "REPLACE" => attributes!(input),
            _ => return Err(s3_error!(InvalidArgument)),
        };
        if source.size > crate::storage::MAX_PART {
            return Err(s3_error!(EntityTooLarge));
        }
        drop(_mutation);
        // S3 copy produces a single logical part. Hash plaintext while reusing authenticated ciphertext.
        let algorithm = input
            .checksum_algorithm
            .as_ref()
            .map(|a| a.as_str())
            .or_else(|| source.checksums.values.keys().next().map(String::as_str))
            .unwrap_or("CRC64NVME");
        checksum::hasher([algorithm])?;
        let (etag, checksums) =
            if source.parts.is_empty() && source.checksums.values.contains_key(algorithm) {
                (source.etag.clone(), source.checksums.clone())
            } else {
                self.hash_content(
                    source.content.clone(),
                    0..source.size,
                    algorithm,
                    activity.clone(),
                )
                .await?
            };
        let object = metadata::Object {
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            content: source.content,
            size: source.size,
            etag: etag.clone(),
            modified: now(),
            attributes: attrs,
            checksums: checksums.clone(),
            parts: vec![],
            upload_id: None,
        };
        ObjectChange {
            store: self,
            destination,
            key: &input.key,
            equal: input.if_match.as_ref(),
            different: input.if_none_match.as_ref(),
            _activity: activity,
        }
        .publish(|| object)
        .await
    }

    pub(crate) async fn delete_object(&self, input: DeleteObjectInput) -> S3Result<()> {
        let _activity = self.active().await?;
        let _mutation = self.lock().await?;
        self.require_bucket(&input.bucket)?;
        let object = self.index.object(&input.bucket, &input.key)?;
        conditions(
            object.as_ref(),
            input.if_match.as_ref(),
            None,
            None,
            None,
            false,
        )?;
        if input
            .if_match_size
            .is_some_and(|size| object.as_ref().is_none_or(|o| o.size as i64 != size))
            || input
                .if_match_last_modified_time
                .as_ref()
                .is_some_and(|time| {
                    object
                        .as_ref()
                        .is_none_or(|o| timestamp(o.modified) != *time)
                })
        {
            return Err(s3_error!(PreconditionFailed));
        }
        self.erase_object(&input.bucket, &input.key).await?;
        Ok(())
    }

    pub(crate) async fn delete_objects(
        &self,
        input: DeleteObjectsInput,
    ) -> S3Result<DeleteObjectsOutput> {
        let _activity = self.active().await?;
        let _mutation = self.lock().await?;
        self.require_bucket(&input.bucket)?;
        if input.delete.objects.len() > MAX_DELETE_OBJECTS || input.mfa.is_some() {
            return Err(s3_error!(InvalidArgument));
        }
        let mut result = DeleteObjectsOutput {
            deleted: Some(vec![]),
            errors: Some(vec![]),
            ..Default::default()
        };
        for item in input.delete.objects {
            let mut error = version(item.version_id.as_deref()).err();
            if error.is_none()
                && let Some(etag) = &item.e_tag
            {
                let condition = if etag.as_strong() == Some("*") {
                    ETagCondition::Any
                } else {
                    ETagCondition::ETag(etag.clone())
                };
                error = match self.index.object(&input.bucket, &item.key)? {
                    None => Some(s3_error!(NoSuchKey)),
                    Some(object) if !matches(&condition, Some(&object), true) => {
                        Some(s3_error!(PreconditionFailed))
                    }
                    _ => None,
                };
            }
            if let Some(error) = error {
                result.errors.as_mut().unwrap().push(s3s::dto::Error {
                    key: Some(item.key),
                    code: Some(error.code().as_str().into()),
                    message: error.message().map(str::to_owned),
                    ..Default::default()
                });
                continue;
            }
            self.erase_object(&input.bucket, &item.key).await?;
            if input.delete.quiet != Some(true) {
                result.deleted.as_mut().unwrap().push(DeletedObject {
                    key: Some(item.key),
                    ..Default::default()
                });
            }
        }
        Ok(result)
    }

    pub(crate) async fn complete_object(
        self: &Arc<Self>,
        input: CompleteMultipartUploadInput,
    ) -> S3Result<metadata::Object> {
        let _activity = self.active().await?;
        let _mutation = self.lock().await?;
        let upload = self.require_upload(&input.bucket, &input.key, &input.upload_id)?;
        let requested = input
            .multipart_upload
            .clone()
            .and_then(|m| m.parts)
            .unwrap_or_default();
        let part_count = requested.len();
        if requested.is_empty() || requested.len() > MAX_MULTIPART_PARTS as usize {
            return Err(s3_error!(InvalidPart));
        }
        if input
            .checksum_type
            .as_ref()
            .is_some_and(|kind| kind.as_str() != upload.checksum_type)
        {
            return Err(s3_error!(BadDigest));
        }
        let mut parts = Vec::with_capacity(requested.len());
        let mut previous = 0;
        let mut size = 0u64;
        for (index, requested) in requested.iter().enumerate() {
            let number = requested
                .part_number
                .ok_or_else(|| s3_error!(InvalidPart))?;
            if number <= previous {
                return Err(s3_error!(InvalidPartOrder));
            }
            // Additional checksum modes require consecutive client part numbers.
            if number != index as i32 + 1 {
                return Err(s3_error!(InvalidPartOrder));
            }
            previous = number;
            let part = upload
                .parts
                .get(&number)
                .ok_or_else(|| s3_error!(InvalidPart))?;
            if requested.e_tag.as_ref().and_then(ETag::as_strong) != Some(&part.etag) {
                return Err(s3_error!(InvalidPart));
            }
            if index + 1 < part_count && part.size < MIN_MULTIPART_PART_BYTES {
                return Err(s3_error!(EntityTooSmall));
            }
            let mut expected = Checksums::default();
            checksum_fields!(collect_field, requested, expected);
            checksum::validate(&part.checksums, &expected)?;
            size = size
                .checked_add(part.size)
                .ok_or_else(|| s3_error!(EntityTooLarge))?;
            parts.push(part.clone());
        }
        if input
            .mpu_object_size
            .is_some_and(|expected| expected < 0 || expected as u64 != size)
        {
            return Err(s3_error!(InvalidRequest));
        }
        let (etag, checksums) =
            checksum::multipart(&parts, &upload.algorithm, &upload.checksum_type)?;
        let mut expected = Checksums::default();
        checksum_fields!(collect_field, input, expected);
        checksum::validate(&checksums, &expected)?;
        conditions(
            self.index.object(&input.bucket, &input.key)?.as_ref(),
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            None,
            None,
            false,
        )?;
        let content = self
            .compose(
                parts
                    .iter()
                    .map(|p| Extent {
                        content: p.content.clone(),
                        offset: 0,
                        size: p.size,
                    })
                    .collect(),
            )
            .await?;
        let object = metadata::Object {
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            content,
            size,
            etag: etag.clone(),
            modified: now(),
            attributes: upload.attributes,
            checksums: checksums.clone(),
            parts,
            upload_id: Some(upload.id.clone()),
        };
        self.publish(&object).await?;
        self.indexed(self.index.remove_upload(&upload.id))?;
        Ok(object)
    }

    async fn publish(&self, object: &metadata::Object) -> crate::Result<()> {
        self.require_bucket(&object.bucket)?;
        let path = self.object_path(&object.bucket, &object.key);
        self.index.begin(&path, &object.bucket, Some(&object.key))?;
        self.write_record(&path, object, true).await?;
        self.indexed(self.index.save_object(object))?;
        self.finish_publication(&path)
    }

    async fn erase_object(&self, bucket: &str, key: &str) -> crate::Result<()> {
        let path = self.object_path(bucket, key);
        self.index.begin(&path, bucket, Some(key))?;
        self.remove_record(&path).await?;
        self.indexed(self.index.remove_object(bucket, key))?;
        self.finish_publication(&path)
    }
}
