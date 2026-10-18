use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use s3s::{S3, S3Request, S3Response, S3Result, dto::*, s3_error};

use crate::{
    S3Backend,
    checksum::{self, assign_field, checksum_fields, collect_field, upload_trailers},
    metadata::{self, Checksums, Extent, attributes, now, timestamp},
    objects::{conditions, source, version},
};

const MAX_KEY_BYTES: usize = 1024;
const MAX_BUCKETS_PER_PAGE: i32 = 10_000;
const MAX_ENTRIES_PER_PAGE: i32 = 1000;
pub(crate) const MAX_MULTIPART_PARTS: i32 = 10_000;

fn output<T>(value: T) -> S3Result<S3Response<T>> {
    Ok(S3Response::new(value))
}

fn request<T>(request: &S3Request<T>) -> S3Result<()> {
    for name in request.headers.keys() {
        let name = name.as_str();
        if let Some(suffix) = name.strip_prefix("x-amz-checksum-")
            && !matches!(suffix, "algorithm" | "mode" | "type")
            && !checksum::ALGORITHMS
                .iter()
                .any(|(_, known)| *known == suffix)
        {
            return Err(s3_error!(
                NotImplemented,
                "The checksum algorithm is unsupported"
            ));
        }
        if name.starts_with("x-amz-server-side-encryption-customer-")
            || name.starts_with("x-amz-copy-source-server-side-encryption-customer-")
            || name.starts_with("x-amz-grant-")
            || name.starts_with("x-amz-object-lock-")
            || matches!(
                name,
                "x-amz-tagging"
                    | "x-amz-tagging-directive"
                    | "x-amz-website-redirect-location"
                    | "x-amz-write-offset-bytes"
                    | "x-amz-request-payer"
                    | "x-amz-bypass-governance-retention"
                    | "x-amz-server-side-encryption-aws-kms-key-id"
                    | "x-amz-server-side-encryption-context"
                    | "x-amz-expected-bucket-owner"
                    | "x-amz-source-expected-bucket-owner"
            )
        {
            return Err(s3_error!(
                NotImplemented,
                "The requested feature is unsupported"
            ));
        }
    }
    if request
        .headers
        .get("x-amz-acl")
        .is_some_and(|value| value != "private")
    {
        return Err(s3_error!(
            NotImplemented,
            "Only private access is supported"
        ));
    }
    if request
        .headers
        .get("x-amz-storage-class")
        .is_some_and(|value| value != "STANDARD")
    {
        return Err(s3_error!(
            NotImplemented,
            "Only STANDARD storage is supported"
        ));
    }
    if request
        .headers
        .get("x-amz-server-side-encryption")
        .is_some_and(|value| value != "AES256")
        || request
            .headers
            .get("x-amz-server-side-encryption-bucket-key-enabled")
            .is_some_and(|value| value != "false")
    {
        return Err(s3_error!(
            NotImplemented,
            "Only AES256 encryption is supported"
        ));
    }
    Ok(())
}

fn key(key: &str) -> S3Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(s3_error!(KeyTooLongError));
    }
    Ok(())
}

macro_rules! write_features {
    ($input:expr) => {
        if $input
            .server_side_encryption
            .as_ref()
            .is_some_and(|a| a.as_str() != "AES256")
            || $input.sse_customer_algorithm.is_some()
            || $input.sse_customer_key.is_some()
            || $input.sse_customer_key_md5.is_some()
            || $input.ssekms_key_id.is_some()
            || $input.ssekms_encryption_context.is_some()
            || $input.bucket_key_enabled == Some(true)
            || $input.acl.as_ref().is_some_and(|a| a.as_str() != "private")
            || $input.grant_full_control.is_some()
            || $input.grant_read.is_some()
            || $input.grant_read_acp.is_some()
            || $input.grant_write_acp.is_some()
            || $input.tagging.is_some()
            || $input.object_lock_mode.is_some()
            || $input.object_lock_legal_hold_status.is_some()
            || $input.object_lock_retain_until_date.is_some()
            || $input.website_redirect_location.is_some()
            || $input
                .storage_class
                .as_ref()
                .is_some_and(|a| a.as_str() != "STANDARD")
        {
            return Err(s3_error!(
                NotImplemented,
                "The requested storage feature is unsupported"
            ));
        }
    };
}
macro_rules! read_features {
    ($input:expr) => {
        version($input.version_id.as_deref())?;
        if $input.sse_customer_algorithm.is_some()
            || $input.sse_customer_key.is_some()
            || $input.sse_customer_key_md5.is_some()
        {
            return Err(s3_error!(
                NotImplemented,
                "Customer encryption keys are unsupported"
            ));
        }
    };
}
macro_rules! object_headers {
    ($object:expr, $target:expr) => {
        $target.e_tag = Some(ETag::Strong($object.etag.clone()));
        $target.last_modified = Some(timestamp($object.modified));
        $target.metadata = Some($object.attributes.metadata.clone());
        $target.content_type = $object
            .attributes
            .content_type
            .clone()
            .or_else(|| Some("application/octet-stream".into()));
        $target.cache_control = $object.attributes.cache_control.clone();
        $target.content_disposition = $object.attributes.content_disposition.clone();
        $target.content_encoding = $object.attributes.content_encoding.clone();
        $target.content_language = $object.attributes.content_language.clone();
        $target.expires = $object.attributes.expires.clone();
        $target.server_side_encryption = Some(ServerSideEncryption::from_static("AES256"));
        $target.accept_ranges = Some("bytes".into());
        if !$object.parts.is_empty() {
            $target.parts_count = Some($object.parts.len() as i32);
        }
    };
}

fn read_range(
    object: &metadata::Object,
    range: Option<&Range>,
    part: Option<i32>,
) -> S3Result<(std::ops::Range<u64>, bool)> {
    if let Some(part) = part {
        if range.is_some() || part < 1 {
            return Err(s3_error!(InvalidArgument));
        }
        if object.parts.is_empty() {
            if part != 1 {
                return Err(s3_error!(InvalidArgument));
            }
            return Ok((0..object.size, true));
        }
        let mut offset = 0;
        for p in &object.parts {
            if p.number == part {
                return Ok((offset..offset + p.size, true));
            }
            offset += p.size;
        }
        return Err(s3_error!(InvalidArgument));
    }
    if let Some(range) = range {
        if object.size == 0 {
            return Err(s3_error!(InvalidRange));
        }
        return Ok((
            range
                .check(object.size)
                .map_err(|_| s3_error!(InvalidRange))?,
            true,
        ));
    }
    Ok((0..object.size, false))
}

fn content_range(range: &std::ops::Range<u64>, total: u64) -> Option<String> {
    (!range.is_empty()).then(|| format!("bytes {}-{}/{total}", range.start, range.end - 1))
}

fn read_checksums<'a>(
    object: &'a metadata::Object,
    range: &std::ops::Range<u64>,
    partial: bool,
) -> Option<&'a Checksums> {
    if !partial || (object.parts.is_empty() && *range == (0..object.size)) {
        return Some(&object.checksums);
    }
    let mut offset = 0;
    for part in &object.parts {
        let end = offset + part.size;
        if *range == (offset..end) {
            return Some(&part.checksums);
        }
        offset = end;
    }
    None
}

#[async_trait::async_trait]
impl S3 for S3Backend {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        request(&req)?;
        let input = req.input;
        if input.acl.as_ref().is_some_and(|a| a.as_str() != "private")
            || input.object_lock_enabled_for_bucket == Some(true)
            || input.grant_full_control.is_some()
            || input.grant_read.is_some()
            || input.grant_write.is_some()
            || input.grant_read_acp.is_some()
            || input.grant_write_acp.is_some()
            || input.bucket_namespace.is_some()
        {
            return Err(s3_error!(NotImplemented));
        }
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        if self.0.index.bucket(&input.bucket)?.is_some() {
            return Err(s3_error!(BucketAlreadyOwnedByYou));
        }
        let bucket = metadata::Bucket {
            name: input.bucket.clone(),
            id: uuid::Uuid::new_v4().to_string(),
            created: now(),
        };
        self.0.create_bucket(&bucket).await?;
        output(CreateBucketOutput {
            location: Some(format!("/{}", input.bucket)),
            ..Default::default()
        })
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        request(&req)?;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let scope = req.extensions.get::<crate::access::BucketScope>();
        let input = req.input;
        let after = decode_token(
            &self.0.cipher,
            "buckets",
            input.prefix.as_deref().unwrap_or(""),
            "",
            input.continuation_token.as_deref(),
        )?;
        let limit = input
            .max_buckets
            .unwrap_or(MAX_BUCKETS_PER_PAGE)
            .clamp(0, MAX_BUCKETS_PER_PAGE) as usize;
        let mut buckets: Vec<_> = self
            .0
            .index
            .buckets()?
            .into_iter()
            .filter(|b| scope.is_none_or(|scope| scope.0 == b.name))
            .filter(|b| b.name.starts_with(input.prefix.as_deref().unwrap_or("")) && b.name > after)
            .collect();
        let truncated = buckets.len() > limit;
        buckets.truncate(limit);
        let next = if truncated {
            buckets.last().map(|b| {
                encode_token(
                    &self.0.cipher,
                    "buckets",
                    input.prefix.as_deref().unwrap_or(""),
                    "",
                    &b.name,
                )
            })
        } else {
            None
        };
        output(ListBucketsOutput {
            buckets: Some(
                buckets
                    .into_iter()
                    .map(|b| Bucket {
                        name: Some(b.name),
                        creation_date: Some(timestamp(b.created)),
                        ..Default::default()
                    })
                    .collect(),
            ),
            continuation_token: next,
            prefix: input.prefix,
            ..Default::default()
        })
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        request(&req)?;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        self.0.require_bucket(&req.input.bucket)?;
        output(HeadBucketOutput {
            bucket_region: Some("us-east-1".into()),
            ..Default::default()
        })
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        request(&req)?;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        self.0.require_bucket(&req.input.bucket)?;
        output(GetBucketLocationOutput::default())
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        request(&req)?;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let bucket = req.input.bucket;
        self.0.require_bucket(&bucket)?;
        if !self.0.index.object_page(&bucket, "", "", 1)?.is_empty()
            || self.0.index.uploads()?.iter().any(|u| u.bucket == bucket)
        {
            return Err(s3_error!(BucketNotEmpty));
        }
        self.0.delete_bucket(&bucket).await?;
        output(DeleteBucketOutput::default())
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        request(&req)?;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        self.0.require_bucket(&req.input.bucket)?;
        output(GetBucketEncryptionOutput {
            server_side_encryption_configuration: Some(ServerSideEncryptionConfiguration {
                rules: vec![ServerSideEncryptionRule {
                    apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                        sse_algorithm: ServerSideEncryption::from_static("AES256"),
                        kms_master_key_id: None,
                    }),
                    ..Default::default()
                }],
            }),
        })
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        request(&req)?;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        self.0.require_bucket(&req.input.bucket)?;
        let rules = &req.input.server_side_encryption_configuration.rules;
        if rules.len() != 1
            || rules.iter().any(|rule| {
                rule.bucket_key_enabled == Some(true)
                    || rule.blocked_encryption_types.is_some()
                    || rule
                        .apply_server_side_encryption_by_default
                        .as_ref()
                        .is_none_or(|default| {
                            default.sse_algorithm.as_str() != "AES256"
                                || default.kms_master_key_id.is_some()
                        })
            })
        {
            return Err(s3_error!(
                NotImplemented,
                "Only mandatory AES256 encryption is supported"
            ));
        }
        output(PutBucketEncryptionOutput::default())
    }

    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        request(&req)?;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        self.0.require_bucket(&req.input.bucket)?;
        output(DeleteBucketEncryptionOutput::default())
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        request(&req)?;
        let input = req.input;
        key(&input.key)?;
        write_features!(input);
        if input.write_offset_bytes.is_some() {
            return Err(s3_error!(NotImplemented));
        }
        let object = self
            .0
            .put_object(input, &req.headers, req.trailing_headers)
            .await?;
        let mut result = PutObjectOutput {
            e_tag: Some(ETag::Strong(object.etag)),
            server_side_encryption: Some(ServerSideEncryption::from_static("AES256")),
            checksum_type: Some(ChecksumType::from_static("FULL_OBJECT")),
            ..Default::default()
        };
        checksum_fields!(assign_field, object.checksums, result);
        output(result)
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        request(&req)?;
        let input = req.input;
        read_features!(input);
        let activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let object = self.0.require_object(&input.bucket, &input.key)?;
        conditions(
            Some(&object),
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            input.if_modified_since.as_ref(),
            input.if_unmodified_since.as_ref(),
            true,
        )?;
        let (range, partial) = read_range(&object, input.range.as_ref(), input.part_number)?;
        let mut result = GetObjectOutput {
            content_length: Some((range.end - range.start) as i64),
            ..Default::default()
        };
        object_headers!(object, result);
        if input
            .checksum_mode
            .as_ref()
            .is_some_and(|m| m.as_str() == "ENABLED")
            && let Some(checksums) = read_checksums(&object, &range, partial)
        {
            checksum_fields!(assign_field, checksums, result);
            result.checksum_type = Some(ChecksumType::from(object.checksums.kind.clone()));
        }
        if input.part_number.is_some() && !object.parts.is_empty() {
            result.parts_count = Some(object.parts.len() as i32);
        }
        if partial {
            result.content_range = content_range(&range, object.size);
        }
        if input.response_content_type.is_some() {
            result.content_type = input.response_content_type;
        }
        if input.response_content_language.is_some() {
            result.content_language = input.response_content_language;
        }
        if input.response_content_encoding.is_some() {
            result.content_encoding = input.response_content_encoding;
        }
        if input.response_content_disposition.is_some() {
            result.content_disposition = input.response_content_disposition;
        }
        if input.response_cache_control.is_some() {
            result.cache_control = input.response_cache_control;
        }
        if input.response_expires.is_some() {
            result.expires = input.response_expires.map(|time| {
                let mut bytes = Vec::new();
                time.format(TimestampFormat::HttpDate, &mut bytes)
                    .expect("timestamp format");
                String::from_utf8(bytes).expect("ASCII timestamp")
            });
        }
        result.body = Some(self.0.stream(object.content, range, activity));
        let mut response = S3Response::new(result);
        if partial {
            response.status = Some(http::StatusCode::PARTIAL_CONTENT);
        }
        Ok(response)
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        request(&req)?;
        let input = req.input;
        read_features!(input);
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let object = self.0.require_object(&input.bucket, &input.key)?;
        conditions(
            Some(&object),
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            input.if_modified_since.as_ref(),
            input.if_unmodified_since.as_ref(),
            true,
        )?;
        let (range, partial) = read_range(&object, input.range.as_ref(), input.part_number)?;
        let mut result = HeadObjectOutput {
            content_length: Some((range.end - range.start) as i64),
            ..Default::default()
        };
        object_headers!(object, result);
        if partial {
            result.content_range = content_range(&range, object.size);
        }
        if input
            .checksum_mode
            .as_ref()
            .is_some_and(|m| m.as_str() == "ENABLED")
            && let Some(checksums) = read_checksums(&object, &range, partial)
        {
            checksum_fields!(assign_field, checksums, result);
            result.checksum_type = Some(ChecksumType::from(object.checksums.kind));
        }
        if input.part_number.is_some() && !object.parts.is_empty() {
            result.parts_count = Some(object.parts.len() as i32);
        }
        output(result)
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        request(&req)?;
        let input = req.input;
        read_features!(input);
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let object = self.0.require_object(&input.bucket, &input.key)?;
        let mut result = GetObjectAttributesOutput {
            last_modified: Some(timestamp(object.modified)),
            ..Default::default()
        };
        for attribute in input.object_attributes {
            match attribute.as_str() {
                "ETag" => result.e_tag = Some(ETag::Strong(object.etag.clone())),
                "ObjectSize" => result.object_size = Some(object.size as i64),
                "StorageClass" => {
                    result.storage_class = Some(StorageClass::from_static("STANDARD"))
                }
                "Checksum" => result.checksum = Some(checksum::dto(&object.checksums)),
                "ObjectParts" => {
                    let marker = input.part_number_marker.unwrap_or(0);
                    let limit = page_limit(input.max_parts)?;
                    let mut parts: Vec<_> = object
                        .parts
                        .iter()
                        .filter(|p| p.number > marker)
                        .take(limit + 1)
                        .collect();
                    let truncated = parts.len() > limit;
                    parts.truncate(limit);
                    result.object_parts = Some(GetObjectAttributesParts {
                        total_parts_count: Some(object.parts.len() as i32),
                        max_parts: Some(limit as i32),
                        part_number_marker: Some(marker),
                        next_part_number_marker: if truncated {
                            parts.last().map(|p| p.number)
                        } else {
                            None
                        },
                        is_truncated: Some(truncated),
                        parts: Some(
                            parts
                                .into_iter()
                                .map(|p| {
                                    let mut part = ObjectPart {
                                        part_number: Some(p.number),
                                        size: Some(p.size as i64),
                                        ..Default::default()
                                    };
                                    checksum_fields!(assign_field, p.checksums, part);
                                    part
                                })
                                .collect(),
                        ),
                    });
                }
                _ => return Err(s3_error!(NotImplemented)),
            }
        }
        output(result)
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        request(&req)?;
        let input = req.input;
        version(input.version_id.as_deref())?;
        self.0.delete_object(input).await?;
        output(DeleteObjectOutput::default())
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        request(&req)?;
        let input = req.input;
        let result = self.0.delete_objects(input).await?;
        output(result)
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        request(&req)?;
        let input = req.input;
        key(&input.key)?;
        write_features!(input);
        let object = self.0.copy_object(input).await?;
        let checksums = object.checksums;
        let mut copied = CopyObjectResult {
            e_tag: Some(ETag::Strong(object.etag)),
            last_modified: Some(timestamp(object.modified)),
            checksum_type: Some(ChecksumType::from(checksums.kind.clone())),
            ..Default::default()
        };
        checksum_fields!(assign_field, checksums, copied);
        output(CopyObjectOutput {
            copy_object_result: Some(copied),
            server_side_encryption: Some(ServerSideEncryption::from_static("AES256")),
            ..Default::default()
        })
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        request(&req)?;
        let input = req.input;
        key(&input.key)?;
        write_features!(input);
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        self.0.require_bucket(&input.bucket)?;
        let algorithm = input
            .checksum_algorithm
            .as_ref()
            .map(|a| a.as_str())
            .unwrap_or("CRC64NVME")
            .to_owned();
        let kind = checksum::kind(&algorithm, input.checksum_type.as_ref().map(|t| t.as_str()))?;
        let upload = metadata::Upload {
            id: uuid::Uuid::new_v4().to_string(),
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            created: now(),
            updated: now(),
            attributes: attributes!(input),
            algorithm: algorithm.clone(),
            checksum_type: kind.clone(),
            parts: BTreeMap::new(),
        };
        self.0.index.save_upload(&upload)?;
        output(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload.id),
            checksum_algorithm: Some(ChecksumAlgorithm::from(algorithm)),
            checksum_type: Some(ChecksumType::from(kind)),
            server_side_encryption: Some(ServerSideEncryption::from_static("AES256")),
            ..Default::default()
        })
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        request(&req)?;
        let mut input = req.input;
        part_number(input.part_number)?;
        if input.sse_customer_algorithm.is_some()
            || input.sse_customer_key.is_some()
            || input.sse_customer_key_md5.is_some()
        {
            return Err(s3_error!(NotImplemented));
        }
        let _activity = self.0.active().await?;
        let initial = {
            let _mutation = self.0.lock().await?;
            self.0
                .require_upload(&input.bucket, &input.key, &input.upload_id)?
        };
        if input
            .checksum_algorithm
            .as_ref()
            .is_some_and(|a| a.as_str() != initial.algorithm)
        {
            return Err(s3_error!(InvalidRequest));
        }
        let mut expected = Checksums::default();
        checksum_fields!(collect_field, input, expected);
        let trailers = upload_trailers(&req.headers, req.trailing_headers)?;
        let staged = self
            .0
            .stage(
                input.body.take(),
                input.content_length,
                expected,
                &initial.algorithm,
                input.content_md5.as_deref(),
                trailers,
            )
            .await?;
        let content = self.0.ingest(&staged).await?;
        self.0
            .record_part(
                &initial,
                metadata::Part {
                    number: input.part_number,
                    content,
                    size: staged.size,
                    etag: staged.etag.clone(),
                    modified: 0,
                    checksums: staged.checksums.clone(),
                },
            )
            .await?;
        let mut result = UploadPartOutput {
            e_tag: Some(ETag::Strong(staged.etag.clone())),
            server_side_encryption: Some(ServerSideEncryption::from_static("AES256")),
            ..Default::default()
        };
        checksum_fields!(assign_field, staged.checksums, result);
        output(result)
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        request(&req)?;
        let input = req.input;
        part_number(input.part_number)?;
        let activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let upload = self
            .0
            .require_upload(&input.bucket, &input.key, &input.upload_id)?;
        let (bucket, key) = source(&input.copy_source)?;
        let source = self.0.require_object(bucket, key)?;
        conditions(
            Some(&source),
            input.copy_source_if_match.as_ref(),
            input.copy_source_if_none_match.as_ref(),
            input.copy_source_if_modified_since.as_ref(),
            input.copy_source_if_unmodified_since.as_ref(),
            false,
        )?;
        let range = match input.copy_source_range.as_deref() {
            None => 0..source.size,
            Some(value) => {
                let value = value
                    .strip_prefix("bytes=")
                    .ok_or_else(|| s3_error!(InvalidArgument))?;
                let (first, last) = value
                    .split_once('-')
                    .ok_or_else(|| s3_error!(InvalidArgument))?;
                let first: u64 = first.parse().map_err(|_| s3_error!(InvalidArgument))?;
                let last: u64 = last.parse().map_err(|_| s3_error!(InvalidArgument))?;
                if first > last || last >= source.size {
                    return Err(s3_error!(InvalidRange));
                }
                first..last + 1
            }
        };
        let size = range.end - range.start;
        if size > crate::storage::MAX_PART {
            return Err(s3_error!(EntityTooLarge));
        }
        drop(_mutation);
        let content = self
            .0
            .compose(vec![Extent {
                content: source.content.clone(),
                offset: range.start,
                size,
            }])
            .await?;
        let (etag, checksums) = self
            .0
            .hash_content(source.content, range, &upload.algorithm, activity.clone())
            .await?;
        let part = self
            .0
            .record_part(
                &upload,
                metadata::Part {
                    number: input.part_number,
                    content,
                    size,
                    etag: etag.clone(),
                    modified: 0,
                    checksums: checksums.clone(),
                },
            )
            .await?;
        let mut copied = CopyPartResult {
            e_tag: Some(ETag::Strong(etag)),
            last_modified: Some(timestamp(part.modified)),
            ..Default::default()
        };
        checksum_fields!(assign_field, checksums, copied);
        output(UploadPartCopyOutput {
            copy_part_result: Some(copied),
            server_side_encryption: Some(ServerSideEncryption::from_static("AES256")),
            ..Default::default()
        })
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        request(&req)?;
        let input = req.input;
        let object = self.0.complete_object(input).await?;
        let checksums = object.checksums;
        let mut result = CompleteMultipartUploadOutput {
            bucket: Some(object.bucket),
            key: Some(object.key),
            e_tag: Some(ETag::Strong(object.etag)),
            server_side_encryption: Some(ServerSideEncryption::from_static("AES256")),
            checksum_type: Some(ChecksumType::from(checksums.kind.clone())),
            ..Default::default()
        };
        checksum_fields!(assign_field, checksums, result);
        output(result)
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        request(&req)?;
        let input = req.input;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let upload = self
            .0
            .require_upload(&input.bucket, &input.key, &input.upload_id)?;
        if input
            .if_match_initiated_time
            .as_ref()
            .is_some_and(|time| *time != timestamp(upload.created))
        {
            return Err(s3_error!(PreconditionFailed));
        }
        self.0.index.remove_upload(&upload.id)?;
        output(AbortMultipartUploadOutput::default())
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        request(&req)?;
        let input = req.input;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let upload = self
            .0
            .require_upload(&input.bucket, &input.key, &input.upload_id)?;
        let marker = input.part_number_marker.unwrap_or(0);
        let limit = page_limit(input.max_parts)?;
        let mut parts: Vec<_> = upload
            .parts
            .values()
            .filter(|p| p.number > marker)
            .take(limit + 1)
            .cloned()
            .collect();
        let truncated = parts.len() > limit;
        parts.truncate(limit);
        let next = if truncated {
            parts.last().map(|p| p.number)
        } else {
            None
        };
        output(ListPartsOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload.id),
            is_truncated: Some(truncated),
            part_number_marker: Some(marker),
            next_part_number_marker: next,
            max_parts: Some(limit as i32),
            checksum_algorithm: Some(ChecksumAlgorithm::from(upload.algorithm)),
            checksum_type: Some(ChecksumType::from(upload.checksum_type)),
            parts: Some(
                parts
                    .into_iter()
                    .map(|part| {
                        let mut result = Part {
                            part_number: Some(part.number),
                            size: Some(part.size as i64),
                            e_tag: Some(ETag::Strong(part.etag)),
                            last_modified: Some(timestamp(part.modified)),
                            ..Default::default()
                        };
                        checksum_fields!(assign_field, part.checksums, result);
                        result
                    })
                    .collect(),
            ),
            ..Default::default()
        })
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        request(&req)?;
        let input = req.input;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        self.0.require_bucket(&input.bucket)?;
        let limit = page_limit(input.max_uploads)?;
        let prefix = input.prefix.as_deref().unwrap_or("");
        let delimiter = input.delimiter.as_deref().unwrap_or("");
        let mut entries = Vec::new();
        let mut prefixes = BTreeSet::new();
        for upload in self.0.index.uploads()? {
            if upload.bucket != input.bucket || !upload.key.starts_with(prefix) {
                continue;
            }
            if let Some(marker) = &input.key_marker
                && (upload.key < *marker
                    || (upload.key == *marker
                        && input
                            .upload_id_marker
                            .as_ref()
                            .is_none_or(|id| upload.id <= *id)))
            {
                continue;
            }
            if !delimiter.is_empty()
                && let Some(offset) = upload.key[prefix.len()..].find(delimiter)
            {
                let p = upload.key[..prefix.len() + offset + delimiter.len()].to_owned();
                if input.key_marker.as_ref().is_none_or(|marker| p > *marker)
                    && prefixes.insert(p.clone())
                {
                    entries.push((p, String::new(), None));
                }
                continue;
            }
            entries.push((upload.key.clone(), upload.id.clone(), Some(upload)));
        }
        entries.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        let truncated = entries.len() > limit;
        entries.truncate(limit);
        let next = entries.last().map(|e| (e.0.clone(), e.1.clone()));
        let mut uploads = vec![];
        let mut common_prefixes = vec![];
        for (key, _, upload) in entries {
            match upload {
                None => common_prefixes.push(CommonPrefix {
                    prefix: Some(encode_key(&key, input.encoding_type.as_ref())?),
                }),
                Some(upload) => uploads.push(MultipartUpload {
                    key: Some(encode_key(&key, input.encoding_type.as_ref())?),
                    upload_id: Some(upload.id),
                    initiated: Some(timestamp(upload.created)),
                    checksum_algorithm: Some(ChecksumAlgorithm::from(upload.algorithm)),
                    checksum_type: Some(ChecksumType::from(upload.checksum_type)),
                    ..Default::default()
                }),
            }
        }
        output(ListMultipartUploadsOutput {
            bucket: Some(input.bucket),
            prefix: input
                .prefix
                .map(|s| encode_key(&s, input.encoding_type.as_ref()))
                .transpose()?,
            delimiter: input
                .delimiter
                .map(|s| encode_key(&s, input.encoding_type.as_ref()))
                .transpose()?,
            key_marker: input
                .key_marker
                .map(|s| encode_key(&s, input.encoding_type.as_ref()))
                .transpose()?,
            upload_id_marker: input.upload_id_marker,
            max_uploads: Some(limit as i32),
            is_truncated: Some(truncated),
            next_key_marker: if truncated {
                next.as_ref()
                    .map(|n| encode_key(&n.0, input.encoding_type.as_ref()))
                    .transpose()?
            } else {
                None
            },
            next_upload_id_marker: if truncated { next.map(|n| n.1) } else { None },
            uploads: Some(uploads),
            common_prefixes: Some(common_prefixes),
            encoding_type: input.encoding_type,
            ..Default::default()
        })
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        request(&req)?;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let input = req.input;
        self.0.require_bucket(&input.bucket)?;
        let prefix = input.prefix.as_deref().unwrap_or("");
        let delimiter = input.delimiter.as_deref().unwrap_or("");
        let after = if input.continuation_token.is_some() {
            decode_token(
                &self.0.cipher,
                &input.bucket,
                prefix,
                delimiter,
                input.continuation_token.as_deref(),
            )?
        } else {
            input.start_after.clone().unwrap_or_default()
        };
        let limit = page_limit(input.max_keys)?;
        let page = list_page(
            &self.0.index,
            &input.bucket,
            prefix,
            delimiter,
            &after,
            limit,
        )?;
        let (contents, prefixes) = page_dto(&page.entries, input.encoding_type.as_ref())?;
        let next = if page.truncated {
            page.entries
                .last()
                .map(|e| encode_token(&self.0.cipher, &input.bucket, prefix, delimiter, e.name()))
        } else {
            None
        };
        output(ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: Some(encode_key(prefix, input.encoding_type.as_ref())?),
            delimiter: input
                .delimiter
                .map(|s| encode_key(&s, input.encoding_type.as_ref()))
                .transpose()?,
            max_keys: Some(limit as i32),
            key_count: Some(page.entries.len() as i32),
            is_truncated: Some(page.truncated),
            contents: Some(contents),
            common_prefixes: Some(prefixes),
            next_continuation_token: next,
            continuation_token: input.continuation_token,
            start_after: input
                .start_after
                .map(|s| encode_key(&s, input.encoding_type.as_ref()))
                .transpose()?,
            encoding_type: input.encoding_type,
            ..Default::default()
        })
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        request(&req)?;
        let _activity = self.0.active().await?;
        let _mutation = self.0.lock().await?;
        let input = req.input;
        self.0.require_bucket(&input.bucket)?;
        let prefix = input.prefix.as_deref().unwrap_or("");
        let delimiter = input.delimiter.as_deref().unwrap_or("");
        let limit = page_limit(input.max_keys)?;
        let page = list_page(
            &self.0.index,
            &input.bucket,
            prefix,
            delimiter,
            input.marker.as_deref().unwrap_or(""),
            limit,
        )?;
        let (contents, prefixes) = page_dto(&page.entries, input.encoding_type.as_ref())?;
        let next = if page.truncated {
            page.entries
                .last()
                .map(|e| encode_key(e.name(), input.encoding_type.as_ref()))
                .transpose()?
        } else {
            None
        };
        output(ListObjectsOutput {
            name: Some(input.bucket),
            prefix: Some(encode_key(prefix, input.encoding_type.as_ref())?),
            delimiter: input
                .delimiter
                .map(|s| encode_key(&s, input.encoding_type.as_ref()))
                .transpose()?,
            marker: input
                .marker
                .map(|s| encode_key(&s, input.encoding_type.as_ref()))
                .transpose()?,
            max_keys: Some(limit as i32),
            is_truncated: Some(page.truncated),
            next_marker: next,
            contents: Some(contents),
            common_prefixes: Some(prefixes),
            encoding_type: input.encoding_type,
            ..Default::default()
        })
    }
}

fn part_number(number: i32) -> S3Result<()> {
    if !(1..=MAX_MULTIPART_PARTS).contains(&number) {
        return Err(s3_error!(InvalidArgument));
    }
    Ok(())
}

fn page_limit(limit: Option<i32>) -> S3Result<usize> {
    if limit.is_some_and(|limit| limit < 0) {
        return Err(s3_error!(InvalidArgument));
    }
    Ok(limit
        .unwrap_or(MAX_ENTRIES_PER_PAGE)
        .min(MAX_ENTRIES_PER_PAGE) as usize)
}

enum Entry {
    Object(Box<metadata::Object>),
    Prefix(String),
}
impl Entry {
    fn name(&self) -> &str {
        match self {
            Self::Object(o) => &o.key,
            Self::Prefix(p) => p,
        }
    }
}
struct Page {
    entries: Vec<Entry>,
    truncated: bool,
}

fn list_page(
    index: &crate::index::Index,
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    after: &str,
    limit: usize,
) -> crate::Result<Page> {
    if limit == 0 {
        return Ok(Page {
            entries: vec![],
            truncated: false,
        });
    }
    let mut entries = Vec::new();
    let mut cursor = after.to_owned();
    let mut last_prefix = String::new();
    loop {
        let objects = index.object_page(bucket, prefix, &cursor, 256)?;
        if objects.is_empty() {
            break;
        }
        for object in objects {
            cursor = object.key.clone();
            if !delimiter.is_empty() {
                if let Some(position) = object.key[prefix.len()..].find(delimiter) {
                    let common = &object.key[..prefix.len() + position + delimiter.len()];
                    if common <= after || common == last_prefix {
                        continue;
                    }
                    last_prefix = common.to_owned();
                    entries.push(Entry::Prefix(common.into()));
                } else {
                    entries.push(Entry::Object(Box::new(object)));
                }
            } else {
                entries.push(Entry::Object(Box::new(object)));
            }
            if entries.len() > limit {
                entries.truncate(limit);
                return Ok(Page {
                    entries,
                    truncated: true,
                });
            }
        }
    }
    Ok(Page {
        entries,
        truncated: false,
    })
}

fn page_dto(
    entries: &[Entry],
    encoding: Option<&EncodingType>,
) -> S3Result<(Vec<Object>, Vec<CommonPrefix>)> {
    let mut objects = vec![];
    let mut prefixes = vec![];
    for entry in entries {
        match entry {
            Entry::Prefix(p) => prefixes.push(CommonPrefix {
                prefix: Some(encode_key(p, encoding)?),
            }),
            Entry::Object(o) => objects.push(Object {
                key: Some(encode_key(&o.key, encoding)?),
                size: Some(o.size as i64),
                e_tag: Some(ETag::Strong(o.etag.clone())),
                last_modified: Some(timestamp(o.modified)),
                storage_class: Some(ObjectStorageClass::from_static("STANDARD")),
                checksum_algorithm: Some(
                    o.checksums
                        .values
                        .keys()
                        .cloned()
                        .map(ChecksumAlgorithm::from)
                        .collect(),
                ),
                checksum_type: Some(ChecksumType::from(o.checksums.kind.clone())),
                ..Default::default()
            }),
        }
    }
    Ok((objects, prefixes))
}

fn encode_key(key: &str, encoding: Option<&EncodingType>) -> S3Result<String> {
    match encoding.map(|e| e.as_str()) {
        None => Ok(key.into()),
        Some("url") => {
            let mut result = String::new();
            for byte in key.bytes() {
                if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                    result.push(byte as char);
                } else {
                    use std::fmt::Write;
                    write!(result, "%{byte:02X}").expect("string write");
                }
            }
            Ok(result)
        }
        _ => Err(s3_error!(InvalidArgument)),
    }
}

fn encode_token(
    cipher: &crate::crypto::Cipher,
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    key: &str,
) -> String {
    let payload = URL_SAFE_NO_PAD.encode(key);
    let signature = cipher.name(&["continuation", bucket, prefix, delimiter, &payload]);
    format!("{payload}.{signature}")
}

fn decode_token(
    cipher: &crate::crypto::Cipher,
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    token: Option<&str>,
) -> S3Result<String> {
    let Some(token) = token else {
        return Ok(String::new());
    };
    let (payload, signature) = token
        .split_once('.')
        .ok_or_else(|| s3_error!(InvalidArgument))?;
    if cipher.name(&["continuation", bucket, prefix, delimiter, payload]) != signature {
        return Err(s3_error!(InvalidArgument));
    }
    String::from_utf8(
        URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| s3_error!(InvalidArgument))?,
    )
    .map_err(|_| s3_error!(InvalidArgument))
}
