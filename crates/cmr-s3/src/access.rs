use std::collections::HashMap;

use s3s::{
    S3Request, S3Result,
    access::{S3Access, S3AccessContext},
    dto::{CopyObjectInput, CopySource, UploadPartCopyInput},
    s3_error,
};

use crate::BucketConfig;

#[derive(Clone)]
pub(crate) struct BucketScope(pub String);

pub(crate) struct BucketAccess(HashMap<String, String>);

impl BucketAccess {
    pub fn new(buckets: &[BucketConfig]) -> Self {
        Self(
            buckets
                .iter()
                .map(|b| (b.access_key.clone(), b.name.clone()))
                .collect(),
        )
    }

    fn source<T>(&self, req: &S3Request<T>, source: &CopySource) -> S3Result<()> {
        let allowed = req
            .credentials
            .as_ref()
            .and_then(|c| self.0.get(&c.access_key));
        match source {
            CopySource::Bucket { bucket, .. }
                if allowed.is_some_and(|name| name.as_str() == bucket.as_ref()) =>
            {
                Ok(())
            }
            _ => Err(s3_error!(
                AccessDenied,
                "The copy source is outside your bucket"
            )),
        }
    }
}

#[async_trait::async_trait]
impl S3Access for BucketAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let bucket = cx
            .credentials()
            .and_then(|c| self.0.get(&c.access_key))
            .ok_or_else(|| s3_error!(AccessDenied, "Signature is required"))?;
        if matches!(cx.s3_op().name(), "CreateBucket" | "DeleteBucket") {
            return Err(s3_error!(
                AccessDenied,
                "Buckets are managed through the configuration file"
            ));
        }
        if cx
            .s3_path()
            .get_bucket_name()
            .is_some_and(|name| name != bucket)
        {
            return Err(s3_error!(
                AccessDenied,
                "The credentials do not grant access to this bucket"
            ));
        }
        cx.extensions_mut().insert(BucketScope(bucket.clone()));
        Ok(())
    }

    async fn copy_object(&self, req: &mut S3Request<CopyObjectInput>) -> S3Result<()> {
        self.source(req, &req.input.copy_source)
    }

    async fn upload_part_copy(&self, req: &mut S3Request<UploadPartCopyInput>) -> S3Result<()> {
        self.source(req, &req.input.copy_source)
    }
}
