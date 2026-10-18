use base64::{Engine, engine::general_purpose::STANDARD};
use s3s::{S3Result, checksum::ChecksumHasher, crypto::Checksum as _, dto, s3_error};

use crate::metadata::{Checksums, Part};

pub(crate) const ALGORITHMS: &[(&str, &str)] = &[
    ("CRC32", "crc32"),
    ("CRC32C", "crc32c"),
    ("CRC64NVME", "crc64nvme"),
    ("SHA1", "sha1"),
    ("SHA256", "sha256"),
    ("SHA512", "sha512"),
    ("MD5", "md5"),
    ("XXHASH64", "xxhash64"),
    ("XXHASH3", "xxhash3"),
    ("XXHASH128", "xxhash128"),
];

pub(crate) fn hasher(
    algorithms: impl IntoIterator<Item = impl AsRef<str>>,
) -> S3Result<ChecksumHasher> {
    let mut result = ChecksumHasher::default();
    for algorithm in algorithms {
        match algorithm.as_ref() {
            "CRC32" => result.crc32 = Some(s3s::crypto::Crc32::new()),
            "CRC32C" => result.crc32c = Some(s3s::crypto::Crc32c::new()),
            "CRC64NVME" => result.crc64nvme = Some(s3s::crypto::Crc64Nvme::new()),
            "SHA1" => result.sha1 = Some(s3s::crypto::Sha1::new()),
            "SHA256" => result.sha256 = Some(s3s::crypto::Sha256::new()),
            "SHA512" => result.sha512 = Some(s3s::crypto::Sha512::new()),
            "MD5" => result.md5 = Some(s3s::crypto::Md5::new()),
            "XXHASH64" => result.xxhash64 = Some(s3s::crypto::XxHash64::new()),
            "XXHASH3" => result.xxhash3 = Some(s3s::crypto::XxHash3::new()),
            "XXHASH128" => result.xxhash128 = Some(s3s::crypto::XxHash128::new()),
            _ => {
                return Err(s3_error!(
                    NotImplemented,
                    "The checksum algorithm is unsupported"
                ));
            }
        }
    }
    Ok(result)
}

macro_rules! checksum_fields {
    ($action:ident, $source:expr, $target:expr) => {
        $action!($source, $target, checksum_crc32, "CRC32");
        $action!($source, $target, checksum_crc32c, "CRC32C");
        $action!($source, $target, checksum_crc64nvme, "CRC64NVME");
        $action!($source, $target, checksum_sha1, "SHA1");
        $action!($source, $target, checksum_sha256, "SHA256");
        $action!($source, $target, checksum_sha512, "SHA512");
        $action!($source, $target, checksum_md5, "MD5");
        $action!($source, $target, checksum_xxhash64, "XXHASH64");
        $action!($source, $target, checksum_xxhash3, "XXHASH3");
        $action!($source, $target, checksum_xxhash128, "XXHASH128");
    };
}
macro_rules! collect_field {
    ($source:expr, $target:expr, $field:ident, $name:literal) => {
        if let Some(value) = &$source.$field {
            $target.values.insert($name.to_owned(), value.clone());
        }
    };
}
macro_rules! assign_field {
    ($source:expr, $target:expr, $field:ident, $name:literal) => {
        $target.$field = $source.values.get($name).cloned();
    };
}
pub(crate) use {assign_field, checksum_fields, collect_field};

pub(crate) fn finish(hasher: ChecksumHasher) -> Checksums {
    let digest = hasher.finalize();
    let mut result = Checksums {
        kind: "FULL_OBJECT".into(),
        ..Default::default()
    };
    checksum_fields!(collect_field, digest, result);
    result
}

pub(crate) fn etag(checksums: &Checksums) -> S3Result<String> {
    let md5 = checksums
        .values
        .get("MD5")
        .ok_or_else(|| s3_error!(InternalError))?;
    Ok(hex::encode(
        STANDARD.decode(md5).map_err(|_| s3_error!(InternalError))?,
    ))
}

pub(crate) fn validate(actual: &Checksums, expected: &Checksums) -> S3Result<()> {
    for (algorithm, expected) in &expected.values {
        if actual.values.get(algorithm) != Some(expected) {
            return Err(s3_error!(
                BadDigest,
                "The supplied checksum differs from the object checksum"
            ));
        }
    }
    Ok(())
}

pub(crate) fn kind(algorithm: &str, requested: Option<&str>) -> S3Result<String> {
    hasher([algorithm])?;
    let kind = requested.unwrap_or(if algorithm == "CRC64NVME" {
        "FULL_OBJECT"
    } else {
        "COMPOSITE"
    });
    if !matches!(kind, "FULL_OBJECT" | "COMPOSITE")
        || (kind == "FULL_OBJECT" && !algorithm.starts_with("CRC"))
        || (kind == "COMPOSITE" && algorithm == "CRC64NVME")
    {
        return Err(s3_error!(
            NotImplemented,
            "The checksum type is unsupported for this algorithm"
        ));
    }
    Ok(kind.into())
}

pub(crate) fn multipart(
    parts: &[Part],
    algorithm: &str,
    kind: &str,
) -> S3Result<(String, Checksums)> {
    let mut etag_hasher = hasher(["MD5"])?;
    for part in parts {
        etag_hasher.update(&hex::decode(&part.etag).map_err(|_| s3_error!(InternalError))?);
    }
    let etag = format!("{}-{}", etag(&finish(etag_hasher))?, parts.len());
    let value = if kind == "FULL_OBJECT" {
        use crc_fast::CrcAlgorithm;
        let crc = match algorithm {
            "CRC32" => CrcAlgorithm::Crc32IsoHdlc,
            "CRC32C" => CrcAlgorithm::Crc32Iscsi,
            "CRC64NVME" => CrcAlgorithm::Crc64Nvme,
            _ => return Err(s3_error!(NotImplemented)),
        };
        let mut combined = crc_fast::checksum(crc, b"");
        for part in parts {
            let bytes = STANDARD
                .decode(
                    part.checksums
                        .values
                        .get(algorithm)
                        .ok_or_else(|| s3_error!(InvalidPart))?,
                )
                .map_err(|_| s3_error!(InvalidPart))?;
            let right = bytes
                .iter()
                .fold(0u64, |value, byte| (value << 8) | u64::from(*byte));
            combined = crc_fast::checksum_combine(crc, combined, right, part.size);
        }
        if algorithm == "CRC64NVME" {
            STANDARD.encode(combined.to_be_bytes())
        } else {
            STANDARD.encode((combined as u32).to_be_bytes())
        }
    } else {
        let mut sum = hasher([algorithm])?;
        for part in parts {
            sum.update(
                &STANDARD
                    .decode(
                        part.checksums
                            .values
                            .get(algorithm)
                            .ok_or_else(|| s3_error!(InvalidPart))?,
                    )
                    .map_err(|_| s3_error!(InvalidPart))?,
            );
        }
        format!("{}-{}", finish(sum).values[algorithm], parts.len())
    };
    Ok((
        etag,
        Checksums {
            kind: kind.into(),
            values: [(algorithm.into(), value)].into(),
        },
    ))
}

pub(crate) fn dto(checksums: &Checksums) -> dto::Checksum {
    let mut result = dto::Checksum::default();
    checksum_fields!(assign_field, checksums, result);
    result.checksum_type = Some(dto::ChecksumType::from(checksums.kind.clone()));
    result
}

pub(crate) fn selected_algorithm(
    algorithm: Option<&dto::ChecksumAlgorithm>,
    expected: &Checksums,
    trailer: Option<&str>,
) -> S3Result<String> {
    let algorithm = algorithm
        .map(|a| a.as_str())
        .or_else(|| expected.values.keys().next().map(String::as_str))
        .or(trailer)
        .unwrap_or("CRC64NVME");
    hasher([algorithm])?;
    Ok(algorithm.into())
}

pub(crate) fn upload_trailers(
    headers: &http::HeaderMap,
    handle: Option<s3s::TrailingHeaders>,
) -> S3Result<Option<(s3s::TrailingHeaders, Vec<String>)>> {
    let Some(names) = headers.get("x-amz-trailer") else {
        return Ok(None);
    };
    let mut algorithms = Vec::new();
    for name in names
        .to_str()
        .map_err(|_| s3_error!(InvalidRequest))?
        .split(',')
    {
        let name = name.trim().to_ascii_lowercase();
        let algorithm = ALGORITHMS
            .iter()
            .find(|(_, suffix)| name == format!("x-amz-checksum-{suffix}"))
            .ok_or_else(|| s3_error!(NotImplemented, "The requested trailer is unsupported"))?;
        algorithms.push(algorithm.0.to_owned());
    }
    Ok(Some((
        handle.ok_or_else(|| s3_error!(InvalidRequest))?,
        algorithms,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multipart_crc_matches_plaintext_and_keeps_client_boundaries() {
        for algorithm in ["CRC32", "CRC32C", "CRC64NVME"] {
            let parts: Vec<_> = [b"first".as_slice(), b"second", b""]
                .into_iter()
                .enumerate()
                .map(|(index, bytes)| {
                    let mut h = hasher(["MD5", algorithm]).unwrap();
                    h.update(bytes);
                    let sums = finish(h);
                    Part {
                        number: index as i32 + 1,
                        content: String::new(),
                        size: bytes.len() as u64,
                        etag: etag(&sums).unwrap(),
                        modified: 0,
                        checksums: sums,
                    }
                })
                .collect();
            let (tag, sums) = multipart(&parts, algorithm, "FULL_OBJECT").unwrap();
            let mut h = hasher([algorithm]).unwrap();
            h.update(b"firstsecond");
            assert_eq!(sums.values, finish(h).values);
            assert!(tag.ends_with("-3"));
        }
    }
}
