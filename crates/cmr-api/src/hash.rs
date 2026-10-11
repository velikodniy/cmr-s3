use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha1::{Digest, Sha1};

use crate::{Error, Result};

/// Content at or below this size is embedded in the hash.
pub(crate) const INLINE_CONTENT_BYTES: usize = 20;

/// A Mail.ru hash, a 20-byte identifier for complete file content.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentHash([u8; 20]);

impl ContentHash {
    /// Wrap an existing 20-byte hash without hashing its bytes.
    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Calculate the Mail.ru hash of file content.
    pub fn compute(bytes: &[u8]) -> Self {
        let mut hasher = ContentHasher::new();
        hasher.update(bytes);
        hasher.finish()
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl FromStr for ContentHash {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.len() != 40 {
            return Err(Error::InvalidHash);
        }
        let mut bytes = [0; 20];
        for (pair, output) in value.as_bytes().as_chunks::<2>().0.iter().zip(&mut bytes) {
            let high = (pair[0] as char).to_digit(16).ok_or(Error::InvalidHash)?;
            let low = (pair[1] as char).to_digit(16).ok_or(Error::InvalidHash)?;
            *output = ((high << 4) | low) as u8;
        }
        Ok(Self(bytes))
    }
}

impl Serialize for ContentHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ContentHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

pub(crate) struct ContentHasher {
    sha1: Sha1,
    prefix: [u8; 20],
    size: u64,
}

impl ContentHasher {
    pub fn new() -> Self {
        let mut sha1 = Sha1::new();
        sha1.update(b"mrCloud");
        Self {
            sha1,
            prefix: [0; 20],
            size: 0,
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        if self.size < INLINE_CONTENT_BYTES as u64 {
            let offset = self.size as usize;
            let count = bytes.len().min(INLINE_CONTENT_BYTES - offset);
            self.prefix[offset..offset + count].copy_from_slice(&bytes[..count]);
        }
        self.size += bytes.len() as u64;
        self.sha1.update(bytes);
    }

    pub fn finish(mut self) -> ContentHash {
        if self.size <= INLINE_CONTENT_BYTES as u64 {
            ContentHash(self.prefix)
        } else {
            self.sha1.update(self.size.to_string().as_bytes());
            ContentHash(self.sha1.finalize().into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn hashes_match_protocol_vectors() {
        let vectors = [
            (vec![], "0000000000000000000000000000000000000000"),
            (b"abc".to_vec(), "6162630000000000000000000000000000000000"),
            (
                (0u8..20).collect(),
                "000102030405060708090a0b0c0d0e0f10111213",
            ),
            (
                (0u8..21).collect(),
                "f76acea396fbd1097f8a679965399d11a2210f98",
            ),
            (
                vec![b'a'; 100_000],
                "45427fe9650e2ef086a5218beb60315222009300",
            ),
        ];
        for (bytes, expected) in vectors {
            assert_eq!(ContentHash::compute(&bytes).to_string(), expected);
        }
    }

    proptest! {
        #[test]
        fn short_content_is_embedded_and_zero_padded(bytes in prop::collection::vec(any::<u8>(), 0..=20)) {
            let hash = ContentHash::compute(&bytes);
            prop_assert_eq!(&hash.as_bytes()[..bytes.len()], bytes.as_slice());
            prop_assert!(hash.as_bytes()[bytes.len()..].iter().all(|&byte| byte == 0));
        }

        #[test]
        fn hash_is_independent_of_chunk_boundaries(
            bytes in prop_oneof![
                prop::collection::vec(any::<u8>(), 0..=21),
                prop::collection::vec(any::<u8>(), 22..8192),
            ],
            cuts in prop::collection::vec(any::<usize>(), 0..32),
        ) {
            let mut cuts: Vec<_> = cuts.into_iter().map(|cut| cut % (bytes.len() + 1)).collect();
            cuts.extend([0, bytes.len()]);
            cuts.sort_unstable();
            let mut hasher = ContentHasher::new();
            for pair in cuts.windows(2) {
                hasher.update(&bytes[pair[0]..pair[1]]);
            }
            prop_assert_eq!(hasher.finish(), ContentHash::compute(&bytes));
        }

        #[test]
        fn hexadecimal_and_json_round_trips_preserve_every_byte(bytes in any::<[u8; 20]>()) {
            let hash = ContentHash::from_bytes(bytes);
            let text = hash.to_string();
            prop_assert_eq!(text.len(), 40);
            prop_assert!(text.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
            for (pair, expected) in text.as_bytes().as_chunks::<2>().0.iter().zip(bytes) {
                let actual = u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap();
                prop_assert_eq!(actual, expected);
            }
            prop_assert_eq!(text.to_uppercase().parse::<ContentHash>().unwrap(), hash);
            let json = serde_json::to_value(hash).unwrap();
            prop_assert_eq!(&json, &serde_json::Value::String(text));
            prop_assert_eq!(serde_json::from_value::<ContentHash>(json).unwrap(), hash);
        }

        #[test]
        fn parsing_accepts_exactly_forty_ascii_hex_digits(
            text in prop_oneof!["[0-9a-fA-F]{40}", ".{0,80}"],
        ) {
            let valid = text.len() == 40 && text.bytes().all(|byte| byte.is_ascii_hexdigit());
            match text.parse::<ContentHash>() {
                Ok(_) => prop_assert!(valid),
                Err(Error::InvalidHash) => prop_assert!(!valid),
                Err(error) => prop_assert!(false, "unexpected error: {error}"),
            }
        }
    }
}
