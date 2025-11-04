use std::pin::Pin;

use bytes::Bytes;
use futures::stream::Stream;
use futures_util::StreamExt;
use thiserror::Error;

use crate::pipeline::processor::Silo;

// Blob storage abstractions.
//
// Implements the streaming-first `BlobStore` trait and helpers used by the
// application. Covers canonical identity per REQ-ZE-ID-CANONICAL and storage
// layout expectations (REQ-ZE-STORAGE-FS).

/// Boxed asynchronous byte stream returned/accepted by the blob store.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, BlobError>> + Send>>;

/// Canonical content identifier (BLAKE3 lowercase hex of exact bytes).
pub type Cid = String;

/// Minimal metadata returned by `head` about a blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobMeta {
    pub cid: Cid,
    pub size_bytes: u64,
    // Optional provider-specific fields are intentionally omitted here; they can
    // be added later as Option<T> without changing core semantics.
}

/// Result returned by `put` describing whether the blob already existed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResult {
    pub cid: Cid,
    pub size_bytes: u64,
    pub existed: bool,
}

/// Errors emitted by blob storage operations.
#[derive(Debug, Error)]
pub enum BlobError {
    #[error("not found")]
    NotFound,

    #[error("invalid cid: {0}")]
    InvalidCid(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("stream error: {0}")]
    Stream(String),
}

impl From<std::io::Error> for BlobError {
    fn from(e: std::io::Error) -> Self {
        BlobError::Io(e.to_string())
    }
}

/// Validate a candidate CID: must be lowercase hex and non-empty.
pub fn validate_cid(cid: &str) -> Result<(), BlobError> {
    // Quick validation: non-empty and only lowercase hex characters.
    if cid.is_empty() {
        return Err(BlobError::InvalidCid(cid.to_string()));
    }
    // Enforce minimum length to match path sharding expectations (at least 2 hex
    // chars) and reject non-hex characters.
    if cid.len() < 2 {
        return Err(BlobError::InvalidCid(cid.to_string()));
    }

    if !cid.chars().all(|c: char| c.is_ascii_digit() || ('a'..='f').contains(&c)) {
        return Err(BlobError::InvalidCid(cid.to_string()));
    }
    Ok(())
}

/// Compute the BLAKE3 CID for a full byte slice.
pub fn blake3_cid(bytes: &[u8]) -> Cid {
    blake3::hash(bytes).to_hex().to_string()
}

/// Compute BLAKE3 cid and size by consuming a `ByteStream`.
/// Returns the cid, total size, and the collected bytes.
pub async fn compute_cid_and_collect(mut stream: ByteStream) -> Result<(Cid, u64, Vec<u8>), BlobError> {
    let mut hasher = blake3::Hasher::new();
    let mut total: u64 = 0;
    let mut out = Vec::new();

    while let Some(chunk_res) = stream.as_mut().next().await {
        let chunk = chunk_res?;
        total = total.checked_add(chunk.len() as u64).ok_or_else(|| BlobError::Io("size overflow".to_string()))?;
        hasher.update(&chunk);
        out.extend_from_slice(&chunk);
    }

    let cid = hasher.finalize().to_hex().to_string();
    Ok((cid, total, out))
}

/// Trait abstracting over blob storage backends.
#[async_trait::async_trait]
pub trait BlobStore: Send + Sync {
    /// Store the provided byte stream into the given `silo` and return the
    /// canonical `PutResult`. Put is idempotent: writing existing content does
    /// not create duplicates; `existed` signals whether the final destination
    /// was already present.
    async fn put(&self, silo: Silo, data: ByteStream) -> Result<PutResult, BlobError>;

    /// Return a byte stream for the given `cid` or `BlobError::NotFound`.
    async fn get(&self, silo: Silo, cid: &str) -> Result<ByteStream, BlobError>;

    /// Return metadata for `cid` if present.
    async fn head(&self, silo: Silo, cid: &str) -> Result<Option<BlobMeta>, BlobError>;

    /// Delete the blob if present. Returns Ok(true) if deleted, Ok(false) if it
    /// did not exist.
    async fn delete(&self, silo: Silo, cid: &str) -> Result<bool, BlobError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream::{self, StreamExt};
    use proptest::prelude::*;

    // Helper to turn Vec<Vec<u8>> into a ByteStream
    fn chunks_to_stream(chunks: Vec<Vec<u8>>) -> ByteStream {
        let s = stream::iter(chunks.into_iter().map(|c| Ok(Bytes::from(c))));
        Box::pin(s)
    }

    // In-memory test double used by multiple tests below.
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    type InMemMap = HashMap<(String, Cid), Vec<u8>>;

    struct InMem {
        map: Arc<Mutex<InMemMap>>,
    }

    impl InMem {
        fn new() -> Self {
            Self { map: Arc::new(Mutex::new(HashMap::new())) }
        }
    }

    #[async_trait::async_trait]
    impl BlobStore for InMem {
        async fn put(&self, silo: Silo, data: ByteStream) -> Result<PutResult, BlobError> {
            let (cid, size, collected) = compute_cid_and_collect(data).await?;
            let key = (silo.slug().to_string(), cid.clone());
            let mut m = self.map.lock().await;
            let existed = m.contains_key(&key);
            if !existed {
                m.insert(key, collected);
            }
            Ok(PutResult{ cid, size_bytes: size, existed })
        }

        async fn get(&self, silo: Silo, cid: &str) -> Result<ByteStream, BlobError> {
            validate_cid(cid)?;
            let key = (silo.slug().to_string(), cid.to_string());
            let m = self.map.lock().await;
            if let Some(buf) = m.get(&key) {
                let b = Bytes::copy_from_slice(buf);
                let s = stream::iter(vec![Ok(b)]);
                Ok(Box::pin(s))
            } else {
                Err(BlobError::NotFound)
            }
        }

        async fn head(&self, silo: Silo, cid: &str) -> Result<Option<BlobMeta>, BlobError> {
            validate_cid(cid)?;
            let key = (silo.slug().to_string(), cid.to_string());
            let m = self.map.lock().await;
            Ok(m.get(&key).map(|v| BlobMeta { cid: cid.to_string(), size_bytes: v.len() as u64 }))
        }

        async fn delete(&self, silo: Silo, cid: &str) -> Result<bool, BlobError> {
            validate_cid(cid)?;
            let key = (silo.slug().to_string(), cid.to_string());
            let mut m = self.map.lock().await;
            Ok(m.remove(&key).is_some())
        }
    }

    #[test]
    fn test_blake3_cid_matches_direct() {
        let data = b"hello world";
        let want = blake3::hash(data).to_hex().to_string();
        let got = blake3_cid(data);
        assert_eq!(want, got);
    }

    proptest!{
        #[test]
        fn prop_chunking_preserves_cid(data in proptest::collection::vec(any::<u8>(), 0..1024)) {
            // Split into arbitrary chunks
            // Split deterministically into chunks up to 64 bytes to simulate
            // arbitrary chunking without relying on RNG helpers.
            let mut chunks: Vec<Vec<u8>> = Vec::new();
            let mut i = 0usize;
            while i < data.len() {
                let rem = data.len() - i;
                let sz = std::cmp::min(64, rem);
                // take at least 1 byte, advance by sz or 1 to avoid infinite loop
                let take = if sz == 0 { 0 } else { std::cmp::max(1, sz / 2) };
                let end = i + take;
                chunks.push(data[i..end].to_vec());
                i = end;
            }

            // Create stream and compute cid
            let stream = chunks_to_stream(chunks.clone());
            let rt = tokio::runtime::Runtime::new().unwrap();
            let (cid, size, collected) = rt.block_on(async move { compute_cid_and_collect(stream).await.unwrap() });

            let want_cid = blake3::hash(&data).to_hex().to_string();
            assert_eq!(cid, want_cid);
            assert_eq!(size as usize, data.len());
            assert_eq!(collected, data);
        }
    }

    #[tokio::test]
    async fn test_inmemory_put_get_head_delete() {
        let store = InMem::new();
        let silo = Silo::Kio;

        // Put data
        let chunks = vec![b"hello".to_vec(), b" ".to_vec(), b"world".to_vec()];
        let stream = chunks_to_stream(chunks.clone());
        let pr = store.put(silo, stream).await.unwrap();
        assert_eq!(pr.size_bytes, 11);

        // Head
        let meta = store.head(silo, &pr.cid).await.unwrap().expect("should exist");
        assert_eq!(meta.size_bytes, 11);

        // Get
        let mut got_stream = store.get(silo, &pr.cid).await.unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = got_stream.next().await {
            let b = chunk.unwrap();
            collected.extend_from_slice(&b);
        }
        assert_eq!(collected.len() as u64, pr.size_bytes);

        // Delete
        let deleted = store.delete(silo, &pr.cid).await.unwrap();
        assert!(deleted);
        let head_after = store.head(silo, &pr.cid).await.unwrap();
        assert!(head_after.is_none());

        // Idempotency: putting same bytes again reports existed == false on first
        // put (we already did that) and true on second, and map length remains
        // unchanged after the second put.
        // Re-create the data stream and perform a second put.
        let chunks2 = vec![b"hello world".to_vec()];
        let stream2 = chunks_to_stream(chunks2);
        let pr2 = store.put(silo, stream2).await.unwrap();
        assert_eq!(pr2.size_bytes, 11);
        // since we deleted, the second put should behave like a fresh put
        assert!(!pr2.existed);

        // Put again to observe existed == true and map length unchanged.
        let stream3 = chunks_to_stream(vec![b"hello world".to_vec()]);
        let pr3 = store.put(silo, stream3).await.unwrap();
        assert!(pr3.existed);

        // Assert underlying map length is 1 for that silo/cid pair
        let map_len = {
            let m = store.map.lock().await;
            m.len()
        };
        assert_eq!(map_len, 1);
    }

    #[tokio::test]
    async fn test_invalid_cid_rejections() {
        let store = InMem::new();
        let silo = Silo::Kio;

        // Uppercase hex should be rejected by validate_cid
        let bad = "AB";
        let g = store.get(silo, bad).await;
        assert!(matches!(g, Err(BlobError::InvalidCid(_))));

        let h = store.head(silo, bad).await;
        assert!(matches!(h, Err(BlobError::InvalidCid(_))));

        let d = store.delete(silo, bad).await;
        assert!(matches!(d, Err(BlobError::InvalidCid(_))));

        // Too-short string
        let too_short = "a";
        assert!(matches!(store.get(silo, too_short).await, Err(BlobError::InvalidCid(_))));
    }
}
