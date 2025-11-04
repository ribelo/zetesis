use std::pin::Pin;

use bytes::Bytes;
use futures::stream::Stream;
use futures_util::StreamExt;
use thiserror::Error;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::paths::AppPaths;
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

/// Durability policy for filesystem writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableWrite {
    /// No explicit fsync (fastest, least durable).
    None,
    /// Fsync the file only (good for most cases).
    FileOnly,
    /// Fsync file and parent directory (recommended for production).
    FileAndDir,
}

/// Filesystem blob store implementation using AppPaths layout.
///
/// Write strategy:
/// - Temp file in shard dir for atomic finalize (hard link, then fallback to rename).
/// - Checksum-on-write: compute BLAKE3 while streaming.
/// - Size verification: compare streamed bytes to file metadata after write.
/// - Idempotency: existing blobs are not replaced.
///
/// Read strategy:
/// - Read paths compute shard path without creating directories.
/// - Stream directly from final blob path.
#[derive(Debug, Clone, bon::Builder)]
pub struct FsBlobStore {
    paths: AppPaths,
    #[builder(default = 65536)]
    chunk_size_bytes: usize,
    #[builder(default = DurableWrite::None)]
    durability: DurableWrite,
}

impl FsBlobStore {
    /// Compute the shard directory path for a given CID without creating it.
    fn shard_dir_unchecked(&self, silo: Silo, cid: &str) -> Result<std::path::PathBuf, BlobError> {
        if cid.len() < 2 {
            return Err(BlobError::InvalidCid(cid.to_string()));
        }
        let prefix = &cid[..2];
        let mut path = self.paths.data_dir();
        path.push("blobs");
        path.push(silo.slug());
        path.push(prefix);
        Ok(path)
    }

    /// Compute the final blob path for a given CID without creating directories.
    fn blob_path_unchecked(&self, silo: Silo, cid: &str) -> Result<std::path::PathBuf, BlobError> {
        let mut path = self.shard_dir_unchecked(silo, cid)?;
        path.push(cid);
        Ok(path)
    }

    /// Fsync a file descriptor.
    async fn fsync_file(&self, file: &mut fs::File) -> Result<(), BlobError> {
        file.sync_all().await.map_err(|e| BlobError::Io(format!("fsync file: {}", e)))
    }

    /// Fsync a directory by opening and syncing it.
    async fn fsync_dir(&self, dir_path: &std::path::Path) -> Result<(), BlobError> {
        // Directory fsync is best-effort; some platforms don't support it.
        match fs::File::open(dir_path).await {
            Ok(dir_file) => {
                if let Err(e) = dir_file.sync_all().await {
                    // Log but don't fail on unsupported platforms (e.g., Windows).
                    tracing::warn!("directory fsync unsupported or failed: {}", e);
                }
            }
            Err(e) => {
                tracing::warn!("failed to open directory for fsync: {}", e);
            }
        }
        Ok(())
    }

    /// Try to finalize by hard linking temp to final path. If hard link fails
    /// with EEXIST, another writer succeeded; delete temp. If EXDEV (cross-device),
    /// fall back to rename.
    async fn finalize_atomic(
        &self,
        temp_path: &std::path::Path,
        final_path: &std::path::Path,
    ) -> Result<bool, BlobError> {
        // EXDEV error code for cross-device link (Linux/Unix).
        #[cfg(target_os = "linux")]
        const EXDEV: i32 = 18;
        #[cfg(not(target_os = "linux"))]
        const EXDEV: i32 = 18; // Common on Unix-like systems

        // Try hard link first (atomic and won't replace existing file).
        match tokio::task::spawn_blocking({
            let temp = temp_path.to_path_buf();
            let final_p = final_path.to_path_buf();
            move || std::fs::hard_link(&temp, &final_p)
        })
        .await
        {
            Ok(Ok(())) => {
                // Hard link succeeded, now delete temp.
                let _ = fs::remove_file(temp_path).await;
                return Ok(false);
            }
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Another writer won; delete temp and report existed.
                let _ = fs::remove_file(temp_path).await;
                return Ok(true);
            }
            Ok(Err(e)) if e.raw_os_error() == Some(EXDEV) => {
                // Cross-device; fall through to rename.
            }
            Ok(Err(e)) => {
                return Err(BlobError::Io(format!("hard link failed: {}", e)));
            }
            Err(e) => {
                return Err(BlobError::Io(format!("hard link task failed: {}", e)));
            }
        }

        // Fall back to rename (atomic but may replace on Unix).
        match fs::rename(temp_path, final_path).await {
            Ok(()) => Ok(false),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Rename failed with EEXIST on some platforms; delete temp.
                let _ = fs::remove_file(temp_path).await;
                Ok(true)
            }
            Err(e) => Err(BlobError::Io(format!("rename failed: {}", e))),
        }
    }
}

#[async_trait::async_trait]
impl BlobStore for FsBlobStore {
    async fn put(&self, silo: Silo, mut data: ByteStream) -> Result<PutResult, BlobError> {
        // Stream to temp file, compute CID and size.
        let temp_dir = self.paths.data_dir();
        let temp_file = tempfile::NamedTempFile::new_in(&temp_dir)
            .map_err(|e| BlobError::Io(format!("create temp file: {}", e)))?;
        let temp_path = temp_file.path().to_path_buf();

        let mut file = fs::File::from_std(temp_file.reopen().map_err(|e| {
            BlobError::Io(format!("reopen temp file: {}", e))
        })?);

        let mut hasher = blake3::Hasher::new();
        let mut total_bytes: u64 = 0;

        while let Some(chunk_res) = data.next().await {
            let chunk = chunk_res.map_err(|e| BlobError::Stream(e.to_string()))?;
            total_bytes = total_bytes
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| BlobError::Io("size overflow".to_string()))?;
            hasher.update(&chunk);
            file.write_all(&chunk)
                .await
                .map_err(|e| BlobError::Io(format!("write chunk: {}", e)))?;
        }

        // Apply durability policy.
        match self.durability {
            DurableWrite::FileOnly | DurableWrite::FileAndDir => {
                self.fsync_file(&mut file).await?;
            }
            DurableWrite::None => {}
        }

        drop(file);

        let cid = hasher.finalize().to_hex().to_string();

        // Verify size matches.
        let metadata = fs::metadata(&temp_path)
            .await
            .map_err(|e| BlobError::Io(format!("stat temp file: {}", e)))?;
        if metadata.len() != total_bytes {
            let _ = fs::remove_file(&temp_path).await;
            return Err(BlobError::Io(format!(
                "size mismatch: wrote {} bytes, file is {}",
                total_bytes,
                metadata.len()
            )));
        }

        // Compute final path (this will create directories for write path).
        let final_path = self
            .paths
            .blob_path(silo, &cid)
            .map_err(|e| BlobError::Io(e.to_string()))?;

        // Check if already exists.
        let existed = final_path.exists();

        if !existed {
            // Move temp to shard dir if not in it already.
            let shard_dir = self.shard_dir_unchecked(silo, &cid)?;
            fs::create_dir_all(&shard_dir)
                .await
                .map_err(|e| BlobError::Io(format!("create shard dir: {}", e)))?;

            // Attempt atomic finalize.
            let finalize_existed = self.finalize_atomic(&temp_path, &final_path).await?;

            // Fsync directory if requested.
            if self.durability == DurableWrite::FileAndDir && !finalize_existed {
                self.fsync_dir(&shard_dir).await?;
            }

            Ok(PutResult {
                cid,
                size_bytes: total_bytes,
                existed: finalize_existed,
            })
        } else {
            // Already exists; clean up temp.
            let _ = fs::remove_file(&temp_path).await;
            Ok(PutResult {
                cid,
                size_bytes: total_bytes,
                existed: true,
            })
        }
    }

    async fn get(&self, silo: Silo, cid: &str) -> Result<ByteStream, BlobError> {
        validate_cid(cid)?;
        let path = self.blob_path_unchecked(silo, cid)?;

        let file = fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BlobError::NotFound
            } else {
                BlobError::Io(format!("open blob: {}", e))
            }
        })?;

        // Stream file in chunks using async-stream.
        let chunk_size = self.chunk_size_bytes;
        let stream = async_stream::try_stream! {
            let mut file = file;
            let mut buf = vec![0u8; chunk_size];
            loop {
                let n = file.read(&mut buf).await.map_err(|e| BlobError::Io(format!("read: {}", e)))?;
                if n == 0 {
                    break;
                }
                yield Bytes::copy_from_slice(&buf[..n]);
            }
        };

        Ok(Box::pin(stream))
    }

    async fn head(&self, silo: Silo, cid: &str) -> Result<Option<BlobMeta>, BlobError> {
        validate_cid(cid)?;
        let path = self.blob_path_unchecked(silo, cid)?;

        match fs::metadata(&path).await {
            Ok(meta) => Ok(Some(BlobMeta {
                cid: cid.to_string(),
                size_bytes: meta.len(),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BlobError::Io(format!("stat blob: {}", e))),
        }
    }

    async fn delete(&self, silo: Silo, cid: &str) -> Result<bool, BlobError> {
        validate_cid(cid)?;
        let path = self.blob_path_unchecked(silo, cid)?;

        match fs::remove_file(&path).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(BlobError::Io(format!("delete blob: {}", e))),
        }
    }
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

    // FsBlobStore tests
    #[tokio::test]
    async fn test_fs_blob_store_put_get_head_delete() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let paths = AppPaths::new(temp.path()).unwrap();
        let store = FsBlobStore::builder().paths(paths).build();
        let silo = Silo::Kio;

        // Put data
        let chunks = vec![b"hello".to_vec(), b" ".to_vec(), b"world".to_vec()];
        let stream = chunks_to_stream(chunks.clone());
        let pr = store.put(silo, stream).await.unwrap();
        assert_eq!(pr.size_bytes, 11);
        assert!(!pr.existed);

        // Head
        let meta = store.head(silo, &pr.cid).await.unwrap().expect("should exist");
        assert_eq!(meta.size_bytes, 11);
        assert_eq!(meta.cid, pr.cid);

        // Get and verify content
        let mut got_stream = store.get(silo, &pr.cid).await.unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = got_stream.next().await {
            let b = chunk.unwrap();
            collected.extend_from_slice(&b);
        }
        assert_eq!(collected, b"hello world");
        assert_eq!(collected.len() as u64, pr.size_bytes);

        // Delete
        let deleted = store.delete(silo, &pr.cid).await.unwrap();
        assert!(deleted);
        let head_after = store.head(silo, &pr.cid).await.unwrap();
        assert!(head_after.is_none());

        // Delete again (idempotent)
        let deleted_again = store.delete(silo, &pr.cid).await.unwrap();
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn test_fs_blob_store_put_idempotency() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let paths = AppPaths::new(temp.path()).unwrap();
        let store = FsBlobStore::builder().paths(paths).build();
        let silo = Silo::Kio;

        // First put
        let data = b"test content".to_vec();
        let stream1 = chunks_to_stream(vec![data.clone()]);
        let pr1 = store.put(silo, stream1).await.unwrap();
        assert!(!pr1.existed);

        // Second put of same content
        let stream2 = chunks_to_stream(vec![data.clone()]);
        let pr2 = store.put(silo, stream2).await.unwrap();
        assert!(pr2.existed);
        assert_eq!(pr1.cid, pr2.cid);
        assert_eq!(pr1.size_bytes, pr2.size_bytes);
    }

    #[tokio::test]
    async fn test_fs_blob_store_invalid_cid() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let paths = AppPaths::new(temp.path()).unwrap();
        let store = FsBlobStore::builder().paths(paths).build();
        let silo = Silo::Kio;

        // Uppercase hex
        let result = store.get(silo, "ABCD").await;
        assert!(matches!(result, Err(BlobError::InvalidCid(_))));

        // Too short
        let result = store.head(silo, "a").await;
        assert!(matches!(result, Err(BlobError::InvalidCid(_))));

        // Invalid characters
        let result = store.delete(silo, "gg").await;
        assert!(matches!(result, Err(BlobError::InvalidCid(_))));
    }

    #[tokio::test]
    async fn test_fs_blob_store_not_found() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let paths = AppPaths::new(temp.path()).unwrap();
        let store = FsBlobStore::builder().paths(paths).build();
        let silo = Silo::Kio;

        let fake_cid = "abcdef1234567890abcdef1234567890";

        // Get non-existent
        let result = store.get(silo, fake_cid).await;
        assert!(matches!(result, Err(BlobError::NotFound)));

        // Head non-existent
        let result = store.head(silo, fake_cid).await.unwrap();
        assert!(result.is_none());

        // Delete non-existent
        let result = store.delete(silo, fake_cid).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_fs_blob_store_path_sharding() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let paths = AppPaths::new(temp.path()).unwrap();
        let store = FsBlobStore::builder().paths(paths.clone()).build();
        let silo = Silo::Kio;

        // Put content and verify it gets sharded correctly
        let data = b"test sharding".to_vec();
        let stream = chunks_to_stream(vec![data.clone()]);
        let pr = store.put(silo, stream).await.unwrap();

        // Verify the blob exists at the expected sharded path
        let expected_path = paths.blob_path(silo, &pr.cid).unwrap();
        assert!(expected_path.exists());

        // Verify the CID is used for sharding (first 2 chars as directory)
        let prefix = &pr.cid[..2];
        let parent = expected_path.parent().unwrap();
        assert!(parent.ends_with(prefix));

        // Verify we can retrieve it
        let mut got_stream = store.get(silo, &pr.cid).await.unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = got_stream.next().await {
            collected.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(collected, data);
    }

    #[tokio::test]
    async fn test_fs_blob_store_empty_content() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let paths = AppPaths::new(temp.path()).unwrap();
        let store = FsBlobStore::builder().paths(paths).build();
        let silo = Silo::Kio;

        // Put empty content
        let stream = chunks_to_stream(vec![]);
        let pr = store.put(silo, stream).await.unwrap();
        assert_eq!(pr.size_bytes, 0);

        // Verify it can be retrieved
        let meta = store.head(silo, &pr.cid).await.unwrap().unwrap();
        assert_eq!(meta.size_bytes, 0);

        let mut got_stream = store.get(silo, &pr.cid).await.unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = got_stream.next().await {
            collected.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(collected.len(), 0);
    }

    #[tokio::test]
    async fn test_fs_blob_store_large_chunks() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let paths = AppPaths::new(temp.path()).unwrap();
        let store = FsBlobStore::builder().paths(paths).chunk_size_bytes(1024).build();
        let silo = Silo::Kio;

        // Create 10KB of data split into 1KB chunks
        let chunk = vec![42u8; 1024];
        let chunks: Vec<Vec<u8>> = (0..10).map(|_| chunk.clone()).collect();

        let stream = chunks_to_stream(chunks);
        let pr = store.put(silo, stream).await.unwrap();
        assert_eq!(pr.size_bytes, 10240);

        // Verify retrieval
        let mut got_stream = store.get(silo, &pr.cid).await.unwrap();
        let mut total = 0u64;
        while let Some(chunk) = got_stream.next().await {
            total += chunk.unwrap().len() as u64;
        }
        assert_eq!(total, 10240);
    }

    #[tokio::test]
    async fn test_fs_blob_store_cid_consistency() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let paths = AppPaths::new(temp.path()).unwrap();
        let store = FsBlobStore::builder().paths(paths).build();
        let silo = Silo::Kio;

        let data = b"consistent content";

        // Compute expected CID
        let expected_cid = blake3_cid(data);

        // Put and verify CID matches
        let stream = chunks_to_stream(vec![data.to_vec()]);
        let pr = store.put(silo, stream).await.unwrap();
        assert_eq!(pr.cid, expected_cid);

        // Verify via head
        let meta = store.head(silo, &pr.cid).await.unwrap().unwrap();
        assert_eq!(meta.cid, expected_cid);
    }
}
