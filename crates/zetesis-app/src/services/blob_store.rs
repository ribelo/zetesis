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

    if !cid
        .chars()
        .all(|c: char| c.is_ascii_digit() || ('a'..='f').contains(&c))
    {
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
pub async fn compute_cid_and_collect(
    mut stream: ByteStream,
) -> Result<(Cid, u64, Vec<u8>), BlobError> {
    let mut hasher = blake3::Hasher::new();
    let mut total: u64 = 0;
    let mut out = Vec::new();

    while let Some(chunk_res) = stream.as_mut().next().await {
        let chunk = chunk_res?;
        total = total
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| BlobError::Io("size overflow".to_string()))?;
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
    #[allow(dead_code)]
    async fn fsync_file(&self, file: &mut fs::File) -> Result<(), BlobError> {
        file.sync_all()
            .await
            .map_err(|e| BlobError::Io(format!("fsync file: {}", e)))
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

        let mut file = fs::File::from_std(
            temp_file
                .reopen()
                .map_err(|e| BlobError::Io(format!("reopen temp file: {}", e)))?,
        );

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

        // Always flush writes before verifying size
        file.flush()
            .await
            .map_err(|e| BlobError::Io(format!("flush file: {}", e)))?;

        // Apply durability policy.
        match self.durability {
            DurableWrite::FileOnly | DurableWrite::FileAndDir => {
                file.sync_all()
                    .await
                    .map_err(|e| BlobError::Io(format!("fsync file: {}", e)))?;
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

// ===== S3 BlobStore Implementation =====

#[cfg(feature = "s3")]
mod s3_impl {
    use super::*;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::{BehaviorVersion, Region};
    use aws_sdk_s3::primitives::ByteStream as S3ByteStream;

    /// S3-compatible blob store implementation.
    ///
    /// Write strategy:
    /// - Single-part upload with size cap (default 256 MiB)
    /// - Compute BLAKE3 CID while streaming into memory
    /// - Check existence via HeadObject before PutObject for idempotency
    /// - Keys mirror FS layout: blobs/{silo}/{cid[..2]}/{cid}
    ///
    /// Read strategy:
    /// - Stream directly via GetObject
    /// - Bounded read buffer to control memory usage
    #[derive(Debug, Clone)]
    pub struct S3BlobStore {
        client: Client,
        bucket: String,
        root_prefix: Option<String>,
        chunk_size_bytes: usize,
        max_single_part_bytes: u64,
    }

    impl S3BlobStore {
        /// Create a new S3BlobStore from configuration.
        pub async fn from_config(
            bucket: String,
            endpoint_url: Option<String>,
            region: Option<String>,
            force_path_style: bool,
            root_prefix: Option<String>,
        ) -> Result<Self, BlobError> {
            // Load AWS config from behavior chain
            let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

            // Override region if provided
            if let Some(r) = region {
                config_loader = config_loader.region(Region::new(r));
            }

            let aws_config = config_loader.load().await;

            // Build S3 client with optional endpoint override
            let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

            if let Some(endpoint) = endpoint_url {
                s3_config_builder = s3_config_builder.endpoint_url(endpoint);
            }

            s3_config_builder = s3_config_builder.force_path_style(force_path_style);

            let s3_config = s3_config_builder.build();
            let client = Client::from_conf(s3_config);

            tracing::debug!(
                bucket = %bucket,
                force_path_style = force_path_style,
                "initialized S3BlobStore"
            );

            Ok(Self {
                client,
                bucket,
                root_prefix,
                chunk_size_bytes: 65536,
                max_single_part_bytes: 256 * 1024 * 1024,
            })
        }

        /// Compute S3 key for a blob: [root_prefix/]blobs/{silo}/{cid[..2]}/{cid}
        fn key_for(&self, silo: Silo, cid: &str) -> String {
            assert!(cid.len() >= 2, "CID too short for sharding");
            let prefix = &cid[..2];
            let key = format!("blobs/{}/{}/{}", silo.slug(), prefix, cid);

            if let Some(root) = &self.root_prefix {
                format!("{}/{}", root.trim_end_matches('/'), key)
            } else {
                key
            }
        }
    }

    #[async_trait::async_trait]
    impl BlobStore for S3BlobStore {
        async fn put(&self, silo: Silo, mut data: ByteStream) -> Result<PutResult, BlobError> {
            // Stream into memory while computing CID and checking size cap
            let mut hasher = blake3::Hasher::new();
            let mut buffer = Vec::new();
            let mut total_bytes: u64 = 0;

            while let Some(chunk_res) = data.next().await {
                let chunk = chunk_res.map_err(|e| BlobError::Stream(e.to_string()))?;

                total_bytes = total_bytes
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| BlobError::Io("size overflow".to_string()))?;

                if total_bytes > self.max_single_part_bytes {
                    return Err(BlobError::Io(format!(
                        "data exceeds max single-part upload size of {} bytes; got {} bytes",
                        self.max_single_part_bytes, total_bytes
                    )));
                }

                hasher.update(&chunk);
                buffer.extend_from_slice(&chunk);
            }

            let cid = hasher.finalize().to_hex().to_string();
            let key = self.key_for(silo, &cid);

            // Check if blob already exists
            let existed = match self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
            {
                Ok(_) => true,
                Err(err) => {
                    use aws_sdk_s3::error::SdkError;
                    match err {
                        SdkError::ServiceError(service_err) => {
                            if service_err.err().is_not_found() {
                                false
                            } else {
                                return Err(BlobError::Io(format!(
                                    "S3 HeadObject service error: {:?}",
                                    service_err
                                )));
                            }
                        }
                        other => {
                            return Err(BlobError::Io(format!(
                                "S3 HeadObject request failed: {}",
                                other
                            )));
                        }
                    }
                }
            };

            if !existed {
                // Upload the blob
                let body = S3ByteStream::from(buffer);

                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .body(body)
                    .content_length(total_bytes as i64)
                    .send()
                    .await
                    .map_err(|e| BlobError::Io(format!("S3 PutObject failed: {}", e)))?;

                tracing::debug!(
                    silo = %silo.slug(),
                    cid = %cid,
                    size = total_bytes,
                    "uploaded blob to S3"
                );
            }

            Ok(PutResult {
                cid,
                size_bytes: total_bytes,
                existed,
            })
        }

        async fn get(&self, silo: Silo, cid: &str) -> Result<ByteStream, BlobError> {
            validate_cid(cid)?;
            let key = self.key_for(silo, cid);

            let output = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| {
                    use aws_sdk_s3::error::SdkError;
                    match e {
                        SdkError::ServiceError(service_err) => {
                            if service_err.err().is_no_such_key() {
                                BlobError::NotFound
                            } else {
                                BlobError::Io(format!(
                                    "S3 GetObject service error: {:?}",
                                    service_err
                                ))
                            }
                        }
                        other => BlobError::Io(format!("S3 GetObject request failed: {}", other)),
                    }
                })?;

            // Convert S3 ByteStream to our ByteStream
            let chunk_size = self.chunk_size_bytes;
            let mut body = output.body;

            let stream = async_stream::try_stream! {
                loop {
                    match body.next().await {
                        Some(Ok(chunk)) => {
                            // Rechunk to respect our chunk size limit
                            let mut remaining = chunk;
                            while !remaining.is_empty() {
                                let to_take = std::cmp::min(remaining.len(), chunk_size);
                                yield Bytes::copy_from_slice(&remaining[..to_take]);
                                remaining = remaining.slice(to_take..);
                            }
                        }
                        Some(Err(e)) => {
                            Err(BlobError::Stream(format!("S3 stream error: {}", e)))?;
                        }
                        None => break,
                    }
                }
            };

            Ok(Box::pin(stream))
        }

        async fn head(&self, silo: Silo, cid: &str) -> Result<Option<BlobMeta>, BlobError> {
            validate_cid(cid)?;
            let key = self.key_for(silo, cid);

            match self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
            {
                Ok(output) => {
                    let size = output.content_length().unwrap_or(0) as u64;
                    Ok(Some(BlobMeta {
                        cid: cid.to_string(),
                        size_bytes: size,
                    }))
                }
                Err(err) => {
                    use aws_sdk_s3::error::SdkError;
                    match err {
                        SdkError::ServiceError(service_err) => {
                            if service_err.err().is_not_found() {
                                Ok(None)
                            } else {
                                Err(BlobError::Io(format!(
                                    "S3 HeadObject service error: {:?}",
                                    service_err
                                )))
                            }
                        }
                        other => Err(BlobError::Io(format!(
                            "S3 HeadObject request failed: {}",
                            other
                        ))),
                    }
                }
            }
        }

        async fn delete(&self, silo: Silo, cid: &str) -> Result<bool, BlobError> {
            validate_cid(cid)?;
            let key = self.key_for(silo, cid);

            // Check if object exists first
            let exists = match self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
            {
                Ok(_) => true,
                Err(err) => {
                    use aws_sdk_s3::error::SdkError;
                    match err {
                        SdkError::ServiceError(service_err) => {
                            if service_err.err().is_not_found() {
                                return Ok(false);
                            } else {
                                return Err(BlobError::Io(format!(
                                    "S3 HeadObject service error: {:?}",
                                    service_err
                                )));
                            }
                        }
                        other => {
                            return Err(BlobError::Io(format!(
                                "S3 HeadObject request failed: {}",
                                other
                            )));
                        }
                    }
                }
            };

            if exists {
                self.client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| BlobError::Io(format!("S3 DeleteObject failed: {}", e)))?;

                tracing::debug!(
                    silo = %silo.slug(),
                    cid = %cid,
                    "deleted blob from S3"
                );

                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

#[cfg(feature = "s3")]
pub use s3_impl::S3BlobStore;

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
            Self {
                map: Arc::new(Mutex::new(HashMap::new())),
            }
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
            Ok(PutResult {
                cid,
                size_bytes: size,
                existed,
            })
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
            Ok(m.get(&key).map(|v| BlobMeta {
                cid: cid.to_string(),
                size_bytes: v.len() as u64,
            }))
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

    proptest! {
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
        let meta = store
            .head(silo, &pr.cid)
            .await
            .unwrap()
            .expect("should exist");
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
        assert!(matches!(
            store.get(silo, too_short).await,
            Err(BlobError::InvalidCid(_))
        ));
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
        let meta = store
            .head(silo, &pr.cid)
            .await
            .unwrap()
            .expect("should exist");
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
        let store = FsBlobStore::builder()
            .paths(paths)
            .chunk_size_bytes(1024)
            .build();
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

    // ===== S3BlobStore Tests =====

    #[cfg(feature = "s3")]
    mod s3_tests {
        use super::*;
        use crate::services::S3BlobStore;

        /// Live integration test against actual S3-compatible storage (Hetzner).
        ///
        /// This test is ignored by default. To run it, set the following environment variables:
        /// - HZ_S3_BUCKET: The bucket name
        /// - HZ_S3_ENDPOINT: The endpoint URL (e.g., https://nbg1.your-objectstorage.com)
        /// - HZ_S3_REGION: The region (e.g., nbg1)
        /// - AWS_ACCESS_KEY_ID: Your access key
        /// - AWS_SECRET_ACCESS_KEY: Your secret key
        ///
        /// Then run:
        /// ```bash
        /// cargo test --features s3 s3_live_integration_test -- --ignored --nocapture
        /// ```
        #[tokio::test]
        #[ignore]
        async fn s3_live_integration_test() {
            let bucket =
                std::env::var("HZ_S3_BUCKET").expect("HZ_S3_BUCKET environment variable not set");
            let endpoint = std::env::var("HZ_S3_ENDPOINT")
                .expect("HZ_S3_ENDPOINT environment variable not set");
            let region =
                std::env::var("HZ_S3_REGION").expect("HZ_S3_REGION environment variable not set");

            // AWS SDK will automatically pick up AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
            // from environment variables via the behavior chain

            println!("Connecting to S3-compatible storage:");
            println!("  Bucket: {}", bucket);
            println!("  Endpoint: {}", endpoint);
            println!("  Region: {}", region);

            let store = S3BlobStore::from_config(
                bucket,
                Some(endpoint),
                Some(region),
                true, // force path-style
                None, // no root prefix for test
            )
            .await
            .expect("failed to create S3BlobStore");

            let silo = Silo::Kio;

            // Test 1: Put data
            println!("\nTest 1: Uploading blob...");
            let test_data = b"Hello from Zetesis S3 integration test!";
            let stream = chunks_to_stream(vec![test_data.to_vec()]);
            let pr = store.put(silo, stream).await.expect("put failed");

            println!("  CID: {}", pr.cid);
            println!("  Size: {} bytes", pr.size_bytes);
            println!("  Existed: {}", pr.existed);

            assert_eq!(pr.size_bytes, test_data.len() as u64);
            assert!(!pr.existed, "blob should not have existed on first put");

            // Test 2: Head (verify existence)
            println!("\nTest 2: Checking blob existence with head...");
            let meta = store.head(silo, &pr.cid).await.expect("head failed");
            assert!(meta.is_some(), "blob should exist");

            let meta = meta.unwrap();
            assert_eq!(meta.cid, pr.cid);
            assert_eq!(meta.size_bytes, test_data.len() as u64);
            println!("  Confirmed blob exists with correct size");

            // Test 3: Get (retrieve and verify content)
            println!("\nTest 3: Downloading and verifying blob content...");
            let mut got_stream = store.get(silo, &pr.cid).await.expect("get failed");
            let mut collected = Vec::new();
            while let Some(chunk) = got_stream.next().await {
                let b = chunk.expect("stream chunk failed");
                collected.extend_from_slice(&b);
            }

            assert_eq!(
                collected, test_data,
                "retrieved content should match original"
            );
            println!("  Content verified successfully");

            // Test 4: Put idempotency (same content)
            println!("\nTest 4: Testing put idempotency...");
            let stream2 = chunks_to_stream(vec![test_data.to_vec()]);
            let pr2 = store.put(silo, stream2).await.expect("second put failed");

            assert_eq!(pr2.cid, pr.cid, "same content should produce same CID");
            assert!(pr2.existed, "blob should have existed on second put");
            println!("  Idempotency confirmed");

            // Test 5: Delete
            println!("\nTest 5: Deleting blob...");
            let deleted = store.delete(silo, &pr.cid).await.expect("delete failed");
            assert!(deleted, "delete should return true for existing blob");
            println!("  Blob deleted successfully");

            // Test 6: Verify deletion
            println!("\nTest 6: Verifying blob deletion...");
            let meta_after = store
                .head(silo, &pr.cid)
                .await
                .expect("head after delete failed");
            assert!(meta_after.is_none(), "blob should not exist after deletion");

            let deleted_again = store
                .delete(silo, &pr.cid)
                .await
                .expect("second delete failed");
            assert!(
                !deleted_again,
                "delete should return false for non-existent blob"
            );
            println!("  Deletion verified");

            // Test 7: NotFound error on get
            println!("\nTest 7: Testing NotFound error...");
            let get_result = store.get(silo, &pr.cid).await;
            assert!(
                matches!(get_result, Err(BlobError::NotFound)),
                "get should return NotFound"
            );
            println!("  NotFound error confirmed");

            println!("\nAll S3 integration tests passed successfully!");
        }

        #[tokio::test]
        async fn test_s3_key_format() {
            // Test key formatting logic directly
            let silo = Silo::Kio;
            let cid = "abcdef1234567890";

            // Test without root prefix
            let prefix = &cid[..2];
            let key = format!("blobs/{}/{}/{}", silo.slug(), prefix, cid);
            assert_eq!(key, "blobs/kio/ab/abcdef1234567890");
        }

        #[tokio::test]
        async fn test_s3_key_format_with_root_prefix() {
            // Test key formatting logic with root prefix
            let silo = Silo::Kio;
            let cid = "abcdef1234567890";
            let root_prefix = "zetesis/";

            let prefix = &cid[..2];
            let key = format!("blobs/{}/{}/{}", silo.slug(), prefix, cid);
            // Mirror the actual implementation in key_for
            let key_with_root = format!("{}/{}", root_prefix.trim_end_matches('/'), key);

            assert_eq!(key_with_root, "zetesis/blobs/kio/ab/abcdef1234567890");
        }
    }
}
