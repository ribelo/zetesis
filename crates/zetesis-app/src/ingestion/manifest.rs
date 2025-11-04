//! Blob manifest writer for mapping upstream document IDs to content-addressable CIDs.
//!
//! Provides an append-only ledger that records the relationship between upstream
//! document identifiers and their corresponding BLAKE3 content identifiers in blob storage.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

/// Errors that can occur during manifest operations.
#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// A single entry in the blob manifest mapping a document ID to its CID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Upstream document identifier (e.g., "123456" from SAOS, "KIO/123" from UZP)
    pub doc_id: String,
    /// Content-addressable identifier (BLAKE3 hex hash of the blob)
    pub cid: String,
    /// Size of the blob in bytes
    pub size_bytes: usize,
    /// Unix timestamp when this entry was written
    pub timestamp: u64,
}

impl ManifestEntry {
    pub fn new(doc_id: String, cid: String, size_bytes: usize) -> Self {
        use std::time::SystemTime;

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            doc_id,
            cid,
            size_bytes,
            timestamp,
        }
    }
}

/// Append-only writer for blob manifest entries.
///
/// Writes newline-delimited JSON (NDJSON) entries to a manifest file.
/// Each line is a self-contained JSON object representing a single mapping.
pub struct ManifestWriter {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl ManifestWriter {
    /// Open or create a manifest file at the given path.
    ///
    /// The file is opened in append mode to support resumable operations.
    /// The parent directory must exist.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, ManifestError> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        let writer = BufWriter::new(file);

        Ok(Self { writer, path })
    }

    /// Write a manifest entry to the file.
    ///
    /// The entry is serialized as JSON and written with a trailing newline.
    /// The write is buffered; call `flush` or `close` to ensure data is persisted.
    pub async fn write(&mut self, entry: &ManifestEntry) -> Result<(), ManifestError> {
        let json = serde_json::to_string(entry)?;
        self.writer.write_all(json.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        Ok(())
    }

    /// Flush buffered data to disk without syncing.
    ///
    /// This ensures data is written to the OS buffer but does not guarantee
    /// it has been persisted to stable storage.
    pub async fn flush(&mut self) -> Result<(), ManifestError> {
        self.writer.flush().await?;
        Ok(())
    }

    /// Flush and fsync data to stable storage.
    ///
    /// This is slower than `flush` but provides durability guarantees.
    /// Use when manifest integrity is critical (e.g., before process exit).
    pub async fn sync(&mut self) -> Result<(), ManifestError> {
        self.writer.flush().await?;
        self.writer.get_ref().sync_all().await?;
        Ok(())
    }

    /// Get the path to the manifest file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Close the writer, flushing buffered data.
    ///
    /// This is automatically called when the writer is dropped, but
    /// calling explicitly allows handling errors.
    pub async fn close(mut self) -> Result<(), ManifestError> {
        self.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_manifest_writer_creates_file() {
        let temp = TempDir::new().unwrap();
        let manifest_path = temp.path().join("manifest.ndjson");

        let mut writer = ManifestWriter::open(&manifest_path).await.unwrap();
        assert_eq!(writer.path(), manifest_path);

        writer.close().await.unwrap();
        assert!(manifest_path.exists());
    }

    #[tokio::test]
    async fn test_manifest_writer_appends_entries() {
        let temp = TempDir::new().unwrap();
        let manifest_path = temp.path().join("manifest.ndjson");

        // Write first entry
        {
            let mut writer = ManifestWriter::open(&manifest_path).await.unwrap();
            let entry = ManifestEntry::new("doc1".to_string(), "cid1".to_string(), 100);
            writer.write(&entry).await.unwrap();
            writer.close().await.unwrap();
        }

        // Write second entry (should append)
        {
            let mut writer = ManifestWriter::open(&manifest_path).await.unwrap();
            let entry = ManifestEntry::new("doc2".to_string(), "cid2".to_string(), 200);
            writer.write(&entry).await.unwrap();
            writer.close().await.unwrap();
        }

        // Verify both entries exist
        let mut file = File::open(&manifest_path).await.unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).await.unwrap();

        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);

        let entry1: ManifestEntry = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(entry1.doc_id, "doc1");
        assert_eq!(entry1.cid, "cid1");
        assert_eq!(entry1.size_bytes, 100);

        let entry2: ManifestEntry = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(entry2.doc_id, "doc2");
        assert_eq!(entry2.cid, "cid2");
        assert_eq!(entry2.size_bytes, 200);
    }

    #[tokio::test]
    async fn test_manifest_entry_has_timestamp() {
        let entry = ManifestEntry::new("doc1".to_string(), "cid1".to_string(), 100);
        assert!(entry.timestamp > 0);
    }

    #[tokio::test]
    async fn test_manifest_writer_flush() {
        let temp = TempDir::new().unwrap();
        let manifest_path = temp.path().join("manifest.ndjson");

        let mut writer = ManifestWriter::open(&manifest_path).await.unwrap();
        let entry = ManifestEntry::new("doc1".to_string(), "cid1".to_string(), 100);
        writer.write(&entry).await.unwrap();
        writer.flush().await.unwrap();

        // File should exist and contain data after flush
        let mut file = File::open(&manifest_path).await.unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).await.unwrap();
        assert!(!contents.is_empty());
    }
}
