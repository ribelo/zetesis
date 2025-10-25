//! Filesystem path helpers (XDG-aware) for LMDB, Milli, and blob storage.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PathError {
    #[error("unable to determine project directories")]
    MissingProjectDirs,
    #[error("failed to create directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid content identifier `{cid}`; expected at least two hexadecimal characters")]
    InvalidContentId { cid: String },
}

/// Container providing filesystem paths for the application. In production this is
/// rooted at `$XDG_DATA_HOME/zetesis`; tests may construct custom instances.
#[derive(Debug, Clone)]
pub struct AppPaths {
    base_dir: PathBuf,
}

impl AppPaths {
    /// Construct paths rooted under `$XDG_DATA_HOME/zetesis`.
    pub fn from_project_dirs() -> Result<Self, PathError> {
        let dirs =
            ProjectDirs::from("dev", "ribelo", "zetesis").ok_or(PathError::MissingProjectDirs)?;
        Self::new(dirs.data_dir())
    }

    /// Construct paths rooted under the provided directory, ensuring it exists.
    pub fn new<P: AsRef<Path>>(base: P) -> Result<Self, PathError> {
        let base = base.as_ref().to_path_buf();
        ensure_dir(&base)?;
        Ok(Self { base_dir: base })
    }

    /// Base data directory.
    pub fn data_dir(&self) -> PathBuf {
        self.base_dir.clone()
    }

    /// LMDB environment directory (`.../lmdb/app`).
    pub fn lmdb_env_dir(&self) -> Result<PathBuf, PathError> {
        self.ensure_child(&["lmdb", "app"])
    }

    /// Base directory for Milli indices (`.../milli`).
    pub fn milli_base_dir(&self) -> Result<PathBuf, PathError> {
        self.ensure_child(&["milli"])
    }

    /// Directory for a specific silo's Milli index (`.../milli/{silo}`).
    pub fn milli_index_dir<S: AsRef<str>>(&self, silo: S) -> Result<PathBuf, PathError> {
        self.milli_dir(silo)
    }

    /// Base directory for blob storage (`.../blobs`).
    pub fn blobs_base_dir(&self) -> Result<PathBuf, PathError> {
        self.ensure_child(&["blobs"])
    }

    /// Directory for blobs belonging to a silo (`.../blobs/{silo}`).
    pub fn blobs_silo_dir<S: AsRef<str>>(&self, silo: S) -> Result<PathBuf, PathError> {
        let segments = vec!["blobs".to_string(), normalize_slug(silo.as_ref())];
        self.ensure_dynamic(&segments)
    }

    /// Full path for a blob identified by a content identifier (CID).
    pub fn blob_path<S: AsRef<str>>(&self, silo: S, cid: &str) -> Result<PathBuf, PathError> {
        if cid.len() < 2 {
            return Err(PathError::InvalidContentId {
                cid: cid.to_owned(),
            });
        }

        let mut path = self.blobs_silo_dir(silo)?;
        let prefix = &cid[..2];
        path.push(prefix);
        ensure_dir(&path)?;
        path.push(cid);
        Ok(path)
    }

    /// Directory for a specific silo's Milli index (`.../milli/{silo}`).
    pub fn milli_dir<S: AsRef<str>>(&self, silo: S) -> Result<PathBuf, PathError> {
        let segments = vec!["milli".to_string(), normalize_slug(silo.as_ref())];
        self.ensure_dynamic(&segments)
    }

    fn ensure_child(&self, segments: &[&str]) -> Result<PathBuf, PathError> {
        let mut path = self.base_dir.clone();
        for segment in segments {
            path.push(segment);
        }
        ensure_dir(&path)
    }

    fn ensure_dynamic(&self, segments: &[String]) -> Result<PathBuf, PathError> {
        let mut path = self.base_dir.clone();
        for segment in segments {
            path.push(segment);
        }
        ensure_dir(&path)
    }
}

fn ensure_dir(path: &Path) -> Result<PathBuf, PathError> {
    if let Err(err) = fs::create_dir_all(path) {
        if err.kind() != io::ErrorKind::AlreadyExists {
            return Err(PathError::CreateDir {
                path: path.to_path_buf(),
                source: err,
            });
        }
    }
    Ok(path.to_path_buf())
}

fn normalize_slug(s: &str) -> String {
    s.trim().to_ascii_lowercase()
}
