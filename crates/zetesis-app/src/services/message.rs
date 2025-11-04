use std::fmt;

use rig::completion::message::{
    Audio, Document, DocumentSourceKind, Image, ImageDetail, MediaType, MimeType, UserContent,
    Video,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataRef {
    Base64(String),
    Uri(String),
}

impl DataRef {
    pub fn base64(data: impl Into<String>) -> Self {
        let value = data.into();
        debug_assert!(!value.trim().is_empty());
        Self::Base64(value)
    }

    pub fn uri(uri: impl Into<String>) -> Self {
        let value = uri.into();
        debug_assert!(!value.trim().is_empty());
        Self::Uri(value)
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Base64(data) | Self::Uri(data) => data,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Part {
    Text {
        text: String,
    },
    Blob {
        #[serde(flatten)]
        data_ref: DataRef,
        mime_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
}

impl Part {
    pub fn text(value: impl Into<String>) -> Self {
        let text = value.into();
        debug_assert!(!text.trim().is_empty());
        Self::Text { text }
    }

    pub fn blob_base64(data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        let mime_type = mime_type.into();
        debug_assert!(!mime_type.trim().is_empty());
        Self::Blob {
            data_ref: DataRef::base64(data),
            mime_type,
            name: None,
        }
    }

    pub fn blob_uri(uri: impl Into<String>, mime_type: impl Into<String>) -> Self {
        let mime_type = mime_type.into();
        debug_assert!(!mime_type.trim().is_empty());
        Self::Blob {
            data_ref: DataRef::uri(uri),
            mime_type,
            name: None,
        }
    }

    pub fn mime_type(&self) -> Option<&str> {
        match self {
            Part::Text { .. } => None,
            Part::Blob { mime_type, .. } => Some(mime_type),
        }
    }

    pub fn is_image(&self) -> bool {
        self.mime_type()
            .and_then(MediaType::from_mime_type)
            .map_or(false, |media| matches!(media, MediaType::Image(_)))
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Part::Text { text } => text.trim().is_empty(),
            Part::Blob { .. } => false,
        }
    }

    pub fn to_user_content(&self) -> Result<UserContent, PartConversionError> {
        match self {
            Part::Text { text } => Ok(UserContent::text(text.clone())),
            Part::Blob {
                data_ref,
                mime_type,
                ..
            } => {
                let media_type = MediaType::from_mime_type(mime_type.as_str())
                    .ok_or_else(|| PartConversionError::UnsupportedMime(mime_type.clone()))?;
                let source = match data_ref {
                    DataRef::Base64(data) => DocumentSourceKind::Base64(data.clone()),
                    DataRef::Uri(uri) => DocumentSourceKind::Url(uri.clone()),
                };
                match media_type {
                    MediaType::Image(kind) => Ok(UserContent::Image(Image {
                        data: source,
                        media_type: Some(kind),
                        detail: Some(ImageDetail::Auto),
                        additional_params: None,
                    })),
                    MediaType::Document(kind) => Ok(UserContent::Document(Document {
                        data: source,
                        media_type: Some(kind),
                        additional_params: None,
                    })),
                    MediaType::Audio(kind) => Ok(UserContent::Audio(Audio {
                        data: source,
                        media_type: Some(kind),
                        additional_params: None,
                    })),
                    MediaType::Video(kind) => Ok(UserContent::Video(Video {
                        data: source,
                        media_type: Some(kind),
                        additional_params: None,
                    })),
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum PartConversionError {
    UnsupportedMime(String),
}

impl fmt::Display for PartConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedMime(mime) => write!(f, "unsupported MIME type `{mime}`"),
        }
    }
}

impl std::error::Error for PartConversionError {}
