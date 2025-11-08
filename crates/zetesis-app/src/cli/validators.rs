#[cfg(feature = "cli-debug")]
const MAX_INLINE_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;

/// Validate PDF file: must exist, have .pdf extension, and be under size limit.
#[cfg(feature = "cli-debug")]
pub fn validate_pdf_file(s: &str) -> Result<std::path::PathBuf, String> {
    let path = std::path::PathBuf::from(s);

    if !path.exists() {
        return Err(format!("file does not exist: {}", s));
    }

    if !path.is_file() {
        return Err(format!("path is not a file: {}", s));
    }

    match path.extension().and_then(|e| e.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("pdf") => {}
        _ => return Err(format!("file must have .pdf extension: {}", s)),
    }

    match std::fs::metadata(&path) {
        Ok(meta) => {
            let size = meta.len() as usize;
            if size > MAX_INLINE_ATTACHMENT_BYTES {
                let limit_mib = (MAX_INLINE_ATTACHMENT_BYTES / (1024 * 1024)).max(1);
                return Err(format!(
                    "file size {} bytes exceeds limit of {} MiB",
                    size, limit_mib
                ));
            }
        }
        Err(e) => return Err(format!("failed to read file metadata: {}", e)),
    }

    Ok(path)
}

/// Validate index name: lowercase ASCII letters, digits, hyphens only, length 1..=32.
pub fn validate_index_slug(s: &str) -> Result<String, String> {
    if s.is_empty() {
        return Err("index name cannot be empty".to_string());
    }

    if s.len() > 32 {
        return Err(format!("index name too long: {} chars (max 32)", s.len()));
    }

    if !s
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
    {
        return Err(
            "index name must contain only lowercase ASCII letters, digits, and hyphens".to_string(),
        );
    }

    Ok(s.to_string())
}

/// Validate worker count: must be between 1 and 64.
pub fn validate_workers(s: &str) -> Result<usize, String> {
    let value = s
        .parse::<usize>()
        .map_err(|_| format!("invalid number: {}", s))?;

    if value == 0 {
        return Err("workers must be at least 1".to_string());
    }

    if value > 64 {
        return Err("workers cannot exceed 64".to_string());
    }

    Ok(value)
}

/// Validate search limit: must be between 1 and 100.
pub fn validate_search_limit(s: &str) -> Result<usize, String> {
    let value = s
        .parse::<usize>()
        .map_err(|_| format!("invalid number: {}", s))?;

    if value == 0 {
        return Err("limit must be at least 1".to_string());
    }

    if value > 100 {
        return Err("limit cannot exceed 100".to_string());
    }

    Ok(value)
}

/// Validate vector search k: must be between 1 and 100.
pub fn validate_vector_k(s: &str) -> Result<usize, String> {
    let value = s
        .parse::<usize>()
        .map_err(|_| format!("invalid number: {}", s))?;

    if value == 0 {
        return Err("k must be at least 1".to_string());
    }

    if value > 100 {
        return Err("k cannot exceed 100".to_string());
    }

    Ok(value)
}

/// Validate hybrid fusion weights: must be finite and between 0.0 and 1.0.
pub fn validate_weight(s: &str) -> Result<f32, String> {
    let value = s
        .parse::<f32>()
        .map_err(|_| format!("invalid number: {}", s))?;
    if !value.is_finite() {
        return Err("weight must be finite".to_string());
    }
    if value < 0.0 || value > 1.0 {
        return Err("weight must be between 0.0 and 1.0".to_string());
    }
    Ok(value)
}
