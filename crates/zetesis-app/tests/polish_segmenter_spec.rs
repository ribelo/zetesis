use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use zetesis_app::services::{PolishSentenceSegmenter, cleanup_text, extract_text_from_pdf};

#[test]
fn splits_real_kio_pdf_smoke() -> Result<(), Box<dyn Error>> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(PathBuf::from)
        .expect("workspace root");

    let data_dir = workspace_root.join("data").join("kio");
    if !data_dir.exists() {
        eprintln!("skipping: data/kio directory not found");
        return Ok(());
    }

    let pdf_path = fs::read_dir(&data_dir)?
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|ext| ext == "pdf"));

    let Some(pdf_path) = pdf_path else {
        eprintln!("skipping: no PDFs in {}", data_dir.display());
        return Ok(());
    };

    let bytes = fs::read(&pdf_path)?;
    let text = match extract_text_from_pdf(&bytes) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("skipping: pdfium not available ({err})");
            return Ok(());
        }
    };

    assert!(
        !text.trim().is_empty(),
        "extracted text from {} should not be empty",
        pdf_path.display()
    );

    let cleaned = cleanup_text(&text);
    let sentences = PolishSentenceSegmenter::split(&cleaned);
    assert!(
        !sentences.is_empty(),
        "segmenter should yield sentences for {}",
        pdf_path.display()
    );

    assert!(
        sentences.iter().all(|s| !s.trim().is_empty()),
        "segmenter produced empty sentences for {}",
        pdf_path.display()
    );

    for sentence in sentences {
        println!("{}\n", sentence);
    }
    Ok(())
}
