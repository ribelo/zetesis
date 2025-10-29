use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use zetesis_app::pdf::extract_text_from_pdf;
use zetesis_app::text::{PolishSentenceSegmenter, cleanup_text};

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

#[derive(Debug, Deserialize)]
struct Case {
    label: String,
    text: String,
    expected_sentences: usize,
    #[serde(default)]
    expectations: Vec<Expectation>,
}

#[derive(Debug, Deserialize)]
struct Expectation {
    index: usize,
    #[serde(default)]
    contains: Option<String>,
    #[serde(default)]
    starts_with: Option<String>,
    #[serde(default)]
    equals_normalized: Option<String>,
}

#[test]
fn yaml_regressions() -> Result<(), Box<dyn Error>> {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(PathBuf::from)
        .expect("workspace root")
        .join("crates/zetesis-app/tests/fixtures/polish_segmenter_cases.yml");

    let content = fs::read_to_string(&fixture_path)?;
    let cases: Vec<Case> = serde_yaml::from_str(&content)?;

    for case in cases {
        let cleaned = cleanup_text(&case.text);
        let sentences = PolishSentenceSegmenter::split(&cleaned);

        assert_eq!(
            sentences.len(),
            case.expected_sentences,
            "expected {} sentences for case `{}` but got {}",
            case.expected_sentences,
            case.label,
            sentences.len()
        );

        for expectation in case.expectations {
            let sentence = sentences.get(expectation.index).unwrap_or_else(|| {
                panic!(
                    "missing sentence {} for case {}",
                    expectation.index, case.label
                )
            });

            if let Some(value) = &expectation.contains {
                assert!(
                    sentence.contains(value),
                    "expected sentence {} in `{}` to contain `{}`, got `{}`",
                    expectation.index,
                    case.label,
                    value,
                    sentence
                );
            }

            if let Some(prefix) = &expectation.starts_with {
                assert!(
                    sentence.starts_with(prefix),
                    "expected sentence {} in `{}` to start with `{}`, got `{}`",
                    expectation.index,
                    case.label,
                    prefix,
                    sentence
                );
            }

            if let Some(expected) = &expectation.equals_normalized {
                let actual_norm = normalize(sentence);
                assert_eq!(
                    actual_norm,
                    normalize(expected),
                    "expected sentence {} in `{}` to equal `{}` (normalized), got `{}`",
                    expectation.index,
                    case.label,
                    expected,
                    sentence
                );
            }
        }
    }

    Ok(())
}

fn normalize(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn enumeration_splits() {
    let text = "Orzeka: 1. jeden 2. dwa 3) trzy 4 ) cztery. A. alfa B. beta.";
    let cleaned = cleanup_text(text);
    let sentences = PolishSentenceSegmenter::split(&cleaned);
    assert_eq!(
        sentences,
        vec![
            "Orzeka:".to_string(),
            "1. jeden".to_string(),
            "2. dwa".to_string(),
            "3) trzy".to_string(),
            "4 ) cztery.".to_string(),
            "A. alfa".to_string(),
            "B. beta.".to_string(),
        ]
    );
}
