use zetesis_app::services::pdf::{PdfTextError, extract_text_from_pdf};

#[test]
fn pdfium_extracts_polish_text() {
    let bytes = include_bytes!("fixtures/kio_sample.pdf");

    let text = match extract_text_from_pdf(bytes) {
        Ok(text) => text,
        Err(PdfTextError::Library(err)) => {
            panic!("Pdfium library not found ({err}). Run `just download-pdfium` before testing.")
        }
        Err(err) => panic!("Pdfium extraction failed: {err}"),
    };

    assert!(
        text.contains("Sygn. akt:KIO 2777/11"),
        "expected case reference in extracted text"
    );
    assert!(
        text.contains("Pełnomocnik"),
        "expected diacritics in extracted text"
    );
}
