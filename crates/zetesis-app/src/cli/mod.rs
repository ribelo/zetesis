use std::path::PathBuf;

use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, ValueEnum};

/// Top-level CLI entry point.
#[derive(Debug, Parser)]
#[command(
    name = "zetesis",
    version,
    author,
    about = "Zetesis document search service"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
    /// Increase logging verbosity (-v, -vv, -vvv).
    #[arg(global = true, short = 'v', long = "verbose", action = ArgAction::Count)]
    pub verbose: u8,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            command: None,
            verbose: 0,
        }
    }
}

impl Cli {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }

    pub fn print_help() {
        let mut cmd = Cli::command();
        let _ = cmd.print_help();
        println!();
    }
}

/// Supported subcommands.
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Run the Zetesis HTTP server.
    Serve(ServeArgs),
    /// Fetch KIO documents from upstream sources.
    FetchKio(FetchKioArgs),
    /// Segment local PDF documents and inspect the resulting sentences.
    SegmentPdf(SegmentPdfArgs),
    /// Render a PDF page-by-page, OCR via a selected provider, and emit JSON results.
    OcrPdf(OcrPdfArgs),
    /// Extract structured KIO decision data from a PDF using Gemini structured output.
    ExtractStructured(ExtractStructuredArgs),
}

#[derive(Debug, Args)]
pub struct ServeArgs;

/// Identify which source we are scraping for KIO judgments.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum KioSource {
    Uzp,
    Saos,
}

pub const DEFAULT_KIO_UZP_URL: &str = "https://orzeczenia.uzp.gov.pl/";
pub const DEFAULT_KIO_SAOS_URL: &str = "https://www.saos.org.pl/";

/// Fetch documents from a KIO data source.
#[derive(Debug, Args)]
pub struct FetchKioArgs {
    /// Base URL of the KIO portal search interface.
    #[arg(long, default_value = DEFAULT_KIO_UZP_URL)]
    pub url: String,
    /// Limit the number of documents to download (omit to fetch everything).
    #[arg(long)]
    pub limit: Option<usize>,
    /// Directory where downloaded documents will be cached.
    #[arg(long, value_name = "DIR")]
    pub output_path: PathBuf,
    /// Number of concurrent download workers (>= 1).
    #[arg(long, default_value_t = 4)]
    pub workers: usize,
    /// Upstream source to pull from (`uzp` >= 2021, `saos` < 2021).
    #[arg(long, value_enum, default_value_t = KioSource::Uzp)]
    pub source: KioSource,
}

/// Segment local documents and print the resulting chunks.
#[derive(Debug, Args)]
pub struct SegmentPdfArgs {
    /// One or more PDF files to inspect.
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Output rendering (human-readable text or JSON lines).
    #[arg(long, value_enum, default_value_t = SegmentOutputFormat::Text)]
    pub format: SegmentOutputFormat,
    /// Include byte offsets for each segment.
    #[arg(long)]
    pub with_offsets: bool,
}

/// Available OCR backends exposed through the CLI.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OcrProviderKind {
    Deepinfra,
    Gemini,
}

/// OCR a PDF via the selected provider and print JSON to stdout.
#[derive(Debug, Args)]
pub struct OcrPdfArgs {
    /// PDF document to process.
    #[arg(value_name = "PDF")]
    pub input: PathBuf,
    /// OCR provider used to send page renders.
    #[arg(long = "ocr-provider", value_enum, default_value_t = OcrProviderKind::Deepinfra)]
    pub provider: OcrProviderKind,
    /// Override the model identifier for the selected provider.
    #[arg(long = "ocr-model", default_value = "allenai/olmOCR-2-7B-1025")]
    pub model: String,
    /// Target width (pixels) when rasterizing each PDF page.
    #[arg(long, default_value_t = 2048)]
    pub render_width: u32,
    /// Maximum edge length (pixels) when preparing images for OCR.
    #[arg(long, default_value_t = 1280)]
    pub image_max_edge: u32,
    /// Optional image detail hint passed to the model (`low`, `high`, `auto`).
    #[arg(long)]
    pub detail: Option<String>,
    /// Maximum tokens requested from the model.
    #[arg(long, default_value_t = 4096)]
    pub max_tokens: u32,
}

/// Extract structured decision data from a PDF.
#[derive(Debug, Args)]
pub struct ExtractStructuredArgs {
    /// PDF document to process.
    #[arg(value_name = "PDF")]
    pub input: PathBuf,
    /// Gemini model identifier to use for structured extraction.
    #[arg(long, default_value = "gemini-2.5-flash-lite-preview-09-2025")]
    pub model: String,
}

/// How to render chunk output.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SegmentOutputFormat {
    Text,
    Json,
}
