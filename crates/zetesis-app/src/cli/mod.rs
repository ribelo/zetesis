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
}

/// How to render chunk output.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SegmentOutputFormat {
    Text,
    Json,
}
