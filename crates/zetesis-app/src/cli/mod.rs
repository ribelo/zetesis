use std::path::PathBuf;

use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, ValueEnum};

use crate::constants::{DEFAULT_EMBEDDER_KEY, DEFAULT_EXTRACTOR_MODEL};

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
    /// Ingest local documents into a Milli index.
    Ingest(IngestArgs),
    /// Run audits and consistency checks.
    Audit(AuditArgs),
    /// Query Milli indexes using keyword or vector search.
    Search(SearchArgs),
    /// Maintain Milli indexes (inspect, backup, purge, recover).
    Db(DbArgs),
    /// Manage asynchronous embedding jobs.
    Jobs(JobsArgs),
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

/// Ingest local documents into a Milli index.
#[derive(Debug, Args)]
pub struct IngestArgs {
    /// Name of the index to ingest into (e.g. `kio`).
    pub index: String,
    /// File or directory containing documents to ingest.
    pub path: PathBuf,
    /// Submit embedding jobs via the Gemini batch API instead of synchronous embedding.
    #[arg(long, action = ArgAction::SetTrue)]
    pub batch: bool,
    /// Limit the number of files processed when ingesting a directory.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Embedding model identifier used for chunk vectors.
    #[arg(long, default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
    /// Gemini model identifier used for structured extraction.
    #[arg(long, default_value = DEFAULT_EXTRACTOR_MODEL)]
    pub extractor_model: String,
    /// Create the Milli index if it does not already exist.
    #[arg(long, action = ArgAction::SetTrue)]
    pub create_index: bool,
}

#[allow(dead_code)]
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
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OcrProviderKind {
    Deepinfra,
    Gemini,
}

/// OCR a PDF via the selected provider and print JSON to stdout.
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SegmentOutputFormat {
    Text,
    Json,
}

/// Audit command namespace.
#[derive(Debug, Args)]
pub struct AuditArgs {
    #[command(subcommand)]
    pub command: AuditCommands,
}

/// Audit subcommands.
#[derive(Debug, Subcommand)]
pub enum AuditCommands {
    /// Audit semantic chunk coverage and basic integrity signals.
    Structured(StructuredAuditArgs),
}

/// Options for the `audit structured` command.
#[derive(Debug, Args)]
pub struct StructuredAuditArgs {
    /// Directory to scan for PDFs (defaults to data/kio/raw/kio).
    #[arg(long, value_name = "DIR", default_value = "data/kio/raw/kio")]
    pub dir: PathBuf,
    /// Number of PDFs to sample.
    #[arg(short = 'n', long = "num", default_value_t = 10)]
    pub num: usize,
    /// Override Gemini model identifier for extraction.
    #[arg(long)]
    pub model: Option<String>,
    /// Output path for TSV (use '-' for stdout).
    #[arg(long = "out", value_name = "TSV", default_value = "-")]
    pub out_path: String,
}

/// Top-level index command namespace.
#[allow(dead_code)]
#[derive(Debug, Args)]
pub struct IndexArgs {
    #[command(subcommand)]
    pub command: IndexCommands,
}

/// Index subcommands.
#[allow(dead_code)]
#[derive(Debug, Subcommand)]
pub enum IndexCommands {
    /// Index structured decisions into Milli.
    Structured(StructuredIndexArgs),
}

/// Options for indexing structured documents.
#[allow(dead_code)]
#[derive(Debug, Args)]
pub struct StructuredIndexArgs {
    /// One or more PDF documents to ingest.
    #[arg(required = true, value_name = "PDF")]
    pub inputs: Vec<PathBuf>,
    /// Gemini model identifier used for structured extraction.
    #[arg(long, default_value = "gemini-2.5-flash-lite-preview-09-2025")]
    pub extractor_model: String,
    /// Embedding model identifier used for chunk vectors.
    #[arg(long, default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
}

/// Top-level namespace for search commands.
#[derive(Debug, Args)]
pub struct SearchArgs {
    #[command(subcommand)]
    pub command: SearchCommands,
}

/// Search subcommands.
#[derive(Debug, Subcommand)]
pub enum SearchCommands {
    /// Perform keyword search across indexed records.
    Keyword(KeywordSearchArgs),
    /// Perform vector similarity search on chunk embeddings.
    Vector(VectorSearchArgs),
}

/// Arguments for keyword search.
#[derive(Debug, Args)]
pub struct KeywordSearchArgs {
    /// Name of the index to query (e.g. `kio`).
    pub index: String,
    /// Query string for keyword search.
    #[arg(long = "q", value_name = "QUERY")]
    pub query: String,
    /// Optional Milli filter expression.
    #[arg(long)]
    pub filter: Option<String>,
    /// Maximum number of results to return.
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
    /// Number of results to skip from the start.
    #[arg(long, default_value_t = 0)]
    pub offset: usize,
    /// Sort specification (`field:asc|desc`). Repeat for multiple fields.
    #[arg(long = "sort", value_delimiter = ',', num_args = 0..)]
    pub sort: Vec<String>,
    /// Comma-separated list of fields to include in the output.
    #[arg(long = "fields", value_delimiter = ',', num_args = 0..)]
    pub fields: Vec<String>,
    /// Pretty-print JSON output.
    #[arg(long, action = ArgAction::SetTrue)]
    pub pretty: bool,
}

/// Arguments for vector search.
#[derive(Debug, Args)]
pub struct VectorSearchArgs {
    /// Name of the index to query (e.g. `kio`).
    pub index: String,
    /// Text prompt to embed and search with.
    #[arg(long = "q", value_name = "QUERY")]
    pub query: String,
    /// Embedder key to use (defaults to configured index embedder).
    #[arg(long = "embedder")]
    pub embedder: Option<String>,
    /// Optional Milli filter expression.
    #[arg(long)]
    pub filter: Option<String>,
    /// Number of nearest neighbours to return.
    #[arg(long = "k", default_value_t = 10)]
    pub top_k: usize,
    /// Comma-separated list of fields to include in the output.
    #[arg(long = "fields", value_delimiter = ',', num_args = 0..)]
    pub fields: Vec<String>,
    /// Pretty-print JSON output.
    #[arg(long, action = ArgAction::SetTrue)]
    pub pretty: bool,
}

/// Top-level namespace for database maintenance commands.
#[derive(Debug, Args)]
pub struct DbArgs {
    #[command(subcommand)]
    pub command: DbCommands,
}

/// Database maintenance subcommands.
#[derive(Debug, Subcommand)]
pub enum DbCommands {
    /// List available Milli indexes and their on-disk sizes.
    List,
    /// Show summary statistics for a specific Milli index.
    Stats(DbStatsArgs),
    /// Fetch a single record by primary key.
    Get(DbGetArgs),
    /// Run an ad-hoc filtered listing of records.
    Find(DbFindArgs),
    /// Create a timestamped backup of a Milli index.
    Backup(DbBackupArgs),
    /// Permanently delete a Milli index directory.
    Purge(DbPurgeArgs),
    /// Restore a Milli index from a backup directory.
    Recover(DbRecoverArgs),
}

/// Arguments for `db stats`.
#[derive(Debug, Args)]
pub struct DbStatsArgs {
    /// Name of the index to inspect (e.g. `kio`).
    pub index: String,
}

/// Arguments for `db get`.
#[derive(Debug, Args)]
pub struct DbGetArgs {
    /// Name of the index to inspect.
    pub index: String,
    /// Primary key of the record to fetch.
    #[arg(long)]
    pub id: String,
    /// Comma-separated list of fields to include in the response.
    #[arg(long = "fields", value_delimiter = ',', num_args = 0..)]
    pub fields: Vec<String>,
    /// Pretty-print JSON output.
    #[arg(long, action = ArgAction::SetTrue)]
    pub pretty: bool,
}

/// Arguments for `db find`.
#[derive(Debug, Args)]
pub struct DbFindArgs {
    /// Name of the index to inspect.
    pub index: String,
    /// Optional Milli filter expression.
    #[arg(long)]
    pub filter: Option<String>,
    /// Maximum number of records to return.
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
    /// Number of records to skip from the start.
    #[arg(long, default_value_t = 0)]
    pub offset: usize,
    /// Comma-separated list of fields to include in the response.
    #[arg(long = "fields", value_delimiter = ',', num_args = 0..)]
    pub fields: Vec<String>,
    /// Pretty-print JSON output.
    #[arg(long, action = ArgAction::SetTrue)]
    pub pretty: bool,
}

/// Arguments for `db backup`.
#[derive(Debug, Args)]
pub struct DbBackupArgs {
    /// Name of the index to back up.
    pub index: String,
    /// Destination directory for backups (defaults to ./backups).
    #[arg(long = "out", value_name = "DIR")]
    pub out: Option<PathBuf>,
}

/// Arguments for `db purge`.
#[derive(Debug, Args)]
pub struct DbPurgeArgs {
    /// Name of the index to remove.
    pub index: String,
}

/// Arguments for `db recover`.
#[derive(Debug, Args)]
pub struct DbRecoverArgs {
    /// Name of the index to restore (will overwrite existing data when --force set).
    pub index: String,
    /// Directory containing the backup to restore from.
    #[arg(long = "from", value_name = "DIR")]
    pub from: PathBuf,
    /// Allow overwriting the target index directory if it already exists.
    #[arg(long, action = ArgAction::SetTrue)]
    pub force: bool,
}

/// Embedding job management command namespace.
#[derive(Debug, Args)]
pub struct JobsArgs {
    #[command(subcommand)]
    pub command: JobsCommands,
}

/// Supported embedding job subcommands.
#[derive(Debug, Subcommand)]
pub enum JobsCommands {
    /// Display counts for pending, running, and completed embedding jobs.
    Status,
    /// Embed pending jobs into vectors.
    Embed(JobsEmbedArgs),
    /// Ingest embedded jobs into Milli.
    Ingest(JobsIngestArgs),
}

/// Options for the `jobs embed` command.
#[derive(Debug, Args)]
pub struct JobsEmbedArgs {
    /// Embedder model key to match queued jobs.
    #[arg(long, default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
    /// Maximum number of jobs to process in this run.
    #[arg(long, default_value_t = 4)]
    pub limit: usize,
}

/// Options for the `jobs ingest` command.
#[derive(Debug, Args)]
pub struct JobsIngestArgs {
    /// Embedder model key to match embedded jobs.
    #[arg(long, default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
    /// Maximum number of jobs to ingest in this run.
    #[arg(long, default_value_t = 4)]
    pub limit: usize,
}
