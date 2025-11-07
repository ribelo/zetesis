use std::path::PathBuf;

use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, ValueEnum};

use crate::constants::{DEFAULT_EMBEDDER_KEY, DEFAULT_EXTRACTOR_MODEL};
use crate::services::HYBRID_DEFAULT_WEIGHT;

/// Top-level CLI entry point.
#[derive(Default, Debug, Parser)]
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
    /// Extract plain text from a PDF (debug feature).
    #[cfg(feature = "cli-debug")]
    Debug(DebugArgs),
}

#[derive(Debug, Args)]
pub struct ServeArgs;

/// Debug command namespace (gated by cli-debug feature).
#[cfg(feature = "cli-debug")]
#[derive(Debug, Args)]
pub struct DebugArgs {
    #[command(subcommand)]
    pub command: DebugCommands,
}

/// Debug subcommands.
#[cfg(feature = "cli-debug")]
#[derive(Debug, Subcommand)]
pub enum DebugCommands {
    /// Extract plain text from a PDF file.
    Text(DebugTextArgs),
}

/// Options for the `debug text` command.
#[cfg(feature = "cli-debug")]
#[derive(Debug, Args)]
pub struct DebugTextArgs {
    /// PDF file to extract text from.
    #[arg(value_parser = validate_pdf_file)]
    pub input: std::path::PathBuf,
}

/// Identify which source we are scraping for KIO judgments.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum KioSource {
    Uzp,
    Saos,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum GenModeArg {
    Sync,
    Batch,
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
    /// Number of concurrent download workers (1-64).
    #[arg(long, default_value_t = 4, value_parser = validate_workers)]
    pub workers: usize,
    /// Upstream source to pull from (`uzp` >= 2021, `saos` < 2021).
    #[arg(long, value_enum, default_value_t = KioSource::Uzp)]
    pub source: KioSource,
}

/// Ingest local documents into a Milli index.
#[derive(Debug, Args)]
pub struct IngestArgs {
    /// Name of the index to ingest into (e.g. `kio`).
    #[arg(value_parser = validate_index_slug)]
    pub index: String,
    /// File or directory containing documents to ingest.
    pub path: PathBuf,
    /// Structured generation mode (`sync` runs locally, `batch` submits jobs to the provider).
    #[arg(long = "gen-mode", value_enum, default_value_t = GenModeArg::Sync)]
    pub gen_mode: GenModeArg,
    /// Limit the number of files processed when ingesting a directory.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Embedding model identifier used for chunk vectors.
    #[arg(long, default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
    /// Gemini model identifier used for structured generation (alias: --extractor-model).
    #[arg(long = "gen-model", default_value = DEFAULT_EXTRACTOR_MODEL, alias = "extractor-model")]
    pub gen_model: String,
    /// Create the Milli index if it does not already exist.
    #[arg(long, action = ArgAction::SetTrue)]
    pub create_index: bool,
    /// Deprecated alias for `--gen-mode batch`.
    #[arg(long, action = ArgAction::SetTrue, hide = true)]
    pub batch: bool,
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
    /// Perform hybrid RRF/weighted search that fuses keyword and vector rankings.
    Hybrid(HybridSearchArgs),
}

/// Arguments for keyword search.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
  # Basic keyword search
  zetesis search keyword kio --q \"procurement law\"

  # Search with filter and custom fields
  zetesis search keyword kio --q \"contract\" --filter 'decision_type = \"WYROK\"' --fields id,sygnatura

  # Search with sorting and pretty output
  zetesis search keyword kio --q \"tender\" --sort date:desc --limit 10 --pretty
")]
pub struct KeywordSearchArgs {
    /// Name of the index to query (e.g. `kio`).
    #[arg(value_parser = validate_index_slug)]
    pub index: String,
    /// Query string for keyword search.
    #[arg(long = "q", value_name = "QUERY")]
    pub query: String,
    /// Optional Milli filter expression.
    #[arg(long)]
    pub filter: Option<String>,
    /// Maximum number of results to return (1-100).
    #[arg(long, default_value_t = 20, value_parser = validate_search_limit)]
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
#[command(after_help = "\
EXAMPLES:
  # Basic vector search
  zetesis search vector kio --q \"cases about public procurement violations\"

  # Vector search with custom embedder and fields
  zetesis search vector kio --q \"contract disputes\" --embedder gemini-embedding-001 --fields id,decision_date

  # Vector search with filter and pretty output
  zetesis search vector kio --q \"appeal decisions\" --filter 'year >= 2023' --k 20 --pretty
")]
pub struct VectorSearchArgs {
    /// Name of the index to query (e.g. `kio`).
    #[arg(value_parser = validate_index_slug)]
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
    /// Number of nearest neighbours to return (1-100).
    #[arg(long = "k", default_value_t = 10, value_parser = validate_vector_k)]
    pub top_k: usize,
    /// Comma-separated list of fields to include in the output.
    #[arg(long = "fields", value_delimiter = ',', num_args = 0..)]
    pub fields: Vec<String>,
    /// Pretty-print JSON output.
    #[arg(long, action = ArgAction::SetTrue)]
    pub pretty: bool,
}

/// Arguments for hybrid search.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
  # Default RRF-based hybrid search
  zetesis search hybrid kio --q \"public procurement dispute\"

  # Hybrid search with weighted fusion
  zetesis search hybrid kio --q \"appeal\" --fusion weighted --keyword-weight 0.7 --vector-weight 0.3
")]
pub struct HybridSearchArgs {
    /// Name of the index to query (e.g. `kio`).
    #[arg(value_parser = validate_index_slug)]
    pub index: String,
    /// Query text shared by both keyword and vector searches.
    #[arg(long = "q", value_name = "QUERY")]
    pub query: String,
    /// Optional Milli filter expression.
    #[arg(long)]
    pub filter: Option<String>,
    /// Embedder key to use for the vector branch.
    #[arg(long = "embedder")]
    pub embedder: Option<String>,
    /// Maximum number of fused results to return (1-100).
    #[arg(long, default_value_t = 20, value_parser = validate_search_limit)]
    pub limit: usize,
    /// Comma-separated list of fields to include in the output.
    #[arg(long = "fields", value_delimiter = ',', num_args = 0..)]
    pub fields: Vec<String>,
    /// Fusion strategy (`rrf` or `weighted`).
    #[arg(long, value_enum, default_value_t = HybridFusionArg::Rrf)]
    pub fusion: HybridFusionArg,
    /// Relative weight for the keyword branch (only used when --fusion weighted).
    #[arg(long = "keyword-weight", default_value_t = HYBRID_DEFAULT_WEIGHT, value_parser = validate_weight)]
    pub keyword_weight: f32,
    /// Relative weight for the vector branch (only used when --fusion weighted).
    #[arg(long = "vector-weight", default_value_t = HYBRID_DEFAULT_WEIGHT, value_parser = validate_weight)]
    pub vector_weight: f32,
    /// Pretty-print JSON output.
    #[arg(long, action = ArgAction::SetTrue)]
    pub pretty: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum HybridFusionArg {
    Rrf,
    Weighted,
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
    #[arg(value_parser = validate_index_slug)]
    pub index: String,
}

/// Arguments for `db get`.
#[derive(Debug, Args)]
pub struct DbGetArgs {
    /// Name of the index to inspect.
    #[arg(value_parser = validate_index_slug)]
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
    #[arg(value_parser = validate_index_slug)]
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
    #[arg(value_parser = validate_index_slug)]
    pub index: String,
    /// Destination directory for backups (defaults to ./backups).
    #[arg(long = "out", value_name = "DIR")]
    pub out: Option<PathBuf>,
}

/// Arguments for `db purge`.
#[derive(Debug, Args)]
pub struct DbPurgeArgs {
    /// Name of the index to remove.
    #[arg(value_parser = validate_index_slug)]
    pub index: String,
}

/// Arguments for `db recover`.
#[derive(Debug, Args)]
pub struct DbRecoverArgs {
    /// Name of the index to restore (will overwrite existing data when --force set).
    #[arg(value_parser = validate_index_slug)]
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

/// Supported generation job subcommands.
#[derive(Debug, Subcommand)]
pub enum JobsCommands {
    /// Display counts for pending, generating, and completed jobs.
    Status(JobsStatusArgs),
    /// Process generation jobs (submit or fetch results).
    Gen(JobsGenArgs),
    /// Reap stale jobs and optionally requeue or fail them.
    Reap(JobsReapArgs),
}

/// Options for the `jobs status` command.
#[derive(Debug, Args)]
pub struct JobsStatusArgs {
    /// Output format (json or table).
    #[arg(long, default_value = "table")]
    pub format: JobsStatusFormat,
}

/// Format for jobs status output.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum JobsStatusFormat {
    Json,
    Table,
}

/// Action to take when reaping stale jobs.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum JobsReapAction {
    /// Requeue stale jobs with retry backoff.
    Requeue,
    /// Mark stale jobs as failed.
    Fail,
    /// Requeue if retries left, fail if exhausted.
    Both,
}

/// Options for the `jobs reap` command.
#[derive(Debug, Args)]
pub struct JobsReapArgs {
    /// Action to take on stale jobs (requeue, fail, or both).
    #[arg(long, default_value = "both")]
    pub action: JobsReapAction,
    /// Dry run mode (don't modify jobs).
    #[arg(long, action = ArgAction::SetTrue)]
    pub dry_run: bool,
}

/// Options for the `jobs gen` command.
#[derive(Debug, Args)]
pub struct JobsGenArgs {
    #[command(subcommand)]
    pub command: JobsGenCommands,
}

#[derive(Debug, Subcommand)]
pub enum JobsGenCommands {
    /// Submit pending generation jobs to the provider.
    Submit(JobsGenSubmitArgs),
    /// Fetch completed generation jobs and finalize indexing.
    Fetch(JobsGenFetchArgs),
}

#[derive(Debug, Args)]
pub struct JobsGenSubmitArgs {
    /// Generation model key to match queued jobs.
    #[arg(long, default_value = DEFAULT_EXTRACTOR_MODEL)]
    pub gen_model: String,
    /// Embedder model key to match queued jobs.
    #[arg(long, default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
    /// Maximum number of jobs to submit in this run.
    #[arg(long, default_value_t = 4)]
    pub limit: usize,
}

#[derive(Debug, Args)]
pub struct JobsGenFetchArgs {
    /// Embedder model key to match in-flight jobs.
    #[arg(long, default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
    /// Maximum number of jobs to fetch in this run.
    #[arg(long, default_value_t = 4)]
    pub limit: usize,
}

// Validator functions for CLI arguments.

/// Maximum attachment size for inline processing (20 MiB).
#[cfg(feature = "cli-debug")]
const MAX_INLINE_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;

/// Validate PDF file: must exist, have .pdf extension, and be under size limit.
#[cfg(feature = "cli-debug")]
fn validate_pdf_file(s: &str) -> Result<std::path::PathBuf, String> {
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
fn validate_index_slug(s: &str) -> Result<String, String> {
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
fn validate_workers(s: &str) -> Result<usize, String> {
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
fn validate_search_limit(s: &str) -> Result<usize, String> {
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
fn validate_vector_k(s: &str) -> Result<usize, String> {
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

fn validate_weight(s: &str) -> Result<f32, String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn validate_index_slug_empty_rejected() {
        let res = validate_index_slug("");
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("cannot be empty"));
    }

    #[test]
    fn validate_index_slug_uppercase_rejected() {
        let res = validate_index_slug("KIO");
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("lowercase"));
    }

    #[test]
    fn validate_index_slug_slash_rejected() {
        let res = validate_index_slug("foo/bar");
        assert!(res.is_err());
    }

    #[test]
    fn validate_index_slug_underscore_rejected() {
        let res = validate_index_slug("foo_bar");
        assert!(res.is_err());
    }

    #[test]
    fn validate_index_slug_too_long_rejected() {
        let long = "a".repeat(33);
        let res = validate_index_slug(&long);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("too long"));
    }

    #[test]
    fn validate_index_slug_valid_accepted() {
        assert!(validate_index_slug("kio").is_ok());
        assert!(validate_index_slug("kio-2023").is_ok());
        assert!(validate_index_slug("test123").is_ok());
    }

    #[test]
    fn fetch_kio_workers_zero_rejected() {
        let res = Cli::try_parse_from(["zetesis", "fetch-kio", "--workers", "0"]);
        assert!(res.is_err());
    }

    #[test]
    fn fetch_kio_workers_over_limit_rejected() {
        let res = Cli::try_parse_from(["zetesis", "fetch-kio", "--workers", "65"]);
        assert!(res.is_err());
    }

    #[test]
    fn fetch_kio_workers_within_range_accepted() {
        let res = Cli::try_parse_from(["zetesis", "fetch-kio", "--workers", "32"]);
        assert!(res.is_ok());
    }

    #[test]
    fn search_keyword_limit_zero_rejected() {
        let res = Cli::try_parse_from([
            "zetesis", "search", "keyword", "kio", "--q", "test", "--limit", "0",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn search_keyword_limit_over_max_rejected() {
        let res = Cli::try_parse_from([
            "zetesis", "search", "keyword", "kio", "--q", "test", "--limit", "101",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn search_keyword_limit_within_range_accepted() {
        let res = Cli::try_parse_from([
            "zetesis", "search", "keyword", "kio", "--q", "test", "--limit", "50",
        ]);
        assert!(res.is_ok());
    }

    #[test]
    fn search_vector_k_zero_rejected() {
        let res = Cli::try_parse_from([
            "zetesis", "search", "vector", "kio", "--q", "test", "--k", "0",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn search_vector_k_over_max_rejected() {
        let res = Cli::try_parse_from([
            "zetesis", "search", "vector", "kio", "--q", "test", "--k", "101",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn search_vector_k_within_range_accepted() {
        let res = Cli::try_parse_from([
            "zetesis", "search", "vector", "kio", "--q", "test", "--k", "20",
        ]);
        assert!(res.is_ok());
    }

    #[test]
    fn weight_validator_enforces_bounds() {
        assert!(validate_weight("0").is_ok());
        assert!(validate_weight("1").is_ok());
        assert!(validate_weight("0.5").is_ok());
        assert!(validate_weight("-0.1").is_err());
        assert!(validate_weight("1.1").is_err());
    }

    #[test]
    fn ingest_invalid_index_name_rejected() {
        let res = Cli::try_parse_from(["zetesis", "ingest", "Invalid_Name", "test.pdf"]);
        assert!(res.is_err());
    }

    #[test]
    fn ingest_valid_index_name_accepted() {
        let res = Cli::try_parse_from(["zetesis", "ingest", "kio", "test.pdf"]);
        assert!(res.is_ok());
    }
}
