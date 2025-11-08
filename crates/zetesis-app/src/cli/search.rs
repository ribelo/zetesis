use clap::{ArgAction, Args, Subcommand, ValueEnum};

use crate::cli::validators::{
    validate_index_slug, validate_search_limit, validate_vector_k, validate_weight,
};
use crate::services::HYBRID_DEFAULT_WEIGHT;

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
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
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
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
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
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
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
