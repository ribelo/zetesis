use crate::pipeline::Silo;
use clap::{ArgAction, CommandFactory, Parser, Subcommand, ValueEnum};

pub mod audit;
pub mod db;
pub mod db_delete;
pub mod fetch_kio;
pub mod ingest;
pub mod jobs;
pub mod search;
pub mod serve;
pub mod silo;
pub mod source;
pub mod sys;
pub mod validators;

#[cfg(feature = "cli-debug")]
pub mod debug;

/// CLI-specific silo selection (currently only one silo exists).
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum CliSilo {
    /// Polish KIO judicial decisions and related data.
    Kio,
}

impl From<CliSilo> for Silo {
    fn from(value: CliSilo) -> Self {
        match value {
            CliSilo::Kio => Silo::Kio,
        }
    }
}

impl CliSilo {
    pub fn slug(self) -> &'static str {
        match self {
            CliSilo::Kio => "kio",
        }
    }
}

impl Default for CliSilo {
    fn default() -> Self {
        CliSilo::Kio
    }
}

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
    /// Scope commands to a specific data silo.
    #[arg(global = true, long = "silo", value_enum, default_value_t = CliSilo::Kio)]
    pub silo: CliSilo,
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
    /// Manage silo resources and indexes.
    Silo(silo::SiloArgs),
    /// Source ingestion pipelines (e.g., KIO fetch).
    Source(source::SourceArgs),
    /// Database/index helpers (preferred namespace).
    Index(db::DbArgs),
    /// Legacy alias for the `index` namespace (deprecated).
    #[command(hide = true)]
    Db(db::DbArgs),
    /// System utilities (serve/init/doctor).
    Sys(sys::SysArgs),
    /// Ingest local documents into a Milli index.
    Ingest(ingest::IngestArgs),
    /// Run audits and consistency checks.
    Audit(audit::AuditArgs),
    /// Query Milli indexes using keyword or vector search.
    Search(search::SearchArgs),
    /// Manage asynchronous embedding jobs.
    Jobs(jobs::JobsArgs),
    /// Legacy `serve` command (deprecated in favor of `sys serve`).
    #[command(hide = true)]
    Serve(serve::ServeArgs),
    /// Legacy `fetch-kio` command (deprecated in favor of `source kio fetch`).
    #[command(hide = true)]
    FetchKio(fetch_kio::FetchKioArgs),
    /// Extract plain text from a PDF (debug feature).
    #[cfg(feature = "cli-debug")]
    Debug(debug::DebugArgs),
}

pub use audit::{AuditArgs, AuditCommands, StructuredAuditArgs};
pub use db::{
    DbArgs, DbBackupArgs, DbCommands, DbCreateArgs, DbFindArgs, DbGetArgs, DbPurgeArgs,
    DbRecoverArgs, DbStatsArgs,
};
pub use db_delete::DbDeleteArgs;
#[cfg(feature = "cli-debug")]
pub use debug::{DebugArgs, DebugCommands, DebugTextArgs};
pub use fetch_kio::{FetchKioArgs, KioSource};
pub use ingest::{GenModeArg, IngestArgs};
pub use jobs::{
    JobsArgs, JobsCommands, JobsGenArgs, JobsGenCommands, JobsGenFetchArgs, JobsGenSubmitArgs,
    JobsReapAction, JobsReapArgs, JobsStatusArgs, JobsStatusFormat,
};
pub use search::{
    HybridFusionArg, HybridSearchArgs, KeywordSearchArgs, SearchArgs, SearchCommands,
    VectorSearchArgs,
};
pub use serve::ServeArgs;
pub use silo::{SiloArgs, SiloCommands, SiloCreateArgs};
pub use source::{SourceArgs, SourceCommands, SourceKioArgs, SourceKioCommands};
pub use sys::{SysArgs, SysCommands, SysDoctorArgs, SysInitArgs};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::validators::{validate_index_slug, validate_weight};
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
        let cli = Cli::try_parse_from(["zetesis", "ingest", "Invalid_Name", "test.pdf"]).unwrap();
        let ingest_args = match cli.command {
            Some(Commands::Ingest(args)) => args,
            _ => panic!("expected ingest command"),
        };
        assert!(validate_index_slug(&ingest_args.args[0]).is_err());
    }

    #[test]
    fn ingest_valid_index_name_accepted() {
        let cli = Cli::try_parse_from(["zetesis", "ingest", "kio", "test.pdf"]).unwrap();
        let ingest_args = match cli.command {
            Some(Commands::Ingest(args)) => args,
            _ => panic!("expected ingest command"),
        };
        assert!(validate_index_slug(&ingest_args.args[0]).is_ok());
    }
}
