use clap::{ArgAction, Args};

use crate::cli::validators::validate_index_slug;

#[derive(Debug, Args)]
pub struct DbDeleteArgs {
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
    #[arg(long = "id")]
    pub id: String,
    #[arg(long, action = ArgAction::SetTrue)]
    pub dry_run: bool,
    #[arg(long = "yes", action = ArgAction::SetTrue)]
    pub assume_yes: bool,
}
