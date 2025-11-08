use clap::{ArgAction, Args};

#[derive(Debug, Args)]
pub struct DbDeleteArgs {
    #[arg(value_parser = super::validate_index_slug)]
    pub index: String,
    #[arg(long = "id")]
    pub id: String,
    #[arg(long, action = ArgAction::SetTrue)]
    pub dry_run: bool,
    #[arg(long = "yes", action = ArgAction::SetTrue)]
    pub assume_yes: bool,
}
