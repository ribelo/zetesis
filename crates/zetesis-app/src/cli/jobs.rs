use clap::{ArgAction, Args, Subcommand, ValueEnum};

use crate::constants::{DEFAULT_EMBEDDER_KEY, DEFAULT_EXTRACTOR_MODEL};

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
