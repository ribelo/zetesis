use clap::{ArgAction, Args, Subcommand};

use crate::cli::serve::ServeArgs;

/// Namespace for system-level utilities.
#[derive(Debug, Args)]
pub struct SysArgs {
    #[command(subcommand)]
    pub command: SysCommands,
}

/// System utilities.
#[derive(Debug, Subcommand)]
pub enum SysCommands {
    /// Run the Zetesis HTTP server.
    Serve(ServeArgs),
    /// Initialize filesystem layout and verify write permissions.
    Init(SysInitArgs),
    /// Diagnose configuration, keys, and storage readiness.
    Doctor(SysDoctorArgs),
}

/// Options for `sys init`.
#[derive(Debug, Args)]
pub struct SysInitArgs {
    /// Skip creating logging/metrics directories.
    #[arg(long, action = ArgAction::SetTrue)]
    pub skip_nonessential: bool,
}

/// Options for `sys doctor`.
#[derive(Debug, Args)]
pub struct SysDoctorArgs {
    /// Show verbose diagnostic output.
    #[arg(long, action = ArgAction::SetTrue)]
    pub verbose: bool,
}
