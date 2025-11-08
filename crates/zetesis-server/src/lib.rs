pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub mod config;
pub mod search;
mod server;

pub use config::*;
pub use search::*;
pub use server::{ServerError, build_api_router, serve};
