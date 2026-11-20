use std::process::ExitCode;

use clap::Parser;

use ocs::cli::{run, Cli};

#[tokio::main]
async fn main() -> ExitCode {
    let code = run(Cli::parse()).await;
    ExitCode::from(code as u8)
}
