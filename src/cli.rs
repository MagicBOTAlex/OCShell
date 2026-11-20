//! Command line interface for OCShell.

use clap::{Parser, Subcommand};

use crate::client;
use crate::server::Server;
use crate::DISCLAIMER;

#[derive(Debug, Parser)]
#[command(
    name = "ocs",
    version,
    about = "OpenComputers shell over WebSocket. LLM-generated and unauthenticated."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the relay server
    Serve {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 5235)]
        port: u16,
        /// Host shown in URLs (default: bind host, or 127.0.0.1 for 0.0.0.0)
        #[arg(long)]
        public_host: Option<String>,
    },
    /// Attach the current terminal
    Attach {
        #[arg(default_value = "ws://127.0.0.1:5235/client")]
        url: String,
    },
}

pub async fn run(cli: Cli) -> i32 {
    match cli.command {
        Command::Serve {
            host,
            port,
            public_host,
        } => serve(&host, port, public_host).await,
        Command::Attach { url } => client::attach(&url).await,
    }
}

async fn serve(host: &str, port: u16, public_host: Option<String>) -> i32 {
    eprintln!("WARNING: {DISCLAIMER}");

    let server = match Server::bind(host, port, public_host).await {
        Ok(server) => server,
        Err(error) => {
            eprintln!("error: cannot listen on {host}:{port}: {error}");
            return 1;
        }
    };

    let public_host = server.public_host().to_string();
    let port = server.port();

    println!("OCShell is LLM-generated, unauthenticated software. See DISCLAIMER.md.");
    println!("agent:  ws://{public_host}:{port}/agent");
    println!("client: ocs attach ws://{public_host}:{port}/client");
    println!("install: wget -f http://{public_host}:{port}/agent.lua /bin/ocshell.lua");
    println!("listening; Ctrl+C to stop");

    if let Err(error) = server.run().await {
        eprintln!("error: server stopped: {error}");
        return 1;
    }
    0
}
