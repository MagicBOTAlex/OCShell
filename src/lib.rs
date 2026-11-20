//! OCShell: an interactive OpenComputers shell over WebSocket.
//!
//! WARNING: LLM-generated, unauthenticated software. See DISCLAIMER.md.

pub mod cli;
pub mod client;
pub mod server;
pub mod ws;

pub const DISCLAIMER: &str =
    "OCShell is LLM-generated, unreviewed software with NO authentication. \
Anyone who can reach the port gets a shell. See DISCLAIMER.md.";
