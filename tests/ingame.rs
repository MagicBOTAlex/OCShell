//! End-to-end test through OS2Playground/ocplay and the real Lua agent.
//!
//! Skipped unless `OCPLAY` points at an `ocplay` binary (the TCP-enabled
//! build).

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::Instant;

use ocs::server::Server;
use ocs::ws::{WsStream, OP_TEXT};

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[tokio::test(flavor = "multi_thread")]
async fn shell_roundtrip() {
    let Ok(ocplay) = std::env::var("OCPLAY") else {
        eprintln!("skipping: OCPLAY is not set");
        return;
    };
    if !Path::new(&ocplay).is_file() {
        eprintln!("skipping: OCPLAY does not point at a file: {ocplay}");
        return;
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let config = root.join("tests").join("computer.yaml");
    let agent = root.join("agent").join("ocshell.lua");

    let server = Server::bind("127.0.0.1", 0, Some("127.0.0.1".to_string()))
        .await
        .unwrap();
    let port = server.port();
    let server_task = tokio::spawn(async move {
        let _ = server.run().await;
    });

    let mut process = Command::new(&ocplay)
        .arg(&config)
        .arg(&agent)
        .arg(format!("ws://127.0.0.1:{port}/agent"))
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start ocplay");

    let stdout = process.stdout.take().unwrap();
    let stderr = process.stderr.take().unwrap();
    let stdout_task = tokio::spawn(async move {
        let mut data = Vec::new();
        let mut stdout = stdout;
        let _ = stdout.read_to_end(&mut data).await;
        data
    });
    let stderr_task = tokio::spawn(async move {
        let mut data = Vec::new();
        let mut stderr = stderr;
        let _ = stderr.read_to_end(&mut data).await;
        data
    });

    let check = tokio::spawn(async move {
        let mut client = None;
        let connect_deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < connect_deadline {
            match WsStream::connect("127.0.0.1", port, "/client", Duration::from_secs(1)).await {
                Ok(stream) => {
                    client = Some(stream);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        let mut client = client.expect("could not attach a client");

        let mut output = Vec::new();
        let prompt_deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < prompt_deadline {
            match tokio::time::timeout(Duration::from_millis(500), client.recv_frame()).await {
                Ok(Ok(frame)) => output.extend_from_slice(&frame.payload),
                Ok(Err(error)) => panic!("websocket closed while waiting for prompt: {error}"),
                Err(_) => {}
            }
            if contains(&output, b"/home #") {
                break;
            }
        }
        assert!(
            contains(&output, b"/home #"),
            "never saw the shell prompt:\n{}",
            String::from_utf8_lossy(&output)
        );

        client.send_frame(OP_TEXT, b"T 6c73").await.unwrap(); // "ls"
        client.send_frame(OP_TEXT, b"K 28 13 1").await.unwrap(); // Enter

        let listing_deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < listing_deadline {
            match tokio::time::timeout(Duration::from_millis(500), client.recv_frame()).await {
                Ok(Ok(frame)) => output.extend_from_slice(&frame.payload),
                Ok(Err(error)) => panic!("websocket closed while waiting for ls: {error}"),
                Err(_) => {}
            }
            if contains(&output, b"ocplay_autorun") {
                break;
            }
        }
        assert!(
            contains(&output, b"ocplay_autorun"),
            "ls output never arrived:\n{}",
            String::from_utf8_lossy(&output)
        );
    });

    let result = check.await;

    process.kill().await.ok();
    let _ = process.wait().await;
    let stdout = stdout_task.await.unwrap_or_default();
    let stderr = stderr_task.await.unwrap_or_default();
    server_task.abort();

    if let Err(error) = result {
        // Surface the emulator's output when the roundtrip fails.
        eprintln!("--- ocplay stdout ---");
        eprintln!("{}", String::from_utf8_lossy(&stdout));
        eprintln!("--- ocplay stderr ---");
        eprintln!("{}", String::from_utf8_lossy(&stderr));
        panic!("ingame roundtrip failed: {error}");
    }
}
