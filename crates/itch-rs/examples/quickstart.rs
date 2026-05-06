//! Connect to an `itch-server` over TCP and print up to five
//! decoded ITCH 5.0 messages to stdout.
//!
//! Build and run:
//!
//! ```bash
//! # In one terminal:
//! cargo run -p itch-server
//!
//! # In another:
//! cargo run -p itch-rs --example quickstart --features tcp
//! ```
//!
//! Pass `--server <addr>` (or set `ITCH_SERVER`) to point at a
//! non-default endpoint. Defaults to `127.0.0.1:9100`.

#![forbid(unsafe_code)]

use std::env;
use std::process::ExitCode;

use futures::StreamExt;
use itch_rs::protocol::Message;
use itch_rs::tcp::{connect, TransportError};

const DEFAULT_SERVER: &str = "127.0.0.1:9100";
const PRINT_LIMIT: usize = 5;

#[tokio::main]
async fn main() -> ExitCode {
    let server = env::var("ITCH_SERVER").unwrap_or_else(|_| DEFAULT_SERVER.to_string());

    let mut conn = match connect(&server).await {
        Ok(c) => c,
        Err(err) => {
            eprintln!("connect to {server} failed: {err}");
            return ExitCode::from(2);
        }
    };
    eprintln!("connected to {server}; printing first {PRINT_LIMIT} messages");

    let mut printed = 0usize;
    while printed < PRINT_LIMIT {
        match conn.next().await {
            Some(Ok(msg)) => {
                println!("{}: {:?}", char::from(msg.tag()), summarize(&msg));
                printed += 1;
            }
            Some(Err(TransportError::Protocol(err))) => {
                eprintln!("bad inner ITCH frame, continuing: {err:?}");
            }
            Some(Err(err)) => {
                eprintln!("fatal transport error: {err:?}");
                return ExitCode::from(3);
            }
            None => {
                eprintln!("server closed before {PRINT_LIMIT} messages");
                return ExitCode::SUCCESS;
            }
        }
    }
    ExitCode::SUCCESS
}

fn summarize(msg: &Message) -> String {
    let h = msg.header();
    format!(
        "stock_locate={} tracking={} ts={}",
        h.stock_locate.as_u16(),
        h.tracking_number.as_u16(),
        h.timestamp.as_u64()
    )
}
