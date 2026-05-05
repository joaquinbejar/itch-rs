//! Demo ITCH 5.0 publisher binary.
//!
//! Thin glue layer per ADR-0012 (`docs/ITCH-SOURCE.md` §6).
//! Wires a pluggable `MessageSource`, a durable `SeqStore` for
//! retransmission, a `SubscriptionPolicy` for warmup, and a transport
//! (tcp by default) into a single publisher.
//!
//! On startup, binds a TCP listener (default `127.0.0.1:9100`,
//! override via `--bind ADDR` or `ITCH_BIND` env var) and serves
//! clients with a configurable message source.
//!
//! # Examples
//!
//! Default behavior (synthetic session):
//! ```bash
//! cargo run -p itch-server
//! ```
//!
//! With custom bind address:
//! ```bash
//! cargo run -p itch-server -- --bind 0.0.0.0:9100
//! ```
//!
//! Legacy env vars still work:
//! ```bash
//! ITCH_BIND=192.168.1.100:9100 cargo run -p itch-server
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use clap::Parser;
use itch_source::{canonical_session, IteratorSource, MessageSource, NullSeqStore, StaticPolicy};
use std::env;
use std::net::SocketAddr;
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

const DEFAULT_BIND: &str = "127.0.0.1:9100";

/// ITCH 5.0 publisher CLI arguments.
#[derive(Parser)]
#[command(about = "ITCH 5.0 publisher")]
struct Args {
    /// Bind address (default: 127.0.0.1:9100, override via ITCH_BIND env var)
    #[arg(long)]
    bind: Option<String>,

    /// Message source: 'iterator' (canonical session, default)
    #[arg(long, default_value = "iterator")]
    source: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let bind_addr = args
        .bind
        .or_else(|| env::var("ITCH_BIND").ok())
        .unwrap_or_else(|| DEFAULT_BIND.to_string());

    let addr: SocketAddr = bind_addr.parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid address: {e}"),
        )
    })?;

    // Construct source (iterator source wrapping canonical session)
    let source: Box<dyn MessageSource> = match args.source.as_str() {
        "iterator" => Box::new(IteratorSource::from(canonical_session())),
        other => {
            error!(source = %other, "unknown source type");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unknown source: {other}"),
            ));
        }
    };

    // Construct store (NullSeqStore for v0.2)
    let store = NullSeqStore::new();

    // Construct policy (empty warmup for now)
    let policy = StaticPolicy::empty();

    info!(addr = %bind_addr, source = %args.source, "binding server");

    // Thin glue: Server::bind().serve() does the heavy lifting
    itch_tcp::Server::bind(addr, source, store, policy)
        .await?
        .serve()
        .await
}
