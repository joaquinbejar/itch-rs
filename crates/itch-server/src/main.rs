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

use std::env;
use std::net::SocketAddr;

use clap::{Parser, ValueEnum};
use itch_source::{
    canonical_session, IteratorSource, MessageSource, RingBufferSeqStore, StaticPolicy,
};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

const DEFAULT_BIND: &str = "127.0.0.1:9100";
const DEFAULT_CACHE_SIZE: usize = 65_536;

/// Source kind selected on the CLI.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum SourceKind {
    /// Wrap the canonical synthetic session via `IteratorSource`.
    Iterator,
    /// `.itch` Glimpse-format file replay (lands with `itch-replay`
    /// in v0.5).
    ReplayGlimpse,
    /// Raw `.itch` capture file replay (lands with `itch-replay`
    /// in v0.5).
    ReplayRaw,
}

/// ITCH 5.0 publisher CLI arguments.
#[derive(Parser)]
#[command(about = "ITCH 5.0 publisher (thin glue over itch-source + itch-tcp)")]
struct Args {
    /// Bind address. Defaults to `127.0.0.1:9100` (also honored
    /// via the `ITCH_BIND` env var for backward compat).
    #[arg(long)]
    bind: Option<String>,

    /// Message source kind. Default `iterator` (canonical session).
    #[arg(long, value_enum, default_value_t = SourceKind::Iterator)]
    source: SourceKind,

    /// Path to the source file (required for `replay-glimpse` /
    /// `replay-raw`).
    #[arg(long)]
    source_path: Option<String>,

    /// `RingBufferSeqStore` capacity (frames retained for
    /// retransmission). Default 65 536.
    #[arg(long, default_value_t = DEFAULT_CACHE_SIZE)]
    cache_size: usize,
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
        .clone()
        .or_else(|| env::var("ITCH_BIND").ok())
        .unwrap_or_else(|| DEFAULT_BIND.to_string());

    let addr: SocketAddr = bind_addr.parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid address: {e}"),
        )
    })?;

    // Construct source based on CLI flag.
    let source: Box<dyn MessageSource> = match args.source {
        SourceKind::Iterator => Box::new(IteratorSource::from(canonical_session())),
        SourceKind::ReplayGlimpse | SourceKind::ReplayRaw => {
            error!(
                source = ?args.source,
                "replay-glimpse / replay-raw require itch-replay (v0.5)"
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "replay sources are not available in v0.2 — see itch-replay (v0.5)",
            ));
        }
    };

    // SeqStore — bounded ring keyed by sequence so a re-attaching
    // subscriber could in principle ask for a retransmit window.
    let store = RingBufferSeqStore::with_capacity(args.cache_size);

    // Demo policy — no warmup.
    let policy = StaticPolicy::empty();

    info!(
        addr = %bind_addr,
        source = ?args.source,
        cache_size = args.cache_size,
        "binding server"
    );

    // Thin glue: Server::bind().serve() drives ingest + accept.
    itch_tcp::Server::bind(addr, source, store, policy)
        .await?
        .serve()
        .await
}
