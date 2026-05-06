//! Demo ITCH 5.0 publisher binary.
//!
//! Thin glue layer per ADR-0012 (`docs/ITCH-SOURCE.md` §6).
//! Wires a pluggable `MessageSource`, a durable `SeqStore` for
//! retransmission, a `SubscriptionPolicy` for warmup, and a
//! runtime-selectable transport (tcp / soup / mold) into a single
//! publisher.
//!
//! On startup, binds a listener (default `127.0.0.1:9100`,
//! override via `--bind ADDR` or `ITCH_BIND` env var) and serves
//! clients with a configurable message source.
//!
//! # Examples
//!
//! Default behavior (synthetic session, TCP transport):
//! ```bash
//! cargo run -p itch-server
//! ```
//!
//! With a custom bind address and the SoupBinTCP transport:
//! ```bash
//! cargo run -p itch-server -- --bind 0.0.0.0:9100 --transport soup
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
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use itch_source::{
    canonical_session, IteratorSource, MessageSource, RingBufferSeqStore, StaticPolicy,
};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

const DEFAULT_BIND: &str = "127.0.0.1:9100";
const DEFAULT_CACHE_SIZE: usize = 65_536;

/// Transport kind selected on the CLI.
///
/// Dispatch is at runtime per ADR-0005 / ADR-0008 — no compile-time
/// `#[cfg(feature = "...")]` switching across transports.
#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum Transport {
    /// Length-prefixed framing over TCP (the default).
    Tcp,
    /// SoupBinTCP 3.00 (production unicast, `itch-soup`).
    Soup,
    /// MoldUDP64 V1.00 (production multicast, `itch-mold`).
    /// Currently a stub — returns an error until the
    /// `MoldPublisher` lands (issue #25).
    Mold,
}

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
#[command(about = "ITCH 5.0 publisher (thin glue over itch-source + a switchable transport)")]
struct Args {
    /// Bind address. Defaults to `127.0.0.1:9100` (also honored
    /// via the `ITCH_BIND` env var for backward compat).
    #[arg(long)]
    bind: Option<String>,

    /// Transport. Default `tcp`. Selects the wire protocol used to
    /// serve clients; runtime dispatch (no `#[cfg(feature)]`).
    #[arg(long, value_enum, default_value_t = Transport::Tcp)]
    transport: Transport,

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
async fn main() -> ExitCode {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!(?err, "server exited with error");
            ExitCode::from(1)
        }
    }
}

async fn run(args: Args) -> std::io::Result<()> {
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
        transport = ?args.transport,
        source = ?args.source,
        cache_size = args.cache_size,
        "binding server"
    );

    // Runtime transport dispatch — never cross-transport edges,
    // each branch consumes exactly one transport crate (ADR-0008).
    match args.transport {
        Transport::Tcp => {
            itch_tcp::Server::bind(addr, source, store, policy)
                .await?
                .serve()
                .await
        }
        Transport::Soup => {
            itch_soup::SoupServer::bind(addr, source, store, policy)
                .await?
                .serve()
                .await
        }
        Transport::Mold => Err(mold_publisher_not_available()),
    }
}

/// Stub error returned by the `--transport mold` branch until
/// [issue #25 — `MoldPublisher`](https://github.com/joaquinbejar/itch-rs/issues/25)
/// lands. Tracking issues #21–#26.
#[cold]
fn mold_publisher_not_available() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "MoldUDP64 publisher is not yet available — tracking issues #25 / #26 \
         (use `--transport tcp` or `--transport soup` in the meantime)",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn default_transport_is_tcp() {
        let args = Args::try_parse_from(["itch-server"]).expect("parse");
        assert_eq!(args.transport, Transport::Tcp);
    }

    #[test]
    fn transport_flag_parses_each_variant() {
        for (flag, expected) in [
            ("tcp", Transport::Tcp),
            ("soup", Transport::Soup),
            ("mold", Transport::Mold),
        ] {
            let args = Args::try_parse_from(["itch-server", "--transport", flag]).expect("parse");
            assert_eq!(args.transport, expected, "flag {flag} did not parse");
        }
    }

    #[test]
    fn transport_flag_rejects_unknown_value() {
        let err = Args::try_parse_from(["itch-server", "--transport", "rdma"]);
        assert!(err.is_err(), "expected parse error for unknown transport");
    }

    #[tokio::test]
    async fn mold_transport_returns_unsupported_error() {
        let args = Args {
            bind: Some("127.0.0.1:0".to_string()),
            transport: Transport::Mold,
            source: SourceKind::Iterator,
            source_path: None,
            cache_size: 1024,
        };
        let err = run(args).await.expect_err("mold should error");
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
        let msg = err.to_string();
        assert!(
            msg.contains("MoldUDP64"),
            "error message should mention MoldUDP64: {msg}"
        );
    }
}
