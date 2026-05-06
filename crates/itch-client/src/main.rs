//! Demo ITCH 5.0 subscriber binary.
//!
//! Connects to the demo server (default `127.0.0.1:9100`, override
//! via `--server ADDR` or `ITCH_SERVER`), decodes incoming messages,
//! and prints one human-readable line per message to stdout. Mirrors
//! the format shown in the workspace `README.md` quickstart.
//!
//! Transport is selectable at runtime via `--transport tcp|soup|mold`
//! per ADR-0008; runtime dispatch — no compile-time
//! `#[cfg(feature)]` switching across transports.
//!
//! `tracing-subscriber` is initialised in `main`; library code does
//! NOT install a global subscriber.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::env;
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use futures::StreamExt;
use itch_protocol::{primitives::Stock, EventCode, Message, Mpid};
use itch_soup::{
    LoginRejectReason, ResilientSoupClient, ResilientSoupConfig, SoupCredentials, SoupError,
};
use itch_tcp::{connect, TransportError};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

const DEFAULT_SERVER: &str = "127.0.0.1:9100";
const DEFAULT_USERNAME: &str = "user";
const DEFAULT_PASSWORD: &str = "pw";

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
    /// `MoldStream` lands (issue #21).
    Mold,
}

/// ITCH 5.0 subscriber CLI arguments.
#[derive(Parser)]
#[command(about = "ITCH 5.0 subscriber (decodes and prints messages)")]
struct Args {
    /// Server address. Defaults to `127.0.0.1:9100` (also honored
    /// via the `ITCH_SERVER` env var for backward compat).
    #[arg(long)]
    server: Option<String>,

    /// Transport. Default `tcp`. Selects the wire protocol used to
    /// connect to the server; runtime dispatch (no
    /// `#[cfg(feature)]`).
    #[arg(long, value_enum, default_value_t = Transport::Tcp)]
    transport: Transport,

    /// SoupBinTCP `Login Request` username (`--transport soup` only).
    #[arg(long, default_value = DEFAULT_USERNAME)]
    soup_username: String,

    /// SoupBinTCP `Login Request` password (`--transport soup` only).
    #[arg(long, default_value = DEFAULT_PASSWORD)]
    soup_password: String,
}

/// Format a single ITCH message as one human-readable line.
///
/// Exhaustive over `Message` variants — adding a new ITCH variant
/// fails to compile here, which is the desired behavior.
fn fmt_message(m: &Message) -> String {
    let h = m.header();
    let ts = h.timestamp.as_u64();
    let tag = char::from(m.tag());
    match m {
        Message::SystemEvent(s) => format!(
            "[{ts:>14} ns] {tag} system_event       code={:?}",
            s.event_code
        ),
        Message::StockDirectory(r) => format!(
            "[{ts:>14} ns] {tag} stock_directory    {:<8} cat={:?} status={:?} round_lot={}",
            stock_str(&r.stock),
            r.market_category,
            r.financial_status,
            r.round_lot_size.as_u32()
        ),
        Message::StockTradingAction(s) => format!(
            "[{ts:>14} ns] {tag} trading_action     {:<8} state={:?}",
            stock_str(&s.stock),
            s.trading_state
        ),
        Message::RegShoRestriction(s) => format!(
            "[{ts:>14} ns] {tag} reg_sho_restrict   {:<8} action={:?}",
            stock_str(&s.stock),
            s.reg_sho_action
        ),
        Message::MarketParticipantPosition(s) => format!(
            "[{ts:>14} ns] {tag} mpid_position      mpid={:<4} {:<8} mode={:?} state={:?}",
            mpid_str(&s.mpid),
            stock_str(&s.stock),
            s.market_maker_mode,
            s.market_participant_state
        ),
        Message::MwcbDeclineLevel(v) => format!(
            "[{ts:>14} ns] {tag} mwcb_decline_level L1={} L2={} L3={}",
            fmt_price8(v.level1.as_u64()),
            fmt_price8(v.level2.as_u64()),
            fmt_price8(v.level3.as_u64())
        ),
        Message::MwcbStatus(s) => {
            format!(
                "[{ts:>14} ns] {tag} mwcb_status        level={:?}",
                s.breached_level
            )
        }
        Message::IpoQuotingPeriodUpdate(s) => format!(
            "[{ts:>14} ns] {tag} ipo_quoting_update {:<8} qual={:?} price={}",
            stock_str(&s.stock),
            s.ipo_quotation_release_qualifier,
            fmt_price4(s.ipo_price.as_u32())
        ),
        Message::AddOrder(a) => format!(
            "[{ts:>14} ns] {tag} add_order          ref={} {:<4} {} {:<8} @ {}",
            a.order_ref.as_u64(),
            fmt_side(&a.side),
            a.shares.as_u32(),
            stock_str(&a.stock),
            fmt_price4(a.price.as_u32())
        ),
        Message::AddOrderWithMpid(a) => format!(
            "[{ts:>14} ns] {tag} add_order_mpid     ref={} {:<4} {} {:<8} @ {} mpid={}",
            a.order_ref.as_u64(),
            fmt_side(&a.side),
            a.shares.as_u32(),
            stock_str(&a.stock),
            fmt_price4(a.price.as_u32()),
            mpid_str(&a.attribution)
        ),
        Message::OrderExecuted(e) => format!(
            "[{ts:>14} ns] {tag} order_executed     ref={} {} match={}",
            e.order_ref.as_u64(),
            e.executed_shares.as_u32(),
            e.match_number.as_u64()
        ),
        Message::OrderExecutedWithPrice(e) => format!(
            "[{ts:>14} ns] {tag} order_exec_price   ref={} {} match={} @ {} {:?}",
            e.order_ref.as_u64(),
            e.executed_shares.as_u32(),
            e.match_number.as_u64(),
            fmt_price4(e.execution_price.as_u32()),
            e.printable
        ),
        Message::OrderCancel(x) => format!(
            "[{ts:>14} ns] {tag} order_cancel       ref={} cancelled={}",
            x.order_ref.as_u64(),
            x.cancelled_shares.as_u32()
        ),
        Message::OrderDelete(d) => format!(
            "[{ts:>14} ns] {tag} order_delete       ref={}",
            d.order_ref.as_u64()
        ),
        Message::OrderReplace(u) => format!(
            "[{ts:>14} ns] {tag} order_replace      old={} new={} {} @ {}",
            u.original_order_ref.as_u64(),
            u.new_order_ref.as_u64(),
            u.shares.as_u32(),
            fmt_price4(u.price.as_u32())
        ),
        Message::TradeNonCross(t) => format!(
            "[{ts:>14} ns] {tag} trade_non_cross    ref={} {:<4} {} {:<8} @ {} match={}",
            t.order_ref.as_u64(),
            fmt_side(&t.side),
            t.shares.as_u32(),
            stock_str(&t.stock),
            fmt_price4(t.price.as_u32()),
            t.match_number.as_u64()
        ),
        Message::CrossTrade(q) => format!(
            "[{ts:>14} ns] {tag} cross_trade        {:<8} {} @ {} match={} type={:?}",
            stock_str(&q.stock),
            q.shares,
            fmt_price4(q.cross_price.as_u32()),
            q.match_number.as_u64(),
            q.cross_type
        ),
        Message::BrokenTrade(b) => format!(
            "[{ts:>14} ns] {tag} broken_trade       match={}",
            b.match_number.as_u64()
        ),
        Message::Noii(n) => format!(
            "[{ts:>14} ns] {tag} noii               {:<8} paired={} imb={} dir={:?} type={:?}",
            stock_str(&n.stock),
            n.paired_shares,
            n.imbalance_shares,
            n.imbalance_direction,
            n.cross_type
        ),
        Message::RetailPriceImprovement(r) => format!(
            "[{ts:>14} ns] {tag} retail_price_imp   {:<8} flag={:?}",
            stock_str(&r.stock),
            r.interest_flag
        ),
    }
}

fn stock_str(s: &Stock) -> String {
    s.as_str().unwrap_or("<bad utf-8>").to_string()
}

fn mpid_str(m: &Mpid) -> String {
    m.as_str().unwrap_or("<bad utf-8>").to_string()
}

fn fmt_side(s: &itch_protocol::Side) -> &'static str {
    match s {
        itch_protocol::Side::Buy => "Buy",
        itch_protocol::Side::Sell => "Sell",
    }
}

fn fmt_price4(raw: u32) -> String {
    // Display as decimal with 4 fractional digits.
    let whole = raw / 10_000;
    let frac = raw % 10_000;
    format!("{whole}.{frac:04}")
}

fn fmt_price8(raw: u64) -> String {
    // Display as decimal with 8 fractional digits (`Price8` scale).
    let whole = raw / 100_000_000;
    let frac = raw % 100_000_000;
    format!("{whole}.{frac:08}")
}

/// Write one formatted line to stdout. Returns:
/// - `Ok(true)`  on a successful write.
/// - `Ok(false)` on `BrokenPipe` (downstream `head`, etc.) — caller
///   should exit cleanly with `SUCCESS`.
/// - `Err(_)`    on any other write failure — caller should exit
///   with a non-zero status.
fn write_line(line: &str) -> std::io::Result<bool> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    match writeln!(out, "{line}") {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(false),
        Err(err) => Err(err),
    }
}

/// Stream loop for `--transport tcp`.
async fn run_tcp(server: &str) -> ExitCode {
    let mut conn = match connect(server).await {
        Ok(c) => c,
        Err(err) => {
            error!(?err, addr = %server, "connect failed");
            return ExitCode::from(2);
        }
    };
    info!(addr = %server, transport = "tcp", "connected; streaming messages");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received; closing connection");
                return ExitCode::SUCCESS;
            }
            next = conn.next() => {
                match next {
                    Some(Ok(msg)) => {
                        let line = fmt_message(&msg);
                        match write_line(&line) {
                            Ok(true) => {}
                            Ok(false) => {
                                info!("stdout closed by peer; exiting");
                                return ExitCode::SUCCESS;
                            }
                            Err(err) => {
                                error!(?err, "stdout write failed");
                                return ExitCode::from(4);
                            }
                        }
                        if matches!(msg, Message::SystemEvent(s) if s.event_code == EventCode::EndOfMessages) {
                            info!("server signaled end of messages");
                            return ExitCode::SUCCESS;
                        }
                    }
                    Some(Err(TransportError::Protocol(err))) => {
                        warn!(?err, "bad inner ITCH frame; continuing");
                    }
                    Some(Err(TransportError::FrameTooLarge { got, max })) => {
                        warn!(got, max, "oversized frame dropped; continuing");
                    }
                    Some(Err(err)) => {
                        error!(?err, "fatal transport error");
                        return ExitCode::from(3);
                    }
                    None => {
                        warn!("server closed before EndOfMessages — premature disconnect");
                        return ExitCode::from(5);
                    }
                }
            }
        }
    }
}

/// Stream loop for `--transport soup`.
async fn run_soup(server: &str, username: &str, password: &str) -> ExitCode {
    let addr: std::net::SocketAddr = match server.parse() {
        Ok(a) => a,
        Err(err) => {
            error!(?err, addr = %server, "invalid SocketAddr for SoupBinTCP");
            return ExitCode::from(2);
        }
    };
    let cfg = ResilientSoupConfig::new(
        vec![addr],
        SoupCredentials::new(username.to_string(), password.to_string()),
    );
    let mut client = ResilientSoupClient::new(cfg);
    info!(addr = %server, transport = "soup", "starting resilient SoupBinTCP client");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received; closing connection");
                return ExitCode::SUCCESS;
            }
            next = client.next_message() => {
                match next {
                    Some(Ok(msg)) => {
                        let line = fmt_message(&msg);
                        match write_line(&line) {
                            Ok(true) => {}
                            Ok(false) => {
                                info!("stdout closed by peer; exiting");
                                return ExitCode::SUCCESS;
                            }
                            Err(err) => {
                                error!(?err, "stdout write failed");
                                return ExitCode::from(4);
                            }
                        }
                        if matches!(msg, Message::SystemEvent(s) if s.event_code == EventCode::EndOfMessages) {
                            info!("server signaled end of messages");
                            return ExitCode::SUCCESS;
                        }
                    }
                    Some(Err(SoupError::Protocol(err))) => {
                        warn!(?err, "bad inner ITCH frame; continuing");
                    }
                    Some(Err(SoupError::SessionEnded)) => {
                        info!("server signaled end of session");
                        return ExitCode::SUCCESS;
                    }
                    Some(Err(SoupError::LoginRejected(reason))) => {
                        let detail = match reason {
                            LoginRejectReason::NotAuthorized => "not authorized",
                            LoginRejectReason::SessionUnavailable => "session unavailable",
                            // `LoginRejectReason` is `#[non_exhaustive]` —
                            // forward-compat arm for future SoupBinTCP
                            // reject codes.
                            _ => "unknown reject code",
                        };
                        error!(?reason, detail, "login rejected — terminal");
                        return ExitCode::from(6);
                    }
                    Some(Err(err)) => {
                        warn!(?err, "transient soup error; client will reconnect");
                    }
                    None => {
                        info!("resilient client terminated");
                        return ExitCode::SUCCESS;
                    }
                }
            }
        }
    }
}

/// Stub for `--transport mold` until `MoldStream` lands (issue #21).
fn run_mold() -> ExitCode {
    ExitCode::from(mold_not_available_code())
}

/// Numeric exit code returned by [`run_mold`]. Lifted to its own
/// helper so the unit test can compare against a concrete `u8`
/// (`ExitCode` does not implement `PartialEq`).
#[cold]
fn mold_not_available_code() -> u8 {
    error!(
        "MoldUDP64 receiver is not yet available — tracking issue #21 \
         (use `--transport tcp` or `--transport soup` in the meantime)"
    );
    1
}

#[tokio::main]
async fn main() -> ExitCode {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let server = args
        .server
        .clone()
        .or_else(|| env::var("ITCH_SERVER").ok())
        .unwrap_or_else(|| DEFAULT_SERVER.to_string());

    match args.transport {
        Transport::Tcp => run_tcp(&server).await,
        Transport::Soup => run_soup(&server, &args.soup_username, &args.soup_password).await,
        Transport::Mold => run_mold(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use itch_protocol::{
        AddOrder, Header, OrderReference, Price4, Shares, Side, Stock, StockLocate, Timestamp,
        TrackingNumber,
    };

    #[test]
    fn fmt_add_order_matches_readme_shape() {
        let m = Message::AddOrder(AddOrder {
            header: Header {
                stock_locate: StockLocate::from_u16(1),
                tracking_number: TrackingNumber::from_u16(0),
                timestamp: Timestamp::from_u64(32_400_005_000_000),
            },
            order_ref: OrderReference::from_u64(1001),
            side: Side::Buy,
            shares: Shares::from_u32(500),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_000),
        });
        let line = fmt_message(&m);
        assert!(line.contains("add_order"));
        assert!(line.contains("ref=1001"));
        assert!(line.contains("Buy"));
        assert!(line.contains("500"));
        assert!(line.contains("AAPL"));
        assert!(line.contains("@ 192.5000"));
    }

    #[test]
    fn fmt_price4_renders_four_decimal_digits() {
        assert_eq!(fmt_price4(1_925_000), "192.5000");
        assert_eq!(fmt_price4(0), "0.0000");
        assert_eq!(fmt_price4(1), "0.0001");
        assert_eq!(fmt_price4(10_000), "1.0000");
    }

    #[test]
    fn fmt_side_strings() {
        assert_eq!(fmt_side(&Side::Buy), "Buy");
        assert_eq!(fmt_side(&Side::Sell), "Sell");
    }

    #[test]
    fn default_transport_is_tcp() {
        let args = Args::try_parse_from(["itch-client"]).expect("parse");
        assert_eq!(args.transport, Transport::Tcp);
    }

    #[test]
    fn transport_flag_parses_each_variant() {
        for (flag, expected) in [
            ("tcp", Transport::Tcp),
            ("soup", Transport::Soup),
            ("mold", Transport::Mold),
        ] {
            let args = Args::try_parse_from(["itch-client", "--transport", flag]).expect("parse");
            assert_eq!(args.transport, expected, "flag {flag} did not parse");
        }
    }

    #[test]
    fn transport_flag_rejects_unknown_value() {
        let err = Args::try_parse_from(["itch-client", "--transport", "rdma"]);
        assert!(err.is_err(), "expected parse error for unknown transport");
    }

    #[test]
    fn mold_transport_returns_failure_exit_code() {
        // The stub branch returns exit code 1 ("mold not yet
        // available"). Tested via the lifted `u8` helper because
        // `ExitCode` itself does not implement `PartialEq`.
        assert_eq!(mold_not_available_code(), 1);
    }
}
