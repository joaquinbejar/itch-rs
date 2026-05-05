//! Demo ITCH 5.0 subscriber binary.
//!
//! Connects to the demo server (default `127.0.0.1:9100`, override
//! via `ITCH_SERVER`), decodes incoming messages, and prints one
//! human-readable line per message to stdout. Mirrors the format
//! shown in the workspace `README.md` quickstart.
//!
//! `tracing-subscriber` is initialised in `main`; library code does
//! NOT install a global subscriber.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::env;
use std::process::ExitCode;

use futures::StreamExt;
use itch_protocol::{primitives::Stock, EventCode, Message, Mpid};
use itch_tcp::{connect, TransportError};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

const DEFAULT_SERVER: &str = "127.0.0.1:9100";

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

#[tokio::main]
async fn main() -> ExitCode {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let server = env::var("ITCH_SERVER").unwrap_or_else(|_| DEFAULT_SERVER.to_string());

    let mut conn = match connect(&server).await {
        Ok(c) => c,
        Err(err) => {
            error!(?err, addr = %server, "connect failed");
            return ExitCode::from(2);
        }
    };
    info!(addr = %server, "connected; streaming messages");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received; closing connection");
                return ExitCode::SUCCESS;
            }
            next = conn.next() => {
                match next {
                    Some(Ok(msg)) => {
                        // Human-readable line on stdout; tracing on
                        // stderr (configured above). Stop on a
                        // broken pipe (downstream `head` etc.).
                        use std::io::Write;
                        let line = fmt_message(&msg);
                        let mut out = std::io::stdout().lock();
                        if let Err(err) = writeln!(out, "{line}") {
                            if err.kind() == std::io::ErrorKind::BrokenPipe {
                                info!("stdout closed by peer; exiting");
                                return ExitCode::SUCCESS;
                            }
                            error!(?err, "stdout write failed");
                            return ExitCode::from(4);
                        }
                        if matches!(msg, Message::SystemEvent(s) if s.event_code == EventCode::EndOfMessages) {
                            info!("server signaled end of messages");
                            return ExitCode::SUCCESS;
                        }
                    }
                    Some(Err(TransportError::Protocol(err))) => {
                        // Bad inner frame must NOT poison the stream;
                        // the codec already advanced past the bad bytes.
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
                        // Reaching `None` means the server closed
                        // before sending `EndOfMessages` (the
                        // EndOfMessages branch above already
                        // early-returns SUCCESS). Treat as a
                        // premature disconnect so callers can
                        // detect truncated runs via exit code 5.
                        warn!("server closed before EndOfMessages — premature disconnect");
                        return ExitCode::from(5);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
