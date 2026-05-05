//! Demo ITCH 5.0 publisher binary.
//!
//! On startup, this binary binds a TCP listener (default
//! `127.0.0.1:9100`, override via `ITCH_BIND`) and replays a
//! deterministic synthetic session to every accepted client. The
//! session covers all 20 ITCH 5.0 message kinds in order and is
//! intended both as a smoke test and as a fixture that any client
//! integration can verify against.
//!
//! Per-message inter-message delay is `1` ms by default; override
//! via `ITCH_DELAY_MS`.
//!
//! See the workspace `README.md` for the runtime walk-through.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::env;
use std::time::Duration;

use futures::SinkExt;
use itch_protocol::{
    AddOrder, AddOrderWithMpid, Authenticity, BreachedLevel, BrokenTrade, CrossTrade, CrossType,
    EventCode, FinancialStatus, Header, ImbalanceDirection, IpoQuotingPeriodUpdate,
    IpoReleaseQualifier, LuldTier, MarketCategory, MarketMakerMode, MarketParticipantPosition,
    MarketParticipantState, Message, Mpid, MwcbDeclineLevel, MwcbStatus, Noii, OrderCancel,
    OrderDelete, OrderExecuted, OrderExecutedWithPrice, OrderReference, OrderReplace, Price4,
    Price8, Printable, RegShoAction, RegShoRestriction, RetailPriceImprovement, RpiInterestFlag,
    Shares, Side, Stock, StockDirectory, StockLocate, StockTradingAction, SystemEvent, Timestamp,
    TrackingNumber, TradeNonCross, TradingState, YesNo,
};
use itch_tcp::{accept, bind};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

const DEFAULT_BIND: &str = "127.0.0.1:9100";
const DEFAULT_DELAY_MS: u64 = 1;

/// Build the canonical synthetic session — every ITCH 5.0 message
/// kind in natural session order. Exhaustive over all 20 variants;
/// new ITCH revisions surface as compile errors via the
/// `assert_canonical_session_covers_all_kinds` test below.
fn canonical_session() -> Vec<Message> {
    let h = |ns: u64, locate: u16| Header {
        stock_locate: StockLocate::from_u16(locate),
        tracking_number: TrackingNumber::from_u16(0),
        timestamp: Timestamp::from_u64(ns),
    };
    let session = h(32_400_000_000_000, 0); // session-level (locate = 0)
    let aapl = h(32_400_005_000_000, 1);

    vec![
        // Session lifecycle
        Message::SystemEvent(SystemEvent {
            header: session,
            event_code: EventCode::StartOfMessages,
        }),
        Message::SystemEvent(SystemEvent {
            header: h(32_400_001_000_000, 0),
            event_code: EventCode::StartOfSystemHours,
        }),
        // Reference data — emit `R` for every symbol that later
        // appears in trade / NOII / RPI messages so a client that
        // seeds state from the directory has a complete picture.
        Message::StockDirectory(StockDirectory {
            header: h(32_400_002_000_000, 1),
            stock: Stock::new("AAPL"),
            market_category: MarketCategory::NasdaqGlobalSelect,
            financial_status: FinancialStatus::Normal,
            round_lot_size: Shares::from_u32(100),
            round_lots_only: YesNo::No,
            issue_classification: b'C',
            issue_subtype: *b"  ",
            authenticity: Authenticity::Live,
            short_sale_threshold: YesNo::No,
            ipo_flag: YesNo::No,
            luld_reference_price_tier: LuldTier::Tier1,
            etp_flag: YesNo::No,
            etp_leverage_factor: 0,
            inverse_indicator: YesNo::No,
        }),
        Message::StockDirectory(StockDirectory {
            header: h(32_400_002_100_000, 2),
            stock: Stock::new("MSFT"),
            market_category: MarketCategory::NasdaqGlobalSelect,
            financial_status: FinancialStatus::Normal,
            round_lot_size: Shares::from_u32(100),
            round_lots_only: YesNo::No,
            issue_classification: b'C',
            issue_subtype: *b"  ",
            authenticity: Authenticity::Live,
            short_sale_threshold: YesNo::No,
            ipo_flag: YesNo::No,
            luld_reference_price_tier: LuldTier::Tier1,
            etp_flag: YesNo::No,
            etp_leverage_factor: 0,
            inverse_indicator: YesNo::No,
        }),
        Message::StockDirectory(StockDirectory {
            header: h(32_400_002_200_000, 3),
            stock: Stock::new("SPY"),
            market_category: MarketCategory::NyseArca,
            financial_status: FinancialStatus::Normal,
            round_lot_size: Shares::from_u32(100),
            round_lots_only: YesNo::No,
            issue_classification: b'O',
            issue_subtype: *b"  ",
            authenticity: Authenticity::Live,
            short_sale_threshold: YesNo::No,
            ipo_flag: YesNo::No,
            luld_reference_price_tier: LuldTier::Tier1,
            etp_flag: YesNo::Yes,
            etp_leverage_factor: 1_000,
            inverse_indicator: YesNo::No,
        }),
        Message::StockTradingAction(StockTradingAction {
            header: h(32_400_003_000_000, 1),
            stock: Stock::new("AAPL"),
            trading_state: TradingState::Trading,
            reserved: b' ',
            reason: *b"NORM",
        }),
        Message::RegShoRestriction(RegShoRestriction {
            header: h(32_400_003_500_000, 1),
            stock: Stock::new("AAPL"),
            reg_sho_action: RegShoAction::NoPriceTest,
        }),
        Message::MarketParticipantPosition(MarketParticipantPosition {
            header: h(32_400_003_700_000, 1),
            mpid: Mpid::new("NSDQ"),
            stock: Stock::new("AAPL"),
            primary_market_maker: YesNo::Yes,
            market_maker_mode: MarketMakerMode::Normal,
            market_participant_state: MarketParticipantState::Active,
        }),
        Message::MwcbDeclineLevel(MwcbDeclineLevel {
            header: h(32_400_003_800_000, 0),
            level1: Price8::from_u64(100_000_000_000),
            level2: Price8::from_u64(200_000_000_000),
            level3: Price8::from_u64(300_000_000_000),
        }),
        Message::MwcbStatus(MwcbStatus {
            header: h(32_400_003_900_000, 0),
            breached_level: BreachedLevel::Level1,
        }),
        Message::IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate {
            header: h(32_400_003_950_000, 0),
            stock: Stock::new("NEWCO"),
            ipo_quotation_release_time: 34_200,
            ipo_quotation_release_qualifier: IpoReleaseQualifier::Anticipated,
            ipo_price: Price4::from_u32(150_0000),
        }),
        Message::SystemEvent(SystemEvent {
            header: h(32_400_004_000_000, 0),
            event_code: EventCode::StartOfMarketHours,
        }),
        // Order lifecycle
        Message::AddOrder(AddOrder {
            header: aapl,
            order_ref: OrderReference::from_u64(1001),
            side: Side::Buy,
            shares: Shares::from_u32(500),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_000),
        }),
        Message::AddOrderWithMpid(AddOrderWithMpid {
            header: h(32_400_006_000_000, 1),
            order_ref: OrderReference::from_u64(1002),
            side: Side::Sell,
            shares: Shares::from_u32(300),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_926_000),
            attribution: Mpid::new("NSDQ"),
        }),
        Message::OrderExecuted(OrderExecuted {
            header: h(32_400_007_000_000, 1),
            order_ref: OrderReference::from_u64(1001),
            executed_shares: Shares::from_u32(100),
            match_number: MatchNumber::from_u64(42),
        }),
        Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
            header: h(32_400_008_000_000, 1),
            order_ref: OrderReference::from_u64(1001),
            executed_shares: Shares::from_u32(50),
            match_number: MatchNumber::from_u64(43),
            printable: Printable::Printable,
            execution_price: Price4::from_u32(1_924_500),
        }),
        Message::OrderCancel(OrderCancel {
            header: h(32_400_009_000_000, 1),
            order_ref: OrderReference::from_u64(1001),
            cancelled_shares: Shares::from_u32(50),
        }),
        Message::OrderReplace(OrderReplace {
            header: h(32_400_009_500_000, 1),
            original_order_ref: OrderReference::from_u64(1002),
            new_order_ref: OrderReference::from_u64(2002),
            shares: Shares::from_u32(250),
            price: Price4::from_u32(1_927_500),
        }),
        Message::OrderDelete(OrderDelete {
            header: h(32_400_009_700_000, 1),
            order_ref: OrderReference::from_u64(2002),
        }),
        // Trades + auctions + administrative
        Message::TradeNonCross(TradeNonCross {
            header: h(32_400_010_000_000, 2),
            order_ref: OrderReference::from_u64(0),
            side: Side::Buy,
            shares: Shares::from_u32(100),
            stock: Stock::new("MSFT"),
            price: Price4::from_u32(3_500_000),
            match_number: MatchNumber::from_u64(7777),
        }),
        Message::CrossTrade(CrossTrade {
            // SPY's locate is 3 (set in its StockDirectory above);
            // the prior MSFT trade used locate 2.
            header: h(32_400_010_500_000, 3),
            shares: 1_000_000,
            stock: Stock::new("SPY"),
            cross_price: Price4::from_u32(4_000_000),
            match_number: MatchNumber::from_u64(99),
            cross_type: CrossType::Closing,
        }),
        Message::BrokenTrade(BrokenTrade {
            header: h(32_400_011_000_000, 2),
            match_number: MatchNumber::from_u64(424_242),
        }),
        Message::Noii(Noii {
            header: h(32_400_012_000_000, 1),
            paired_shares: 1_000_000,
            imbalance_shares: 500_000,
            imbalance_direction: ImbalanceDirection::Buy,
            stock: Stock::new("AAPL"),
            far_price: Price4::from_u32(1_926_000),
            near_price: Price4::from_u32(1_925_500),
            current_reference_price: Price4::from_u32(1_925_500),
            cross_type: CrossType::Closing,
            price_variation: PriceVariation::Pct1To2,
        }),
        Message::RetailPriceImprovement(RetailPriceImprovement {
            header: h(32_400_013_000_000, 1),
            stock: Stock::new("AAPL"),
            interest_flag: RpiInterestFlag::BothSides,
        }),
        // Session shutdown
        Message::SystemEvent(SystemEvent {
            header: h(32_400_014_000_000, 0),
            event_code: EventCode::EndOfMarketHours,
        }),
        Message::SystemEvent(SystemEvent {
            header: h(32_400_015_000_000, 0),
            event_code: EventCode::EndOfSystemHours,
        }),
        Message::SystemEvent(SystemEvent {
            header: h(32_400_016_000_000, 0),
            event_code: EventCode::EndOfMessages,
        }),
    ]
}

use itch_protocol::{MatchNumber, PriceVariation};

async fn replay_to_client(
    mut conn: itch_tcp::ItchConnection,
    peer: std::net::SocketAddr,
    session: std::sync::Arc<Vec<Message>>,
    delay: Duration,
) {
    info!(?peer, msgs = session.len(), "publishing synthetic session");
    let last = session.len().saturating_sub(1);
    for (i, msg) in session.iter().enumerate() {
        if let Err(err) = conn.send(*msg).await {
            warn!(
                ?peer,
                ?err,
                sent = i,
                "client send failed; dropping connection"
            );
            return;
        }
        debug!(?peer, tag = %char::from(msg.tag()), "sent");
        // Only sleep between messages, not after the last one — keeps
        // the total session length deterministic at
        // `(N-1) * delay`.
        if !delay.is_zero() && i != last {
            tokio::time::sleep(delay).await;
        }
    }
    info!(?peer, "session complete; closing connection");
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let bind_addr = env::var("ITCH_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string());
    let delay_ms: u64 = env::var("ITCH_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_DELAY_MS);
    let delay = Duration::from_millis(delay_ms);

    let listener = bind(&bind_addr).await?;
    info!(addr = %bind_addr, ?delay, "itch-server ready");

    let session = std::sync::Arc::new(canonical_session());
    info!(messages = session.len(), "synthetic session prepared");

    // Per-connection task per accept; multiple clients run in
    // parallel — a slow consumer no longer blocks new accepts.
    // Ctrl-C cancels accept and detaches the spawned tasks; the
    // production soup / mold servers in v0.3 grow a graceful-EOS
    // shutdown path on top of this skeleton.
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received; shutting down");
                return Ok(());
            }
            res = accept(&listener) => {
                match res {
                    Ok((conn, peer)) => {
                        let session = session.clone();
                        tokio::spawn(replay_to_client(conn, peer, session, delay));
                    }
                    Err(err) => {
                        error!(?err, "accept failed");
                        return Err(err);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Map a `Message` to its variant name. Exhaustive match —
    /// adding a new ITCH variant fails to compile here, which forces
    /// `canonical_session_covers_every_message_variant` to be
    /// updated in lock-step.
    fn variant_of(m: &Message) -> &'static str {
        match m {
            Message::SystemEvent(_) => "SystemEvent",
            Message::StockDirectory(_) => "StockDirectory",
            Message::StockTradingAction(_) => "StockTradingAction",
            Message::RegShoRestriction(_) => "RegShoRestriction",
            Message::MarketParticipantPosition(_) => "MarketParticipantPosition",
            Message::MwcbDeclineLevel(_) => "MwcbDeclineLevel",
            Message::MwcbStatus(_) => "MwcbStatus",
            Message::IpoQuotingPeriodUpdate(_) => "IpoQuotingPeriodUpdate",
            Message::AddOrder(_) => "AddOrder",
            Message::AddOrderWithMpid(_) => "AddOrderWithMpid",
            Message::OrderExecuted(_) => "OrderExecuted",
            Message::OrderExecutedWithPrice(_) => "OrderExecutedWithPrice",
            Message::OrderCancel(_) => "OrderCancel",
            Message::OrderDelete(_) => "OrderDelete",
            Message::OrderReplace(_) => "OrderReplace",
            Message::TradeNonCross(_) => "TradeNonCross",
            Message::CrossTrade(_) => "CrossTrade",
            Message::BrokenTrade(_) => "BrokenTrade",
            Message::Noii(_) => "Noii",
            Message::RetailPriceImprovement(_) => "RetailPriceImprovement",
        }
    }

    #[test]
    fn canonical_session_covers_every_message_variant() {
        let session = canonical_session();
        let expected = [
            "SystemEvent",
            "StockDirectory",
            "StockTradingAction",
            "RegShoRestriction",
            "MarketParticipantPosition",
            "MwcbDeclineLevel",
            "MwcbStatus",
            "IpoQuotingPeriodUpdate",
            "AddOrder",
            "AddOrderWithMpid",
            "OrderExecuted",
            "OrderExecutedWithPrice",
            "OrderCancel",
            "OrderDelete",
            "OrderReplace",
            "TradeNonCross",
            "CrossTrade",
            "BrokenTrade",
            "Noii",
            "RetailPriceImprovement",
        ];
        for name in expected {
            assert!(
                session.iter().any(|m| variant_of(m) == name),
                "session missing variant {name}"
            );
        }
        for m in &session {
            let v = variant_of(m);
            assert!(expected.contains(&v), "unexpected variant in session: {v}");
        }
    }

    #[test]
    fn canonical_session_starts_with_start_of_messages() {
        let session = canonical_session();
        match session.first() {
            Some(Message::SystemEvent(s)) => {
                assert_eq!(s.event_code, EventCode::StartOfMessages);
            }
            other => panic!("expected SystemEvent first, got {other:?}"),
        }
    }

    #[test]
    fn canonical_session_ends_with_end_of_messages() {
        let session = canonical_session();
        match session.last() {
            Some(Message::SystemEvent(s)) => {
                assert_eq!(s.event_code, EventCode::EndOfMessages);
            }
            other => panic!("expected SystemEvent last, got {other:?}"),
        }
    }
}
