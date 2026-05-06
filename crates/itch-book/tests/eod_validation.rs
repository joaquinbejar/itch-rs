//! End-of-day OHLCV validation harness for the v0.4 acceptance bound
//! "Replaying a sample day's ITCH against `itch-book` reproduces
//! NASDAQ's end-of-day prints within 0 cents difference"
//! (`docs/ROADMAP.md` v0.5).
//!
//! This is the **synthetic-day** path: the day's message stream is
//! generated in code, the expected OHLCV summary is hand-derived in
//! the same file, and the test asserts exact equality at the cent
//! level (`Price4` byte-equal, no tolerance).
//!
//! The complementary **public-capture** path (Option A — replay a
//! NASDAQ-licensed `.itch` file under `vendor/captures/<date>.itch`)
//! lives behind the `vendor-captures` Cargo feature. See
//! [`capture_replay_placeholder`] for the placeholder anchoring the
//! integration point.

use itch_book::{BookManager, OhlcvAccumulator, OhlcvBar};
use itch_protocol::{
    AddOrder, AddOrderWithMpid, Authenticity, CrossTrade, CrossType, EventCode, FinancialStatus,
    Header, LuldTier, MarketCategory, MatchNumber, Message, Mpid, OrderDelete, OrderExecuted,
    OrderExecutedWithPrice, OrderReference, Price4, Printable, Shares, Side, Stock, StockDirectory,
    StockLocate, SystemEvent, Timestamp, TrackingNumber, TradeNonCross, YesNo,
};

const AAPL: u16 = 1;
const MSFT: u16 = 2;
const GOOG: u16 = 3;

fn header_for(locate: u16, ts_offset: u64) -> Header {
    Header {
        stock_locate: StockLocate::from_u16(locate),
        tracking_number: TrackingNumber::from_u16(0),
        timestamp: Timestamp::from_u64(32_400_000_000_000 + ts_offset),
    }
}

fn system_event(code: EventCode) -> Message {
    Message::SystemEvent(SystemEvent {
        header: Header {
            stock_locate: StockLocate::from_u16(0),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(32_400_000_000_000),
        },
        event_code: code,
    })
}

fn directory(locate: u16, symbol: &str) -> Message {
    Message::StockDirectory(StockDirectory {
        header: header_for(locate, 0),
        stock: Stock::new(symbol),
        market_category: MarketCategory::NasdaqGlobalSelect,
        financial_status: FinancialStatus::Normal,
        round_lot_size: Shares::from_u32(100),
        round_lots_only: YesNo::No,
        issue_classification: b'C',
        issue_subtype: [b' '; 2],
        authenticity: Authenticity::Live,
        short_sale_threshold: YesNo::No,
        ipo_flag: YesNo::No,
        luld_reference_price_tier: LuldTier::Tier1,
        etp_flag: YesNo::No,
        etp_leverage_factor: 0,
        inverse_indicator: YesNo::No,
    })
}

fn add(locate: u16, ts: u64, order_ref: u64, side: Side, shares: u32, price: u32) -> Message {
    Message::AddOrder(AddOrder {
        header: header_for(locate, ts),
        order_ref: OrderReference::from_u64(order_ref),
        side,
        shares: Shares::from_u32(shares),
        stock: Stock::default(),
        price: Price4::from_u32(price),
    })
}

fn add_mpid(
    locate: u16,
    ts: u64,
    order_ref: u64,
    side: Side,
    shares: u32,
    price: u32,
    mpid: &str,
) -> Message {
    Message::AddOrderWithMpid(AddOrderWithMpid {
        header: header_for(locate, ts),
        order_ref: OrderReference::from_u64(order_ref),
        side,
        shares: Shares::from_u32(shares),
        stock: Stock::default(),
        price: Price4::from_u32(price),
        attribution: Mpid::new(mpid),
    })
}

fn execute(locate: u16, ts: u64, order_ref: u64, shares: u32, match_number: u64) -> Message {
    Message::OrderExecuted(OrderExecuted {
        header: header_for(locate, ts),
        order_ref: OrderReference::from_u64(order_ref),
        executed_shares: Shares::from_u32(shares),
        match_number: MatchNumber::from_u64(match_number),
    })
}

fn execute_with_price(
    locate: u16,
    ts: u64,
    order_ref: u64,
    shares: u32,
    match_number: u64,
    printable: Printable,
    price: u32,
) -> Message {
    Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
        header: header_for(locate, ts),
        order_ref: OrderReference::from_u64(order_ref),
        executed_shares: Shares::from_u32(shares),
        match_number: MatchNumber::from_u64(match_number),
        printable,
        execution_price: Price4::from_u32(price),
    })
}

fn delete(locate: u16, ts: u64, order_ref: u64) -> Message {
    Message::OrderDelete(OrderDelete {
        header: header_for(locate, ts),
        order_ref: OrderReference::from_u64(order_ref),
    })
}

fn p_trade(locate: u16, ts: u64, shares: u32, price: u32, match_number: u64) -> Message {
    Message::TradeNonCross(TradeNonCross {
        header: header_for(locate, ts),
        // Post-2010-12-06 quirk: order_ref is always 0 on the wire.
        order_ref: OrderReference::from_u64(0),
        // Post-2014-07-14 quirk: side is always Buy on the wire.
        side: Side::Buy,
        shares: Shares::from_u32(shares),
        stock: Stock::default(),
        price: Price4::from_u32(price),
        match_number: MatchNumber::from_u64(match_number),
    })
}

fn cross(
    locate: u16,
    ts: u64,
    shares: u64,
    price: u32,
    match_number: u64,
    kind: CrossType,
) -> Message {
    Message::CrossTrade(CrossTrade {
        header: header_for(locate, ts),
        shares,
        stock: Stock::default(),
        cross_price: Price4::from_u32(price),
        match_number: MatchNumber::from_u64(match_number),
        cross_type: kind,
    })
}

/// Build the synthetic trading day used by the EOD validation test.
///
/// Three symbols (AAPL=1, MSFT=2, GOOG=3) trade through a small but
/// representative sequence: session start, R Stock Directory for each
/// symbol, then per-symbol order-book activity (`A` / `F` / `E` / `X`
/// / `D`) interleaved with public trade prints (`P` / `Q` / `C`),
/// session end. The expected per-symbol OHLCV is hand-derived inside
/// the test below.
fn synthetic_day() -> Vec<Message> {
    let mut s = Vec::with_capacity(64);

    // ----- session start -----
    s.push(system_event(EventCode::StartOfMessages));
    s.push(directory(AAPL, "AAPL"));
    s.push(directory(MSFT, "MSFT"));
    s.push(directory(GOOG, "GOOG"));

    // ===== AAPL =====
    // Resting orders so the book has state during execution.
    s.push(add(AAPL, 100, 1001, Side::Buy, 1_000, 1_900_000));
    s.push(add(AAPL, 101, 1002, Side::Sell, 800, 1_950_000));
    s.push(add_mpid(AAPL, 102, 1003, Side::Buy, 500, 1_880_000, "NSDQ"));
    // E executions (no OHLCV effect — they pair with the P prints below).
    s.push(execute(AAPL, 200, 1001, 100, 5_001));
    s.push(execute(AAPL, 201, 1002, 200, 5_002));
    // P trade prints — these drive AAPL's OHLCV.
    //
    // open = 1_900_000  (first print)
    // high = 1_950_000
    // low  = 1_880_000
    s.push(p_trade(AAPL, 300, 100, 1_900_000, 5_001));
    s.push(p_trade(AAPL, 301, 200, 1_950_000, 5_002));
    s.push(p_trade(AAPL, 302, 150, 1_880_000, 5_003));
    s.push(p_trade(AAPL, 303, 250, 1_910_000, 5_004));
    s.push(p_trade(AAPL, 304, 300, 1_920_000, 5_005));
    // Q closing cross — counts as a trade. New close = 1_900_000.
    s.push(cross(AAPL, 400, 500, 1_900_000, 5_006, CrossType::Closing));
    // Cleanup: delete remaining open orders.
    s.push(delete(AAPL, 500, 1003));

    // ===== MSFT =====
    s.push(add(MSFT, 110, 2001, Side::Buy, 600, 3_500_000));
    s.push(add(MSFT, 111, 2002, Side::Sell, 400, 3_550_000));
    // E execution (paired with the first P trade, no OHLCV effect on its own).
    s.push(execute(MSFT, 210, 2001, 100, 6_001));
    // P trades drive MSFT OHLCV.
    s.push(p_trade(MSFT, 310, 100, 3_500_000, 6_001));
    s.push(p_trade(MSFT, 311, 200, 3_550_000, 6_002));
    // C printable — counts.
    s.push(execute_with_price(
        MSFT,
        312,
        2002,
        50,
        6_003,
        Printable::Printable,
        3_520_000,
    ));
    // C non-printable — does NOT count toward OHLCV (and is paired
    // with no OHLCV state change).
    s.push(execute_with_price(
        MSFT,
        313,
        2002,
        75,
        6_004,
        Printable::NonPrintable,
        3_500_000,
    ));
    // P trade closes lower than open → low advances.
    s.push(p_trade(MSFT, 314, 150, 3_480_000, 6_005));
    // Cleanup.
    s.push(delete(MSFT, 510, 2001));
    s.push(delete(MSFT, 511, 2002));

    // ===== GOOG =====
    s.push(add(GOOG, 120, 3001, Side::Buy, 200, 2_800_000));
    s.push(add(GOOG, 121, 3002, Side::Sell, 300, 2_850_000));
    // Single P trade + closing cross.
    s.push(p_trade(GOOG, 320, 100, 2_800_000, 7_001));
    s.push(cross(
        GOOG,
        420,
        1_000,
        2_810_000,
        7_002,
        CrossType::Closing,
    ));
    s.push(delete(GOOG, 520, 3001));
    s.push(delete(GOOG, 521, 3002));

    // ----- session end -----
    s.push(system_event(EventCode::EndOfMessages));

    s
}

/// Hand-derived expected per-symbol OHLCV for the synthetic day.
///
/// Verifying by inspection (each row corresponds to the price /
/// shares pairs above):
///
/// AAPL:
/// - prints: P 1900x100, P 1950x200, P 1880x150, P 1910x250, P 1920x300, Q 1900x500
/// - open=1_900_000, high=1_950_000, low=1_880_000, close=1_900_000
/// - volume=100+200+150+250+300+500=1500, trade_count=6
///
/// MSFT:
/// - prints: P 3500x100, P 3550x200, C(printable) 3520x50,
///   C(non-printable) 3500x75 IGNORED, P 3480x150
/// - open=3_500_000, high=3_550_000, low=3_480_000, close=3_480_000
/// - volume=100+200+50+150=500, trade_count=4
///
/// GOOG:
/// - prints: P 2800x100, Q 2810x1000
/// - open=2_800_000, high=2_810_000, low=2_800_000, close=2_810_000
/// - volume=100+1000=1100, trade_count=2
const EXPECTED_BARS: [OhlcvBar; 3] = [
    OhlcvBar {
        stock_locate: StockLocate::from_u16(AAPL),
        open: Price4::from_u32(1_900_000),
        high: Price4::from_u32(1_950_000),
        low: Price4::from_u32(1_880_000),
        close: Price4::from_u32(1_900_000),
        volume: 1_500,
        trade_count: 6,
    },
    OhlcvBar {
        stock_locate: StockLocate::from_u16(MSFT),
        open: Price4::from_u32(3_500_000),
        high: Price4::from_u32(3_550_000),
        low: Price4::from_u32(3_480_000),
        close: Price4::from_u32(3_480_000),
        volume: 500,
        trade_count: 4,
    },
    OhlcvBar {
        stock_locate: StockLocate::from_u16(GOOG),
        open: Price4::from_u32(2_800_000),
        high: Price4::from_u32(2_810_000),
        low: Price4::from_u32(2_800_000),
        close: Price4::from_u32(2_810_000),
        volume: 1_100,
        trade_count: 2,
    },
];

#[test]
fn synthetic_day_eod_ohlcv_matches_expected() {
    let mut mgr = BookManager::new();
    let mut accumulator = OhlcvAccumulator::new();
    let day = synthetic_day();

    for msg in &day {
        // Tests can use expect with a reason string; this is a fixture
        // and any failure would indicate a bug in the synthetic stream
        // builder above.
        mgr.apply(msg).expect("manager apply on synthetic day");
        accumulator.apply(msg);
    }

    // Manager observed every message, including the 4 session/dir
    // bracketing messages.
    assert_eq!(mgr.last_seq() as usize, day.len(), "last_seq mismatch");
    assert_eq!(accumulator.len(), 3, "expected 3 symbols with prints");

    for expected in &EXPECTED_BARS {
        let got = accumulator
            .bar(expected.stock_locate)
            .expect("symbol bar present");
        assert_eq!(
            got.open, expected.open,
            "open mismatch on {:?}",
            expected.stock_locate
        );
        assert_eq!(
            got.high, expected.high,
            "high mismatch on {:?}",
            expected.stock_locate
        );
        assert_eq!(
            got.low, expected.low,
            "low mismatch on {:?}",
            expected.stock_locate
        );
        assert_eq!(
            got.close, expected.close,
            "close mismatch on {:?}",
            expected.stock_locate
        );
        assert_eq!(
            got.volume, expected.volume,
            "volume mismatch on {:?}",
            expected.stock_locate
        );
        assert_eq!(
            got.trade_count, expected.trade_count,
            "trade_count mismatch on {:?}",
            expected.stock_locate
        );
    }
}

#[test]
fn synthetic_day_directory_cache_populated_for_every_symbol() {
    // Smoke test that the BookManager side of the harness still
    // routes Stock Directory messages correctly — guards against an
    // accidental regression in the manager that would invalidate the
    // OHLCV reconciliation indirectly.
    let mut mgr = BookManager::new();
    let mut accumulator = OhlcvAccumulator::new();
    for msg in synthetic_day() {
        mgr.apply(&msg).expect("manager apply");
        accumulator.apply(&msg);
    }
    assert_eq!(
        mgr.symbol(StockLocate::from_u16(AAPL)),
        Some(&Stock::new("AAPL"))
    );
    assert_eq!(
        mgr.symbol(StockLocate::from_u16(MSFT)),
        Some(&Stock::new("MSFT"))
    );
    assert_eq!(
        mgr.symbol(StockLocate::from_u16(GOOG)),
        Some(&Stock::new("GOOG"))
    );
}

/// Placeholder for the future public-capture-backed EOD validator.
///
/// When a NASDAQ-licensed capture file lands at
/// `vendor/captures/<date>.itch` and `itch-replay` is wired up to
/// produce a `Message` stream from it, this test will:
///
/// 1. Open the capture file via `itch_replay::ReplaySource` (or the
///    equivalent reader).
/// 2. Drive every decoded message through `BookManager` and
///    `OhlcvAccumulator`.
/// 3. Cross-check the resulting per-symbol OHLCV against the
///    NASDAQ-published end-of-day summary CSV for the same date.
/// 4. Assert exact (0-cent) equality on every reconciled symbol.
///
/// The test stays `#[ignore]` even under the `vendor-captures`
/// feature until the capture path is committed — the feature flag
/// alone is not sufficient to guarantee the file is present in any
/// given checkout. See `docs/TESTING.md` §10 for the full
/// integration plan.
#[cfg(feature = "vendor-captures")]
#[test]
#[ignore = "requires a NASDAQ-licensed capture under vendor/captures/; tracked separately"]
fn capture_replay_placeholder() {
    // Intentionally empty: the integration point is documented above
    // and will be filled in when the capture path lands.
}
