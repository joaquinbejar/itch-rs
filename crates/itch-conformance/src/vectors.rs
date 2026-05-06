//! Canonical wire-byte conformance vectors, one per ITCH 5.0
//! message kind plus the documented `TradeNonCross` quirks.
//!
//! Each [`ConformanceVector`] pairs a hand-crafted byte literal with
//! a constructor for its expected decoded [`Message`]. Both
//! directions are immutable contracts:
//!
//! - `Message::decode(bytes)` MUST equal `(message)()`.
//! - `(message)().encode(buf)` MUST yield `bytes` byte-for-byte.
//!
//! The `Message` field is a `fn() -> Message` rather than a `const`
//! because the inner DTOs reach through `Stock::new` / `Mpid::new`,
//! which aren't `const fn`. The constructor is pure and free of
//! allocation, so calling it inside a test loop is cheap.
//!
//! # Immutability
//!
//! These vectors are wire-format contracts shared with downstream
//! consumers per [ADR-0007](https://github.com/joaquinbejar/itch-rs/blob/main/docs/adr/0007-a-la-carte-crates.md).
//! Any change to a published vector — bytes or `message` field — is
//! a breaking wire-format regression and ships only as a major
//! release. New vectors may be added in minor releases. Never delete
//! or mutate an existing one.

use itch_protocol::{
    AddOrder, AddOrderWithMpid, Authenticity, BreachedLevel, BrokenTrade, CrossTrade, CrossType,
    EventCode, FinancialStatus, Header, ImbalanceDirection, IpoQuotingPeriodUpdate,
    IpoReleaseQualifier, LuldTier, MarketCategory, MarketMakerMode, MarketParticipantPosition,
    MarketParticipantState, MatchNumber, Message, Mpid, MwcbDeclineLevel, MwcbStatus, Noii,
    OrderCancel, OrderDelete, OrderExecuted, OrderExecutedWithPrice, OrderReference, OrderReplace,
    Price4, Price8, Printable, RegShoAction, RegShoRestriction, RetailPriceImprovement,
    RpiInterestFlag, Shares, Side, Stock, StockDirectory, StockLocate, StockTradingAction,
    SystemEvent, Timestamp, TrackingNumber, TradeNonCross, TradingState, YesNo,
};

/// One canonical conformance vector.
///
/// Pairs a static byte literal with a constructor for the
/// `Message` that should decode out of those bytes (and that should
/// encode back into them). The struct is intentionally tiny —
/// downstream tests iterate [`all`] and assert byte-for-byte and
/// field-for-field equality.
#[derive(Debug, Clone, Copy)]
pub struct ConformanceVector {
    /// Stable, human-readable identifier (e.g.
    /// `"add_order_aapl_buy_500_at_192_50"`). Use as a label in
    /// per-vector test failure messages.
    pub name: &'static str,
    /// Canonical wire bytes. The first byte is the ITCH type tag; the
    /// remainder is the body in the spec's offset table.
    pub bytes: &'static [u8],
    /// Pure constructor for the expected decoded `Message`. Allocation-
    /// free; calling this inside a hot loop is intentional.
    pub message: fn() -> Message,
}

// ---------------------------------------------------------------------------
// Helper headers shared across vectors, mirroring the workspace
// goldens in `crates/itch-protocol/tests/golden.rs` byte-for-byte.
// ---------------------------------------------------------------------------

#[inline]
fn stock_header() -> Header {
    Header {
        stock_locate: StockLocate::from_u16(1),
        tracking_number: TrackingNumber::from_u16(2),
        timestamp: Timestamp::from_u64(0x0000_1234_5678),
    }
}

#[inline]
fn session_header() -> Header {
    Header {
        stock_locate: StockLocate::from_u16(0),
        tracking_number: TrackingNumber::from_u16(2),
        timestamp: Timestamp::from_u64(0x0000_1234_5678),
    }
}

// ---------------------------------------------------------------------------
// Per-message byte literals + constructors. Each pair is a verbatim
// copy of the corresponding test in `crates/itch-protocol/tests/golden.rs`.
// ---------------------------------------------------------------------------

// 4.1 — System Event (S)
#[rustfmt::skip]
const SYSTEM_EVENT_BYTES: &[u8] = &[
    b'S',
    0x00, 0x00,                                              // stock_locate = 0 (session-level)
    0x00, 0x02,                                              // tracking_number = 2
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                      // timestamp (u48)
    b'O',                                                    // event_code = StartOfMessages
];

fn system_event_message() -> Message {
    Message::SystemEvent(SystemEvent {
        header: session_header(),
        event_code: EventCode::StartOfMessages,
    })
}

// 4.2.1 — Stock Directory (R)
#[rustfmt::skip]
const STOCK_DIRECTORY_BYTES: &[u8] = &[
    b'R',
    0x00, 0x01,                                              // stock_locate = 1
    0x00, 0x02,                                              // tracking_number = 2
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                      // timestamp
    b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',          // stock = "AAPL    "
    b'Q',                                                    // market_category
    b'N',                                                    // financial_status
    0x00, 0x00, 0x00, 0x64,                                  // round_lot_size = 100
    b'N',                                                    // round_lots_only
    b'C',                                                    // issue_classification
    b' ', b' ',                                              // issue_subtype
    b'P',                                                    // authenticity = Live
    b'N',                                                    // short_sale_threshold
    b'N',                                                    // ipo_flag
    b'1',                                                    // luld_reference_price_tier
    b'N',                                                    // etp_flag
    0x00, 0x00, 0x00, 0x00,                                  // etp_leverage_factor
    b'N',                                                    // inverse_indicator
];

fn stock_directory_message() -> Message {
    Message::StockDirectory(StockDirectory {
        header: stock_header(),
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
    })
}

// 4.2.2 — Stock Trading Action (H)
#[rustfmt::skip]
const STOCK_TRADING_ACTION_BYTES: &[u8] = &[
    b'H',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',
    b'T',                                                    // trading_state
    b' ',                                                    // reserved
    b'N', b'O', b'R', b'M',                                  // reason
];

fn stock_trading_action_message() -> Message {
    Message::StockTradingAction(StockTradingAction {
        header: stock_header(),
        stock: Stock::new("AAPL"),
        trading_state: TradingState::Trading,
        reserved: b' ',
        reason: *b"NORM",
    })
}

// 4.2.3 — Reg SHO Restriction (Y)
#[rustfmt::skip]
const REG_SHO_RESTRICTION_BYTES: &[u8] = &[
    b'Y',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    b'T', b'S', b'L', b'A', b' ', b' ', b' ', b' ',
    b'2',                                                    // reg_sho_action = InEffect
];

fn reg_sho_restriction_message() -> Message {
    Message::RegShoRestriction(RegShoRestriction {
        header: stock_header(),
        stock: Stock::new("TSLA"),
        reg_sho_action: RegShoAction::InEffect,
    })
}

// 4.2.4 — Market Participant Position (L)
#[rustfmt::skip]
const MARKET_PARTICIPANT_POSITION_BYTES: &[u8] = &[
    b'L',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    b'N', b'S', b'D', b'Q',                                  // mpid
    b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',          // stock
    b'Y',                                                    // primary_market_maker
    b'N',                                                    // market_maker_mode = Normal
    b'A',                                                    // market_participant_state = Active
];

fn market_participant_position_message() -> Message {
    Message::MarketParticipantPosition(MarketParticipantPosition {
        header: stock_header(),
        mpid: Mpid::new("NSDQ"),
        stock: Stock::new("AAPL"),
        primary_market_maker: YesNo::Yes,
        market_maker_mode: MarketMakerMode::Normal,
        market_participant_state: MarketParticipantState::Active,
    })
}

// 4.2.5.1 — MWCB Decline Level (V)
#[rustfmt::skip]
const MWCB_DECLINE_LEVEL_BYTES: &[u8] = &[
    b'V',
    0x00, 0x00,                                              // stock_locate = 0
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x17, 0x48, 0x76, 0xE8, 0x00,          // level1
    0x00, 0x00, 0x00, 0x2E, 0x90, 0xED, 0xD0, 0x00,          // level2
    0x00, 0x00, 0x00, 0x45, 0xD9, 0x64, 0xB8, 0x00,          // level3
];

fn mwcb_decline_level_message() -> Message {
    Message::MwcbDeclineLevel(MwcbDeclineLevel {
        header: session_header(),
        level1: Price8::from_u64(100_000_000_000),
        level2: Price8::from_u64(200_000_000_000),
        level3: Price8::from_u64(300_000_000_000),
    })
}

// 4.2.5.2 — MWCB Status (W)
#[rustfmt::skip]
const MWCB_STATUS_BYTES: &[u8] = &[
    b'W',
    0x00, 0x00,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    b'2',                                                    // breached_level = Level2
];

fn mwcb_status_message() -> Message {
    Message::MwcbStatus(MwcbStatus {
        header: session_header(),
        breached_level: BreachedLevel::Level2,
    })
}

// 4.2.6 — IPO Quoting Period Update (K)
#[rustfmt::skip]
const IPO_QUOTING_PERIOD_UPDATE_BYTES: &[u8] = &[
    b'K',
    0x00, 0x00,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    b'N', b'E', b'W', b'C', b'O', b' ', b' ', b' ',
    0x00, 0x00, 0x85, 0x98,                                  // 34_200 (09:30 ET)
    b'A',                                                    // qualifier = Anticipated
    0x00, 0x16, 0xE3, 0x60,                                  // ipo_price = 1_500_000
];

fn ipo_quoting_period_update_message() -> Message {
    Message::IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate {
        header: session_header(),
        stock: Stock::new("NEWCO"),
        ipo_quotation_release_time: 34_200,
        ipo_quotation_release_qualifier: IpoReleaseQualifier::Anticipated,
        ipo_price: Price4::from_u32(1_500_000),
    })
}

// 4.3.1 — Add Order — No MPID (A)
#[rustfmt::skip]
const ADD_ORDER_BYTES: &[u8] = &[
    b'A',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,          // order_ref = 1001
    b'B',                                                    // side = Buy
    0x00, 0x00, 0x01, 0xF4,                                  // shares = 500
    b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',
    0x00, 0x1D, 0x5F, 0x88,                                  // price = 1_925_000
];

fn add_order_message() -> Message {
    Message::AddOrder(AddOrder {
        header: stock_header(),
        order_ref: OrderReference::from_u64(1001),
        side: Side::Buy,
        shares: Shares::from_u32(500),
        stock: Stock::new("AAPL"),
        price: Price4::from_u32(1_925_000),
    })
}

// 4.3.2 — Add Order — With MPID (F)
#[rustfmt::skip]
const ADD_ORDER_WITH_MPID_BYTES: &[u8] = &[
    b'F',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xEA,          // order_ref = 1002
    b'S',                                                    // side = Sell
    0x00, 0x00, 0x01, 0x2C,                                  // shares = 300
    b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',
    0x00, 0x1D, 0x63, 0x70,                                  // price = 1_926_000
    b'N', b'S', b'D', b'Q',                                  // attribution
];

fn add_order_with_mpid_message() -> Message {
    Message::AddOrderWithMpid(AddOrderWithMpid {
        header: stock_header(),
        order_ref: OrderReference::from_u64(1002),
        side: Side::Sell,
        shares: Shares::from_u32(300),
        stock: Stock::new("AAPL"),
        price: Price4::from_u32(1_926_000),
        attribution: Mpid::new("NSDQ"),
    })
}

// 4.4.1 — Order Executed (E)
#[rustfmt::skip]
const ORDER_EXECUTED_BYTES: &[u8] = &[
    b'E',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,          // order_ref = 1001
    0x00, 0x00, 0x00, 0x64,                                  // executed_shares = 100
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A,          // match_number = 42
];

fn order_executed_message() -> Message {
    Message::OrderExecuted(OrderExecuted {
        header: stock_header(),
        order_ref: OrderReference::from_u64(1001),
        executed_shares: Shares::from_u32(100),
        match_number: MatchNumber::from_u64(42),
    })
}

// 4.4.2 — Order Executed With Price (C)
#[rustfmt::skip]
const ORDER_EXECUTED_WITH_PRICE_BYTES: &[u8] = &[
    b'C',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,          // order_ref
    0x00, 0x00, 0x00, 0x32,                                  // executed_shares = 50
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2B,          // match_number = 43
    b'Y',                                                    // printable
    0x00, 0x1D, 0x5D, 0x94,                                  // execution_price = 1_924_500
];

fn order_executed_with_price_message() -> Message {
    Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
        header: stock_header(),
        order_ref: OrderReference::from_u64(1001),
        executed_shares: Shares::from_u32(50),
        match_number: MatchNumber::from_u64(43),
        printable: Printable::Printable,
        execution_price: Price4::from_u32(1_924_500),
    })
}

// 4.4.3 — Order Cancel (X)
#[rustfmt::skip]
const ORDER_CANCEL_BYTES: &[u8] = &[
    b'X',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,          // order_ref
    0x00, 0x00, 0x00, 0x4B,                                  // cancelled_shares = 75
];

fn order_cancel_message() -> Message {
    Message::OrderCancel(OrderCancel {
        header: stock_header(),
        order_ref: OrderReference::from_u64(1001),
        cancelled_shares: Shares::from_u32(75),
    })
}

// 4.4.4 — Order Delete (D)
#[rustfmt::skip]
const ORDER_DELETE_BYTES: &[u8] = &[
    b'D',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,          // order_ref = 1001
];

fn order_delete_message() -> Message {
    Message::OrderDelete(OrderDelete {
        header: stock_header(),
        order_ref: OrderReference::from_u64(1001),
    })
}

// 4.4.5 — Order Replace (U)
#[rustfmt::skip]
const ORDER_REPLACE_BYTES: &[u8] = &[
    b'U',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,          // original_order_ref
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xD1,          // new_order_ref = 2001
    0x00, 0x00, 0x01, 0xA9,                                  // shares = 425
    0x00, 0x1D, 0x69, 0x4C,                                  // price = 1_927_500
];

fn order_replace_message() -> Message {
    Message::OrderReplace(OrderReplace {
        header: stock_header(),
        original_order_ref: OrderReference::from_u64(1001),
        new_order_ref: OrderReference::from_u64(2001),
        shares: Shares::from_u32(425),
        price: Price4::from_u32(1_927_500),
    })
}

// 4.5.1 — Trade (Non-Cross) (P) — post-2014 quirk
#[rustfmt::skip]
const TRADE_NON_CROSS_POST_2014_BYTES: &[u8] = &[
    b'P',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,          // order_ref = 0
    b'B',                                                    // side = Buy (always)
    0x00, 0x00, 0x00, 0xC8,                                  // shares = 200
    b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',
    0x00, 0x1D, 0x61, 0x7C,                                  // price = 1_925_500
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1E, 0x61,          // match_number = 7777
];

fn trade_non_cross_post_2014_message() -> Message {
    Message::TradeNonCross(TradeNonCross {
        header: stock_header(),
        order_ref: OrderReference::from_u64(0),
        side: Side::Buy,
        shares: Shares::from_u32(200),
        stock: Stock::new("AAPL"),
        price: Price4::from_u32(1_925_500),
        match_number: MatchNumber::from_u64(7777),
    })
}

// 4.5.1 — Trade (Non-Cross) (P) — pre-2014 shape
#[rustfmt::skip]
const TRADE_NON_CROSS_PRE_2014_BYTES: &[u8] = &[
    b'P',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xD2,          // order_ref = 1234
    b'S',                                                    // side = Sell
    0x00, 0x00, 0x00, 0x96,                                  // shares = 150
    b'M', b'S', b'F', b'T', b' ', b' ', b' ', b' ',
    0x00, 0x35, 0x67, 0xE0,                                  // price = 3_500_000
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0xB8,          // match_number = 8888
];

fn trade_non_cross_pre_2014_message() -> Message {
    Message::TradeNonCross(TradeNonCross {
        header: stock_header(),
        order_ref: OrderReference::from_u64(1234),
        side: Side::Sell,
        shares: Shares::from_u32(150),
        stock: Stock::new("MSFT"),
        price: Price4::from_u32(3_500_000),
        match_number: MatchNumber::from_u64(8888),
    })
}

// 4.5.2 — Cross Trade (Q)
#[rustfmt::skip]
const CROSS_TRADE_BYTES: &[u8] = &[
    b'Q',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x42, 0x40,          // shares = 1_000_000 (u64)
    b'S', b'P', b'Y', b' ', b' ', b' ', b' ', b' ',
    0x00, 0x3D, 0x09, 0x00,                                  // cross_price = 4_000_000
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63,          // match_number = 99
    b'C',                                                    // cross_type = Closing
];

fn cross_trade_message() -> Message {
    Message::CrossTrade(CrossTrade {
        header: stock_header(),
        shares: 1_000_000,
        stock: Stock::new("SPY"),
        cross_price: Price4::from_u32(4_000_000),
        match_number: MatchNumber::from_u64(99),
        cross_type: CrossType::Closing,
    })
}

// 4.5.3 — Broken Trade (B)
#[rustfmt::skip]
const BROKEN_TRADE_BYTES: &[u8] = &[
    b'B',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x79, 0x32,          // match_number = 424242
];

fn broken_trade_message() -> Message {
    Message::BrokenTrade(BrokenTrade {
        header: stock_header(),
        match_number: MatchNumber::from_u64(424_242),
    })
}

// 4.6 — NOII (I)
#[rustfmt::skip]
const NOII_BYTES: &[u8] = &[
    b'I',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x42, 0x40,          // paired_shares
    0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xA1, 0x20,          // imbalance_shares = 500_000
    b'B',                                                    // imbalance_direction = Buy
    b'Q', b'Q', b'Q', b' ', b' ', b' ', b' ', b' ',
    0x00, 0x38, 0x75, 0x20,                                  // far_price
    0x00, 0x38, 0x9C, 0x30,                                  // near_price
    0x00, 0x38, 0x88, 0xA8,                                  // current_reference_price
    b'C',                                                    // cross_type
    b'1',                                                    // price_variation = Pct1To2
];

fn noii_message() -> Message {
    Message::Noii(Noii {
        header: stock_header(),
        paired_shares: 1_000_000,
        imbalance_shares: 500_000,
        imbalance_direction: ImbalanceDirection::Buy,
        stock: Stock::new("QQQ"),
        far_price: Price4::from_u32(3_700_000),
        near_price: Price4::from_u32(3_710_000),
        current_reference_price: Price4::from_u32(3_705_000),
        cross_type: CrossType::Closing,
        price_variation: itch_protocol::PriceVariation::Pct1To2,
    })
}

// 4.7 — Retail Price Improvement (N)
#[rustfmt::skip]
const RPI_BYTES: &[u8] = &[
    b'N',
    0x00, 0x01,
    0x00, 0x02,
    0x00, 0x00, 0x12, 0x34, 0x56, 0x78,
    b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',
    b'A',                                                    // interest_flag = BothSides
];

fn rpi_message() -> Message {
    Message::RetailPriceImprovement(RetailPriceImprovement {
        header: stock_header(),
        stock: Stock::new("AAPL"),
        interest_flag: RpiInterestFlag::BothSides,
    })
}

// ---------------------------------------------------------------------------
// Public statics — one per ITCH 5.0 message kind plus the documented
// `TradeNonCross` quirks.
// ---------------------------------------------------------------------------

/// `S` System Event vector.
pub static SYSTEM_EVENT: ConformanceVector = ConformanceVector {
    name: "system_event_start_of_messages",
    bytes: SYSTEM_EVENT_BYTES,
    message: system_event_message,
};

/// `R` Stock Directory vector.
pub static STOCK_DIRECTORY: ConformanceVector = ConformanceVector {
    name: "stock_directory_aapl",
    bytes: STOCK_DIRECTORY_BYTES,
    message: stock_directory_message,
};

/// `H` Stock Trading Action vector.
pub static STOCK_TRADING_ACTION: ConformanceVector = ConformanceVector {
    name: "stock_trading_action_aapl_trading",
    bytes: STOCK_TRADING_ACTION_BYTES,
    message: stock_trading_action_message,
};

/// `Y` Reg SHO Restriction vector.
pub static REG_SHO_RESTRICTION: ConformanceVector = ConformanceVector {
    name: "reg_sho_restriction_in_effect",
    bytes: REG_SHO_RESTRICTION_BYTES,
    message: reg_sho_restriction_message,
};

/// `L` Market Participant Position vector.
pub static MARKET_PARTICIPANT_POSITION: ConformanceVector = ConformanceVector {
    name: "market_participant_position_aapl_nsdq_active",
    bytes: MARKET_PARTICIPANT_POSITION_BYTES,
    message: market_participant_position_message,
};

/// `V` MWCB Decline Level vector.
pub static MWCB_DECLINE_LEVEL: ConformanceVector = ConformanceVector {
    name: "mwcb_decline_level",
    bytes: MWCB_DECLINE_LEVEL_BYTES,
    message: mwcb_decline_level_message,
};

/// `W` MWCB Status vector.
pub static MWCB_STATUS: ConformanceVector = ConformanceVector {
    name: "mwcb_status_level2",
    bytes: MWCB_STATUS_BYTES,
    message: mwcb_status_message,
};

/// `K` IPO Quoting Period Update vector.
pub static IPO_QUOTING_PERIOD_UPDATE: ConformanceVector = ConformanceVector {
    name: "ipo_quoting_period_update",
    bytes: IPO_QUOTING_PERIOD_UPDATE_BYTES,
    message: ipo_quoting_period_update_message,
};

/// `A` Add Order — No MPID vector.
pub static ADD_ORDER: ConformanceVector = ConformanceVector {
    name: "add_order_aapl_buy_500_at_192_50",
    bytes: ADD_ORDER_BYTES,
    message: add_order_message,
};

/// `F` Add Order — With MPID vector.
pub static ADD_ORDER_WITH_MPID: ConformanceVector = ConformanceVector {
    name: "add_order_with_mpid_aapl_sell_300_nsdq",
    bytes: ADD_ORDER_WITH_MPID_BYTES,
    message: add_order_with_mpid_message,
};

/// `E` Order Executed vector.
pub static ORDER_EXECUTED: ConformanceVector = ConformanceVector {
    name: "order_executed",
    bytes: ORDER_EXECUTED_BYTES,
    message: order_executed_message,
};

/// `C` Order Executed With Price vector.
pub static ORDER_EXECUTED_WITH_PRICE: ConformanceVector = ConformanceVector {
    name: "order_executed_with_price",
    bytes: ORDER_EXECUTED_WITH_PRICE_BYTES,
    message: order_executed_with_price_message,
};

/// `X` Order Cancel vector.
pub static ORDER_CANCEL: ConformanceVector = ConformanceVector {
    name: "order_cancel",
    bytes: ORDER_CANCEL_BYTES,
    message: order_cancel_message,
};

/// `D` Order Delete vector.
pub static ORDER_DELETE: ConformanceVector = ConformanceVector {
    name: "order_delete",
    bytes: ORDER_DELETE_BYTES,
    message: order_delete_message,
};

/// `U` Order Replace vector.
pub static ORDER_REPLACE: ConformanceVector = ConformanceVector {
    name: "order_replace",
    bytes: ORDER_REPLACE_BYTES,
    message: order_replace_message,
};

/// `P` Trade Non-Cross — post-2014-07-14 shape
/// (`order_ref = 0`, `side = Buy`).
pub static TRADE_NON_CROSS_POST_2014: ConformanceVector = ConformanceVector {
    name: "trade_non_cross_post_2014_quirk",
    bytes: TRADE_NON_CROSS_POST_2014_BYTES,
    message: trade_non_cross_post_2014_message,
};

/// `P` Trade Non-Cross — pre-2014 shape with arbitrary
/// `order_ref` and `side`. Ensures decoders preserve the legacy
/// quirk fields verbatim.
pub static TRADE_NON_CROSS_PRE_2014: ConformanceVector = ConformanceVector {
    name: "trade_non_cross_pre_2014_shape",
    bytes: TRADE_NON_CROSS_PRE_2014_BYTES,
    message: trade_non_cross_pre_2014_message,
};

/// `Q` Cross Trade vector.
pub static CROSS_TRADE: ConformanceVector = ConformanceVector {
    name: "cross_trade_closing",
    bytes: CROSS_TRADE_BYTES,
    message: cross_trade_message,
};

/// `B` Broken Trade vector.
pub static BROKEN_TRADE: ConformanceVector = ConformanceVector {
    name: "broken_trade",
    bytes: BROKEN_TRADE_BYTES,
    message: broken_trade_message,
};

/// `I` NOII vector.
pub static NOII: ConformanceVector = ConformanceVector {
    name: "noii_closing_buy_imbalance",
    bytes: NOII_BYTES,
    message: noii_message,
};

/// `N` Retail Price Improvement Indicator vector.
pub static RETAIL_PRICE_IMPROVEMENT: ConformanceVector = ConformanceVector {
    name: "retail_price_improvement_both_sides",
    bytes: RPI_BYTES,
    message: rpi_message,
};

/// Storage for [`all`]. Defined as a const so the slice can be
/// returned with `'static` lifetime.
const ALL_VECTORS: &[ConformanceVector] = &[
    SYSTEM_EVENT,
    STOCK_DIRECTORY,
    STOCK_TRADING_ACTION,
    REG_SHO_RESTRICTION,
    MARKET_PARTICIPANT_POSITION,
    MWCB_DECLINE_LEVEL,
    MWCB_STATUS,
    IPO_QUOTING_PERIOD_UPDATE,
    ADD_ORDER,
    ADD_ORDER_WITH_MPID,
    ORDER_EXECUTED,
    ORDER_EXECUTED_WITH_PRICE,
    ORDER_CANCEL,
    ORDER_DELETE,
    ORDER_REPLACE,
    TRADE_NON_CROSS_POST_2014,
    TRADE_NON_CROSS_PRE_2014,
    CROSS_TRADE,
    BROKEN_TRADE,
    NOII,
    RETAIL_PRICE_IMPROVEMENT,
];

/// All conformance vectors, in spec section order.
///
/// Iterate this in your conformance test to assert your decoder /
/// encoder matches every documented ITCH 5.0 message kind plus the
/// two `TradeNonCross` quirk shapes.
#[must_use]
#[inline]
pub fn all() -> &'static [ConformanceVector] {
    ALL_VECTORS
}
