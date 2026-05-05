//! Golden hex byte vectors locking the ITCH 5.0 wire format.
//!
//! Each test pairs a hand-crafted byte literal (with per-byte
//! offset comments) and the expected decoded `Message`. Both
//! directions are asserted: `decode(bytes) == expected` AND
//! `encode(expected) == bytes`. A mismatch in either direction is a
//! wire-format regression.
//!
//! **Goldens are immutable.** Updating a vector means the wire
//! format changed, which is a major bump per ADR-0007. Never
//! delete or change a vector to make a refactor compile.

#![forbid(unsafe_code)]

use itch_protocol::*;

fn assert_golden(bytes: &[u8], expected: &Message) {
    let decoded = Message::decode(bytes).expect("decode golden");
    assert_eq!(&decoded, expected, "decoded mismatch");

    let mut buf = vec![0u8; expected.encoded_len()];
    let n = expected.encode(&mut buf).expect("encode golden");
    assert_eq!(n, bytes.len(), "encode length mismatch");
    assert_eq!(&buf[..n], bytes, "encoded bytes mismatch");
}

// Common header bytes used across most goldens:
//   stock_locate    = 0x0001                      → wire `00 01`
//   tracking_number = 0x0002                      → wire `00 02`
//   timestamp       = 0x0000_1234_5678 (305 419 896 ns since midnight)
//                                                 → wire `00 00 12 34 56 78` (u48)
fn stock_header() -> Header {
    Header {
        stock_locate: StockLocate::from_u16(1),
        tracking_number: TrackingNumber::from_u16(2),
        timestamp: Timestamp::from_u64(0x0000_1234_5678),
    }
}

fn session_header() -> Header {
    Header {
        stock_locate: StockLocate::from_u16(0),
        tracking_number: TrackingNumber::from_u16(2),
        timestamp: Timestamp::from_u64(0x0000_1234_5678),
    }
}

// =============================================================================
// 4.1 — System Event (S, body 11 B → wire 12 B)
// =============================================================================

#[test]
fn golden_system_event_start_of_messages() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'S',                                              // tag
        0x00, 0x00,                                        // stock_locate = 0
        0x00, 0x02,                                        // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                // timestamp (u48)
        b'O',                                              // event_code = StartOfMessages
    ];
    assert_golden(
        BYTES,
        &Message::SystemEvent(SystemEvent {
            header: Header {
                stock_locate: StockLocate::from_u16(0),
                tracking_number: TrackingNumber::from_u16(2),
                timestamp: Timestamp::from_u64(0x0000_1234_5678),
            },
            event_code: EventCode::StartOfMessages,
        }),
    );
}

// =============================================================================
// 4.2.1 — Stock Directory (R, body 38 B → wire 39 B)
// =============================================================================

#[test]
fn golden_stock_directory_aapl() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'R',                                                          // tag
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',                // stock = "AAPL    "
        b'Q',                                                          // market_category = NasdaqGlobalSelect
        b'N',                                                          // financial_status = Normal
        0x00, 0x00, 0x00, 0x64,                                        // round_lot_size = 100
        b'N',                                                          // round_lots_only = No
        b'C',                                                          // issue_classification
        b' ', b' ',                                                    // issue_subtype = "  "
        b'P',                                                          // authenticity = Live
        b'N',                                                          // short_sale_threshold = No
        b'N',                                                          // ipo_flag = No
        b'1',                                                          // luld_reference_price_tier = Tier1
        b'N',                                                          // etp_flag = No
        0x00, 0x00, 0x00, 0x00,                                        // etp_leverage_factor = 0
        b'N',                                                          // inverse_indicator = No
    ];
    assert_golden(
        BYTES,
        &Message::StockDirectory(StockDirectory {
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
        }),
    );
}

// =============================================================================
// 4.2.2 — Stock Trading Action (H, body 24 B → wire 25 B)
// =============================================================================

#[test]
fn golden_stock_trading_action_aapl_trading() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'H',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',
        b'T',                          // trading_state = Trading
        b' ',                          // reserved
        b'N', b'O', b'R', b'M',        // reason = "NORM"
    ];
    assert_golden(
        BYTES,
        &Message::StockTradingAction(StockTradingAction {
            header: stock_header(),
            stock: Stock::new("AAPL"),
            trading_state: TradingState::Trading,
            reserved: b' ',
            reason: *b"NORM",
        }),
    );
}

// =============================================================================
// 4.2.3 — Reg SHO Restriction (Y, body 19 B → wire 20 B)
// =============================================================================

#[test]
fn golden_reg_sho_restriction_in_effect() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'Y',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        b'T', b'S', b'L', b'A', b' ', b' ', b' ', b' ',
        b'2',                          // reg_sho_action = InEffect
    ];
    assert_golden(
        BYTES,
        &Message::RegShoRestriction(RegShoRestriction {
            header: stock_header(),
            stock: Stock::new("TSLA"),
            reg_sho_action: RegShoAction::InEffect,
        }),
    );
}

// =============================================================================
// 4.2.4 — Market Participant Position (L, body 25 B → wire 26 B)
// =============================================================================

#[test]
fn golden_market_participant_position_aapl_nsdq_active() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'L',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        b'N', b'S', b'D', b'Q',                                        // mpid
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',                // stock
        b'Y',                                                           // primary_market_maker = Yes
        b'N',                                                           // market_maker_mode = Normal
        b'A',                                                           // market_participant_state = Active
    ];
    assert_golden(
        BYTES,
        &Message::MarketParticipantPosition(MarketParticipantPosition {
            header: stock_header(),
            mpid: Mpid::new("NSDQ"),
            stock: Stock::new("AAPL"),
            primary_market_maker: YesNo::Yes,
            market_maker_mode: MarketMakerMode::Normal,
            market_participant_state: MarketParticipantState::Active,
        }),
    );
}

// =============================================================================
// 4.2.5.1 — MWCB Decline Level (V, body 34 B → wire 35 B)
// =============================================================================

#[test]
fn golden_mwcb_decline_level() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'V',
        0x00, 0x00,                                                    // stock_locate = 0 (session-level)
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x17, 0x48, 0x76, 0xE8, 0x00,                // level1 = 100_000_000_000
        0x00, 0x00, 0x00, 0x2E, 0x90, 0xED, 0xD0, 0x00,                // level2 = 200_000_000_000
        0x00, 0x00, 0x00, 0x45, 0xD9, 0x64, 0xB8, 0x00,                // level3 = 300_000_000_000
    ];
    assert_golden(
        BYTES,
        &Message::MwcbDeclineLevel(MwcbDeclineLevel {
            header: session_header(),
            level1: Price8::from_u64(100_000_000_000),
            level2: Price8::from_u64(200_000_000_000),
            level3: Price8::from_u64(300_000_000_000),
        }),
    );
}

// =============================================================================
// 4.2.5.2 — MWCB Status (W, body 11 B → wire 12 B)
// =============================================================================

#[test]
fn golden_mwcb_status_level2() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'W',
        0x00, 0x00,                                                    // stock_locate = 0 (session-level)
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        b'2',
    ];
    assert_golden(
        BYTES,
        &Message::MwcbStatus(MwcbStatus {
            header: session_header(),
            breached_level: BreachedLevel::Level2,
        }),
    );
}

// =============================================================================
// 4.2.6 — IPO Quoting Period Update (K, body 27 B → wire 28 B)
// =============================================================================

#[test]
fn golden_ipo_quoting_period_update() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'K',
        0x00, 0x00,                                                    // stock_locate = 0 (session-level)
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        b'N', b'E', b'W', b'C', b'O', b' ', b' ', b' ',
        0x00, 0x00, 0x85, 0x98,                                         // 34_200 (09:30 ET)
        b'A',                                                            // qualifier = Anticipated
        0x00, 0x16, 0xE3, 0x60,                                          // ipo_price = 1_500_000 (150.0000)
    ];
    assert_golden(
        BYTES,
        &Message::IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate {
            header: session_header(),
            stock: Stock::new("NEWCO"),
            ipo_quotation_release_time: 34_200,
            ipo_quotation_release_qualifier: IpoReleaseQualifier::Anticipated,
            ipo_price: Price4::from_u32(150_0000),
        }),
    );
}

// =============================================================================
// 4.3.1 — Add Order — No MPID (A, body 35 B → wire 36 B)
// =============================================================================

#[test]
fn golden_add_order_aapl_buy_500_at_192_50() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'A',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,                // order_ref = 1001
        b'B',                                                           // side = Buy
        0x00, 0x00, 0x01, 0xF4,                                         // shares = 500
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',                 // stock
        0x00, 0x1D, 0x5F, 0x88,                                         // price = 1_925_000 (192.5000)
    ];
    assert_golden(
        BYTES,
        &Message::AddOrder(AddOrder {
            header: stock_header(),
            order_ref: OrderReference::from_u64(1001),
            side: Side::Buy,
            shares: Shares::from_u32(500),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_000),
        }),
    );
}

// =============================================================================
// 4.3.2 — Add Order — With MPID (F, body 39 B → wire 40 B)
// =============================================================================

#[test]
fn golden_add_order_with_mpid_aapl_sell_300_nsdq() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'F',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xEA,                // order_ref = 1002
        b'S',                                                           // side = Sell
        0x00, 0x00, 0x01, 0x2C,                                         // shares = 300
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',
        0x00, 0x1D, 0x63, 0x70,                                         // price = 1_926_000 (192.6000)
        b'N', b'S', b'D', b'Q',                                         // attribution
    ];
    assert_golden(
        BYTES,
        &Message::AddOrderWithMpid(AddOrderWithMpid {
            header: stock_header(),
            order_ref: OrderReference::from_u64(1002),
            side: Side::Sell,
            shares: Shares::from_u32(300),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_926_000),
            attribution: Mpid::new("NSDQ"),
        }),
    );
}

// =============================================================================
// 4.4.1 — Order Executed (E, body 30 B → wire 31 B)
// =============================================================================

#[test]
fn golden_order_executed() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'E',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,                // order_ref = 1001
        0x00, 0x00, 0x00, 0x64,                                         // executed_shares = 100
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A,                 // match_number = 42
    ];
    assert_golden(
        BYTES,
        &Message::OrderExecuted(OrderExecuted {
            header: stock_header(),
            order_ref: OrderReference::from_u64(1001),
            executed_shares: Shares::from_u32(100),
            match_number: MatchNumber::from_u64(42),
        }),
    );
}

// =============================================================================
// 4.4.2 — Order Executed With Price (C, body 35 B → wire 36 B)
// =============================================================================

#[test]
fn golden_order_executed_with_price() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'C',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,                // order_ref = 1001
        0x00, 0x00, 0x00, 0x32,                                         // executed_shares = 50
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2B,                 // match_number = 43
        b'Y',                                                           // printable = Printable
        0x00, 0x1D, 0x5D, 0x94,                                         // execution_price = 1_924_500
    ];
    assert_golden(
        BYTES,
        &Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
            header: stock_header(),
            order_ref: OrderReference::from_u64(1001),
            executed_shares: Shares::from_u32(50),
            match_number: MatchNumber::from_u64(43),
            printable: Printable::Printable,
            execution_price: Price4::from_u32(1_924_500),
        }),
    );
}

// =============================================================================
// 4.4.3 — Order Cancel (X, body 22 B → wire 23 B)
// =============================================================================

#[test]
fn golden_order_cancel() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'X',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,                // order_ref = 1001
        0x00, 0x00, 0x00, 0x4B,                                         // cancelled_shares = 75
    ];
    assert_golden(
        BYTES,
        &Message::OrderCancel(OrderCancel {
            header: stock_header(),
            order_ref: OrderReference::from_u64(1001),
            cancelled_shares: Shares::from_u32(75),
        }),
    );
}

// =============================================================================
// 4.4.4 — Order Delete (D, body 18 B → wire 19 B)
// =============================================================================

#[test]
fn golden_order_delete() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'D',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,                // order_ref = 1001
    ];
    assert_golden(
        BYTES,
        &Message::OrderDelete(OrderDelete {
            header: stock_header(),
            order_ref: OrderReference::from_u64(1001),
        }),
    );
}

// =============================================================================
// 4.4.5 — Order Replace (U, body 34 B → wire 35 B)
// =============================================================================

#[test]
fn golden_order_replace() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'U',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE9,                // original_order_ref = 1001
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xD1,                // new_order_ref = 2001
        0x00, 0x00, 0x01, 0xA9,                                         // shares = 425
        0x00, 0x1D, 0x69, 0x4C,                                         // price = 1_927_500
    ];
    assert_golden(
        BYTES,
        &Message::OrderReplace(OrderReplace {
            header: stock_header(),
            original_order_ref: OrderReference::from_u64(1001),
            new_order_ref: OrderReference::from_u64(2001),
            shares: Shares::from_u32(425),
            price: Price4::from_u32(1_927_500),
        }),
    );
}

// =============================================================================
// 4.5.1 — Trade (Non-Cross) (P, body 43 B → wire 44 B)
// =============================================================================

#[test]
fn golden_trade_non_cross_post_2014_quirk() {
    // Post-2014-07-14 shape: order_ref = 0, side = Buy.
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'P',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,                // order_ref = 0
        b'B',                                                           // side = Buy
        0x00, 0x00, 0x00, 0xC8,                                         // shares = 200
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',
        0x00, 0x1D, 0x61, 0x7C,                                         // price = 1_925_500
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1E, 0x61,                 // match_number = 7777
    ];
    assert_golden(
        BYTES,
        &Message::TradeNonCross(TradeNonCross {
            header: stock_header(),
            order_ref: OrderReference::from_u64(0),
            side: Side::Buy,
            shares: Shares::from_u32(200),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_500),
            match_number: MatchNumber::from_u64(7777),
        }),
    );
}

#[test]
fn golden_trade_non_cross_pre_2014_shape() {
    // Pre-2014 shape with arbitrary values. Codec must preserve.
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'P',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xD2,                // order_ref = 1234
        b'S',                                                           // side = Sell
        0x00, 0x00, 0x00, 0x96,                                         // shares = 150
        b'M', b'S', b'F', b'T', b' ', b' ', b' ', b' ',
        0x00, 0x35, 0x67, 0xE0,                                         // price = 3_500_000
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0xB8,                 // match_number = 8888
    ];
    assert_golden(
        BYTES,
        &Message::TradeNonCross(TradeNonCross {
            header: stock_header(),
            order_ref: OrderReference::from_u64(1234),
            side: Side::Sell,
            shares: Shares::from_u32(150),
            stock: Stock::new("MSFT"),
            price: Price4::from_u32(3_500_000),
            match_number: MatchNumber::from_u64(8888),
        }),
    );
}

// =============================================================================
// 4.5.2 — Cross Trade (Q, body 39 B → wire 40 B)
// =============================================================================

#[test]
fn golden_cross_trade_closing() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'Q',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x42, 0x40,                // shares = 1_000_000 (u64)
        b'S', b'P', b'Y', b' ', b' ', b' ', b' ', b' ',
        0x00, 0x3D, 0x09, 0x00,                                         // cross_price = 4_000_000
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63,                 // match_number = 99
        b'C',                                                           // cross_type = Closing
    ];
    assert_golden(
        BYTES,
        &Message::CrossTrade(CrossTrade {
            header: stock_header(),
            shares: 1_000_000,
            stock: Stock::new("SPY"),
            cross_price: Price4::from_u32(4_000_000),
            match_number: MatchNumber::from_u64(99),
            cross_type: CrossType::Closing,
        }),
    );
}

// =============================================================================
// 4.5.3 — Broken Trade (B, body 18 B → wire 19 B)
// =============================================================================

#[test]
fn golden_broken_trade() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'B',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x79, 0x32,                // match_number = 424242
    ];
    assert_golden(
        BYTES,
        &Message::BrokenTrade(BrokenTrade {
            header: stock_header(),
            match_number: MatchNumber::from_u64(424242),
        }),
    );
}

// =============================================================================
// 4.6 — NOII (I, body 49 B → wire 50 B)
// =============================================================================

#[test]
fn golden_noii_closing_buy_imbalance() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'I',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x42, 0x40,                // paired_shares = 1_000_000
        0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xA1, 0x20,                // imbalance_shares = 500_000
        b'B',                                                           // imbalance_direction = Buy
        b'Q', b'Q', b'Q', b' ', b' ', b' ', b' ', b' ',
        0x00, 0x38, 0x75, 0x20,                                         // far_price = 3_700_000
        0x00, 0x38, 0x9C, 0x30,                                         // near_price = 3_710_000
        0x00, 0x38, 0x88, 0xA8,                                         // current_reference_price = 3_705_000
        b'C',                                                           // cross_type = Closing
        b'1',                                                           // price_variation = Pct1To2
    ];
    assert_golden(
        BYTES,
        &Message::Noii(Noii {
            header: stock_header(),
            paired_shares: 1_000_000,
            imbalance_shares: 500_000,
            imbalance_direction: ImbalanceDirection::Buy,
            stock: Stock::new("QQQ"),
            far_price: Price4::from_u32(3_700_000),
            near_price: Price4::from_u32(3_710_000),
            current_reference_price: Price4::from_u32(3_705_000),
            cross_type: CrossType::Closing,
            price_variation: PriceVariation::Pct1To2,
        }),
    );
}

// =============================================================================
// 4.7 — Retail Price Improvement (N, body 19 B → wire 20 B)
// =============================================================================

#[test]
fn golden_retail_price_improvement_both_sides() {
    #[rustfmt::skip]
    const BYTES: &[u8] = &[
        b'N',
        0x00, 0x01,                                                    // stock_locate = 1
        0x00, 0x02,                                                    // tracking_number = 2
        0x00, 0x00, 0x12, 0x34, 0x56, 0x78,                            // timestamp (u48)
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ',
        b'A',                          // interest_flag = BothSides
    ];
    assert_golden(
        BYTES,
        &Message::RetailPriceImprovement(RetailPriceImprovement {
            header: stock_header(),
            stock: Stock::new("AAPL"),
            interest_flag: RpiInterestFlag::BothSides,
        }),
    );
}
