//! Per-message encode → decode roundtrip tests for ITCH 5.0.
//!
//! Each `Message` variant is constructed with non-trivial values for
//! every field, encoded into a stack buffer, decoded back, and
//! asserted equal. Catches every offset mistake immediately.
//!
//! The full set is exhaustive over the 20 ITCH 5.0 message kinds.

#![forbid(unsafe_code)]

use itch_protocol::*;

// Per-message buffers are sized exactly to `Message::encoded_len()`
// inside `roundtrip()`. The largest ITCH 5.0 message is 50 bytes;
// the assert on `n == encoded_len()` catches any drift between
// `body_len()` and what `encode_body()` actually wrote.

fn header() -> Header {
    Header {
        stock_locate: StockLocate::from_u16(0x1234),
        tracking_number: TrackingNumber::from_u16(0x5678),
        timestamp: Timestamp::from_u64(0x0000_DEAD_BEEF_CAFE),
    }
}

fn header_zero_locate() -> Header {
    Header {
        stock_locate: StockLocate::from_u16(0),
        ..header()
    }
}

fn roundtrip(m: Message) {
    // Size the buffer from the message itself — catches drift
    // between `body_len()` and the bytes actually written.
    let mut buf = vec![0u8; m.encoded_len()];
    let n = m.encode(&mut buf).expect("encode");
    assert_eq!(n, m.encoded_len(), "encoded_len mismatch for {m:?}");
    let decoded = Message::decode(&buf[..n]).expect("decode");
    assert_eq!(decoded, m, "round-trip mismatch for {m:?}");
}

// ---------------------------------------------------------------------------
// Per-kind roundtrip tests
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_system_event() {
    for code in [
        EventCode::StartOfMessages,
        EventCode::StartOfSystemHours,
        EventCode::StartOfMarketHours,
        EventCode::EndOfMarketHours,
        EventCode::EndOfSystemHours,
        EventCode::EndOfMessages,
    ] {
        roundtrip(Message::SystemEvent(SystemEvent {
            header: header_zero_locate(),
            event_code: code,
        }));
    }
}

#[test]
fn roundtrip_stock_directory() {
    roundtrip(Message::StockDirectory(StockDirectory {
        header: header(),
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
    }));
}

#[test]
fn roundtrip_stock_trading_action() {
    roundtrip(Message::StockTradingAction(StockTradingAction {
        header: header(),
        stock: Stock::new("MSFT"),
        trading_state: TradingState::Trading,
        reserved: b' ',
        reason: *b"NORM",
    }));
}

#[test]
fn roundtrip_reg_sho_restriction() {
    for action in [
        RegShoAction::NoPriceTest,
        RegShoAction::InEffectIntradayDrop,
        RegShoAction::InEffect,
    ] {
        roundtrip(Message::RegShoRestriction(RegShoRestriction {
            header: header(),
            stock: Stock::new("TSLA"),
            reg_sho_action: action,
        }));
    }
}

#[test]
fn roundtrip_market_participant_position() {
    roundtrip(Message::MarketParticipantPosition(
        MarketParticipantPosition {
            header: header(),
            mpid: Mpid::new("NSDQ"),
            stock: Stock::new("AAPL"),
            primary_market_maker: YesNo::Yes,
            market_maker_mode: MarketMakerMode::Normal,
            market_participant_state: MarketParticipantState::Active,
        },
    ));
}

#[test]
fn roundtrip_mwcb_decline_level() {
    roundtrip(Message::MwcbDeclineLevel(MwcbDeclineLevel {
        header: header_zero_locate(),
        level1: Price8::from_u64(100_000_000_000),
        level2: Price8::from_u64(200_000_000_000),
        level3: Price8::from_u64(300_000_000_000),
    }));
}

#[test]
fn roundtrip_mwcb_status() {
    for level in [
        BreachedLevel::Level1,
        BreachedLevel::Level2,
        BreachedLevel::Level3,
    ] {
        roundtrip(Message::MwcbStatus(MwcbStatus {
            header: header_zero_locate(),
            breached_level: level,
        }));
    }
}

#[test]
fn roundtrip_ipo_quoting_period_update() {
    for q in [
        IpoReleaseQualifier::Anticipated,
        IpoReleaseQualifier::Cancelled,
    ] {
        roundtrip(Message::IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate {
            header: header_zero_locate(),
            stock: Stock::new("NEWCO"),
            ipo_quotation_release_time: 34_200, // 09:30 ET in seconds since midnight
            ipo_quotation_release_qualifier: q,
            ipo_price: Price4::from_u32(150_0000),
        }));
    }
}

#[test]
fn roundtrip_add_order() {
    for side in [Side::Buy, Side::Sell] {
        roundtrip(Message::AddOrder(AddOrder {
            header: header(),
            order_ref: OrderReference::from_u64(1001),
            side,
            shares: Shares::from_u32(500),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_000),
        }));
    }
}

#[test]
fn roundtrip_add_order_with_full_8byte_stock_no_padding() {
    roundtrip(Message::AddOrder(AddOrder {
        header: header(),
        order_ref: OrderReference::from_u64(1003),
        side: Side::Buy,
        shares: Shares::from_u32(100),
        stock: Stock::from_bytes(*b"LONGSYMB"),
        price: Price4::from_u32(2_000_000),
    }));
}

#[test]
fn roundtrip_add_order_with_embedded_space_stock() {
    roundtrip(Message::AddOrder(AddOrder {
        header: header(),
        order_ref: OrderReference::from_u64(1004),
        side: Side::Buy,
        shares: Shares::from_u32(100),
        stock: Stock::from_bytes(*b"AB CD   "),
        price: Price4::from_u32(2_000_000),
    }));
}

#[test]
fn roundtrip_add_order_max_field_values() {
    roundtrip(Message::AddOrder(AddOrder {
        header: Header {
            stock_locate: StockLocate::from_u16(u16::MAX),
            tracking_number: TrackingNumber::from_u16(u16::MAX),
            timestamp: Timestamp::from_u64((1u64 << 48) - 1),
        },
        order_ref: OrderReference::from_u64(u64::MAX),
        side: Side::Sell,
        shares: Shares::from_u32(u32::MAX),
        stock: Stock::from_bytes(*b"ZZZZZZZZ"),
        price: Price4::from_u32(u32::MAX),
    }));
}

#[test]
fn roundtrip_add_order_with_mpid() {
    roundtrip(Message::AddOrderWithMpid(AddOrderWithMpid {
        header: header(),
        order_ref: OrderReference::from_u64(1002),
        side: Side::Sell,
        shares: Shares::from_u32(300),
        stock: Stock::new("AAPL"),
        price: Price4::from_u32(1_926_000),
        attribution: Mpid::new("NSDQ"),
    }));
}

#[test]
fn roundtrip_order_executed() {
    roundtrip(Message::OrderExecuted(OrderExecuted {
        header: header(),
        order_ref: OrderReference::from_u64(1001),
        executed_shares: Shares::from_u32(100),
        match_number: MatchNumber::from_u64(42),
    }));
}

#[test]
fn roundtrip_order_executed_with_price() {
    for printable in [Printable::NonPrintable, Printable::Printable] {
        roundtrip(Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
            header: header(),
            order_ref: OrderReference::from_u64(1001),
            executed_shares: Shares::from_u32(50),
            match_number: MatchNumber::from_u64(43),
            printable,
            execution_price: Price4::from_u32(1_924_500),
        }));
    }
}

#[test]
fn roundtrip_order_cancel() {
    roundtrip(Message::OrderCancel(OrderCancel {
        header: header(),
        order_ref: OrderReference::from_u64(1001),
        cancelled_shares: Shares::from_u32(75),
    }));
}

#[test]
fn roundtrip_order_delete() {
    roundtrip(Message::OrderDelete(OrderDelete {
        header: header(),
        order_ref: OrderReference::from_u64(1001),
    }));
}

#[test]
fn roundtrip_order_replace() {
    roundtrip(Message::OrderReplace(OrderReplace {
        header: header(),
        original_order_ref: OrderReference::from_u64(1001),
        new_order_ref: OrderReference::from_u64(2001),
        shares: Shares::from_u32(425),
        price: Price4::from_u32(1_927_500),
    }));
}

#[test]
fn roundtrip_trade_non_cross_post_2014_quirk() {
    // Post-2014-07-14 shape: order_ref=0, side=Buy. Must roundtrip.
    roundtrip(Message::TradeNonCross(TradeNonCross {
        header: header(),
        order_ref: OrderReference::from_u64(0),
        side: Side::Buy,
        shares: Shares::from_u32(200),
        stock: Stock::new("AAPL"),
        price: Price4::from_u32(1_925_500),
        match_number: MatchNumber::from_u64(7777),
    }));
}

#[test]
fn roundtrip_trade_non_cross_pre_2014_shape() {
    // Pre-2014 shape with arbitrary values. Codec must preserve.
    roundtrip(Message::TradeNonCross(TradeNonCross {
        header: header(),
        order_ref: OrderReference::from_u64(1234),
        side: Side::Sell,
        shares: Shares::from_u32(150),
        stock: Stock::new("MSFT"),
        price: Price4::from_u32(3_500_000),
        match_number: MatchNumber::from_u64(8888),
    }));
}

#[test]
fn roundtrip_cross_trade() {
    for ct in [
        CrossType::Opening,
        CrossType::Closing,
        CrossType::Halted,
        CrossType::Intraday,
    ] {
        roundtrip(Message::CrossTrade(CrossTrade {
            header: header(),
            shares: u64::MAX, // u64 — auctions can exceed u32
            stock: Stock::new("SPY"),
            cross_price: Price4::from_u32(4_000_000),
            match_number: MatchNumber::from_u64(99),
            cross_type: ct,
        }));
    }
}

#[test]
fn roundtrip_broken_trade() {
    roundtrip(Message::BrokenTrade(BrokenTrade {
        header: header(),
        match_number: MatchNumber::from_u64(424242),
    }));
}

#[test]
fn roundtrip_noii() {
    for direction in [
        ImbalanceDirection::Buy,
        ImbalanceDirection::Sell,
        ImbalanceDirection::NoImbalance,
        ImbalanceDirection::InsufficientToCalc,
    ] {
        roundtrip(Message::Noii(Noii {
            header: header(),
            paired_shares: 1_000_000,
            imbalance_shares: 500_000,
            imbalance_direction: direction,
            stock: Stock::new("QQQ"),
            far_price: Price4::from_u32(3_700_000),
            near_price: Price4::from_u32(3_710_000),
            current_reference_price: Price4::from_u32(3_705_000),
            cross_type: CrossType::Closing,
            price_variation: PriceVariation::Pct1To2,
        }));
    }
}

#[test]
fn roundtrip_retail_price_improvement() {
    for f in [
        RpiInterestFlag::BuySide,
        RpiInterestFlag::SellSide,
        RpiInterestFlag::BothSides,
        RpiInterestFlag::None,
    ] {
        roundtrip(Message::RetailPriceImprovement(RetailPriceImprovement {
            header: header(),
            stock: Stock::new("AAPL"),
            interest_flag: f,
        }));
    }
}

// ---------------------------------------------------------------------------
// Sweep + edge cases
// ---------------------------------------------------------------------------

#[test]
fn back_to_back_sweep_all_twenty_kinds_decodes_in_order() {
    // Stream-level regression: encode one of every kind into a
    // single concatenated buffer, then decode the whole stream and
    // assert each decoded message matches the expected one in order.
    // Catches length-advancement bugs that per-kind unit tests miss.
    let h = header();
    let h0 = header_zero_locate();
    let messages: Vec<Message> = vec![
        Message::SystemEvent(SystemEvent {
            header: h0,
            event_code: EventCode::StartOfMessages,
        }),
        Message::StockDirectory(StockDirectory {
            header: h,
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
        Message::StockTradingAction(StockTradingAction {
            header: h,
            stock: Stock::new("AAPL"),
            trading_state: TradingState::Trading,
            reserved: b' ',
            reason: *b"NORM",
        }),
        Message::RegShoRestriction(RegShoRestriction {
            header: h,
            stock: Stock::new("TSLA"),
            reg_sho_action: RegShoAction::InEffect,
        }),
        Message::MarketParticipantPosition(MarketParticipantPosition {
            header: h,
            mpid: Mpid::new("NSDQ"),
            stock: Stock::new("AAPL"),
            primary_market_maker: YesNo::Yes,
            market_maker_mode: MarketMakerMode::Normal,
            market_participant_state: MarketParticipantState::Active,
        }),
        Message::MwcbDeclineLevel(MwcbDeclineLevel {
            header: h0,
            level1: Price8::from_u64(100_000_000_000),
            level2: Price8::from_u64(200_000_000_000),
            level3: Price8::from_u64(300_000_000_000),
        }),
        Message::MwcbStatus(MwcbStatus {
            header: h0,
            breached_level: BreachedLevel::Level2,
        }),
        Message::IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate {
            header: h0,
            stock: Stock::new("NEWCO"),
            ipo_quotation_release_time: 34_200,
            ipo_quotation_release_qualifier: IpoReleaseQualifier::Anticipated,
            ipo_price: Price4::from_u32(150_0000),
        }),
        Message::AddOrder(AddOrder {
            header: h,
            order_ref: OrderReference::from_u64(1001),
            side: Side::Buy,
            shares: Shares::from_u32(500),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_000),
        }),
        Message::AddOrderWithMpid(AddOrderWithMpid {
            header: h,
            order_ref: OrderReference::from_u64(1002),
            side: Side::Sell,
            shares: Shares::from_u32(300),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_926_000),
            attribution: Mpid::new("NSDQ"),
        }),
        Message::OrderExecuted(OrderExecuted {
            header: h,
            order_ref: OrderReference::from_u64(1001),
            executed_shares: Shares::from_u32(100),
            match_number: MatchNumber::from_u64(42),
        }),
        Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
            header: h,
            order_ref: OrderReference::from_u64(1001),
            executed_shares: Shares::from_u32(50),
            match_number: MatchNumber::from_u64(43),
            printable: Printable::Printable,
            execution_price: Price4::from_u32(1_924_500),
        }),
        Message::OrderCancel(OrderCancel {
            header: h,
            order_ref: OrderReference::from_u64(1001),
            cancelled_shares: Shares::from_u32(75),
        }),
        Message::OrderDelete(OrderDelete {
            header: h,
            order_ref: OrderReference::from_u64(1001),
        }),
        Message::OrderReplace(OrderReplace {
            header: h,
            original_order_ref: OrderReference::from_u64(1001),
            new_order_ref: OrderReference::from_u64(2001),
            shares: Shares::from_u32(425),
            price: Price4::from_u32(1_927_500),
        }),
        Message::TradeNonCross(TradeNonCross {
            header: h,
            order_ref: OrderReference::from_u64(0),
            side: Side::Buy,
            shares: Shares::from_u32(200),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_500),
            match_number: MatchNumber::from_u64(7777),
        }),
        Message::CrossTrade(CrossTrade {
            header: h,
            shares: 1_000_000,
            stock: Stock::new("SPY"),
            cross_price: Price4::from_u32(4_000_000),
            match_number: MatchNumber::from_u64(99),
            cross_type: CrossType::Closing,
        }),
        Message::BrokenTrade(BrokenTrade {
            header: h,
            match_number: MatchNumber::from_u64(424242),
        }),
        Message::Noii(Noii {
            header: h,
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
        Message::RetailPriceImprovement(RetailPriceImprovement {
            header: h,
            stock: Stock::new("AAPL"),
            interest_flag: RpiInterestFlag::BothSides,
        }),
    ];

    assert_eq!(messages.len(), 20, "must cover all 20 kinds");

    let total: usize = messages.iter().map(|m| m.encoded_len()).sum();
    let mut buf = vec![0u8; total];
    let mut off = 0;
    for m in &messages {
        let n = m.encode(&mut buf[off..]).expect("encode");
        off += n;
    }
    assert_eq!(off, total);

    let mut cursor = 0;
    for expected in &messages {
        let decoded = Message::decode(&buf[cursor..]).expect("decode");
        assert_eq!(decoded, *expected);
        cursor += decoded.encoded_len();
    }
    assert_eq!(cursor, total);
}

#[test]
fn decode_empty_returns_truncated() {
    let err = Message::decode(&[]).expect_err("empty must fail");
    matches_truncated(&err);
}

#[test]
fn decode_unknown_tag_returns_unknown_message_type() {
    let err = Message::decode(b"~").expect_err("unknown tag must fail");
    match err {
        ProtocolError::UnknownMessageType(b) => assert_eq!(b, b'~'),
        other => panic!("expected UnknownMessageType, got {other:?}"),
    }
}

#[test]
fn decode_truncated_body_returns_truncated() {
    // Tag 'A' (AddOrder body 35) but only 5 bytes of body provided.
    let bytes = [b'A', 0u8, 0u8, 0u8, 0u8, 0u8];
    matches_truncated(&Message::decode(&bytes).expect_err("must fail"));
}

#[test]
fn decode_invalid_enum_code_returns_invalid_enum_code() {
    // Build a SystemEvent body with a bogus event_code byte 'Z'.
    let mut bytes = [0u8; 12];
    bytes[0] = b'S';
    bytes[11] = b'Z';
    match Message::decode(&bytes).expect_err("must fail") {
        ProtocolError::InvalidEnumCode { field, code } => {
            assert_eq!(field, "EventCode");
            assert_eq!(code, b'Z');
        }
        other => panic!("expected InvalidEnumCode, got {other:?}"),
    }
}

#[test]
fn encode_into_too_small_buffer_returns_buffer_too_small() {
    let m = Message::OrderDelete(OrderDelete {
        header: header(),
        order_ref: OrderReference::from_u64(1001),
    });
    let mut tiny = [0u8; 5];
    match m.encode(&mut tiny).expect_err("must fail") {
        ProtocolError::BufferTooSmall { need, got } => {
            assert_eq!(need, m.encoded_len());
            assert_eq!(got, 5);
        }
        other => panic!("expected BufferTooSmall, got {other:?}"),
    }
}

fn matches_truncated(err: &ProtocolError) {
    match err {
        ProtocolError::Truncated { .. } => {}
        other => panic!("expected Truncated, got {other:?}"),
    }
}
