//! Shared `proptest` strategies for the property tests.
//!
//! Per `docs/TESTING.md` §3 every primitive newtype and every
//! closed-set ASCII enum has a strategy here. `any_message` is
//! exhaustive over the 20 ITCH 5.0 message kinds (no `_ =>`).

#![allow(dead_code)]

use itch_protocol::{
    AddOrder, AddOrderWithMpid, Authenticity, BreachedLevel, BrokenTrade, CrossTrade, CrossType,
    EventCode, FinancialStatus, Header, ImbalanceDirection, IpoQuotingPeriodUpdate,
    IpoReleaseQualifier, LuldTier, MarketCategory, MarketMakerMode, MarketParticipantPosition,
    MarketParticipantState, MatchNumber, Message, Mpid, MwcbDeclineLevel, MwcbStatus, Noii,
    OrderCancel, OrderDelete, OrderExecuted, OrderExecutedWithPrice, OrderReference, OrderReplace,
    Price4, Price8, PriceVariation, Printable, RegShoAction, RegShoRestriction,
    RetailPriceImprovement, RpiInterestFlag, Shares, Side, Stock, StockDirectory, StockLocate,
    StockTradingAction, SystemEvent, Timestamp, TrackingNumber, TradeNonCross, TradingState, YesNo,
};
use proptest::prelude::*;

// ---------- primitives ----------

pub fn any_stock_locate() -> impl Strategy<Value = StockLocate> {
    any::<u16>().prop_map(StockLocate::from_u16)
}

pub fn any_tracking_number() -> impl Strategy<Value = TrackingNumber> {
    any::<u16>().prop_map(TrackingNumber::from_u16)
}

pub fn any_order_reference() -> impl Strategy<Value = OrderReference> {
    any::<u64>().prop_map(OrderReference::from_u64)
}

pub fn any_match_number() -> impl Strategy<Value = MatchNumber> {
    any::<u64>().prop_map(MatchNumber::from_u64)
}

pub fn any_shares() -> impl Strategy<Value = Shares> {
    any::<u32>().prop_map(Shares::from_u32)
}

pub fn any_timestamp() -> impl Strategy<Value = Timestamp> {
    // u48 — Timestamp::MAX = (1 << 48) - 1.
    (0u64..=Timestamp::MAX).prop_map(Timestamp::from_u64)
}

pub fn any_price4() -> impl Strategy<Value = Price4> {
    any::<u32>().prop_map(Price4::from_u32)
}

pub fn any_price8() -> impl Strategy<Value = Price8> {
    any::<u64>().prop_map(Price8::from_u64)
}

/// 8 ASCII bytes excluding NUL — the spec keeps unused trailing bytes
/// space-padded but in-range characters are ASCII printable.
pub fn any_stock() -> impl Strategy<Value = Stock> {
    proptest::array::uniform8(0x20u8..0x7Fu8).prop_map(Stock::from_bytes)
}

/// 4 ASCII bytes (printable + space).
pub fn any_mpid() -> impl Strategy<Value = Mpid> {
    proptest::array::uniform4(0x20u8..0x7Fu8).prop_map(Mpid::from_bytes)
}

pub fn any_header() -> impl Strategy<Value = Header> {
    (any_stock_locate(), any_tracking_number(), any_timestamp()).prop_map(
        |(stock_locate, tracking_number, timestamp)| Header {
            stock_locate,
            tracking_number,
            timestamp,
        },
    )
}

// ---------- enums ----------

pub fn any_event_code() -> impl Strategy<Value = EventCode> {
    use EventCode::*;
    prop_oneof![
        Just(StartOfMessages),
        Just(StartOfSystemHours),
        Just(StartOfMarketHours),
        Just(EndOfMarketHours),
        Just(EndOfSystemHours),
        Just(EndOfMessages),
    ]
}

pub fn any_market_category() -> impl Strategy<Value = MarketCategory> {
    use MarketCategory::*;
    prop_oneof![
        Just(NasdaqGlobalSelect),
        Just(NasdaqGlobalMarket),
        Just(NasdaqCapitalMarket),
        Just(NyseMkt),
        Just(Nyse),
        Just(NyseArca),
        Just(BatsZ),
        Just(Iex),
        Just(Unavailable),
    ]
}

pub fn any_financial_status() -> impl Strategy<Value = FinancialStatus> {
    use FinancialStatus::*;
    prop_oneof![
        Just(Normal),
        Just(Deficient),
        Just(Delinquent),
        Just(Bankrupt),
        Just(Suspended),
        Just(DeficientAndBankrupt),
        Just(DeficientAndDelinquent),
        Just(DelinquentAndBankrupt),
        Just(DeficientDelinquentBankrupt),
        Just(CreationsRedemptionsSuspended),
        Just(Unavailable),
    ]
}

pub fn any_authenticity() -> impl Strategy<Value = Authenticity> {
    prop_oneof![Just(Authenticity::Live), Just(Authenticity::Test)]
}

pub fn any_yes_no() -> impl Strategy<Value = YesNo> {
    prop_oneof![Just(YesNo::Yes), Just(YesNo::No), Just(YesNo::Unavailable)]
}

pub fn any_luld_tier() -> impl Strategy<Value = LuldTier> {
    use LuldTier::*;
    prop_oneof![Just(Tier1), Just(Tier2), Just(Unavailable)]
}

pub fn any_trading_state() -> impl Strategy<Value = TradingState> {
    use TradingState::*;
    prop_oneof![
        Just(Halted),
        Just(Paused),
        Just(QuotationOnly),
        Just(Trading)
    ]
}

pub fn any_reg_sho_action() -> impl Strategy<Value = RegShoAction> {
    use RegShoAction::*;
    prop_oneof![
        Just(NoPriceTest),
        Just(InEffectIntradayDrop),
        Just(InEffect),
    ]
}

pub fn any_market_maker_mode() -> impl Strategy<Value = MarketMakerMode> {
    use MarketMakerMode::*;
    prop_oneof![
        Just(Normal),
        Just(Passive),
        Just(Syndicate),
        Just(PreSyndicate),
        Just(Penalty),
    ]
}

pub fn any_market_participant_state() -> impl Strategy<Value = MarketParticipantState> {
    use MarketParticipantState::*;
    prop_oneof![
        Just(Active),
        Just(ExcusedOrWithdrawn),
        Just(Withdrawn),
        Just(Suspended),
        Just(Deleted),
    ]
}

pub fn any_breached_level() -> impl Strategy<Value = BreachedLevel> {
    use BreachedLevel::*;
    prop_oneof![Just(Level1), Just(Level2), Just(Level3)]
}

pub fn any_ipo_release_qualifier() -> impl Strategy<Value = IpoReleaseQualifier> {
    use IpoReleaseQualifier::*;
    prop_oneof![Just(Anticipated), Just(Cancelled)]
}

pub fn any_side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

pub fn any_printable() -> impl Strategy<Value = Printable> {
    prop_oneof![Just(Printable::Printable), Just(Printable::NonPrintable)]
}

pub fn any_cross_type() -> impl Strategy<Value = CrossType> {
    use CrossType::*;
    prop_oneof![Just(Opening), Just(Closing), Just(Halted), Just(Intraday)]
}

pub fn any_imbalance_direction() -> impl Strategy<Value = ImbalanceDirection> {
    use ImbalanceDirection::*;
    prop_oneof![
        Just(Buy),
        Just(Sell),
        Just(NoImbalance),
        Just(InsufficientToCalc),
    ]
}

pub fn any_price_variation() -> impl Strategy<Value = PriceVariation> {
    use PriceVariation::*;
    prop_oneof![
        Just(LessThan1Percent),
        Just(Pct1To2),
        Just(Pct2To3),
        Just(Pct3To4),
        Just(Pct4To5),
        Just(Pct5To6),
        Just(Pct6To7),
        Just(Pct7To8),
        Just(Pct8To9),
        Just(Pct9To10),
        Just(Pct10To20),
        Just(Pct20To30),
        Just(Pct30OrGreater),
        Just(CannotBeCalculated),
    ]
}

pub fn any_rpi_interest_flag() -> impl Strategy<Value = RpiInterestFlag> {
    use RpiInterestFlag::*;
    prop_oneof![Just(BuySide), Just(SellSide), Just(BothSides), Just(None)]
}

// ---------- messages ----------

pub fn any_system_event() -> impl Strategy<Value = SystemEvent> {
    (any_header(), any_event_code())
        .prop_map(|(header, event_code)| SystemEvent { header, event_code })
}

pub fn any_stock_directory() -> impl Strategy<Value = StockDirectory> {
    (
        // Group A — 6 fields.
        (
            any_header(),
            any_stock(),
            any_market_category(),
            any_financial_status(),
            any_shares(),
            any_yes_no(),
        ),
        // Group B — 6 fields.
        (
            proptest::array::uniform2(0x20u8..0x7Fu8),
            any_authenticity(),
            any_yes_no(),
            any_yes_no(),
            any_luld_tier(),
            any_yes_no(),
        ),
        // Group C — 2 fields (etp_leverage_factor + inverse_indicator).
        (any::<u32>(), any_yes_no()),
    )
        .prop_map(
            |(
                (header, stock, market_category, financial_status, round_lot_size, round_lots_only),
                (
                    issue_subtype,
                    authenticity,
                    short_sale_threshold,
                    ipo_flag,
                    luld_reference_price_tier,
                    etp_flag,
                ),
                (etp_leverage_factor, inverse_indicator),
            )| {
                StockDirectory {
                    header,
                    stock,
                    market_category,
                    financial_status,
                    round_lot_size,
                    round_lots_only,
                    issue_classification: b'C',
                    issue_subtype,
                    authenticity,
                    short_sale_threshold,
                    ipo_flag,
                    luld_reference_price_tier,
                    etp_flag,
                    etp_leverage_factor,
                    inverse_indicator,
                }
            },
        )
}

pub fn any_stock_trading_action() -> impl Strategy<Value = StockTradingAction> {
    (
        any_header(),
        any_stock(),
        any_trading_state(),
        proptest::array::uniform4(0x20u8..0x7Fu8),
    )
        .prop_map(
            |(header, stock, trading_state, reason)| StockTradingAction {
                header,
                stock,
                trading_state,
                reserved: b' ',
                reason,
            },
        )
}

pub fn any_reg_sho_restriction() -> impl Strategy<Value = RegShoRestriction> {
    (any_header(), any_stock(), any_reg_sho_action()).prop_map(|(header, stock, reg_sho_action)| {
        RegShoRestriction {
            header,
            stock,
            reg_sho_action,
        }
    })
}

pub fn any_market_participant_position() -> impl Strategy<Value = MarketParticipantPosition> {
    (
        any_header(),
        any_mpid(),
        any_stock(),
        any_yes_no(),
        any_market_maker_mode(),
        any_market_participant_state(),
    )
        .prop_map(
            |(
                header,
                mpid,
                stock,
                primary_market_maker,
                market_maker_mode,
                market_participant_state,
            )| MarketParticipantPosition {
                header,
                mpid,
                stock,
                primary_market_maker,
                market_maker_mode,
                market_participant_state,
            },
        )
}

pub fn any_mwcb_decline_level() -> impl Strategy<Value = MwcbDeclineLevel> {
    (any_header(), any_price8(), any_price8(), any_price8()).prop_map(
        |(header, level1, level2, level3)| MwcbDeclineLevel {
            header,
            level1,
            level2,
            level3,
        },
    )
}

pub fn any_mwcb_status() -> impl Strategy<Value = MwcbStatus> {
    (any_header(), any_breached_level()).prop_map(|(header, breached_level)| MwcbStatus {
        header,
        breached_level,
    })
}

pub fn any_ipo_quoting_period_update() -> impl Strategy<Value = IpoQuotingPeriodUpdate> {
    (
        any_header(),
        any_stock(),
        any::<u32>(),
        any_ipo_release_qualifier(),
        any_price4(),
    )
        .prop_map(
            |(
                header,
                stock,
                ipo_quotation_release_time,
                ipo_quotation_release_qualifier,
                ipo_price,
            )| IpoQuotingPeriodUpdate {
                header,
                stock,
                ipo_quotation_release_time,
                ipo_quotation_release_qualifier,
                ipo_price,
            },
        )
}

pub fn any_add_order() -> impl Strategy<Value = AddOrder> {
    (
        any_header(),
        any_order_reference(),
        any_side(),
        any_shares(),
        any_stock(),
        any_price4(),
    )
        .prop_map(|(header, order_ref, side, shares, stock, price)| AddOrder {
            header,
            order_ref,
            side,
            shares,
            stock,
            price,
        })
}

pub fn any_add_order_with_mpid() -> impl Strategy<Value = AddOrderWithMpid> {
    (any_add_order(), any_mpid()).prop_map(|(base, attribution)| AddOrderWithMpid {
        header: base.header,
        order_ref: base.order_ref,
        side: base.side,
        shares: base.shares,
        stock: base.stock,
        price: base.price,
        attribution,
    })
}

pub fn any_order_executed() -> impl Strategy<Value = OrderExecuted> {
    (
        any_header(),
        any_order_reference(),
        any_shares(),
        any_match_number(),
    )
        .prop_map(
            |(header, order_ref, executed_shares, match_number)| OrderExecuted {
                header,
                order_ref,
                executed_shares,
                match_number,
            },
        )
}

pub fn any_order_executed_with_price() -> impl Strategy<Value = OrderExecutedWithPrice> {
    (any_order_executed(), any_printable(), any_price4()).prop_map(
        |(base, printable, execution_price)| OrderExecutedWithPrice {
            header: base.header,
            order_ref: base.order_ref,
            executed_shares: base.executed_shares,
            match_number: base.match_number,
            printable,
            execution_price,
        },
    )
}

pub fn any_order_cancel() -> impl Strategy<Value = OrderCancel> {
    (any_header(), any_order_reference(), any_shares()).prop_map(
        |(header, order_ref, cancelled_shares)| OrderCancel {
            header,
            order_ref,
            cancelled_shares,
        },
    )
}

pub fn any_order_delete() -> impl Strategy<Value = OrderDelete> {
    (any_header(), any_order_reference())
        .prop_map(|(header, order_ref)| OrderDelete { header, order_ref })
}

pub fn any_order_replace() -> impl Strategy<Value = OrderReplace> {
    (
        any_header(),
        any_order_reference(),
        any_order_reference(),
        any_shares(),
        any_price4(),
    )
        .prop_map(
            |(header, original_order_ref, new_order_ref, shares, price)| OrderReplace {
                header,
                original_order_ref,
                new_order_ref,
                shares,
                price,
            },
        )
}

pub fn any_trade_non_cross() -> impl Strategy<Value = TradeNonCross> {
    (
        any_header(),
        any_order_reference(),
        any_side(),
        any_shares(),
        any_stock(),
        any_price4(),
        any_match_number(),
    )
        .prop_map(
            |(header, order_ref, side, shares, stock, price, match_number)| TradeNonCross {
                header,
                order_ref,
                side,
                shares,
                stock,
                price,
                match_number,
            },
        )
}

pub fn any_cross_trade() -> impl Strategy<Value = CrossTrade> {
    (
        any_header(),
        any::<u64>(),
        any_stock(),
        any_price4(),
        any_match_number(),
        any_cross_type(),
    )
        .prop_map(
            |(header, shares, stock, cross_price, match_number, cross_type)| CrossTrade {
                header,
                shares,
                stock,
                cross_price,
                match_number,
                cross_type,
            },
        )
}

pub fn any_broken_trade() -> impl Strategy<Value = BrokenTrade> {
    (any_header(), any_match_number()).prop_map(|(header, match_number)| BrokenTrade {
        header,
        match_number,
    })
}

pub fn any_noii() -> impl Strategy<Value = Noii> {
    (
        any_header(),
        any::<u64>(),
        any::<u64>(),
        any_imbalance_direction(),
        any_stock(),
        any_price4(),
        any_price4(),
        any_price4(),
        any_cross_type(),
        any_price_variation(),
    )
        .prop_map(
            |(
                header,
                paired_shares,
                imbalance_shares,
                imbalance_direction,
                stock,
                far_price,
                near_price,
                current_reference_price,
                cross_type,
                price_variation,
            )| Noii {
                header,
                paired_shares,
                imbalance_shares,
                imbalance_direction,
                stock,
                far_price,
                near_price,
                current_reference_price,
                cross_type,
                price_variation,
            },
        )
}

pub fn any_retail_price_improvement() -> impl Strategy<Value = RetailPriceImprovement> {
    (any_header(), any_stock(), any_rpi_interest_flag()).prop_map(
        |(header, stock, interest_flag)| RetailPriceImprovement {
            header,
            stock,
            interest_flag,
        },
    )
}

/// Exhaustive `Message` strategy — one branch per variant.
pub fn any_message() -> impl Strategy<Value = Message> {
    prop_oneof![
        any_system_event().prop_map(Message::SystemEvent),
        any_stock_directory().prop_map(Message::StockDirectory),
        any_stock_trading_action().prop_map(Message::StockTradingAction),
        any_reg_sho_restriction().prop_map(Message::RegShoRestriction),
        any_market_participant_position().prop_map(Message::MarketParticipantPosition),
        any_mwcb_decline_level().prop_map(Message::MwcbDeclineLevel),
        any_mwcb_status().prop_map(Message::MwcbStatus),
        any_ipo_quoting_period_update().prop_map(Message::IpoQuotingPeriodUpdate),
        any_add_order().prop_map(Message::AddOrder),
        any_add_order_with_mpid().prop_map(Message::AddOrderWithMpid),
        any_order_executed().prop_map(Message::OrderExecuted),
        any_order_executed_with_price().prop_map(Message::OrderExecutedWithPrice),
        any_order_cancel().prop_map(Message::OrderCancel),
        any_order_delete().prop_map(Message::OrderDelete),
        any_order_replace().prop_map(Message::OrderReplace),
        any_trade_non_cross().prop_map(Message::TradeNonCross),
        any_cross_trade().prop_map(Message::CrossTrade),
        any_broken_trade().prop_map(Message::BrokenTrade),
        any_noii().prop_map(Message::Noii),
        any_retail_price_improvement().prop_map(Message::RetailPriceImprovement),
    ]
}
