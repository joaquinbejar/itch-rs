//! Criterion benchmarks for `Message::decode` latency per message kind.
//!
//! Measures end-to-end decode time on a single pre-built buffer for each
//! of the 20 ITCH 5.0 message types. Target: < 100 ns per message.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use itch_protocol::*;

fn bench_decode(c: &mut Criterion) {
    // SystemEvent
    let system_event_buf = [
        0x00, 0x00, // stock_locate (0)
        0x00, 0x00, // tracking_number (0)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // timestamp
        b'S', // message_type
    ];
    c.bench_function("decode_system_event", |b| {
        b.iter(|| Message::decode(black_box(&system_event_buf)))
    });

    // StockDirectory
    let stock_dir_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'R', // message_type
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ', // stock
        b'Q', // market_category
        b'N', // financial_status
        0x00, 0x00, 0x00, 0x64, // round_lot_size
        b'N', // round_lots_only
        b'C', // issue_classification
        b' ', b' ', // issue_subtype
        b'P', // authenticity
        b'N', // short_sale_threshold
        b'N', // ipo_flag
        b'1', // luld_reference_price_tier
        b'N', // etp_flag
        0x00, 0x00, 0x00, 0x00, // etp_leverage_factor
        b'N', // inverse_indicator
    ];
    c.bench_function("decode_stock_directory", |b| {
        b.iter(|| Message::decode(black_box(&stock_dir_buf)))
    });

    // StockTradingAction
    let stock_trading_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'H', // message_type
        b'M', b'S', b'F', b'T', b' ', b' ', b' ', b' ', // stock
        b'T', // trading_state
        b' ', // reserved
        b'N', b'O', b'R', b'M', // reason
    ];
    c.bench_function("decode_stock_trading_action", |b| {
        b.iter(|| Message::decode(black_box(&stock_trading_buf)))
    });

    // RegShoRestriction
    let reg_sho_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'Y', // message_type
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ', // stock
        b'0', // reg_sho_action
    ];
    c.bench_function("decode_reg_sho_restriction", |b| {
        b.iter(|| Message::decode(black_box(&reg_sho_buf)))
    });

    // MarketParticipantPosition
    let mkt_part_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'L', // message_type
        0x00, 0x00, 0x00, 0x01, // mpid
        b'C', b'X', b'C', b'M', // (rest of mpid)
        b' ', b' ', b' ', b' ', // stock
        b'Y', // primary_market_maker
        b'B', // market_maker_mode
        b'A', // market_participation_quote
    ];
    c.bench_function("decode_market_participant_position", |b| {
        b.iter(|| Message::decode(black_box(&mkt_part_buf)))
    });

    // MwcbDeclineLevel
    let mwcb_decline_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'V', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x10, 0x00, // decline_level_1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x10, 0x00, // decline_level_2
        0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x10, 0x00, // decline_level_3
    ];
    c.bench_function("decode_mwcb_decline_level", |b| {
        b.iter(|| Message::decode(black_box(&mwcb_decline_buf)))
    });

    // MwcbStatus
    let mwcb_status_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'W', // message_type
        b'1', // breached_level
    ];
    c.bench_function("decode_mwcb_status", |b| {
        b.iter(|| Message::decode(black_box(&mwcb_status_buf)))
    });

    // IpoQuotingPeriodUpdate
    let ipo_update_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'K', // message_type
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ', // stock
        0x00, 0x00, 0x12, 0x34, // ipo_quotation_release_time
        b'R', // ipo_quotation_release_qualifier
        0x00, 0x00, 0x12, 0x34, // ipo_price
    ];
    c.bench_function("decode_ipo_quoting_period_update", |b| {
        b.iter(|| Message::decode(black_box(&ipo_update_buf)))
    });

    // AddOrder
    let add_order_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'A', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // order_ref
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ', // stock
        b'B', // side
        0x00, 0x00, 0x27, 0x10, // shares
        0x00, 0x00, 0x3A, 0x98, // price
    ];
    c.bench_function("decode_add_order", |b| {
        b.iter(|| Message::decode(black_box(&add_order_buf)))
    });

    // AddOrderWithMpid
    let add_order_mpid_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'F', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // order_ref
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ', // stock
        b'B', // side
        0x00, 0x00, 0x27, 0x10, // shares
        0x00, 0x00, 0x3A, 0x98, // price
        b'C', b'X', b'C', b'M', // attribution
    ];
    c.bench_function("decode_add_order_with_mpid", |b| {
        b.iter(|| Message::decode(black_box(&add_order_mpid_buf)))
    });

    // OrderExecuted
    let order_exec_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'E', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // order_ref
        0x00, 0x00, 0x10, 0x00, // executed_shares
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // match_number
    ];
    c.bench_function("decode_order_executed", |b| {
        b.iter(|| Message::decode(black_box(&order_exec_buf)))
    });

    // OrderExecutedWithPrice
    let order_exec_price_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'C', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // order_ref
        0x00, 0x00, 0x10, 0x00, // executed_shares
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // match_number
        b'Y', // printable
        0x00, 0x00, 0x3A, 0x98, // execution_price
    ];
    c.bench_function("decode_order_executed_with_price", |b| {
        b.iter(|| Message::decode(black_box(&order_exec_price_buf)))
    });

    // OrderCancel
    let order_cancel_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'X', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // order_ref
        0x00, 0x00, 0x10, 0x00, // cancelled_shares
    ];
    c.bench_function("decode_order_cancel", |b| {
        b.iter(|| Message::decode(black_box(&order_cancel_buf)))
    });

    // OrderDelete
    let order_delete_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'D', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // order_ref
    ];
    c.bench_function("decode_order_delete", |b| {
        b.iter(|| Message::decode(black_box(&order_delete_buf)))
    });

    // OrderReplace
    let order_replace_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'U', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // order_ref
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // new_order_ref
        0x00, 0x00, 0x27, 0x10, // shares
        0x00, 0x00, 0x3A, 0x98, // price
    ];
    c.bench_function("decode_order_replace", |b| {
        b.iter(|| Message::decode(black_box(&order_replace_buf)))
    });

    // TradeNonCross
    let trade_non_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'P', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // order_ref
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ', // stock
        b'B', // side
        0x00, 0x00, 0x27, 0x10, // shares
        0x00, 0x00, 0x3A, 0x98, // price
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // match_number
    ];
    c.bench_function("decode_trade_non_cross", |b| {
        b.iter(|| Message::decode(black_box(&trade_non_buf)))
    });

    // CrossTrade
    let cross_trade_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'Q', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // shares_in_cross
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ', // stock
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cross_price (u64)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // match_number
        b'O', // cross_type
    ];
    c.bench_function("decode_cross_trade", |b| {
        b.iter(|| Message::decode(black_box(&cross_trade_buf)))
    });

    // BrokenTrade
    let broken_trade_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'B', // message_type
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // match_number
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // broken_match_number
    ];
    c.bench_function("decode_broken_trade", |b| {
        b.iter(|| Message::decode(black_box(&broken_trade_buf)))
    });

    // Noii
    let noii_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'I', // message_type
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ', // stock
        0x00, 0x00, 0x27, 0x10, // paired_shares
        0x00, 0x00, 0x10, 0x00, // imbalance_shares
        b'B', // imbalance_direction
        0x00, 0x00, 0x3A, 0x98, // stock_reference_price
        0x00, 0x00, 0x3A, 0x97, // bid_price
        0x00, 0x00, 0x3A, 0x99, // ask_price
        0x00, 0x00, 0x00, 0x00, // ipo_allocation_price
        0x00, 0x00, 0x00, 0x00, // nearest_ipo_match_price
        b'O', // cross_type
        b'L', // price_variation_indicator
    ];
    c.bench_function("decode_noii", |b| {
        b.iter(|| Message::decode(black_box(&noii_buf)))
    });

    // RetailPriceImprovement
    let rpi_buf = [
        0x12, 0x34, // stock_locate
        0x56, 0x78, // tracking_number
        0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // timestamp
        b'N', // message_type
        b'A', b'A', b'P', b'L', b' ', b' ', b' ', b' ', // stock
        b'B', // interest_flag
    ];
    c.bench_function("decode_retail_price_improvement", |b| {
        b.iter(|| Message::decode(black_box(&rpi_buf)))
    });
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
