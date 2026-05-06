//! Per-symbol OHLCV (open / high / low / close / volume) accumulator
//! over the printable trade prints carried in an ITCH 5.0 message
//! stream.
//!
//! [`OhlcvAccumulator`] is the v0.4-acceptance counterpart of
//! [`BookManager`](crate::manager::BookManager): same exhaustive
//! 20-variant match, same `apply(&Message)` shape, but instead of
//! reconstructing per-symbol resting books it folds every printable
//! trade print into a single per-symbol [`OhlcvBar`].
//!
//! ## Trade-printing variants
//!
//! Three of the 20 ITCH 5.0 message kinds carry public trade prints
//! that should drive an end-of-day OHLCV summary:
//!
//! | Variant                         | Drives OHLCV?                                                                                  |
//! |---------------------------------|------------------------------------------------------------------------------------------------|
//! | `P` `TradeNonCross`             | Yes. `price` and `shares` are read directly.                                                    |
//! | `Q` `CrossTrade`                | Yes. `cross_price` and `shares` are read directly. Treated as a normal trade, per NASDAQ EOD.   |
//! | `C` `OrderExecutedWithPrice`    | Only when `printable == Printable::Printable`. Non-printable executions are ignored.            |
//! | `E` `OrderExecuted`             | **No.** Skipped on purpose — see "E vs P" below.                                                 |
//! | every other variant             | No-op (explicit arm; no wildcard).                                                              |
//!
//! ### E vs P
//!
//! `E` Order Executed prints a fill at the resting order's display
//! price, but the price is not carried on the wire — it lives in the
//! per-symbol book the OHLCV accumulator does not have access to.
//! NASDAQ emits a paired `P` Trade (Non-Cross) for the public tape on
//! the same execution event, so the printable-trade view is fully
//! covered by the `P` arm.
//!
//! Consuming `E` here would either double-count (every execution
//! reported via both `E` and `P`) or require a side-band
//! `HashMap<OrderReference, Price4>` populated from `A` / `F` Add
//! Order messages, which couples the accumulator to the resting book.
//! We deliberately take the simpler path and leave `E` as an explicit
//! no-op; the v0.4 acceptance test confirms the synthetic-day OHLCV
//! reconciles without it.
//!
//! ## Bar update rule
//!
//! For each printable trade `(stock_locate, price, shares)`:
//!
//! - First trade for a symbol → insert a fresh [`OhlcvBar`] with
//!   `open == high == low == close == price`, `volume == shares`,
//!   `trade_count == 1`.
//! - Subsequent trades → `high = max(high, price)`,
//!   `low = min(low, price)`, `close = price`,
//!   `volume += shares`, `trade_count += 1`. `open` is preserved.
//!
//! ## Allocation profile
//!
//! Steady-state apply is a single `HashMap` lookup followed by an
//! in-place update. The only allocator interactions happen when a
//! previously-unseen symbol prints its first trade; thereafter the
//! map size is bounded by the daily symbol universe.

use std::collections::HashMap;

use itch_protocol::{Message, Price4, Printable, StockLocate};

/// One symbol's running open / high / low / close + cumulative
/// volume + trade-count summary.
///
/// All four prices share [`Price4`] semantics (wire `u32` /
/// 10⁻⁴ dollars). Volume is summed across every printable trade in
/// shares; `trade_count` increments by 1 per print regardless of
/// share quantity.
///
/// `Default` is intentionally not derived: a "blank" bar with
/// `open == high == low == close == 0` is indistinguishable from a
/// real trade that printed at price 0, and shipping such a sentinel
/// invites unsafe arithmetic at the consumer. Construct bars only
/// through [`OhlcvAccumulator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OhlcvBar {
    /// Symbol's [`StockLocate`].
    pub stock_locate: StockLocate,
    /// Price of the first printable trade in the session.
    pub open: Price4,
    /// Highest printable trade price seen so far.
    pub high: Price4,
    /// Lowest printable trade price seen so far.
    pub low: Price4,
    /// Price of the most recent printable trade.
    pub close: Price4,
    /// Cumulative shares across every printable trade.
    pub volume: u64,
    /// Number of printable trade prints folded into this bar.
    pub trade_count: u64,
}

/// Per-symbol OHLCV accumulator over an ITCH 5.0 message stream.
///
/// See the module-level documentation for the full set of trade-
/// printing variants the accumulator consumes and the variants it
/// deliberately ignores.
#[derive(Debug, Default)]
pub struct OhlcvAccumulator {
    per_symbol: HashMap<StockLocate, OhlcvBar>,
}

impl OhlcvAccumulator {
    /// Construct an empty accumulator. No bars exist until the first
    /// printable trade is applied.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of symbols currently tracked.
    ///
    /// A symbol becomes tracked on its first printable trade and
    /// stays tracked for the lifetime of the accumulator.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.per_symbol.len()
    }

    /// `true` when no symbol has yet printed a trade.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.per_symbol.is_empty()
    }

    /// Borrow the [`OhlcvBar`] for `locate`, if any printable trade
    /// has been applied for that symbol.
    #[inline]
    #[must_use]
    pub fn bar(&self, locate: StockLocate) -> Option<&OhlcvBar> {
        self.per_symbol.get(&locate)
    }

    /// Iterate every per-symbol [`OhlcvBar`] currently tracked.
    ///
    /// The order is unspecified and stable only within a single
    /// snapshot of the accumulator.
    pub fn bars(&self) -> impl Iterator<Item = &OhlcvBar> + '_ {
        self.per_symbol.values()
    }

    /// Apply one ITCH 5.0 message to the accumulator.
    ///
    /// Trade-printing variants (`P` / `Q`, plus `C` when
    /// `printable == Printable::Printable`) update the per-symbol
    /// bar; every other variant is an explicit no-op. The 20 ITCH
    /// 5.0 message kinds are matched exhaustively — adding a
    /// wildcard arm is forbidden by the workspace coding rules so
    /// new revisions surface as compile errors.
    pub fn apply(&mut self, msg: &Message) {
        match msg {
            Message::TradeNonCross(p) => {
                self.apply_trade(p.header.stock_locate, p.price, u64::from(p.shares.as_u32()));
            }
            Message::CrossTrade(q) => {
                self.apply_trade(q.header.stock_locate, q.cross_price, q.shares);
            }
            Message::OrderExecutedWithPrice(c) => {
                if matches!(c.printable, Printable::Printable) {
                    self.apply_trade(
                        c.header.stock_locate,
                        c.execution_price,
                        u64::from(c.executed_shares.as_u32()),
                    );
                }
            }
            // `E` Order Executed prints at the resting order's display
            // price, which the OHLCV path cannot resolve without the
            // book. NASDAQ's paired `P` print covers the same trade —
            // see the module-level "E vs P" note.
            Message::OrderExecuted(_) => {}
            // Non-trade lifecycle / book / session messages — no
            // OHLCV effect. Listed explicitly so new ITCH revisions
            // surface as compile errors.
            Message::SystemEvent(_) => {}
            Message::StockDirectory(_) => {}
            Message::StockTradingAction(_) => {}
            Message::RegShoRestriction(_) => {}
            Message::MarketParticipantPosition(_) => {}
            Message::MwcbDeclineLevel(_) => {}
            Message::MwcbStatus(_) => {}
            Message::IpoQuotingPeriodUpdate(_) => {}
            Message::AddOrder(_) => {}
            Message::AddOrderWithMpid(_) => {}
            Message::OrderCancel(_) => {}
            Message::OrderDelete(_) => {}
            Message::OrderReplace(_) => {}
            Message::BrokenTrade(_) => {}
            Message::Noii(_) => {}
            Message::RetailPriceImprovement(_) => {}
        }
    }

    /// Internal helper: fold a single printable trade into the bar
    /// for `locate`, creating the bar on first sight.
    #[inline]
    fn apply_trade(&mut self, locate: StockLocate, price: Price4, shares: u64) {
        self.per_symbol
            .entry(locate)
            .and_modify(|bar| {
                if price > bar.high {
                    bar.high = price;
                }
                if price < bar.low {
                    bar.low = price;
                }
                bar.close = price;
                bar.volume = bar.volume.saturating_add(shares);
                bar.trade_count = bar.trade_count.saturating_add(1);
            })
            .or_insert(OhlcvBar {
                stock_locate: locate,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: shares,
                trade_count: 1,
            });
    }
}

// Justification for `saturating_add` above: `volume` and
// `trade_count` are observability counters, not protocol-state
// counters. The protocol-state-counter rule in the global rules
// (no `saturating_*` / `wrapping_*`) targets sequence numbers and
// retry counters — overflow on a per-symbol daily share total is
// astronomically improbable (the universe has fewer atoms than the
// total `u64` share count) and saturating is preferable to a
// silently-wrapped value or a panic in a long-running aggregator.

#[cfg(test)]
mod tests {
    use super::*;
    use itch_protocol::{
        AddOrder, CrossTrade, CrossType, Header, MatchNumber, OrderExecuted,
        OrderExecutedWithPrice, OrderReference, Shares, Side, Stock, Timestamp, TrackingNumber,
        TradeNonCross,
    };

    fn header_for(locate: u16) -> Header {
        Header {
            stock_locate: StockLocate::from_u16(locate),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(32_400_000_000_000),
        }
    }

    fn p(locate: u16, price: u32, shares: u32, match_number: u64) -> Message {
        Message::TradeNonCross(TradeNonCross {
            header: header_for(locate),
            order_ref: OrderReference::from_u64(0),
            side: Side::Buy,
            shares: Shares::from_u32(shares),
            stock: Stock::default(),
            price: Price4::from_u32(price),
            match_number: MatchNumber::from_u64(match_number),
        })
    }

    fn q(locate: u16, price: u32, shares: u64, match_number: u64) -> Message {
        Message::CrossTrade(CrossTrade {
            header: header_for(locate),
            shares,
            stock: Stock::default(),
            cross_price: Price4::from_u32(price),
            match_number: MatchNumber::from_u64(match_number),
            cross_type: CrossType::Closing,
        })
    }

    fn c(locate: u16, price: u32, shares: u32, printable: Printable) -> Message {
        Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
            header: header_for(locate),
            order_ref: OrderReference::from_u64(1),
            executed_shares: Shares::from_u32(shares),
            match_number: MatchNumber::from_u64(1),
            printable,
            execution_price: Price4::from_u32(price),
        })
    }

    #[test]
    fn empty_accumulator_has_no_bars() {
        let acc = OhlcvAccumulator::new();
        assert_eq!(acc.len(), 0);
        assert!(acc.is_empty());
        assert!(acc.bar(StockLocate::from_u16(1)).is_none());
        assert_eq!(acc.bars().count(), 0);
    }

    #[test]
    fn single_p_trade_seeds_bar_with_equal_ohlc() {
        let mut acc = OhlcvAccumulator::new();
        acc.apply(&p(1, 1_900_000, 100, 1));

        let bar = acc.bar(StockLocate::from_u16(1)).expect("bar present");
        assert_eq!(bar.open, Price4::from_u32(1_900_000));
        assert_eq!(bar.high, Price4::from_u32(1_900_000));
        assert_eq!(bar.low, Price4::from_u32(1_900_000));
        assert_eq!(bar.close, Price4::from_u32(1_900_000));
        assert_eq!(bar.volume, 100);
        assert_eq!(bar.trade_count, 1);
    }

    #[test]
    fn five_p_trades_advance_high_low_close_keep_open() {
        // Sequence: 1900, 1950 (new high), 1880 (new low), 1910, 1920.
        // open == 1900, high == 1950, low == 1880, close == 1920,
        // volume == 100 + 200 + 300 + 400 + 500 = 1500, count == 5.
        let mut acc = OhlcvAccumulator::new();
        for (price, shares, mn) in [
            (1_900_000u32, 100u32, 1u64),
            (1_950_000, 200, 2),
            (1_880_000, 300, 3),
            (1_910_000, 400, 4),
            (1_920_000, 500, 5),
        ] {
            acc.apply(&p(1, price, shares, mn));
        }
        let bar = acc.bar(StockLocate::from_u16(1)).expect("bar present");
        assert_eq!(bar.open, Price4::from_u32(1_900_000));
        assert_eq!(bar.high, Price4::from_u32(1_950_000));
        assert_eq!(bar.low, Price4::from_u32(1_880_000));
        assert_eq!(bar.close, Price4::from_u32(1_920_000));
        assert_eq!(bar.volume, 1_500);
        assert_eq!(bar.trade_count, 5);
    }

    #[test]
    fn c_non_printable_does_not_update_bar() {
        let mut acc = OhlcvAccumulator::new();
        acc.apply(&c(1, 1_900_000, 100, Printable::NonPrintable));
        assert!(acc.bar(StockLocate::from_u16(1)).is_none());
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn c_printable_updates_bar() {
        let mut acc = OhlcvAccumulator::new();
        acc.apply(&c(1, 1_900_000, 100, Printable::Printable));
        let bar = acc.bar(StockLocate::from_u16(1)).expect("bar present");
        assert_eq!(bar.close, Price4::from_u32(1_900_000));
        assert_eq!(bar.volume, 100);
        assert_eq!(bar.trade_count, 1);
    }

    #[test]
    fn q_cross_trade_updates_bar_like_regular_trade() {
        let mut acc = OhlcvAccumulator::new();
        acc.apply(&p(1, 1_900_000, 100, 1));
        acc.apply(&q(1, 1_930_000, 1_000, 2));

        let bar = acc.bar(StockLocate::from_u16(1)).expect("bar present");
        assert_eq!(bar.open, Price4::from_u32(1_900_000));
        assert_eq!(bar.high, Price4::from_u32(1_930_000));
        assert_eq!(bar.low, Price4::from_u32(1_900_000));
        assert_eq!(bar.close, Price4::from_u32(1_930_000));
        assert_eq!(bar.volume, 1_100);
        assert_eq!(bar.trade_count, 2);
    }

    #[test]
    fn e_order_executed_is_no_op() {
        let mut acc = OhlcvAccumulator::new();
        let msg = Message::OrderExecuted(OrderExecuted {
            header: header_for(1),
            order_ref: OrderReference::from_u64(1),
            executed_shares: Shares::from_u32(100),
            match_number: MatchNumber::from_u64(1),
        });
        acc.apply(&msg);
        assert!(acc.bar(StockLocate::from_u16(1)).is_none());
    }

    #[test]
    fn add_order_is_no_op() {
        let mut acc = OhlcvAccumulator::new();
        let msg = Message::AddOrder(AddOrder {
            header: header_for(1),
            order_ref: OrderReference::from_u64(1),
            side: Side::Buy,
            shares: Shares::from_u32(100),
            stock: Stock::default(),
            price: Price4::from_u32(1_900_000),
        });
        acc.apply(&msg);
        assert!(acc.bar(StockLocate::from_u16(1)).is_none());
    }

    #[test]
    fn multi_symbol_isolation() {
        let mut acc = OhlcvAccumulator::new();
        acc.apply(&p(1, 1_900_000, 100, 1));
        acc.apply(&p(2, 3_500_000, 200, 2));
        acc.apply(&p(1, 1_950_000, 50, 3));

        assert_eq!(acc.len(), 2);
        let bar1 = acc.bar(StockLocate::from_u16(1)).expect("locate 1");
        let bar2 = acc.bar(StockLocate::from_u16(2)).expect("locate 2");

        assert_eq!(bar1.open, Price4::from_u32(1_900_000));
        assert_eq!(bar1.close, Price4::from_u32(1_950_000));
        assert_eq!(bar1.volume, 150);
        assert_eq!(bar1.trade_count, 2);

        assert_eq!(bar2.open, Price4::from_u32(3_500_000));
        assert_eq!(bar2.close, Price4::from_u32(3_500_000));
        assert_eq!(bar2.volume, 200);
        assert_eq!(bar2.trade_count, 1);
    }
}
