//! Single-symbol L2 (price-level) order-book reconstruction.
//!
//! An [`L2Book`] aggregates resting orders into per-price share
//! totals on each side. It is **per-symbol**: the multi-symbol manager
//! lives in a separate crate iteration (see `docs/ROADMAP.md` v0.5,
//! issue #38) and indexes a `HashMap<StockLocate, L2Book>`.
//!
//! The apply path is sync, allocation-bounded, and matches every
//! [`Message`] variant explicitly so new ITCH revisions surface as
//! compile errors.

use std::collections::{BTreeMap, HashMap};

use itch_protocol::{Message, OrderReference, Price4, Side, StockLocate};

use crate::error::BookError;

/// Per-order bookkeeping entry stored under each
/// [`OrderReference`] in the book index.
///
/// # Note
///
/// Exposed `pub` for ergonomic re-export from the crate root, but
/// **not part of the stable public surface**. Treat this struct as an
/// implementation detail — its shape may change in any minor release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderEntry {
    /// Resting limit price of the order.
    pub price: Price4,
    /// Remaining open shares on the order.
    pub shares: u32,
    /// Buy or sell side.
    pub side: Side,
    /// Symbol identifier the order belongs to.
    pub stock_locate: StockLocate,
}

/// Single-symbol L2 (price-level) book.
///
/// Maintains two `BTreeMap<Price4, u64>` (one per side, total shares
/// per price level) and a `HashMap<OrderReference, OrderEntry>` order
/// index used to resolve per-order mutations
/// (`E` / `C` / `X` / `D` / `U`) without scanning the levels.
///
/// The book is pinned to one [`StockLocate`] at construction time;
/// messages for other symbols are silently no-ops, so an [`L2Book`]
/// can be driven by a global multi-symbol stream without filtering
/// upstream.
#[derive(Debug, Clone)]
pub struct L2Book {
    stock_locate: StockLocate,
    bids: BTreeMap<Price4, u64>,
    asks: BTreeMap<Price4, u64>,
    orders: HashMap<OrderReference, OrderEntry>,
}

impl L2Book {
    /// Construct an empty book pinned to `stock_locate`.
    #[inline]
    #[must_use]
    pub fn new(stock_locate: StockLocate) -> Self {
        Self {
            stock_locate,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
        }
    }

    /// Symbol identifier this book is pinned to.
    #[inline]
    #[must_use]
    pub fn stock_locate(&self) -> StockLocate {
        self.stock_locate
    }

    /// Highest bid price level and its total shares, if any.
    ///
    /// Returns `None` when the bid side is empty.
    #[inline]
    #[must_use]
    pub fn best_bid(&self) -> Option<(Price4, u64)> {
        self.bids.iter().next_back().map(|(p, s)| (*p, *s))
    }

    /// Lowest ask price level and its total shares, if any.
    ///
    /// Returns `None` when the ask side is empty.
    #[inline]
    #[must_use]
    pub fn best_ask(&self) -> Option<(Price4, u64)> {
        self.asks.iter().next().map(|(p, s)| (*p, *s))
    }

    /// Iterate price levels on `side` from best to worst.
    ///
    /// `Side::Buy` yields the bid book in **descending** price order
    /// (best bid first). `Side::Sell` yields the ask book in
    /// **ascending** price order (best ask first).
    pub fn levels(&self, side: Side) -> Box<dyn Iterator<Item = (Price4, u64)> + '_> {
        match side {
            Side::Buy => Box::new(self.bids.iter().rev().map(|(p, s)| (*p, *s))),
            Side::Sell => Box::new(self.asks.iter().map(|(p, s)| (*p, *s))),
        }
    }

    /// Number of open orders currently in the index.
    #[inline]
    #[must_use]
    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    /// Number of distinct price levels on `side`.
    #[inline]
    #[must_use]
    pub fn levels_count(&self, side: Side) -> usize {
        match side {
            Side::Buy => self.bids.len(),
            Side::Sell => self.asks.len(),
        }
    }

    /// Apply one ITCH 5.0 message to the book.
    ///
    /// The 20 [`Message`] variants are matched exhaustively. Variants
    /// that have no L2 effect (`S`, `R`, `H`, `Y`, `L`, `V`, `W`, `K`,
    /// `P`, `Q`, `B`, `I`, `N`) return `Ok(())` immediately. The eight
    /// book-mutating variants are documented in the table at
    /// [the issue tracker](https://github.com/joaquinbejar/itch-rs/issues/36).
    ///
    /// Messages for a different `stock_locate` (or referencing an
    /// order that is not in the index) are silently dropped — this
    /// lets the same book be driven by a global multi-symbol stream.
    ///
    /// # Errors
    ///
    /// - [`BookError::OverExecution`] if `E` / `C` / `X` would remove
    ///   more shares than the resting order has.
    /// - [`BookError::UnknownOrderRef`] is reserved for callers that
    ///   pre-filter the stream to a single symbol and still see a
    ///   message for a missing order; the global-stream path silently
    ///   drops such messages.
    pub fn apply(&mut self, msg: &Message) -> Result<(), BookError> {
        match msg {
            // ----- L2-mutating variants -----
            Message::AddOrder(m) => {
                if m.header.stock_locate != self.stock_locate {
                    return Ok(());
                }
                self.add_order(m.order_ref, m.side, m.shares.as_u32(), m.price);
                Ok(())
            }
            Message::AddOrderWithMpid(m) => {
                if m.header.stock_locate != self.stock_locate {
                    return Ok(());
                }
                // Attribution / MPID is irrelevant for L2.
                self.add_order(m.order_ref, m.side, m.shares.as_u32(), m.price);
                Ok(())
            }
            Message::OrderExecuted(m) => self.execute(m.order_ref, m.executed_shares.as_u32()),
            Message::OrderExecutedWithPrice(m) => {
                // Execution price on `C` is the trade print, not a
                // resting level price — only the share count moves
                // the book. The `printable` flag is irrelevant here.
                self.execute(m.order_ref, m.executed_shares.as_u32())
            }
            Message::OrderCancel(m) => self.execute(m.order_ref, m.cancelled_shares.as_u32()),
            Message::OrderDelete(m) => {
                self.delete(m.order_ref);
                Ok(())
            }
            Message::OrderReplace(m) => self.replace(
                m.original_order_ref,
                m.new_order_ref,
                m.shares.as_u32(),
                m.price,
            ),

            // ----- session / informational variants — no L2 effect -----
            Message::SystemEvent(_) => Ok(()), // S — no-op for L2
            Message::StockDirectory(_) => Ok(()), // R — no-op for L2
            Message::StockTradingAction(_) => Ok(()), // H — no-op for L2
            Message::RegShoRestriction(_) => Ok(()), // Y — no-op for L2
            Message::MarketParticipantPosition(_) => Ok(()), // L — no-op for L2
            Message::MwcbDeclineLevel(_) => Ok(()), // V — no-op for L2
            Message::MwcbStatus(_) => Ok(()),  // W — no-op for L2
            Message::IpoQuotingPeriodUpdate(_) => Ok(()), // K — no-op for L2
            Message::TradeNonCross(_) => Ok(()), // P — no-op for L2 (trade print only)
            Message::CrossTrade(_) => Ok(()),  // Q — no-op for L2 (cross print only)
            Message::BrokenTrade(_) => Ok(()), // B — no-op for L2
            Message::Noii(_) => Ok(()),        // I — no-op for L2
            Message::RetailPriceImprovement(_) => Ok(()), // N — no-op for L2
        }
    }

    // -----------------------------------------------------------------
    // Internal mutators
    // -----------------------------------------------------------------

    fn add_order(&mut self, order_ref: OrderReference, side: Side, shares: u32, price: Price4) {
        // Insert into the order index.
        self.orders.insert(
            order_ref,
            OrderEntry {
                price,
                shares,
                side,
                stock_locate: self.stock_locate,
            },
        );
        // Add to the level total.
        let level = match side {
            Side::Buy => self.bids.entry(price).or_insert(0),
            Side::Sell => self.asks.entry(price).or_insert(0),
        };
        *level = level.saturating_add(u64::from(shares));
    }

    fn execute(&mut self, order_ref: OrderReference, shares: u32) -> Result<(), BookError> {
        // Resolve the order. If it isn't in the index, this message is
        // for a different symbol's book — silently drop.
        let entry = match self.orders.get_mut(&order_ref) {
            Some(e) => e,
            None => return Ok(()),
        };

        if shares > entry.shares {
            return Err(BookError::OverExecution {
                order: order_ref,
                executed: shares,
                remaining: entry.shares,
            });
        }

        entry.shares -= shares;
        let exhausted = entry.shares == 0;
        let price = entry.price;
        let side = entry.side;

        // Reduce the level total.
        Self::reduce_level(
            match side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            },
            price,
            u64::from(shares),
        );

        if exhausted {
            self.orders.remove(&order_ref);
        }
        Ok(())
    }

    fn delete(&mut self, order_ref: OrderReference) {
        let entry = match self.orders.remove(&order_ref) {
            Some(e) => e,
            None => return, // belongs to a different symbol's book
        };
        Self::reduce_level(
            match entry.side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            },
            entry.price,
            u64::from(entry.shares),
        );
    }

    fn replace(
        &mut self,
        original_ref: OrderReference,
        new_ref: OrderReference,
        new_shares: u32,
        new_price: Price4,
    ) -> Result<(), BookError> {
        // Look up the original. If it's missing, this replace is for a
        // different book's symbol — drop silently.
        let original = match self.orders.remove(&original_ref) {
            Some(e) => e,
            None => return Ok(()),
        };

        // Remove the original's full remaining shares from its level.
        Self::reduce_level(
            match original.side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            },
            original.price,
            u64::from(original.shares),
        );

        // Insert the replacement under the new ref, preserving side.
        self.add_order(new_ref, original.side, new_shares, new_price);
        Ok(())
    }

    /// Subtract `shares` from `levels[price]`, removing the entry when
    /// it reaches zero. Saturates at zero defensively — the codec
    /// invariant says we never over-subtract once the order index has
    /// been validated, but the saturation keeps the book non-negative
    /// even on a dirty capture.
    fn reduce_level(levels: &mut BTreeMap<Price4, u64>, price: Price4, shares: u64) {
        if let Some(level) = levels.get_mut(&price) {
            *level = level.saturating_sub(shares);
            if *level == 0 {
                levels.remove(&price);
            }
        }
    }
}
