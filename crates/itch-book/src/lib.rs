//! Order-book reconstruction (L2 price levels) from a stream of
//! NASDAQ TotalView-ITCH 5.0 messages.
//!
//! `itch-book` consumes decoded [`Message`](itch_protocol::Message)
//! values — produced by `itch-protocol` and any of the sibling
//! transports — and maintains a per-symbol level-2 book: total shares
//! aggregated per price level on each side, backed by an order-
//! reference index that resolves per-order mutations
//! (`E` / `C` / `X` / `D` / `U`) without scanning the levels.
//!
//! # Scope (v0.2)
//!
//! - **L2 price-level book** for a single symbol — see [`L2Book`].
//! - **L3 per-order book** with FIFO queue priority — see [`L3Book`].
//! - **Multi-symbol manager** indexed by
//!   [`StockLocate`](itch_protocol::StockLocate) — see
//!   [`BookManager`].
//! - **Per-symbol OHLCV accumulator** over printable trade prints —
//!   see [`OhlcvAccumulator`]. Drives the v0.4 acceptance EOD
//!   validation harness.
//! - Sync apply path; no transport dependency, no allocator on the
//!   steady-state hot path beyond a bounded `BTreeMap` / `HashMap`
//!   insert per new order.
//! - Exhaustive match over every [`Message`](itch_protocol::Message)
//!   variant. New ITCH revisions surface as compile errors.
//!
//! # Out of scope
//!
//! - Republication as a `MessageSource` — see `docs/ROADMAP.md` v0.5
//!   and ADR-0007. The book reconstructions described here are the
//!   foundation those layers build on.
//!
//! # Module layout
//!
//! - [`book`] — [`L2Book`] and the per-order [`OrderEntry`] record.
//! - [`l3`] — [`L3Book`] and the [`L3OrderEntry`] record (FIFO queue
//!   priority, `summary_l2()` cross-check).
//! - [`manager`] — [`BookManager`], the multi-symbol router.
//! - [`ohlcv`] — [`OhlcvAccumulator`] and [`OhlcvBar`], the per-
//!   symbol open/high/low/close + volume + trade-count summary over
//!   printable trade prints.
//! - [`error`] — [`BookError`], the only error type returned by
//!   [`L2Book::apply`], [`L3Book::apply`], and
//!   [`BookManager::apply`].
//!
//! # Example
//!
//! ```
//! use itch_book::L2Book;
//! use itch_protocol::{
//!     AddOrder, Header, Message, OrderReference, Price4, Shares, Side, Stock,
//!     StockLocate, Timestamp, TrackingNumber,
//! };
//!
//! let stock_locate = StockLocate::from_u16(42);
//! let mut book = L2Book::new(stock_locate);
//!
//! let add = Message::AddOrder(AddOrder {
//!     header: Header {
//!         stock_locate,
//!         tracking_number: TrackingNumber::from_u16(0),
//!         timestamp: Timestamp::from_u64(32_400_000_000_000),
//!     },
//!     order_ref: OrderReference::from_u64(1),
//!     side: Side::Buy,
//!     shares: Shares::from_u32(500),
//!     stock: Stock::new("AAPL"),
//!     price: Price4::from_u32(1_925_000),
//! });
//!
//! book.apply(&add).expect("clean add");
//! assert_eq!(book.best_bid(), Some((Price4::from_u32(1_925_000), 500)));
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod book;
pub mod error;
pub mod l3;
pub mod manager;
pub mod ohlcv;

pub use book::{L2Book, OrderEntry};
pub use error::BookError;
pub use l3::{L3Book, L3OrderEntry};
pub use manager::BookManager;
pub use ohlcv::{OhlcvAccumulator, OhlcvBar};
