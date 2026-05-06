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
//! # Scope (v0.1)
//!
//! - **L2 price-level book** for a single symbol — see [`L2Book`].
//! - Sync apply path; no transport dependency, no allocator on the
//!   steady-state hot path beyond a bounded `BTreeMap` / `HashMap`
//!   insert per new order.
//! - Exhaustive match over every [`Message`](itch_protocol::Message)
//!   variant. New ITCH revisions surface as compile errors.
//!
//! # Out of scope (v0.1)
//!
//! - **L3 per-order book** with queue priority — issue #37.
//! - **Multi-symbol book manager** indexed by
//!   [`StockLocate`](itch_protocol::StockLocate) — issue #38.
//! - Republication as a `MessageSource` — see `docs/ROADMAP.md` v0.5
//!   and ADR-0007. The L2 reconstruction described here is the
//!   foundation those layers build on.
//!
//! # Module layout
//!
//! - [`book`] — [`L2Book`] and the per-order [`OrderEntry`] record.
//! - [`error`] — [`BookError`], the only error type returned by
//!   [`L2Book::apply`].
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

pub use book::{L2Book, OrderEntry};
pub use error::BookError;
