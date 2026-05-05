//! NASDAQ TotalView-ITCH 5.0 message types and binary codec.
//!
//! `itch-protocol` is the leaf crate of the workspace: a strongly
//! typed domain model on top of a hand-rolled big-endian codec,
//! with **no I/O, no `async`, no `tokio`** dependencies. Transports
//! (`itch-tcp`, future `itch-soup`, future `itch-mold`) plug into it
//! via `tokio_util::codec::Framed`.
//!
//! # Public API at a glance
//!
//! - **Primitives** — newtypes for every ITCH 5.0 wire field:
//!   [`Stock`], [`Mpid`], [`Price4`], [`Price8`], [`Timestamp`],
//!   [`StockLocate`], [`TrackingNumber`], [`OrderReference`],
//!   [`MatchNumber`], [`Shares`].
//! - **Enums** — closed-set ASCII codes turned into Rust enums:
//!   [`EventCode`], [`Side`], [`MarketCategory`], [`FinancialStatus`],
//!   [`TradingState`], [`ImbalanceDirection`], [`CrossType`], …
//!   All implement [`AlphaCoded`] for one-byte conversion.
//! - **Messages** — 20 DTOs (one per ITCH 5.0 message kind: `S`, `R`,
//!   `H`, `Y`, `L`, `V`, `W`, `K`, `A`, `F`, `E`, `C`, `X`, `D`, `U`,
//!   `P`, `Q`, `B`, `I`, `N`) plus a 10-byte [`Header`] and the
//!   exhaustive [`Message`] enum.
//! - **Codec** — [`Encode`] and [`Decode`] traits plus
//!   [`Message::encode`]/[`Message::decode`] for whole frames
//!   (1-byte tag + body).
//! - **Errors** — [`ProtocolError`], a `#[non_exhaustive]` leaf
//!   `thiserror` enum with `Truncated`, `BufferTooSmall`,
//!   `UnknownMessageType`, and `InvalidEnumCode { field, code }`.
//!
//! # The `Encode` / `Decode` contract
//!
//! Every message DTO and [`Header`] implements both traits:
//!
//! ```text
//! body_len()       — exact body byte count (excludes the 1-byte tag)
//! encode_body(buf) — write big-endian wire bytes into `buf`
//! decode_body(buf) — parse from a slice, validating every field
//! ```
//!
//! [`Message::encode`] writes `1 + body_len()` bytes (tag + body);
//! [`Message::decode`] parses the same shape and returns the typed
//! variant. A [`ProtocolError::UnknownMessageType`] is returned for
//! any tag outside the closed set.
//!
//! # `no_std`-friendly note
//!
//! Per [ADR-0005](https://github.com/joaquinbejar/itch-rs/blob/main/docs/adr/0005-std-by-default-no-std-opt-in.md),
//! the codec uses only `core::` and stack arithmetic — no `Vec`, no
//! `String`, no allocator on the hot path. v0.1 ships with `std`
//! always on; a `default-features = false` flag lands when the first
//! `no_std` consumer needs it.
//!
//! # Example
//!
//! ```
//! use itch_protocol::{
//!     AddOrder, Decode, Encode, Header, Message, OrderReference, Price4,
//!     Shares, Side, Stock, StockLocate, Timestamp, TrackingNumber,
//! };
//!
//! let original = Message::AddOrder(AddOrder {
//!     header: Header {
//!         stock_locate: StockLocate::from_u16(1),
//!         tracking_number: TrackingNumber::from_u16(0),
//!         timestamp: Timestamp::from_u64(32_400_005_000_000),
//!     },
//!     order_ref: OrderReference::from_u64(1001),
//!     side: Side::Buy,
//!     shares: Shares::from_u32(500),
//!     stock: Stock::new("AAPL"),
//!     price: Price4::from_u32(1_925_000),
//! });
//!
//! let mut buf = vec![0u8; original.encoded_len()];
//! original.encode(&mut buf).unwrap();
//!
//! let decoded = Message::decode(&buf).unwrap();
//! assert_eq!(original, decoded);
//! ```
//!
//! See the workspace `README.md` for the high-level architecture and
//! `docs/PROTOCOL-SPEC.md` for the wire format.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codec;
pub mod enums;
pub mod error;
pub mod messages;
pub mod primitives;

pub use codec::{Decode, Encode};

pub use enums::{
    AlphaCoded, Authenticity, BreachedLevel, CrossType, EventCode, FinancialStatus,
    ImbalanceDirection, IpoReleaseQualifier, LuldTier, MarketCategory, MarketMakerMode,
    MarketParticipantState, PriceVariation, Printable, RegShoAction, RpiInterestFlag, Side,
    TradingState, YesNo,
};
pub use error::ProtocolError;
pub use messages::{
    AddOrder, AddOrderWithMpid, BrokenTrade, CrossTrade, Header, IpoQuotingPeriodUpdate,
    MarketParticipantPosition, Message, MwcbDeclineLevel, MwcbStatus, Noii, OrderCancel,
    OrderDelete, OrderExecuted, OrderExecutedWithPrice, OrderReplace, RegShoRestriction,
    RetailPriceImprovement, StockDirectory, StockTradingAction, SystemEvent, TradeNonCross,
};
pub use primitives::{
    MatchNumber, Mpid, OrderReference, Price4, Price8, Shares, Stock, StockLocate, Timestamp,
    TimestampError, TrackingNumber,
};
