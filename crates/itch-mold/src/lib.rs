//! MoldUDP64 V1.00 multicast transport for `itch-protocol`.
//!
//! This crate implements the wire codec, receiver, gap-recovery
//! client, request server, and publisher for NASDAQ's MoldUDP64
//! V1.00 transport — see `docs/TRANSPORT-SPEC.md` §4 and
//! [ADR-0010](https://github.com/joaquinbejar/itch-rs/blob/main/docs/adr/0010-mold-gap-recovery.md).
//!
//! ```text
//! ┌──────────────────────────────┬─────────────────────────────────────┐
//! │       Header (20 bytes)      │   N Message Blocks (variable)       │
//! └──────────────────────────────┴─────────────────────────────────────┘
//!
//! Header
//! ┌──────────────────┬──────────────────┬───────────────────┐
//! │ Session (10 B)   │ SeqNo (8 B u64)  │ MsgCount (2 B u16)│
//! └──────────────────┴──────────────────┴───────────────────┘
//! ```
//!
//! ## Public surface
//!
//! - **Codec** — [`MoldPacket`], [`MoldPacketHeader`],
//!   [`MessageBlock`], plus constants
//!   ([`HEADER_LEN`], [`MSG_COUNT_HEARTBEAT`],
//!   [`MSG_COUNT_END_OF_SESSION`], [`MAX_BLOCK_LEN`],
//!   [`DEFAULT_PACKING_MTU`]).
//! - **Errors** — [`MoldError`], `#[non_exhaustive]`,
//!   `#[from]` over `std::io::Error` and
//!   [`itch_protocol::ProtocolError`].
//! - **Receiver / publisher / request server** — land in subsequent
//!   issues (#21–#26).
//!
//! ## Bounded buffering
//!
//! [`MAX_BLOCK_LEN`] caps the inner ITCH message length at 1 KiB to
//! match `itch-tcp` / `itch-soup`. A peer cannot force unbounded
//! buffering at the codec layer.
//!
//! ## Stream-poison resistance
//!
//! On a malformed packet the receiver task drops the offending
//! datagram, surfaces a single typed error to the caller, and
//! resumes reading on the next datagram. UDP's datagram boundary
//! makes this trivial — there is no inter-datagram state to
//! resync, only the sequence-number gap state machine in #22.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod codec;
mod error;
mod event;
mod receiver;

pub use codec::{
    session_from_str, MessageBlock, MoldPacket, MoldPacketHeader, BLOCK_LEN_PREFIX,
    DEFAULT_PACKING_MTU, HEADER_LEN, MAX_BLOCK_LEN, MSG_COUNT_END_OF_SESSION, MSG_COUNT_HEARTBEAT,
};
pub use error::MoldError;
pub use event::MoldEvent;
pub use receiver::{
    MoldConfig, MoldStream, DEFAULT_MAX_PENDING, DEFAULT_SILENCE_DEAD_LINK,
    DEFAULT_SILENCE_WARNING, RECV_BUFFER_LEN,
};

/// Convenience alias used across the receiver / publisher API.
pub type MoldResult<T> = Result<T, MoldError>;
