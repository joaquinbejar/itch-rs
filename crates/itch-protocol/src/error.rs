//! Error types for the codec layer.
//!
//! [`ProtocolError`] is the single error type returned by every
//! `itch-protocol` decode / encode operation. The enum is
//! `#[non_exhaustive]` so new structured variants can be added
//! without a major bump.

use thiserror::Error;

/// Codec-layer errors returned by `Encode` / `Decode` and the
/// closed-set [`AlphaCoded`](crate::AlphaCoded) decoders.
///
/// Other fallible APIs in this crate (notably the primitives'
/// constructors such as [`Timestamp::try_new`](crate::Timestamp::try_new))
/// have their own typed errors so they can stay independent of
/// codec evolution.
///
/// `ProtocolError` is `#[non_exhaustive]`: new variants may be added
/// in minor releases as the codec grows. Consumers should match
/// exhaustively with a fallback arm or use the variant-specific
/// helpers.
#[non_exhaustive]
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// A byte intended for one of the closed-set ASCII enums did not
    /// match any known variant. `field` carries the human-readable
    /// name of the enum that failed to decode (e.g., `"Side"`,
    /// `"CrossType"`).
    #[error("invalid enum code 0x{code:02x} for field {field}")]
    InvalidEnumCode {
        /// Name of the enum that rejected the byte.
        field: &'static str,
        /// The offending byte from the wire.
        code: u8,
    },

    /// The decode buffer was shorter than the message body required.
    #[error("decode buffer truncated: needed {need} bytes, got {got}")]
    Truncated {
        /// Number of bytes the codec needed to advance.
        need: usize,
        /// Number of bytes actually available.
        got: usize,
    },

    /// The encode output buffer was shorter than the message body
    /// required.
    #[error("encode buffer too small: needed {need} bytes, got {got}")]
    BufferTooSmall {
        /// Number of bytes the codec needed to write.
        need: usize,
        /// Number of bytes actually available in the output buffer.
        got: usize,
    },

    /// The 1-byte type tag did not match any of the 20 ITCH 5.0
    /// message kinds.
    #[error("unknown ITCH message type tag 0x{0:02x}")]
    UnknownMessageType(
        /// The offending tag byte from the wire.
        u8,
    ),
}
