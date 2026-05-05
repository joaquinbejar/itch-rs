//! Error types for the codec layer.
//!
//! [`ProtocolError`] is the single error type returned by every
//! `itch-protocol` decode / encode operation. The enum is
//! `#[non_exhaustive]` so new structured variants can be added
//! without a major bump.

use thiserror::Error;

/// Every fallible `itch-protocol` operation returns this enum.
///
/// New variants may be added in minor releases; consumers should
/// match exhaustively with a fallback arm or use the variant-specific
/// helpers exposed elsewhere in the crate.
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
}
