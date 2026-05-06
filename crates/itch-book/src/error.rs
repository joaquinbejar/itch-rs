//! Errors raised when applying ITCH 5.0 messages to an [`L2Book`].
//!
//! [`L2Book`]: crate::book::L2Book

use itch_protocol::{OrderReference, Side};
use thiserror::Error;

/// Errors raised by [`L2Book::apply`].
///
/// In a multi-symbol stream, [`L2Book`] silently drops messages that
/// reference orders not in its index — those orders belong to a
/// different symbol's book. As a result, [`Self::UnknownOrderRef`] is
/// effectively unreachable for a properly partitioned feed; it remains
/// useful when the caller has pre-filtered the stream to a single
/// symbol and a dirty capture references a missing order.
///
/// [`L2Book`]: crate::book::L2Book
/// [`L2Book::apply`]: crate::book::L2Book::apply
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BookError {
    /// The message references an order that is not in the book index.
    ///
    /// Only returned when the caller has already filtered the stream
    /// to a single symbol — a multi-symbol feed will silently no-op
    /// the message instead.
    #[error("unknown order reference {0:?}")]
    UnknownOrderRef(OrderReference),

    /// An execution / cancel removed more shares than the resting
    /// order had.
    #[error("execution {executed} exceeds remaining {remaining} on order {order:?}")]
    OverExecution {
        /// Reference number of the resting order.
        order: OrderReference,
        /// Shares the message attempted to execute or cancel.
        executed: u32,
        /// Shares remaining on the resting order.
        remaining: u32,
    },

    /// A trade or replace claimed a different side than the resting
    /// order — kept for completeness; not produced by the v0.1 apply
    /// rules but reserved for future cross-validation.
    #[error("side mismatch on order {order:?}: expected {expected:?}, got {got:?}")]
    MismatchedSide {
        /// Reference number of the resting order.
        order: OrderReference,
        /// Side recorded when the order was added.
        expected: Side,
        /// Side carried by the offending message.
        got: Side,
    },
}
