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

    /// FIFO queue invariant violation detected while applying an `E`
    /// or `C` execution.
    ///
    /// Per the ITCH 5.0 spec, a full execution always exhausts the
    /// order at the *front* of its price-level queue. The L3 book
    /// pops the front of the queue when an order's remaining shares
    /// reach zero and asserts that the popped reference matches the
    /// executed order. A mismatch indicates either a malformed
    /// capture or a bug in the apply rules — surface it instead of
    /// silently corrupting queue priority.
    ///
    /// The L2 book never raises this variant; the queue invariant is
    /// L3-only.
    #[error("FIFO violation: expected front-of-queue order {expected:?}, popped {got:?}")]
    FifoViolation {
        /// Order reference the apply rule expected at the front of
        /// the level queue (the order whose remaining hit zero).
        expected: OrderReference,
        /// Order reference actually found at the front of the queue.
        got: OrderReference,
    },
}
