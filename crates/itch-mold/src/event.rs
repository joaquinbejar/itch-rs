//! Public `MoldEvent` enum yielded by [`MoldStream`].
//!
//! Per ADR-0010, the receiver is a `Stream<Item = MoldResult<MoldEvent>>`
//! rather than a `Stream<Item = MoldResult<Message>>`. Heartbeats,
//! end-of-session, and observability gap events are first-class
//! variants so callers can wire them into metrics / dashboards
//! without re-implementing the gap state machine.

use itch_protocol::Message;

/// Events surfaced by the MoldUDP64 receiver.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoldEvent {
    /// One in-order ITCH message decoded from a sequenced data
    /// block.
    Message {
        /// Sequence number assigned by the publisher (the first
        /// block of the first packet starts at 1; each subsequent
        /// block increments by 1).
        sequence: u64,
        /// Decoded inner ITCH message.
        message: Message,
    },

    /// Heartbeat packet (`MsgCount = 0`). Carries the next-expected
    /// sequence number from the publisher's point of view; useful
    /// for observability and silent-link detection.
    Heartbeat {
        /// Next-expected sequence number per the heartbeat header.
        next_seq: u64,
    },

    /// End-of-session packet (`MsgCount = 0xFFFF`). The receiver
    /// emits this **once**, then the stream resolves to `None` on
    /// subsequent polls.
    EndOfSession {
        /// Next-expected sequence number per the EOS header.
        next_seq: u64,
    },

    /// Gap detected on the wire — the receiver observed a packet
    /// whose first sequence number was greater than the locally
    /// tracked next-expected. Pure observability — gap recovery
    /// (request server, retransmission) lands in subsequent issues.
    Gap {
        /// First missing sequence number (inclusive).
        from: u64,
        /// Last missing sequence number (inclusive).
        to: u64,
    },
}

impl MoldEvent {
    /// Convenience accessor: returns the inner [`Message`] if this
    /// event is a `MoldEvent::Message`, else `None`.
    #[must_use]
    pub fn as_message(&self) -> Option<&Message> {
        match self {
            Self::Message { message, .. } => Some(message),
            _ => None,
        }
    }
}
