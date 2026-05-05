use futures::Stream;
use itch_protocol::Message;
use std::error::Error;

/// Error type for ITCH message sources.
///
/// # Variants
///
/// - `Exhausted` — source produced fewer messages than expected; stream is now `None`.
/// - `Backend(Box<dyn Error>)` — backing store failed; may be transient or fatal.
/// - `Invariant(&'static str)` — source internal contract violated (treat as panic-level).
///
/// # Errors
///
/// Every `MessageSource` yield either `Ok(Message)` or `Err(SourceError)`.
/// Once `Exhausted` or `Invariant` is returned, subsequent yields are `None`.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// Source ended prematurely (no more messages available).
    #[error("source exhausted prematurely")]
    Exhausted,

    /// Backing store failed (file, DB, network, etc.).
    #[error("source backend error: {0}")]
    Backend(#[source] Box<dyn Error + Send + Sync + 'static>),

    /// Source internal invariant violated.
    #[error("invariant violated: {0}")]
    Invariant(&'static str),
}

/// Marker trait for ITCH message sources.
///
/// Any `Stream<Item = Result<Message, SourceError>> + Send + Unpin` automatically
/// implements `MessageSource` via a blanket impl. This design keeps the trait
/// ergonomic for users who already speak `futures::Stream` and custom message adapters.
///
/// # Contract
///
/// - Messages must be yielded in **publication order** (oldest first).
/// - Messages **must not be dropped or reordered** mid-stream.
/// - Sequence numbers are owned by the **transport**, not the source.
///
/// # Backpressure
///
/// Sources are not responsible for rate-limiting; the async runtime handles backpressure.
/// If the sink (e.g., broadcast channel) is slow, the source drains via channel buffering.
///
/// See [`itch_protocol::Message`] and `docs/ITCH-SOURCE.md` § 3.1, 7, 8 for details.
pub trait MessageSource: Stream<Item = Result<Message, SourceError>> + Send + Unpin {}

impl<T> MessageSource for T where T: Stream<Item = Result<Message, SourceError>> + Send + Unpin {}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::{self, StreamExt};
    use itch_protocol::enums::EventCode;
    use itch_protocol::messages::{Header, SystemEvent};

    #[test]
    fn blanket_impl_iter_stream() {
        let m1 = Message::SystemEvent(SystemEvent {
            header: Header::default(),
            event_code: EventCode::StartOfMessages,
        });
        let m2 = Message::SystemEvent(SystemEvent {
            header: Header::default(),
            event_code: EventCode::EndOfMessages,
        });
        let _src: Box<dyn MessageSource> = Box::new(stream::iter(vec![Ok(m1), Ok(m2)]));
    }

    #[test]
    fn error_exhausted() {
        // A stream with Exhausted is a valid MessageSource
        let err = SourceError::Exhausted;
        let _src: Box<dyn MessageSource> = Box::new(stream::iter(vec![Err(err)]));
    }

    #[tokio::test]
    async fn box_dyn_message_source() {
        // Object-safe: even though MessageSource is a marker trait,
        // it can be boxed and stored
        let src: Box<dyn MessageSource> = Box::new(stream::iter(vec![]));
        let mut pinned = Box::pin(src);
        assert!(pinned.next().await.is_none());
    }
}
