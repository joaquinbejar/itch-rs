//! `ChannelSource` — `MessageSource` over a `tokio::sync::mpsc`
//! channel.
//!
//! The pragmatic real-world adapter: a producer pushes messages
//! into the channel, the publisher consumes via the
//! [`crate::MessageSource`] trait. Backpressure is handled by tokio
//! — a slow consumer naturally throttles the producer (bounded
//! variant only).
//!
//! Two flavors:
//!
//! - [`ChannelSource::bounded`] — bounded mpsc, returns the
//!   `Sender` for the producer to push into.
//! - [`ChannelSource::unbounded`] — unbounded mpsc; returns an
//!   [`UnboundedChannelSource`] paired with the matching sender.
//!
//! Clean end-of-stream is `Ready(None)` (sender dropped), not
//! `SourceError::Exhausted`. `Exhausted` is reserved for
//! recoverable / premature termination.
//!
//! See `docs/ITCH-SOURCE.md` §4 and §6.2 (matching-engine
//! adapter).

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use itch_protocol::Message;
use tokio::sync::mpsc;

use crate::SourceError;

/// `MessageSource` over a bounded `tokio::sync::mpsc` channel.
///
/// Construct via [`ChannelSource::bounded`].
pub struct ChannelSource {
    rx: mpsc::Receiver<Message>,
}

impl ChannelSource {
    /// Wrap an existing bounded `Receiver`.
    #[must_use]
    pub fn from_receiver(rx: mpsc::Receiver<Message>) -> Self {
        Self { rx }
    }

    /// Construct a bounded channel source. Returns the `Sender` for
    /// the producer to push into and the source for the publisher
    /// to drain.
    ///
    /// `capacity` is forwarded to [`tokio::sync::mpsc::channel`].
    #[must_use]
    pub fn bounded(capacity: usize) -> (mpsc::Sender<Message>, Self) {
        let (tx, rx) = mpsc::channel(capacity);
        (tx, Self { rx })
    }

    /// Construct an unbounded channel source. Returns the
    /// [`UnboundedSender`](mpsc::UnboundedSender) for the producer
    /// to push into and the source for the publisher to drain.
    ///
    /// Prefer [`Self::bounded`] for production code — unbounded
    /// channels disable backpressure.
    #[must_use]
    pub fn unbounded() -> (mpsc::UnboundedSender<Message>, UnboundedChannelSource) {
        let (tx, rx) = mpsc::unbounded_channel();
        (tx, UnboundedChannelSource { rx })
    }
}

impl Stream for ChannelSource {
    type Item = Result<Message, SourceError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// `MessageSource` over an unbounded `tokio::sync::mpsc` channel.
///
/// Construct via [`ChannelSource::unbounded`].
pub struct UnboundedChannelSource {
    rx: mpsc::UnboundedReceiver<Message>,
}

impl Stream for UnboundedChannelSource {
    type Item = Result<Message, SourceError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MessageSource;
    use futures::StreamExt;
    use itch_protocol::{enums::EventCode, messages::SystemEvent, Header};

    fn msg(code: EventCode) -> Message {
        Message::SystemEvent(SystemEvent {
            header: Header::default(),
            event_code: code,
        })
    }

    #[tokio::test]
    async fn bounded_drains_then_returns_none_when_sender_drops() {
        let (tx, mut src) = ChannelSource::bounded(8);
        for code in [
            EventCode::StartOfMessages,
            EventCode::StartOfMarketHours,
            EventCode::EndOfMarketHours,
            EventCode::EndOfMessages,
        ] {
            tx.send(msg(code)).await.unwrap();
        }
        drop(tx);

        let collected: Vec<_> = src.by_ref().collect().await;
        assert_eq!(collected.len(), 4);
        for item in &collected {
            assert!(item.is_ok());
        }
        assert!(src.next().await.is_none(), "post-drop poll must be None");
    }

    #[tokio::test]
    async fn bounded_capacity_enforced() {
        let (tx, _src) = ChannelSource::bounded(2);
        tx.send(msg(EventCode::StartOfMessages)).await.unwrap();
        tx.send(msg(EventCode::StartOfMarketHours)).await.unwrap();
        let third = tx.try_send(msg(EventCode::EndOfMessages));
        assert!(matches!(
            third,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        ));
    }

    #[tokio::test]
    async fn unbounded_drains_then_returns_none_when_sender_drops() {
        let (tx, mut src) = ChannelSource::unbounded();
        for code in [EventCode::StartOfMessages, EventCode::EndOfMessages] {
            tx.send(msg(code)).unwrap();
        }
        drop(tx);

        let mut count = 0;
        while let Some(item) = src.next().await {
            assert!(item.is_ok());
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn channel_source_is_message_source() {
        let (_tx, src) = ChannelSource::bounded(1);
        let _: &dyn MessageSource = &src;
    }

    #[test]
    fn unbounded_channel_source_is_message_source() {
        let (_tx, src) = ChannelSource::unbounded();
        let _: &dyn MessageSource = &src;
    }
}
