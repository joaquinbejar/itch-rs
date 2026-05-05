//! `IteratorSource` — `MessageSource` over any
//! `IntoIterator<Item = Message>`.
//!
//! The swiss-army-knife adapter for tests, fixtures, and replays.
//! Pair it with [`crate::canonical_session`] to feed the demo
//! session through any transport built on `MessageSource`.
//!
//! Clean end-of-stream is `Ready(None)`.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use itch_protocol::Message;

use crate::SourceError;

/// `MessageSource` over an `Iterator<Item = Message>`.
///
/// `Send + Unpin` is required of the wrapped iterator so the
/// resulting source can be dyn-dispatched as a
/// [`crate::MessageSource`].
pub struct IteratorSource<I>
where
    I: Iterator<Item = Message> + Send + Unpin,
{
    iter: I,
}

impl<I> IteratorSource<I>
where
    I: Iterator<Item = Message> + Send + Unpin,
{
    /// Wrap any `IntoIterator<Item = Message>`.
    pub fn new<II>(iter: II) -> Self
    where
        II: IntoIterator<Item = Message, IntoIter = I>,
    {
        Self {
            iter: iter.into_iter(),
        }
    }
}

impl<I> Stream for IteratorSource<I>
where
    I: Iterator<Item = Message> + Send + Unpin,
{
    type Item = Result<Message, SourceError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.iter.next().map(Ok))
    }
}

impl From<Vec<Message>> for IteratorSource<std::vec::IntoIter<Message>> {
    fn from(v: Vec<Message>) -> Self {
        Self::new(v)
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
    async fn iterator_source_yields_then_returns_none() {
        let messages = vec![
            msg(EventCode::StartOfMessages),
            msg(EventCode::EndOfMessages),
        ];
        let mut src = IteratorSource::new(messages.clone());
        let collected: Vec<_> = src.by_ref().collect().await;
        assert_eq!(collected.len(), 2);
        for (i, item) in collected.iter().enumerate() {
            let m = item.as_ref().unwrap();
            assert_eq!(*m, messages[i]);
        }
        assert!(src.next().await.is_none());
    }

    #[tokio::test]
    async fn iterator_source_from_vec() {
        let v = vec![msg(EventCode::StartOfMessages)];
        let src: IteratorSource<_> = v.into();
        let collected: Vec<_> = src.collect().await;
        assert_eq!(collected.len(), 1);
    }

    #[tokio::test]
    async fn iterator_source_from_canonical_session() {
        use crate::canonical_session;
        let src = IteratorSource::new(canonical_session());
        let collected: Vec<_> = src.collect().await;
        // canonical_session covers all 20 ITCH 5.0 message kinds plus
        // session-lifecycle markers; just check we drained without
        // error (length is locked by canonical_session's own tests).
        assert!(collected.len() >= 20);
        for item in &collected {
            assert!(item.is_ok());
        }
    }

    #[test]
    fn iterator_source_is_message_source() {
        let src = IteratorSource::new(Vec::<Message>::new());
        let _: &dyn MessageSource = &src;
    }
}
