use crate::SourceError;
use futures::Stream;
use itch_protocol::Message;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Broadcast one `MessageSource` to multiple consumers via `tokio::sync::broadcast`.
///
/// `Tee` wraps a source stream and broadcasts each message to N listeners.
/// Useful for publishing the same feed on multiple transports (e.g., SoupBinTCP + MoldUDP64).
///
/// # Example
///
/// ```no_run
/// use itch_source::{ChannelSource, Tee};
/// use tokio::sync::mpsc;
/// use futures::stream::StreamExt;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let (tx, rx) = mpsc::channel(100);
/// let source = ChannelSource::from_receiver(rx);
/// let tee = Tee::new(Box::pin(source));
///
/// // Two consumers of the same broadcast
/// let mut sub1 = tee.subscribe();
/// let mut sub2 = tee.subscribe();
///
/// # Ok(())
/// # }
/// ```
pub struct Tee {
    source: Pin<Box<dyn Stream<Item = Result<Message, SourceError>> + Send + Unpin>>,
    broadcast: tokio::sync::broadcast::Sender<Message>,
}

impl Tee {
    /// Create a new broadcast tee from a pinned boxed stream.
    pub fn new(
        source: Pin<Box<dyn Stream<Item = Result<Message, SourceError>> + Send + Unpin>>,
    ) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(100);
        Self {
            source,
            broadcast: tx,
        }
    }

    /// Subscribe to the broadcast (returns a receiver).
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Message> {
        self.broadcast.subscribe()
    }
}

impl Stream for Tee {
    type Item = Result<Message, SourceError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.source.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                // Broadcast to all subscribers, but don't fail if no one is listening
                let _ = self.broadcast.send(msg);
                Poll::Ready(Some(Ok(msg)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
