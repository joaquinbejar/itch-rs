use crate::{MessageSource, SourceError};
use futures::Stream;
use itch_protocol::Message;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Interior message + source index for priority-queue ordering.
#[derive(Clone)]
pub struct PendingMessage {
    /// Timestamp from the message (used for ordering).
    pub timestamp: u64,
    /// Index of the source this message came from (for tie-breaking).
    pub source_idx: usize,
    /// The actual ITCH message.
    pub msg: Message,
}

impl PartialEq for PendingMessage {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.source_idx == other.source_idx
    }
}

impl Eq for PendingMessage {}

impl PartialOrd for PendingMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap (smallest timestamp first)
        other
            .timestamp
            .cmp(&self.timestamp)
            .then(other.source_idx.cmp(&self.source_idx))
    }
}

/// Merge multiple message sources by timestamp (smallest first).
///
/// Yields messages from N input sources ordered by timestamp, breaking ties
/// by source order (first source in the input vec wins ties).
/// Implements `MessageSource` so it can be used as a source itself.
///
/// # Example
///
/// ```no_run
/// use itch_source::{ChannelSource, merge_by_timestamp};
/// use tokio::sync::mpsc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let (_tx1, rx1) = mpsc::channel(100);
/// let (_tx2, rx2) = mpsc::channel(100);
///
/// let source1 = ChannelSource::from_receiver(rx1);
/// let source2 = ChannelSource::from_receiver(rx2);
///
/// let merged = merge_by_timestamp(vec![source1, source2]);
/// # Ok(())
/// # }
/// ```
///
/// # Note
///
/// This is a placeholder that demonstrates the structure. A production
/// merge would poll all N sources concurrently and maintain pending
/// messages for each source in the priority queue. For v0.2, this
/// serves as a documented combinator signature; see issue #58 for
/// full async implementation in v0.3+.
pub struct MergeByTimestamp {
    heap: BinaryHeap<PendingMessage>,
}

impl MergeByTimestamp {
    /// Create an empty merge (for testing/examples).
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    /// Insert a pre-assembled message into the merge queue.
    pub fn push(&mut self, pending: PendingMessage) {
        self.heap.push(pending);
    }
}

impl Default for MergeByTimestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl Stream for MergeByTimestamp {
    type Item = Result<Message, SourceError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Pop next message from heap if available
        if let Some(pending) = self.heap.pop() {
            return Poll::Ready(Some(Ok(pending.msg)));
        }

        // Heap is empty; signal exhaustion
        Poll::Ready(None)
    }
}

/// Helper function to create a merge from multiple sources (placeholder for v0.3).
///
/// # Note
///
/// This is a type-signature placeholder. Full implementation requires
/// concurrent polling of N sources and maintaining pending queues per source.
/// See issue #58 for the full async merge design.
pub fn merge_by_timestamp<S>(_sources: Vec<S>) -> MergeByTimestamp
where
    S: MessageSource + 'static,
{
    MergeByTimestamp::new()
}
