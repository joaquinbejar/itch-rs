use crate::{SeqStore, SourceError, SubscriptionPolicy};
use itch_protocol::Message;
use std::sync::Arc;

/// Warmup policy backed by a `SeqStore`.
///
/// Reads all stored messages (from earliest through latest) and returns them as warmup.
/// Useful for publishing the current L2 book snapshot or end-of-day state to new sessions.
///
/// # Example
///
/// ```no_run
/// use itch_source::{RingBufferSeqStore, WarmupFromSeqStore, SubscriptionPolicy};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = RingBufferSeqStore::new();
/// // ... populate store with messages ...
/// let policy = WarmupFromSeqStore::new(Arc::new(store));
/// let warmup = policy.warmup().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct WarmupFromSeqStore<S: SeqStore> {
    store: Arc<S>,
}

impl<S: SeqStore> WarmupFromSeqStore<S> {
    /// Create a warmup policy from a store.
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl<S: SeqStore> SubscriptionPolicy for WarmupFromSeqStore<S> {
    async fn warmup(&self) -> Result<Vec<Message>, SourceError> {
        let latest = self
            .store
            .latest()
            .await
            .map_err(|e| SourceError::Backend(Box::new(e)))?;

        // Empty store returns 0
        if latest == 0 {
            return Ok(vec![]);
        }

        let earliest = self
            .store
            .earliest()
            .await
            .map_err(|e| SourceError::Backend(Box::new(e)))?;

        // Calculate count: from earliest through latest inclusive
        let count = (latest - earliest + 1) as usize;
        let entries = self
            .store
            .range(earliest, count)
            .await
            .map_err(|e| SourceError::Backend(Box::new(e)))?;

        let messages = entries.into_iter().map(|(_, msg)| msg).collect();
        Ok(messages)
    }
}
