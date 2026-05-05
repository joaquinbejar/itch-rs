//! `NullSeqStore` — no-op `SeqStore` for transports that do not
//! need reconnect / retransmission.

use crate::SeqStore;
use itch_protocol::Message;

/// `SeqStore` implementation that drops every `store` call and
/// returns empty / `0` from every read.
///
/// Use when the transport does not need to answer retransmission
/// requests — e.g., `itch-tcp` demos, one-shot file replayers, or
/// downstream consumers that handle gap recovery elsewhere.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSeqStore;

impl NullSeqStore {
    /// Construct a new `NullSeqStore`.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl SeqStore for NullSeqStore {
    type Error = std::convert::Infallible;

    async fn store(&self, _seq: u64, _msg: &Message) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn range(&self, _from: u64, _count: usize) -> Result<Vec<(u64, Message)>, Self::Error> {
        Ok(Vec::new())
    }

    async fn latest(&self) -> Result<u64, Self::Error> {
        Ok(0)
    }

    async fn earliest(&self) -> Result<u64, Self::Error> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::assert_seq_store_contract;

    #[tokio::test]
    async fn null_seq_store_satisfies_contract() {
        let store = NullSeqStore::new();
        assert_seq_store_contract(&store).await;
    }
}
