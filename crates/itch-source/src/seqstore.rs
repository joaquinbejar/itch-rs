use itch_protocol::Message;

/// Durable frame storage for gap recovery (async trait).
///
/// Used by transports to persist frames so they can answer
/// retransmission requests (MoldUDP64) or reconnect-with-resume
/// (SoupBinTCP).
///
/// Implementations are provided in issue #50.
#[async_trait::async_trait]
pub trait SeqStore: Send + Sync {
    /// Error type for store operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a message at sequence number.
    async fn store(&self, seq: u64, msg: Message) -> Result<(), Self::Error>;

    /// Retrieve messages in sequence range [since, until) (exclusive upper bound).
    async fn range(&self, since: u64, until: u64) -> Result<Vec<(u64, Message)>, Self::Error>;

    /// Get earliest stored sequence number (or None if store is empty).
    async fn earliest(&self) -> Result<Option<u64>, Self::Error>;

    /// Get latest stored sequence number (or None if store is empty).
    async fn latest(&self) -> Result<Option<u64>, Self::Error>;
}
