use super::SourceError;
use itch_protocol::Message;

/// Subscription policy: controls what messages are sent to each session.
///
/// Current scope (v0.1): **warmup only**. Each session is fed a fixed
/// warmup sequence (e.g., reference data, end-of-day snapshot) before
/// live message stream starts.
///
/// Future scope (v0.3+): entitlement-based filtering per session
/// (per `SessionFilter` design, not yet implemented).
///
/// Implementations are provided in issue #51.
#[async_trait::async_trait]
pub trait SubscriptionPolicy: Send + Sync {
    /// Warmup messages to send to each new session (e.g., reference data, snapshots).
    ///
    /// Warmup is sent in-order before the first live message.
    /// If warmup fails, the session connection is rejected.
    async fn warmup(&self) -> Result<Vec<Message>, SourceError>;
}
