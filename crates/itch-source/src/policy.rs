use super::SourceError;
use itch_protocol::Message;

/// Subscription policy: controls what messages are sent to each session.
///
/// Current scope (v0.2): **warmup only**. Each session is fed a fixed
/// warmup sequence (e.g., reference data, end-of-day snapshot) before
/// live message stream starts.
///
/// Future scope (v0.3+): entitlement-based filtering per session
/// (per `SessionFilter` design, not yet implemented).
///
/// See `docs/ITCH-SOURCE.md` §3.3 (contract), §5.2 (transport usage),
/// §12.3 (extension example), §13 (FAQ on cache invalidation patterns).
#[async_trait::async_trait]
pub trait SubscriptionPolicy: Send + Sync {
    /// Warmup messages to send to each new session (e.g., reference data, snapshots).
    ///
    /// Warmup is sent in-order before the first live message.
    /// If warmup fails, the session connection is rejected.
    ///
    /// # Auth & Filtering
    ///
    /// This trait is **not responsible** for auth, entitlement, or per-subscriber
    /// filtering. Those concerns belong in transport-level hooks (see ADR-0012 § G,
    /// `docs/ITCH-SOURCE.md` §13). A future `SessionFilter` (v0.3) will handle that.
    async fn warmup(&self) -> Result<Vec<Message>, SourceError>;
}

/// Default warmup policy: fixed message set.
///
/// `StaticPolicy` returns the same warmup sequence for every session.
/// Useful for demos and tests; production typically uses a stateful policy
/// backed by a snapshot service (see issue 58: `WarmupFromSeqStore`).
#[derive(Clone, Debug)]
pub struct StaticPolicy {
    messages: Vec<Message>,
}

impl StaticPolicy {
    /// Create a policy with no warmup.
    pub fn empty() -> Self {
        Self { messages: vec![] }
    }

    /// Create a policy with fixed messages.
    pub fn new(messages: Vec<Message>) -> Self {
        Self { messages }
    }
}

#[async_trait::async_trait]
impl SubscriptionPolicy for StaticPolicy {
    async fn warmup(&self) -> Result<Vec<Message>, SourceError> {
        Ok(self.messages.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_policy_returns_empty() {
        let policy = StaticPolicy::empty();
        let warmup = policy.warmup().await;
        assert!(warmup.is_ok());
        assert!(warmup.unwrap().is_empty());
    }

    #[tokio::test]
    async fn new_policy_returns_messages() {
        let msg = Message::SystemEvent(itch_protocol::SystemEvent {
            header: itch_protocol::Header {
                stock_locate: itch_protocol::primitives::StockLocate::from_u16(0),
                tracking_number: itch_protocol::primitives::TrackingNumber::from_u16(0),
                timestamp: itch_protocol::primitives::Timestamp::from_u64(0),
            },
            event_code: itch_protocol::EventCode::StartOfMessages,
        });

        let messages = vec![msg];
        let policy = StaticPolicy::new(messages.clone());
        let warmup = policy.warmup().await;
        assert!(warmup.is_ok());
        assert_eq!(warmup.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn policy_is_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<StaticPolicy>();
    }

    #[tokio::test]
    async fn concurrent_warmup_calls() {
        let msg = Message::SystemEvent(itch_protocol::SystemEvent {
            header: itch_protocol::Header {
                stock_locate: itch_protocol::primitives::StockLocate::from_u16(0),
                tracking_number: itch_protocol::primitives::TrackingNumber::from_u16(0),
                timestamp: itch_protocol::primitives::Timestamp::from_u64(0),
            },
            event_code: itch_protocol::EventCode::StartOfMessages,
        });

        let policy = std::sync::Arc::new(StaticPolicy::new(vec![msg]));

        let mut tasks = vec![];
        for _ in 0..100 {
            let p = policy.clone();
            tasks.push(tokio::spawn(async move { p.warmup().await }));
        }

        for task in tasks {
            let result = task.await.unwrap();
            assert!(result.is_ok());
            assert_eq!(result.unwrap().len(), 1);
        }
    }
}
