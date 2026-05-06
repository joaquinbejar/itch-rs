#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use itch_protocol::{Message, ProtocolError};
use itch_source::SourceError;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message as RdMessage;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Initial offset behaviour for a brand-new consumer group.
///
/// Maps directly onto the rdkafka `auto.offset.reset` property:
/// `Earliest` → `"earliest"` (replay from the start of the topic),
/// `Latest` → `"latest"` (only consume records published after the
/// consumer joins).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AutoOffsetReset {
    /// Start from the earliest available offset (replay history).
    Earliest,
    /// Start from the latest available offset (live tail only).
    Latest,
}

impl AutoOffsetReset {
    /// Render the variant as the rdkafka string literal.
    #[inline]
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            AutoOffsetReset::Earliest => "earliest",
            AutoOffsetReset::Latest => "latest",
        }
    }
}

/// Configuration for [`KafkaSource`].
///
/// `brokers`, `group_id`, and `topic` are required and have no
/// defaults. `additional_props` is a pass-through into the underlying
/// rdkafka [`ClientConfig`]; any value supplied there overrides the
/// defaults applied by [`KafkaSource::connect`] (`enable.auto.commit`
/// and `auto.offset.reset`).
#[derive(Clone, Debug)]
pub struct KafkaSourceConfig {
    /// Comma-separated bootstrap servers (rdkafka `bootstrap.servers`).
    pub brokers: String,
    /// Consumer group ID (rdkafka `group.id`).
    pub group_id: String,
    /// Topic to subscribe to.
    pub topic: String,
    /// Initial offset behaviour for a brand-new consumer group.
    pub auto_offset_reset: AutoOffsetReset,
    /// Pass-through key / value pairs for arbitrary rdkafka client
    /// properties (e.g. `("security.protocol", "SSL")`).
    pub additional_props: Vec<(String, String)>,
}

/// Errors that can occur while constructing a [`KafkaSource`].
///
/// Per-record errors observed during streaming are surfaced as
/// `Err(SourceError::Backend(...))` on the `Stream` rather than via
/// this enum, which is reserved for setup-time failures.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum KafkaSourceError {
    /// Underlying `rdkafka` error (client construction, subscribe, etc.).
    #[error("kafka error: {0}")]
    Kafka(#[from] KafkaError),

    /// Failure decoding a record body during the initial probe.
    /// Per-record decode failures during streaming are surfaced as
    /// `SourceError::Backend(...)` and do not appear here.
    #[error("decode error: {0}")]
    Decode(ProtocolError),

    /// Caller-supplied configuration was rejected before any I/O.
    #[error("invalid configuration: {reason}")]
    Config {
        /// Static description of the rejected configuration.
        reason: &'static str,
    },
}

/// Decoder closure type: maps a record payload (`&[u8]`) to a
/// `Message` or a `ProtocolError`. Stored as a boxed `dyn` to allow
/// runtime composition of envelope-stripping pipelines on top of
/// `Message::decode`.
type Decoder = Box<dyn Fn(&[u8]) -> Result<Message, ProtocolError> + Send + Sync>;

/// Kafka-backed [`itch_source::MessageSource`].
///
/// Wraps an [`rdkafka::consumer::StreamConsumer`] subscribed to a
/// single topic. Each polled record runs through the configured
/// decoder (default: [`Message::decode`]) and is yielded as
/// `Ok(Message)`. Per-record decode failures or transport errors
/// surface as `Err(SourceError::Backend(_))` and the stream continues
/// on the next poll — recovery is rdkafka's responsibility, not the
/// source's.
///
/// # Lifetime contract
///
/// `KafkaSource` owns the underlying [`StreamConsumer`]. Each call to
/// `Stream::poll_next` creates a fresh
/// [`rdkafka::consumer::MessageStream`] borrowing the consumer for the
/// duration of that poll, polls it once, and drops it before returning.
/// rdkafka guarantees the consumer's internal queue persists across
/// these short-lived `MessageStream` lifetimes, so no records are
/// dropped between polls.
///
/// `Send` and `Unpin` are derived from the inner consumer, satisfying
/// the [`itch_source::MessageSource`] marker trait via the blanket impl.
pub struct KafkaSource {
    consumer: StreamConsumer,
    /// Topic name retained for diagnostics and resubscribe.
    topic: String,
    decoder: Decoder,
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl KafkaSource {
    /// Connect to Kafka, build the consumer, and subscribe to the
    /// configured topic.
    ///
    /// Sensible defaults applied to the rdkafka [`ClientConfig`]:
    /// - `bootstrap.servers` ← `config.brokers`
    /// - `group.id`          ← `config.group_id`
    /// - `enable.auto.commit` ← `"true"` (offsets auto-committed on
    ///   the rdkafka background timer; documented in the README)
    /// - `auto.offset.reset` ← derived from `config.auto_offset_reset`
    ///
    /// `config.additional_props` is applied last and may override any
    /// of the above keys.
    ///
    /// On success, an `INFO` event is logged with the topic name and
    /// group id.
    ///
    /// # Errors
    ///
    /// - [`KafkaSourceError::Config`] if any required string
    ///   (`brokers`, `group_id`, `topic`) is empty.
    /// - [`KafkaSourceError::Kafka`] if rdkafka fails to construct
    ///   the consumer or to subscribe to the topic.
    pub async fn connect(config: KafkaSourceConfig) -> Result<Self, KafkaSourceError> {
        if config.brokers.is_empty() {
            return Err(KafkaSourceError::Config {
                reason: "brokers must not be empty",
            });
        }
        if config.group_id.is_empty() {
            return Err(KafkaSourceError::Config {
                reason: "group_id must not be empty",
            });
        }
        if config.topic.is_empty() {
            return Err(KafkaSourceError::Config {
                reason: "topic must not be empty",
            });
        }

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", config.auto_offset_reset.as_str());

        for (k, v) in &config.additional_props {
            client_config.set(k.as_str(), v.as_str());
        }

        let consumer: StreamConsumer = client_config.create()?;
        consumer.subscribe(&[config.topic.as_str()])?;

        info!(
            topic = %config.topic,
            group_id = %config.group_id,
            auto_offset_reset = config.auto_offset_reset.as_str(),
            "KafkaSource subscribed"
        );

        Ok(Self {
            consumer,
            topic: config.topic,
            decoder: Box::new(default_decoder),
        })
    }

    /// Replace the default decoder.
    ///
    /// The default decoder is `Message::decode`. Custom decoders are
    /// useful when the upstream producer wraps the ITCH bytes in an
    /// outer envelope (length prefix, headers, framing). The decoder
    /// runs on each record's payload before it is yielded by the
    /// `Stream`; a `ProtocolError` returned from the decoder becomes a
    /// `SourceError::Backend(...)` on the stream.
    #[must_use]
    pub fn with_decoder<F>(mut self, decoder: F) -> Self
    where
        F: Fn(&[u8]) -> Result<Message, ProtocolError> + Send + Sync + 'static,
    {
        self.decoder = Box::new(decoder);
        self
    }

    /// Topic name this source is subscribed to.
    #[inline]
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

#[inline]
fn default_decoder(bytes: &[u8]) -> Result<Message, ProtocolError> {
    Message::decode(bytes)
}

impl Stream for KafkaSource {
    type Item = Result<Message, SourceError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Borrow the consumer per poll to obtain a fresh
        // `MessageStream`. rdkafka holds the actual record queue on
        // the consumer itself, so this short-lived borrow does not
        // drop pending records — see "Lifetime contract" on
        // `KafkaSource`.
        let this = self.get_mut();
        let mut stream = this.consumer.stream();
        match Pin::new(&mut stream).poll_next(cx) {
            Poll::Ready(Some(Ok(borrowed))) => {
                let payload = match borrowed.payload() {
                    Some(p) => p,
                    None => {
                        warn!(topic = %this.topic, "kafka record had no payload");
                        return Poll::Ready(Some(Err(SourceError::Backend(
                            "kafka record had no payload".to_string().into(),
                        ))));
                    }
                };

                match (this.decoder)(payload) {
                    Ok(msg) => {
                        debug!(
                            topic = %this.topic,
                            partition = borrowed.partition(),
                            offset = borrowed.offset(),
                            payload_len = payload.len(),
                            "kafka record decoded"
                        );
                        Poll::Ready(Some(Ok(msg)))
                    }
                    Err(err) => {
                        warn!(
                            topic = %this.topic,
                            partition = borrowed.partition(),
                            offset = borrowed.offset(),
                            error = %err,
                            "kafka record decode failed"
                        );
                        Poll::Ready(Some(Err(SourceError::Backend(
                            format!("decode error: {err}").into(),
                        ))))
                    }
                }
            }
            Poll::Ready(Some(Err(err))) => {
                warn!(topic = %this.topic, error = %err, "kafka consumer error");
                Poll::Ready(Some(Err(SourceError::Backend(
                    format!("kafka error: {err}").into(),
                ))))
            }
            // rdkafka's `MessageStream` never returns `Ready(None)` in
            // practice — the consumer drives indefinitely. Map it to
            // end-of-stream defensively in case a future rdkafka
            // revision changes the contract.
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_offset_reset_renders_to_rdkafka_strings() {
        assert_eq!(AutoOffsetReset::Earliest.as_str(), "earliest");
        assert_eq!(AutoOffsetReset::Latest.as_str(), "latest");
    }

    #[test]
    fn auto_offset_reset_eq_and_copy() {
        let a = AutoOffsetReset::Earliest;
        let b = a; // Copy
        assert_eq!(a, b);
        assert_ne!(AutoOffsetReset::Earliest, AutoOffsetReset::Latest);
    }

    #[tokio::test]
    async fn connect_rejects_empty_brokers() {
        let cfg = KafkaSourceConfig {
            brokers: String::new(),
            group_id: "g".into(),
            topic: "t".into(),
            auto_offset_reset: AutoOffsetReset::Latest,
            additional_props: vec![],
        };
        let err = KafkaSource::connect(cfg).await.expect_err("must error");
        assert!(matches!(
            err,
            KafkaSourceError::Config {
                reason: "brokers must not be empty"
            }
        ));
    }

    #[tokio::test]
    async fn connect_rejects_empty_group_id() {
        let cfg = KafkaSourceConfig {
            brokers: "127.0.0.1:9092".into(),
            group_id: String::new(),
            topic: "t".into(),
            auto_offset_reset: AutoOffsetReset::Latest,
            additional_props: vec![],
        };
        let err = KafkaSource::connect(cfg).await.expect_err("must error");
        assert!(matches!(
            err,
            KafkaSourceError::Config {
                reason: "group_id must not be empty"
            }
        ));
    }

    #[tokio::test]
    async fn connect_rejects_empty_topic() {
        let cfg = KafkaSourceConfig {
            brokers: "127.0.0.1:9092".into(),
            group_id: "g".into(),
            topic: String::new(),
            auto_offset_reset: AutoOffsetReset::Latest,
            additional_props: vec![],
        };
        let err = KafkaSource::connect(cfg).await.expect_err("must error");
        assert!(matches!(
            err,
            KafkaSourceError::Config {
                reason: "topic must not be empty"
            }
        ));
    }
}
