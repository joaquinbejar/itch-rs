# itch-source-kafka

Kafka-backed [`MessageSource`] for ITCH 5.0 publishers. Consumes a topic
where each record value is a serialized ITCH message, decodes via
`Message::decode` (or a user-supplied decoder), and yields
`Result<Message, SourceError>` so it slots into any
`itch-source` consumer for free via the marker-trait blanket impl.

## Overview

`KafkaSource` is a thin adapter over [`rdkafka`'s `StreamConsumer`].
It is intended for fan-out scenarios where an upstream service
publishes ITCH messages to a topic and one or more publishers
(SoupBinTCP, MoldUDP64, replay) downstream consume from it.

## Quick Start

```rust,no_run
use futures::StreamExt;
use itch_source_kafka::{AutoOffsetReset, KafkaSource, KafkaSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = KafkaSourceConfig {
        brokers: "localhost:9092".to_string(),
        group_id: "itch-publisher".to_string(),
        topic: "itch.us.equities.v1".to_string(),
        auto_offset_reset: AutoOffsetReset::Latest,
        additional_props: vec![],
    };

    let mut source = KafkaSource::connect(config).await?;

    while let Some(item) = source.next().await {
        match item {
            Ok(msg) => println!("got: {:?}", msg),
            Err(err) => eprintln!("source error: {err}"),
        }
    }

    Ok(())
}
```

## Build dependency

`rdkafka` is enabled with the `cmake-build` feature, which compiles
`librdkafka` from source via CMake at build time. This eliminates the
need for a system-installed `librdkafka` and yields a portable build,
at the cost of roughly two extra minutes on the first compile (the
build is cached afterwards). For environments where a system
`librdkafka` is acceptable, this dependency choice can be revisited.

## Auto-commit policy (v0.1)

The consumer is configured with `enable.auto.commit=true`, so offsets
are checkpointed by `librdkafka` on its background timer (5 s by
default). For at-least-once redelivery semantics on a publisher
crash, downgrade to manual commit by passing
`("enable.auto.commit", "false")` in `additional_props` and call
`StoreOffsets` / `Commit` from your own logic — note that the v0.1
public API does not expose the underlying consumer for this. A
follow-up issue tracks ergonomic manual-commit support.

## Custom decoders

By default, every record value is fed straight into `Message::decode`.
For envelopes (length-prefixed, framed, or otherwise wrapped), use
`KafkaSource::with_decoder` to install a closure that strips the
envelope before decoding the inner ITCH bytes:

```rust,no_run
use itch_protocol::Message;
use itch_source_kafka::{AutoOffsetReset, KafkaSource, KafkaSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = KafkaSourceConfig {
        brokers: "localhost:9092".to_string(),
        group_id: "itch-publisher".to_string(),
        topic: "itch.framed.v1".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        additional_props: vec![],
    };

    let _source = KafkaSource::connect(config).await?.with_decoder(|bytes| {
        // Strip a literal "MESSAGE:" prefix before delegating.
        let prefix = b"MESSAGE:";
        let inner = bytes.strip_prefix(prefix.as_ref()).unwrap_or(bytes);
        Message::decode(inner)
    });

    Ok(())
}
```

## Testing

Integration tests target a real Kafka broker. They are skip-not-fail
when no broker is reachable: set `ITCH_KAFKA_BROKERS` to point at one,
otherwise the tests log a "skipping" line and return Ok.

```bash
# In one terminal:
docker run --rm -p 9092:9092 \
    -e KAFKA_CFG_NODE_ID=1 \
    -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
    -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
    -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
    -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    bitnami/kafka:3.7

# In another:
ITCH_KAFKA_BROKERS=127.0.0.1:9092 \
    cargo test -p itch-source-kafka --test integration
```

Without `ITCH_KAFKA_BROKERS` (CI default) every test prints a
"skipping" line and returns Ok within ~100 ms; CI is therefore green
on a host with no Kafka.

## References

- `docs/ITCH-SOURCE.md` § 4: `MessageSource` blanket impl
- `docs/adr/0012-data-source-abstraction.md`: source abstraction

[`MessageSource`]: https://docs.rs/itch-source/latest/itch_source/source/trait.MessageSource.html
[`rdkafka`'s `StreamConsumer`]: https://docs.rs/rdkafka/latest/rdkafka/consumer/struct.StreamConsumer.html
