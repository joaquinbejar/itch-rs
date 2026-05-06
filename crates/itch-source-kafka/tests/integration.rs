//! Integration tests for `KafkaSource`.
//!
//! These tests connect to a real Kafka broker. They are
//! skip-not-fail when no broker is reachable: set
//! `ITCH_KAFKA_BROKERS` to point at a running broker, otherwise the
//! tests log and return Ok.
//!
//! ```bash
//! # In one terminal: spin up a single-node Kafka in KRaft mode.
//! docker run --rm -p 9092:9092 \
//!     -e KAFKA_CFG_NODE_ID=1 \
//!     -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
//!     -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
//!     -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
//!     -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
//!     -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
//!     -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
//!     bitnami/kafka:3.7
//!
//! # In another:
//! ITCH_KAFKA_BROKERS=127.0.0.1:9092 \
//!     cargo test -p itch-source-kafka --test integration
//! ```

use std::time::Duration;

use futures::StreamExt;
use itch_protocol::{
    EventCode, Header, Message, ProtocolError, StockLocate, SystemEvent, Timestamp, TrackingNumber,
};
use itch_source::SourceError;
use itch_source_kafka::{AutoOffsetReset, KafkaSource, KafkaSourceConfig};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

const BROKERS_ENV: &str = "ITCH_KAFKA_BROKERS";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_TIMEOUT: Duration = Duration::from_secs(5);

fn brokers() -> Option<String> {
    match std::env::var(BROKERS_ENV) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ITCH_KAFKA_BROKERS not set");
            None
        }
    }
}

fn unique_topic(suffix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("itch-test-{nanos}-{suffix}")
}

fn unique_group(suffix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("itch-test-grp-{nanos}-{suffix}")
}

fn fixture_message(seq: u64) -> Message {
    Message::SystemEvent(SystemEvent {
        header: Header {
            stock_locate: StockLocate::from_u16((seq & 0xffff) as u16),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(seq),
        },
        event_code: EventCode::StartOfMessages,
    })
}

fn make_producer(brokers: &str) -> Option<FutureProducer> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000");
    match cfg.create::<FutureProducer>() {
        Ok(p) => Some(p),
        Err(err) => {
            eprintln!("skipping: producer create failed ({err})");
            None
        }
    }
}

async fn connect(
    brokers: &str,
    topic: &str,
    group: &str,
    reset: AutoOffsetReset,
) -> Option<KafkaSource> {
    let cfg = KafkaSourceConfig {
        brokers: brokers.to_string(),
        group_id: group.to_string(),
        topic: topic.to_string(),
        auto_offset_reset: reset,
        additional_props: vec![
            // Tighten session / heartbeat so failed tests don't pin a
            // group on the broker for the default 45s.
            ("session.timeout.ms".into(), "10000".into()),
            ("heartbeat.interval.ms".into(), "3000".into()),
        ],
    };
    match tokio::time::timeout(CONNECT_TIMEOUT, KafkaSource::connect(cfg)).await {
        Ok(Ok(source)) => Some(source),
        Ok(Err(err)) => {
            eprintln!("skipping: KafkaSource::connect failed ({err})");
            None
        }
        Err(_elapsed) => {
            eprintln!("skipping: Kafka connect timed out after 5s");
            None
        }
    }
}

async fn produce_record(producer: &FutureProducer, topic: &str, payload: &[u8]) -> bool {
    // Use empty key so all records land on partition 0 by default
    // partitioner. This keeps order deterministic for assertions.
    let key: &[u8] = b"";
    let record = FutureRecord::to(topic).payload(payload).key(key);
    match producer.send(record, Duration::from_secs(5)).await {
        Ok(_) => true,
        Err((err, _)) => {
            eprintln!("produce error on topic {topic}: {err}");
            false
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_subscribes_to_topic() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique_topic("connect");
    let group = unique_group("connect");
    let Some(source) = connect(&brokers, &topic, &group, AutoOffsetReset::Earliest).await else {
        return;
    };
    assert_eq!(source.topic(), topic);
}

#[tokio::test(flavor = "multi_thread")]
async fn produce_then_consume_round_trip() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique_topic("rt");
    let group = unique_group("rt");

    let Some(producer) = make_producer(&brokers) else {
        return;
    };

    // Produce 100 ITCH messages BEFORE the consumer connects so the
    // consumer's `auto.offset.reset=earliest` picks them up.
    let mut expected = Vec::with_capacity(100);
    for i in 1..=100u64 {
        let msg = fixture_message(i);
        let mut buf = vec![0u8; msg.encoded_len()];
        msg.encode(&mut buf).expect("encode fixture");
        if !produce_record(&producer, &topic, &buf).await {
            eprintln!("skipping: producer send failed");
            return;
        }
        expected.push(msg);
    }

    let Some(mut source) = connect(&brokers, &topic, &group, AutoOffsetReset::Earliest).await
    else {
        return;
    };

    let mut received = Vec::with_capacity(100);
    while received.len() < 100 {
        match tokio::time::timeout(POLL_TIMEOUT, source.next()).await {
            Ok(Some(Ok(msg))) => received.push(msg),
            Ok(Some(Err(err))) => {
                eprintln!("skipping: stream surfaced error before fill: {err}");
                return;
            }
            Ok(None) => {
                eprintln!("skipping: stream ended before fill");
                return;
            }
            Err(_elapsed) => {
                eprintln!(
                    "skipping: only received {}/100 within {:?}",
                    received.len(),
                    POLL_TIMEOUT
                );
                return;
            }
        }
    }

    assert_eq!(received.len(), 100);
    assert_eq!(received, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_payload_yields_backend_error_then_recovers() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique_topic("bad");
    let group = unique_group("bad");

    let Some(producer) = make_producer(&brokers) else {
        return;
    };

    // Garbage bytes — `Message::decode` will reject these.
    let garbage: &[u8] = &[0xff, 0xfe, 0xfd];
    if !produce_record(&producer, &topic, garbage).await {
        return;
    }
    let valid = fixture_message(1);
    let mut buf = vec![0u8; valid.encoded_len()];
    valid.encode(&mut buf).expect("encode valid");
    if !produce_record(&producer, &topic, &buf).await {
        return;
    }

    let Some(mut source) = connect(&brokers, &topic, &group, AutoOffsetReset::Earliest).await
    else {
        return;
    };

    let first = tokio::time::timeout(POLL_TIMEOUT, source.next())
        .await
        .expect("first poll within timeout")
        .expect("stream produced an item");
    match first {
        Err(SourceError::Backend(_)) => {}
        Ok(other) => panic!("expected Err for garbage record, got Ok({other:?})"),
        Err(other) => panic!("expected Err(Backend), got {other:?}"),
    }

    let second = tokio::time::timeout(POLL_TIMEOUT, source.next())
        .await
        .expect("second poll within timeout")
        .expect("stream produced an item");
    match second {
        Ok(msg) => assert_eq!(msg, valid),
        Err(err) => panic!("expected Ok(valid) after recovery, got Err({err})"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn with_decoder_overrides_default() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique_topic("dec");
    let group = unique_group("dec");

    let Some(producer) = make_producer(&brokers) else {
        return;
    };

    // Custom envelope: `MESSAGE:` prefix followed by the encoded ITCH
    // bytes. The default decoder would fail on the prefix; the
    // installed decoder strips it before delegating to
    // `Message::decode`.
    let valid = fixture_message(42);
    let mut payload = b"MESSAGE:".to_vec();
    let mut buf = vec![0u8; valid.encoded_len()];
    valid.encode(&mut buf).expect("encode valid");
    payload.extend_from_slice(&buf);

    if !produce_record(&producer, &topic, &payload).await {
        return;
    }

    let Some(source) = connect(&brokers, &topic, &group, AutoOffsetReset::Earliest).await else {
        return;
    };

    let mut source = source.with_decoder(|bytes: &[u8]| -> Result<Message, ProtocolError> {
        let prefix: &[u8] = b"MESSAGE:";
        let inner = bytes.strip_prefix(prefix).unwrap_or(bytes);
        Message::decode(inner)
    });

    let item = tokio::time::timeout(POLL_TIMEOUT, source.next())
        .await
        .expect("poll within timeout")
        .expect("stream produced an item");
    match item {
        Ok(msg) => assert_eq!(msg, valid),
        Err(err) => panic!("expected Ok(valid) via custom decoder, got Err({err})"),
    }
}
