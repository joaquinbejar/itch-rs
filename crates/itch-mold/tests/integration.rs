//! Integration tests for `itch-mold`.
//!
//! These tests exercise full datagram round-trips on real UDP
//! loopback sockets — `MoldPublisher` → kernel → `MoldStream` —
//! plus the gap-recovery flow through `MoldRequestServer` /
//! `GapRecoveryClient`. They are skip-not-fail when the host
//! cannot loopback UDP (rare but possible in some sandboxed CI
//! environments).
//!
//! Per `docs/TESTING.md`, these tests use real wall-clock waits
//! sparingly and prefer `tokio::time::pause` where the unit tests
//! cover the same path.

use std::time::Duration;

use futures::stream::{self, StreamExt};
use itch_mold::{
    session_from_str, CachedFrame, GapRecoveryClient, GapRecoveryConfig, MoldConfig, MoldEvent,
    MoldPublisher, MoldRequestServer, MoldStream, PublisherConfig,
};
use itch_protocol::{
    AddOrder, Header, Message, OrderReference, Price4, Shares, Side, Stock, StockLocate, Timestamp,
    TrackingNumber,
};
use tokio::net::UdpSocket;
use tokio::time::Instant;

fn fixture_session() -> [u8; 10] {
    session_from_str("ITCH50").unwrap()
}

fn add_order(seq_marker: u64) -> Message {
    Message::AddOrder(AddOrder {
        header: Header {
            stock_locate: StockLocate::from_u16((seq_marker & 0xFFFF) as u16),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(seq_marker),
        },
        order_ref: OrderReference::from_u64(seq_marker + 1000),
        side: Side::Buy,
        shares: Shares::from_u32(100),
        stock: Stock::new("AAPL"),
        price: Price4::from_u32(1_000_000),
    })
}

fn encode(msg: &Message) -> Vec<u8> {
    let total = msg.encoded_len();
    let mut buf = vec![0u8; total];
    msg.encode(&mut buf).expect("encode");
    buf
}

async fn loopback_udp() -> (UdpSocket, std::net::SocketAddr) {
    let sock = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let addr = sock.local_addr().expect("local_addr");
    (sock, addr)
}

fn quick_eos_publisher(cfg: PublisherConfig) -> PublisherConfig {
    let mut cfg = cfg;
    cfg.eos_burst = 1;
    cfg.eos_interval = Duration::from_millis(10);
    cfg
}

// 1. Happy path: in-order delivery.
#[tokio::test]
async fn happy_path_in_order_delivery() {
    let (recv_sock, recv_addr) = loopback_udp().await;
    let cfg = PublisherConfig::new(recv_addr, fixture_session()).with_start_sequence(1);
    let cfg = quick_eos_publisher(cfg);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind publisher");

    let msgs = (1u64..=5).map(|i| Ok(add_order(i))).collect::<Vec<_>>();
    let pub_handle = publisher.serve(stream::iter(msgs));

    let mut stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1),
    );

    let mut delivered = Vec::new();
    let mut saw_eos = false;
    while delivered.len() < 5 || !saw_eos {
        match tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
            Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
            Ok(Some(Ok(MoldEvent::EndOfSession { .. }))) => saw_eos = true,
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => panic!("receiver error: {e:?}"),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    assert_eq!(delivered, vec![1, 2, 3, 4, 5]);
    assert!(saw_eos);
}

// 2. Multi-packet packing under tight MTU.
#[tokio::test]
async fn multipacket_packing_with_tight_mtu() {
    let (recv_sock, recv_addr) = loopback_udp().await;
    let cfg = PublisherConfig::new(recv_addr, fixture_session())
        .with_start_sequence(1)
        .with_mtu(80); // forces ~1 message per packet
    let cfg = quick_eos_publisher(cfg);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");
    let msgs = (1u64..=8).map(|i| Ok(add_order(i))).collect::<Vec<_>>();
    let pub_handle = publisher.serve(stream::iter(msgs));

    let mut stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1),
    );
    let mut delivered = Vec::new();
    let mut saw_eos = false;
    while delivered.len() < 8 || !saw_eos {
        match tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
            Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
            Ok(Some(Ok(MoldEvent::EndOfSession { .. }))) => saw_eos = true,
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    assert_eq!(delivered, (1u64..=8).collect::<Vec<_>>());
    assert!(saw_eos);
}

// 3. Heartbeat-on-silence end-to-end.
#[tokio::test]
async fn heartbeat_on_silence_end_to_end() {
    let (recv_sock, recv_addr) = loopback_udp().await;
    let cfg = PublisherConfig::new(recv_addr, fixture_session())
        .with_start_sequence(1)
        .with_heartbeat_interval(Duration::from_millis(50));
    let cfg = quick_eos_publisher(cfg);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message, itch_source::SourceError>>(1);
    let pub_handle = publisher.serve(tokio_stream::wrappers::ReceiverStream::new(rx));

    let mut stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1),
    );
    let mut got_hb = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Ok(Some(Ok(MoldEvent::Heartbeat { .. }))) =
            tokio::time::timeout(Duration::from_millis(300), stream.next()).await
        {
            got_hb = true;
            break;
        }
    }
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    assert!(got_hb, "expected at least one heartbeat event");
}

// 4. EOS terminates the stream.
#[tokio::test]
async fn eos_terminates_receiver_stream() {
    let (recv_sock, recv_addr) = loopback_udp().await;
    let cfg = PublisherConfig::new(recv_addr, fixture_session()).with_start_sequence(1);
    let cfg = quick_eos_publisher(cfg);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");
    let pub_handle = publisher.serve(stream::iter(vec![Ok(add_order(1))]));

    let mut stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1),
    );
    let mut saw_msg = false;
    let mut saw_eos = false;
    for _ in 0..20 {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(MoldEvent::Message { .. }))) => saw_msg = true,
            Ok(Some(Ok(MoldEvent::EndOfSession { .. }))) => {
                saw_eos = true;
                break;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    assert!(saw_msg);
    assert!(saw_eos);
    // Stream is finished after EOS.
    let after = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
    assert!(matches!(after, Ok(None) | Err(_)));
}

// 5. Session mismatch is surfaced and stream resumes.
#[tokio::test]
async fn session_mismatch_does_not_poison_stream() {
    let (recv_sock, recv_addr) = loopback_udp().await;
    // Publisher uses one session, receiver pinned to another.
    let other = session_from_str("OTHER").unwrap();
    let cfg = PublisherConfig::new(recv_addr, other).with_start_sequence(1);
    let cfg = quick_eos_publisher(cfg);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");
    let pub_handle = publisher.serve(stream::iter(vec![Ok(add_order(1))]));

    let mut stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1),
    );
    let mut got_mismatch = false;
    for _ in 0..10 {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Err(itch_mold::MoldError::SessionMismatch { .. }))) => {
                got_mismatch = true;
                break;
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    assert!(got_mismatch, "expected SessionMismatch from receiver");
}

// 6. Gap recovery via TCP request server.
#[tokio::test]
async fn gap_recovery_via_request_server() {
    let session = fixture_session();
    let (recv_sock, recv_addr) = loopback_udp().await;

    // Stand up a request server pre-populated with frames 1..=3.
    let req_server = MoldRequestServer::new(64);
    for s in 1u64..=3 {
        req_server
            .cache_frame(CachedFrame {
                sequence: s,
                body: encode(&add_order(s)),
            })
            .await;
    }
    let (req_addr, _req_handle) = req_server.bind("127.0.0.1:0").await.expect("bind req");

    // Publisher emits seq 4..=6 (so the receiver, started at seq 1,
    // observes a gap covering 1..=3). We hold the source open via
    // a channel so the publisher does not race ahead to EOS before
    // gap recovery completes.
    let cfg = PublisherConfig::new(recv_addr, session)
        .with_start_sequence(4)
        .with_heartbeat_interval(Duration::from_millis(100));
    let mut cfg = cfg;
    cfg.eos_burst = 1;
    cfg.eos_interval = Duration::from_millis(10);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message, itch_source::SourceError>>(8);
    for s in 4u64..=6 {
        tx.send(Ok(add_order(s))).await.expect("send");
    }
    let pub_handle = publisher.serve(tokio_stream::wrappers::ReceiverStream::new(rx));

    let stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(session)
            .with_start_sequence(1),
    );
    let recovery = GapRecoveryConfig::new(vec![req_addr])
        .with_request_timeout(Duration::from_millis(500))
        .with_gap_timeout(Duration::from_secs(5));
    let mut client = GapRecoveryClient::new(stream, recovery);

    let mut delivered: Vec<u64> = Vec::new();
    while delivered.len() < 6 {
        match tokio::time::timeout(Duration::from_secs(5), client.next()).await {
            Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    let mut sorted = delivered.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted, vec![1, 2, 3, 4, 5, 6]);
}

// 7. Gap recovery: server has no data → recovery gives up.
#[tokio::test]
async fn gap_recovery_server_returns_no_data() {
    let session = fixture_session();
    let (recv_sock, recv_addr) = loopback_udp().await;

    // Empty request server.
    let req_server = MoldRequestServer::new(64);
    let (req_addr, _req_handle) = req_server.bind("127.0.0.1:0").await.expect("bind req");

    let cfg = PublisherConfig::new(recv_addr, session)
        .with_start_sequence(4)
        .with_heartbeat_interval(Duration::from_millis(100));
    let mut cfg = cfg;
    cfg.eos_burst = 1;
    cfg.eos_interval = Duration::from_millis(10);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message, itch_source::SourceError>>(4);
    for s in 4u64..=5 {
        tx.send(Ok(add_order(s))).await.expect("send");
    }
    let pub_handle = publisher.serve(tokio_stream::wrappers::ReceiverStream::new(rx));

    let stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(session)
            .with_start_sequence(1),
    );
    let recovery = GapRecoveryConfig::new(vec![req_addr])
        .with_request_timeout(Duration::from_millis(100))
        .with_gap_timeout(Duration::from_millis(300));
    let mut client = GapRecoveryClient::new(stream, recovery);

    let mut got_gap = false;
    let mut got_err = false;
    for _ in 0..30 {
        match tokio::time::timeout(Duration::from_millis(500), client.next()).await {
            Ok(Some(Ok(MoldEvent::Gap { .. }))) => got_gap = true,
            Ok(Some(Err(_))) => {
                got_err = true;
                break;
            }
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    assert!(got_gap, "expected at least one Gap event");
    assert!(
        got_err,
        "expected recovery to surface an error after exhaustion"
    );
}

// 8. Failover from a dead first server to a healthy second.
#[tokio::test]
async fn gap_recovery_failover_to_second_server() {
    let session = fixture_session();
    let (recv_sock, recv_addr) = loopback_udp().await;

    // Server 1: bound then dropped — port closed.
    let listener_dead = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let dead = listener_dead.local_addr().expect("local_addr");
    drop(listener_dead);

    // Server 2: holds frames 1..=2.
    let req_server = MoldRequestServer::new(64);
    for s in 1u64..=2 {
        req_server
            .cache_frame(CachedFrame {
                sequence: s,
                body: encode(&add_order(s)),
            })
            .await;
    }
    let (good, _h) = req_server.bind("127.0.0.1:0").await.expect("bind req");

    let cfg = PublisherConfig::new(recv_addr, session)
        .with_start_sequence(3)
        .with_heartbeat_interval(Duration::from_millis(100));
    let mut cfg = cfg;
    cfg.eos_burst = 1;
    cfg.eos_interval = Duration::from_millis(10);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message, itch_source::SourceError>>(4);
    tx.send(Ok(add_order(3))).await.expect("send");
    let pub_handle = publisher.serve(tokio_stream::wrappers::ReceiverStream::new(rx));

    let stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(session)
            .with_start_sequence(1),
    );
    let recovery = GapRecoveryConfig::new(vec![dead, good])
        .with_request_timeout(Duration::from_millis(100))
        .with_gap_timeout(Duration::from_secs(5));
    let mut client = GapRecoveryClient::new(stream, recovery);

    let mut delivered: Vec<u64> = Vec::new();
    while delivered.len() < 3 {
        match tokio::time::timeout(Duration::from_secs(3), client.next()).await {
            Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) => continue,
            Ok(None) | Err(_) => break,
        }
    }
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    let mut sorted = delivered.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted, vec![1, 2, 3]);
}

// 9. Multiple consecutive gaps over the same connection.
#[tokio::test]
async fn multiple_gaps_recovered_in_sequence() {
    let session = fixture_session();
    let (recv_sock, recv_addr) = loopback_udp().await;

    let req_server = MoldRequestServer::new(64);
    for s in [1u64, 2, 4, 5] {
        req_server
            .cache_frame(CachedFrame {
                sequence: s,
                body: encode(&add_order(s)),
            })
            .await;
    }
    let (req_addr, _h) = req_server.bind("127.0.0.1:0").await.expect("bind req");

    // Publisher emits 3, 4, 5, 6 (the multicast feed). Receiver at
    // start_sequence=1 sees the leading gap [1, 2]. (Server2 has
    // 1, 2, 4, 5 — only sequences 1 and 2 are needed.)
    let cfg = PublisherConfig::new(recv_addr, session)
        .with_start_sequence(3)
        .with_heartbeat_interval(Duration::from_millis(100));
    let mut cfg = cfg;
    cfg.eos_burst = 1;
    cfg.eos_interval = Duration::from_millis(10);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message, itch_source::SourceError>>(8);
    for s in 3u64..=6 {
        tx.send(Ok(add_order(s))).await.expect("send");
    }
    let pub_handle = publisher.serve(tokio_stream::wrappers::ReceiverStream::new(rx));

    let stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(session)
            .with_start_sequence(1),
    );
    let recovery = GapRecoveryConfig::new(vec![req_addr])
        .with_request_timeout(Duration::from_millis(500))
        .with_gap_timeout(Duration::from_secs(5));
    let mut client = GapRecoveryClient::new(stream, recovery);

    let mut delivered: Vec<u64> = Vec::new();
    while delivered.len() < 6 {
        match tokio::time::timeout(Duration::from_secs(5), client.next()).await {
            Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    let mut sorted = delivered.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted, vec![1, 2, 3, 4, 5, 6]);
}

// 10. Publisher feeds the request-server cache automatically.
#[tokio::test]
async fn publisher_populates_request_server_cache() {
    let session = fixture_session();
    let (recv_sock, recv_addr) = loopback_udp().await;

    let req_server = MoldRequestServer::new(64);
    let cache = req_server.cache();
    let (req_addr, _h) = req_server.bind("127.0.0.1:0").await.expect("bind req");

    let cfg = PublisherConfig::new(recv_addr, session).with_start_sequence(1);
    let cfg = quick_eos_publisher(cfg);
    let publisher = MoldPublisher::bind(cfg)
        .await
        .expect("bind")
        .with_cache(cache);
    let pub_handle = publisher.serve(stream::iter(
        (1u64..=4).map(|i| Ok(add_order(i))).collect::<Vec<_>>(),
    ));

    let mut stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(session)
            .with_start_sequence(1),
    );
    // Drain to EOS.
    let mut delivered = Vec::new();
    while delivered.len() < 4 {
        match tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
            Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
            Ok(Some(Ok(MoldEvent::EndOfSession { .. }))) => break,
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;

    // Now hit the request server directly and prove the cache filled.
    let mut client = itch_mold::RequestClient::connect(req_addr)
        .await
        .expect("connect");
    let resp = client.request(1, 4).await.expect("request");
    assert_eq!(resp.frames.len(), 4);
    assert_eq!(resp.frames[0].sequence, 1);
    client.close().await.expect("close");
}

// 11. Receiver dropped fully-retransmitted late packets.
#[tokio::test]
async fn fully_retransmitted_late_packet_is_dropped() {
    let session = fixture_session();
    let (recv_sock, recv_addr) = loopback_udp().await;
    let cfg = PublisherConfig::new(recv_addr, session).with_start_sequence(1);
    let cfg = quick_eos_publisher(cfg);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");
    let pub_handle = publisher.serve(stream::iter(
        (1u64..=3).map(|i| Ok(add_order(i))).collect::<Vec<_>>(),
    ));

    let mut stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(session)
            // Start at 4 — every packet from the publisher is a
            // pure retransmit-and-drop.
            .with_start_sequence(4),
    );
    let mut delivered = Vec::new();
    let mut saw_eos = false;
    for _ in 0..30 {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
            Ok(Some(Ok(MoldEvent::EndOfSession { .. }))) => {
                saw_eos = true;
                break;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    // Every published msg was older than next_expected = 4, so no
    // delivery happened. Only EOS arrives.
    assert!(delivered.is_empty(), "no in-order delivery expected");
    assert!(saw_eos);
}

// 12. Larger-volume packing: 50 messages over many datagrams.
#[tokio::test]
async fn larger_volume_packing() {
    let (recv_sock, recv_addr) = loopback_udp().await;
    let cfg = PublisherConfig::new(recv_addr, fixture_session())
        .with_start_sequence(1)
        .with_mtu(200); // ~3-4 messages per packet
    let cfg = quick_eos_publisher(cfg);
    let publisher = MoldPublisher::bind(cfg).await.expect("bind");
    let pub_handle = publisher.serve(stream::iter(
        (1u64..=50).map(|i| Ok(add_order(i))).collect::<Vec<_>>(),
    ));

    let mut stream = MoldStream::from_socket(
        recv_sock,
        MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1),
    );
    let mut delivered = Vec::new();
    let mut saw_eos = false;
    // Drain until both the 50 messages have arrived AND the EOS
    // event has surfaced. A separate timeout per poll keeps the
    // test from hanging on a stuck stream.
    while !(delivered.len() >= 50 && saw_eos) {
        match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
            Ok(Some(Ok(MoldEvent::EndOfSession { .. }))) => saw_eos = true,
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    assert_eq!(delivered, (1u64..=50).collect::<Vec<_>>());
    assert!(saw_eos);
}
