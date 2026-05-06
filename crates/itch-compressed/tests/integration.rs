//! End-to-end integration tests for `itch-compressed`.
//!
//! Spins up an in-process TCP server speaking SoupBinTCP via
//! [`itch_soup::SoupCodec`], scripts a sequence of packets that
//! includes pre-compressed `S Sequenced Data` payloads, and drives
//! a real [`CompressedSoupConnection`] / [`ResilientCompressedSoupClient`]
//! against it.
//!
//! All timing-sensitive tests use a short backoff to avoid wall-
//! clock flakiness.

use std::time::Duration;

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use itch_compressed::{
    compress_messages, CompressedError, CompressedSoupConnection, ResilientCompressedSoupClient,
};
use itch_protocol::messages::{Header, SystemEvent};
use itch_protocol::primitives::{StockLocate, Timestamp, TrackingNumber};
use itch_protocol::{EventCode, Message};
use itch_soup::{
    LoginAccepted, LoginRejectReason, ResilientSoupConfig, SoupCodec, SoupCredentials, SoupPacket,
};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

fn sample_message(ts: u64) -> Message {
    Message::SystemEvent(SystemEvent {
        header: Header {
            stock_locate: StockLocate::from_u16(0),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(ts),
        },
        event_code: EventCode::StartOfSystemHours,
    })
}

fn creds() -> SoupCredentials {
    SoupCredentials::new("alice", "secret")
}

/// Spin up an in-process server that accepts up to N connections
/// and replays a per-connection script of `SoupPacket`s. After the
/// script ends the server simply drops the socket.
async fn spawn_scripted_server(
    scripts: Vec<Vec<SoupPacket>>,
    session: String,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let handle = tokio::spawn(async move {
        for script in scripts {
            let (sock, _peer) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let mut framed = Framed::new(sock, SoupCodec::new());
            // Read the LoginRequest; ignore everything except the
            // requested sequence (so we can resume).
            let req = match framed.next().await {
                Some(Ok(p)) => p,
                _ => continue,
            };
            let requested_seq = match req {
                SoupPacket::LoginRequest(r) => r.requested_sequence,
                _ => continue,
            };
            let assigned_seq = if requested_seq == 0 { 1 } else { requested_seq };
            if framed
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: session.clone(),
                    sequence: assigned_seq,
                }))
                .await
                .is_err()
            {
                continue;
            }
            for pkt in script {
                if framed.send(pkt).await.is_err() {
                    break;
                }
            }
            drop(framed);
        }
    });
    (addr, handle)
}

/// Test 1 — happy path: 100 messages packed into one compressed
/// `S` payload yield 100 stream items.
#[tokio::test]
async fn test_happy_path_100_messages_in_one_compressed_payload() {
    let inputs: Vec<Message> = (0..100).map(|i| sample_message(i as u64)).collect();
    let compressed = compress_messages(&inputs, 0).expect("compress");

    let scripts = vec![vec![
        SoupPacket::SequencedData(compressed),
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "S0001".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");
    assert_eq!(conn.session(), "S0001");

    let mut got = 0u32;
    while let Some(item) = conn.next_message().await {
        match item {
            Ok(_msg) => got += 1,
            Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded)) => break,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert_eq!(got, 100);
    let _ = server.await;
}

/// Test 2 — boundary: 5 messages in one compressed payload yield
/// exactly 5 stream items in order.
#[tokio::test]
async fn test_boundary_five_messages_one_payload_yields_five() {
    let inputs: Vec<Message> = (0..5).map(|i| sample_message(i as u64 + 1000)).collect();
    let compressed = compress_messages(&inputs, 3).expect("compress");

    let scripts = vec![vec![
        SoupPacket::SequencedData(compressed),
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "BOUND".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");

    let mut decoded: Vec<Message> = Vec::new();
    while let Some(item) = conn.next_message().await {
        match item {
            Ok(msg) => decoded.push(msg),
            Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded)) => break,
            Err(other) => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(decoded.len(), 5);
    for (got, expected) in decoded.iter().zip(inputs.iter()) {
        assert_eq!(format!("{got:?}"), format!("{expected:?}"));
    }
    let _ = server.await;
}

/// Test 3 — corrupted compressed frame surfaces
/// `CompressedError::Compression` and a subsequent valid packet
/// decodes cleanly (drop-and-resume).
#[tokio::test]
async fn test_corrupted_frame_then_valid_packet_decodes() {
    let inputs = vec![sample_message(42)];
    let valid = compress_messages(&inputs, 0).expect("compress");
    let corrupted = vec![0xDE, 0xAD, 0xBE, 0xEF];

    let scripts = vec![vec![
        SoupPacket::SequencedData(corrupted),
        SoupPacket::SequencedData(valid),
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "CORRUPT".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");

    let mut saw_compression_error = false;
    let mut saw_valid_message = false;
    let mut saw_session_ended = false;
    while let Some(item) = conn.next_message().await {
        match item {
            Ok(_) => saw_valid_message = true,
            Err(CompressedError::Compression { .. }) => saw_compression_error = true,
            Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded)) => {
                saw_session_ended = true;
                break;
            }
            Err(other) => panic!("unexpected: {other:?}"),
        }
    }
    assert!(saw_compression_error, "must surface Compression error");
    assert!(
        saw_valid_message,
        "must continue and decode the next valid packet"
    );
    assert!(saw_session_ended, "must observe SessionEnded");
    let _ = server.await;
}

/// Test 4 — heartbeat (`H`), debug (`+`) packets are forwarded
/// silently (no stream items emitted) around real data, and EOS
/// is surfaced as `SessionEnded`.
#[tokio::test]
async fn test_heartbeat_debug_eos_forwarding() {
    let inputs = vec![sample_message(1), sample_message(2)];
    let payload = compress_messages(&inputs, 0).expect("compress");

    let scripts = vec![vec![
        SoupPacket::ServerHeartbeat,
        SoupPacket::Debug(b"informational".to_vec()),
        SoupPacket::SequencedData(payload),
        SoupPacket::ServerHeartbeat,
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "HB".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");

    let mut got = 0u32;
    let mut session_ended = false;
    while let Some(item) = conn.next_message().await {
        match item {
            Ok(_) => got += 1,
            Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded)) => {
                session_ended = true;
                break;
            }
            Err(other) => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(got, 2);
    assert!(session_ended);
    let _ = server.await;
}

/// Test 5 — login-rejected forwarding: a `J` reply during
/// handshake surfaces `CompressedError::Soup(LoginRejected(_))`
/// from `connect`.
#[tokio::test]
async fn test_login_rejected_surfaces_soup_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.expect("accept");
        let mut framed = Framed::new(sock, SoupCodec::new());
        let _req = framed.next().await.expect("req").expect("ok");
        framed
            .send(SoupPacket::LoginRejected(LoginRejectReason::NotAuthorized))
            .await
            .expect("rej");
    });

    let err = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect_err("rejected");
    match err {
        CompressedError::Soup(itch_soup::SoupError::LoginRejected(
            LoginRejectReason::NotAuthorized,
        )) => {}
        other => panic!("expected LoginRejected(NotAuthorized), got {other:?}"),
    }
    let _ = server.await;
}

/// Test 6 — ITCH error inside decompressed payload yields
/// `CompressedError::Protocol(_)` and the stream continues.
#[tokio::test]
async fn test_bad_itch_inside_decompressed_payload_emits_protocol() {
    // Compress a single byte that is NOT a valid ITCH tag.
    let bogus = [b'!'];
    let bad_payload = zstd::stream::encode_all(&bogus[..], 0).expect("compress raw");
    let good = compress_messages(&[sample_message(99)], 0).expect("compress");

    let scripts = vec![vec![
        SoupPacket::SequencedData(bad_payload),
        SoupPacket::SequencedData(good),
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "ITCH".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");

    let mut saw_protocol = false;
    let mut saw_good = false;
    let mut saw_eos = false;
    while let Some(item) = conn.next_message().await {
        match item {
            Ok(_) => saw_good = true,
            Err(CompressedError::Protocol(_)) => saw_protocol = true,
            Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded)) => {
                saw_eos = true;
                break;
            }
            Err(other) => panic!("unexpected: {other:?}"),
        }
    }
    assert!(saw_protocol, "must surface Protocol error");
    assert!(saw_good, "must continue and decode the next valid packet");
    assert!(saw_eos);
    let _ = server.await;
}

/// Test 7 — resilient client: server drops connection mid-stream;
/// client reconnects and resumes via sequence number.
#[tokio::test]
async fn test_resilient_client_resumes_across_socket_drop() {
    // First connection delivers 1 compressed `S` containing 2
    // messages, then drops. Second connection delivers 1
    // compressed `S` containing 2 more messages, then EOS. Client
    // must see 4 total.
    let first = compress_messages(&[sample_message(1), sample_message(2)], 0).expect("c1");
    let second = compress_messages(&[sample_message(3), sample_message(4)], 0).expect("c2");

    let scripts = vec![
        vec![SoupPacket::SequencedData(first)],
        vec![SoupPacket::SequencedData(second), SoupPacket::EndOfSession],
    ];
    let (addr, server) = spawn_scripted_server(scripts, "RES".into()).await;

    let cfg = ResilientSoupConfig::new(vec![addr], creds())
        .with_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .with_login_timeout(Duration::from_secs(2));
    let mut client = ResilientCompressedSoupClient::new(cfg);

    let mut got = 0u32;
    let mut steps = 0;
    loop {
        steps += 1;
        assert!(steps < 100, "infinite loop guard");
        match client.next_message().await {
            Some(Ok(_)) => got += 1,
            Some(Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded))) => break,
            Some(Err(_)) => continue,
            None => break,
        }
    }
    assert_eq!(got, 4, "delivered every message across the gap");
    assert_eq!(client.last_session(), Some("RES"));
    let _ = server.await;
}

/// Test 8 — resilient client: fatal `LoginRejected` terminates
/// the stream.
#[tokio::test]
async fn test_resilient_client_login_rejected_terminates() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.expect("accept");
        let mut framed = Framed::new(sock, SoupCodec::new());
        let _req = framed.next().await.expect("req").expect("ok");
        framed
            .send(SoupPacket::LoginRejected(LoginRejectReason::NotAuthorized))
            .await
            .expect("rej");
    });

    let cfg = ResilientSoupConfig::new(vec![addr], SoupCredentials::new("bob", "wrong"))
        .with_max_attempts(5)
        .with_backoff(Duration::from_millis(1), Duration::from_millis(5))
        .with_login_timeout(Duration::from_millis(200));
    let mut client = ResilientCompressedSoupClient::new(cfg);

    match client.next_message().await {
        Some(Err(CompressedError::Soup(itch_soup::SoupError::LoginRejected(
            LoginRejectReason::NotAuthorized,
        )))) => {}
        other => panic!("expected LoginRejected(NotAuthorized), got {other:?}"),
    }
    assert!(client.next_message().await.is_none());
    let _ = server.await;
}

/// Test 9 — multiple compressed packets across one connection:
/// each `S` packet decompresses independently.
#[tokio::test]
async fn test_multiple_compressed_packets_one_connection() {
    let p1 = compress_messages(&[sample_message(1)], 0).expect("c1");
    let p2 = compress_messages(&[sample_message(2), sample_message(3)], 0).expect("c2");
    let p3 = compress_messages(
        &[sample_message(4), sample_message(5), sample_message(6)],
        0,
    )
    .expect("c3");

    let scripts = vec![vec![
        SoupPacket::SequencedData(p1),
        SoupPacket::SequencedData(p2),
        SoupPacket::SequencedData(p3),
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "MULTI".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");
    let mut got = 0u32;
    while let Some(item) = conn.next_message().await {
        match item {
            Ok(_) => got += 1,
            Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded)) => break,
            Err(other) => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(got, 6);
    let _ = server.await;
}

/// Test 10 — empty compressed payload (zero ITCH messages inside)
/// is consumed silently and the stream proceeds to the next
/// packet.
#[tokio::test]
async fn test_empty_compressed_payload_consumed_silently() {
    let empty = compress_messages(&[], 0).expect("compress empty");
    let good = compress_messages(&[sample_message(7)], 0).expect("compress good");

    let scripts = vec![vec![
        SoupPacket::SequencedData(empty),
        SoupPacket::SequencedData(good),
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "EMPTY".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");
    let mut got = 0u32;
    while let Some(item) = conn.next_message().await {
        match item {
            Ok(_) => got += 1,
            Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded)) => break,
            Err(other) => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(got, 1);
    let _ = server.await;
}

/// Test 11 — `into_stream()` smoke test.
#[tokio::test]
async fn test_resilient_into_stream_smoke() {
    let payload = compress_messages(&[sample_message(1)], 0).expect("compress");
    let scripts = vec![vec![
        SoupPacket::SequencedData(payload),
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "STREAM".into()).await;

    let cfg = ResilientSoupConfig::new(vec![addr], creds())
        .with_backoff(Duration::from_millis(1), Duration::from_millis(5))
        .with_login_timeout(Duration::from_millis(200))
        .with_max_attempts(2);
    let client = ResilientCompressedSoupClient::new(cfg);

    let mut stream = client.into_stream();
    let mut got = 0u32;
    let mut steps = 0;
    while let Some(item) = StreamExt::next(&mut stream).await {
        steps += 1;
        assert!(steps < 30);
        if item.is_ok() {
            got += 1;
        }
    }
    assert_eq!(got, 1);
    let _ = server.await;
}
