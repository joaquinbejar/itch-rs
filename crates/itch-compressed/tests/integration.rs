//! End-to-end integration tests for `itch-compressed`.
//!
//! Spins up an in-process TCP server speaking SoupBinTCP via
//! [`itch_soup::SoupCodec`], scripts a sequence of packets that
//! includes pre-compressed `S Sequenced Data` payloads, and drives
//! a real [`CompressedSoupConnection`] / [`ResilientCompressedSoupClient`]
//! against it.
//!
//! The 8-scenario matrix from issue #41:
//!
//! 1. Happy path: 1000 messages → 1000 decompressed items.
//! 2. Corrupted zstd frame mid-stream → `CompressedError::Compression`,
//!    next valid frame decodes.
//! 3. Boundary: 5 ITCH messages inside 1 compressed payload → 5
//!    yielded in order.
//! 4. Resilient client: server drops after 50 messages → reconnect +
//!    resume from sequence 51.
//! 5. Heartbeat (`H`) packet not decompressed (filtered silently).
//! 6. EOS (`Z`) packet not decompressed (surfaced once as
//!    `SessionEnded`).
//! 7. Login reject (`J`) packet forwarded as
//!    `Soup(LoginRejected(_))`.
//! 8. ITCH error inside decompressed payload → `Protocol(_)`,
//!    stream continues.
//!
//! All timing-sensitive tests use a short backoff to avoid wall-
//! clock flakiness.
//!
//! Test #9 (`sandbox_smoke`) is `#[ignore]` and gated on three env
//! vars (`ITCH_COMPRESSED_HOST`, `ITCH_COMPRESSED_USER`,
//! `ITCH_COMPRESSED_PASS`). It performs a manual smoke check
//! against NASDAQ's compressed sandbox and is **not** part of the
//! CI gate. See `docs/TESTING.md` §4 for instructions.

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

/// Pack `messages` into one or more compressed `S` payloads,
/// chunking so each compressed blob fits inside SoupBinTCP's
/// 1 KiB `MAX_MESSAGE_LEN`. Used by tests that exercise large
/// streams (e.g. 1000 messages).
fn pack_into_compressed_chunks(messages: &[Message], chunk: usize) -> Vec<Vec<u8>> {
    messages
        .chunks(chunk)
        .map(|c| compress_messages(c, 0).expect("compress chunk"))
        .collect()
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

// =====================================================================
// Scenario 1 — Happy path: 1000 messages → 1000 decompressed items.
// =====================================================================

/// Test 1 — 1000 ITCH messages, packed into multiple compressed
/// `S` payloads (chunked to fit SoupBinTCP's 1 KiB packet ceiling),
/// arrive intact and in order.
#[tokio::test]
async fn test_happy_path_1000_messages_in_compressed_payloads() {
    let inputs: Vec<Message> = (0..1000).map(|i| sample_message(i as u64)).collect();
    // Each SystemEvent is 12 B; chunk so the compressed blob
    // comfortably fits inside SoupBinTCP's 1 KiB packet body.
    let chunks = pack_into_compressed_chunks(&inputs, 64);

    let mut script: Vec<SoupPacket> = chunks.into_iter().map(SoupPacket::SequencedData).collect();
    script.push(SoupPacket::EndOfSession);

    let (addr, server) = spawn_scripted_server(vec![script], "S0001".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");
    assert_eq!(conn.session(), "S0001");

    let mut decoded: Vec<Message> = Vec::with_capacity(1000);
    while let Some(item) = conn.next_message().await {
        match item {
            Ok(msg) => decoded.push(msg),
            Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded)) => break,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert_eq!(decoded.len(), 1000, "1000 messages must round-trip");
    for (got, expected) in decoded.iter().zip(inputs.iter()) {
        assert_eq!(format!("{got:?}"), format!("{expected:?}"));
    }
    let _ = server.await;
}

// =====================================================================
// Scenario 2 — Corrupted zstd frame mid-stream.
// =====================================================================

/// Test 2 — corrupted compressed frame surfaces
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

// =====================================================================
// Scenario 3 — Boundary: 5 ITCH messages inside 1 compressed payload.
// =====================================================================

/// Test 3 — 5 messages in one compressed payload yield exactly 5
/// stream items in order.
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

// =====================================================================
// Scenario 4 — Resilient client: drop after 50 → resume from 51.
// =====================================================================

/// Test 4 — first connection delivers 50 messages (sequences 1..=50)
/// then the server drops the socket. The resilient client
/// reconnects and the server resumes from sequence 51 (delivering
/// messages 51..=100), then closes with EOS. Verifies sequence-
/// resume correctness across a mid-stream socket drop.
#[tokio::test]
async fn test_resilient_client_drops_after_50_resumes_from_51() {
    let first_half: Vec<Message> = (1..=50).map(|i| sample_message(i as u64)).collect();
    let second_half: Vec<Message> = (51..=100).map(|i| sample_message(i as u64)).collect();
    // Pack each half into chunks so the compressed payload fits
    // SoupBinTCP's 1 KiB packet ceiling.
    let first_chunks = pack_into_compressed_chunks(&first_half, 25);
    let second_chunks = pack_into_compressed_chunks(&second_half, 25);

    let first_script: Vec<SoupPacket> = first_chunks
        .into_iter()
        .map(SoupPacket::SequencedData)
        .collect();
    // First connection drops without EOS, exercising the resilient
    // client's reconnect path.

    let mut second_script: Vec<SoupPacket> = second_chunks
        .into_iter()
        .map(SoupPacket::SequencedData)
        .collect();
    second_script.push(SoupPacket::EndOfSession);

    let (addr, server) = scripted_server_with_resume(
        vec![first_script, second_script],
        "RESUME".into(),
        50, // first connection acks LoginRequest with assigned_seq=1; second connection must resume from 51
    )
    .await;

    let cfg = ResilientSoupConfig::new(vec![addr], creds())
        .with_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .with_login_timeout(Duration::from_secs(2));
    let mut client = ResilientCompressedSoupClient::new(cfg);

    let mut got = 0u32;
    let mut steps = 0;
    let mut saw_eos = false;
    let mut last_seq_after_first_half: Option<u64> = None;
    loop {
        steps += 1;
        assert!(steps < 1000, "infinite loop guard");
        match client.next_message().await {
            Some(Ok(_)) => {
                got += 1;
                if got == 50 {
                    last_seq_after_first_half = Some(client.next_expected_sequence());
                }
            }
            Some(Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded))) => {
                saw_eos = true;
                break;
            }
            Some(Err(_)) => continue, // transient: socket drop, allow reconnect
            None => break,
        }
    }
    assert_eq!(
        got, 100,
        "must deliver every message across the mid-stream drop"
    );
    assert!(
        saw_eos,
        "must observe EndOfSession on the second connection"
    );
    assert_eq!(
        last_seq_after_first_half,
        Some(51),
        "after the 50th message the client must request sequence 51 on reconnect"
    );
    assert_eq!(client.last_session(), Some("RESUME"));
    let _ = server.await;
}

/// Spin up a scripted server that, on each connection, honours
/// `requested_sequence` from the client's `LoginRequest`. Used by
/// the resume test to verify the client correctly negotiates
/// `requested_sequence = 51` on reconnect after delivering 50
/// messages.
async fn scripted_server_with_resume(
    scripts: Vec<Vec<SoupPacket>>,
    session: String,
    expected_resume_seq: u64,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let handle = tokio::spawn(async move {
        let mut conn_idx = 0u32;
        for script in scripts {
            let (sock, _peer) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let mut framed = Framed::new(sock, SoupCodec::new());
            let req = match framed.next().await {
                Some(Ok(p)) => p,
                _ => continue,
            };
            let requested_seq = match req {
                SoupPacket::LoginRequest(r) => r.requested_sequence,
                _ => continue,
            };
            // First connection: client sends 0 (most recent), we
            // assign 1 so the first message lands at sequence 1.
            // Second connection: client must request the resume
            // sequence we expect (51).
            let assigned_seq = if conn_idx == 0 {
                if requested_seq == 0 {
                    1
                } else {
                    requested_seq
                }
            } else {
                assert_eq!(
                    requested_seq,
                    expected_resume_seq + 1,
                    "second connection must resume from sequence {}",
                    expected_resume_seq + 1
                );
                requested_seq
            };
            conn_idx += 1;
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

// =====================================================================
// Scenario 5 — Heartbeat (`H`) packet not decompressed.
// =====================================================================

/// Test 5 — `H` server-heartbeat packets are filtered silently;
/// they never reach the user as either a `Message` or an error,
/// and `H`'s payload is never fed to zstd.
#[tokio::test]
async fn test_heartbeat_packet_not_decompressed() {
    let inputs = vec![sample_message(1), sample_message(2)];
    let payload = compress_messages(&inputs, 0).expect("compress");

    let scripts = vec![vec![
        SoupPacket::ServerHeartbeat,
        SoupPacket::ServerHeartbeat,
        SoupPacket::SequencedData(payload),
        SoupPacket::ServerHeartbeat,
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "HB".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");

    let mut got = 0u32;
    let mut errors = 0u32;
    let mut session_ended = false;
    while let Some(item) = conn.next_message().await {
        match item {
            Ok(_) => got += 1,
            Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded)) => {
                session_ended = true;
                break;
            }
            Err(_) => errors += 1,
        }
    }
    assert_eq!(got, 2, "only the 2 sequenced-data messages reach the user");
    assert_eq!(errors, 0, "heartbeats must not surface as errors");
    assert!(session_ended);
    let _ = server.await;
}

// =====================================================================
// Scenario 6 — EOS (`Z`) packet forwarding.
// =====================================================================

/// Test 6 — `Z EndOfSession` is surfaced once as
/// `CompressedError::Soup(SoupError::SessionEnded)` and the
/// stream returns `None` on subsequent polls. The `Z` payload is
/// never decompressed.
#[tokio::test]
async fn test_eos_packet_forwarded_then_stream_terminates() {
    let payload = compress_messages(&[sample_message(7)], 0).expect("compress");

    let scripts = vec![vec![
        SoupPacket::SequencedData(payload),
        SoupPacket::EndOfSession,
    ]];
    let (addr, server) = spawn_scripted_server(scripts, "EOS".into()).await;

    let mut conn = CompressedSoupConnection::connect(addr, creds(), "", 0)
        .await
        .expect("connect");

    // First we receive the message.
    let first = conn.next_message().await;
    assert!(matches!(first, Some(Ok(_))));

    // Then EOS surfaces as SessionEnded.
    let second = conn.next_message().await;
    match second {
        Some(Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded))) => {}
        other => panic!("expected SessionEnded, got {other:?}"),
    }

    // Subsequent polls return None (terminal).
    let third = conn.next_message().await;
    assert!(third.is_none(), "stream must terminate after EOS");
    let _ = server.await;
}

// =====================================================================
// Scenario 7 — Login reject (`J`) packet forwarding.
// =====================================================================

/// Test 7 — a `J LoginRejected` reply during handshake surfaces
/// `CompressedError::Soup(LoginRejected(_))` from `connect`, and
/// the reject reason is forwarded verbatim from the SoupBinTCP
/// layer.
#[tokio::test]
async fn test_login_rejected_packet_forwarding() {
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

// =====================================================================
// Scenario 8 — ITCH error inside decompressed payload.
// =====================================================================

/// Test 8 — a bad inner ITCH frame inside a successfully-
/// decompressed payload yields `CompressedError::Protocol(_)`
/// once; the next valid `S` packet decodes cleanly (drop-and-
/// resume).
#[tokio::test]
async fn test_bad_itch_inside_decompressed_payload_then_recovery() {
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

// =====================================================================
// Sandbox smoke test (manual; not part of CI).
// =====================================================================

/// Manual smoke check against NASDAQ's compressed sandbox.
///
/// **Not** part of CI. Gated on three env vars:
///
/// - `ITCH_COMPRESSED_HOST` — `host:port` of the sandbox endpoint.
/// - `ITCH_COMPRESSED_USER` — username issued by NASDAQ.
/// - `ITCH_COMPRESSED_PASS` — password issued by NASDAQ.
///
/// Run with:
///
/// ```text
/// export ITCH_COMPRESSED_HOST=…
/// export ITCH_COMPRESSED_USER=…
/// export ITCH_COMPRESSED_PASS=…
/// cargo test -p itch-compressed -- --ignored sandbox_smoke
/// ```
///
/// The test connects, reads up to 10 messages (or stops on EOS /
/// timeout), logs each, and disconnects gracefully via `logout`.
/// **Never commit credentials.** See `docs/TESTING.md` §4 for
/// full instructions.
#[tokio::test]
#[ignore = "manual sandbox smoke test; requires ITCH_COMPRESSED_{HOST,USER,PASS}"]
async fn sandbox_smoke() {
    let host = match std::env::var("ITCH_COMPRESSED_HOST") {
        Ok(h) => h,
        Err(_) => {
            eprintln!(
                "skipping sandbox_smoke: ITCH_COMPRESSED_HOST not set. \
                 Re-read docs/TESTING.md §4 for the env vars required."
            );
            return;
        }
    };
    let user = match std::env::var("ITCH_COMPRESSED_USER") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping sandbox_smoke: ITCH_COMPRESSED_USER not set.");
            return;
        }
    };
    let pass = match std::env::var("ITCH_COMPRESSED_PASS") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("skipping sandbox_smoke: ITCH_COMPRESSED_PASS not set.");
            return;
        }
    };

    // Initialise tracing so the smoke run prints meaningful logs.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,itch_compressed=debug")
            }),
        )
        .with_test_writer()
        .try_init();

    let addr: std::net::SocketAddr = host
        .parse()
        .expect("ITCH_COMPRESSED_HOST must be host:port");
    tracing::info!(%addr, "sandbox_smoke: connecting");

    // Bound the whole exchange so a hung sandbox does not stall CI
    // if someone accidentally enables this with `--ignored`.
    let outcome = tokio::time::timeout(Duration::from_secs(30), async move {
        let mut conn = CompressedSoupConnection::connect(
            addr,
            SoupCredentials::new(&user, &pass),
            "",
            0, // resume from most recent
        )
        .await
        .expect("connect to sandbox");
        tracing::info!(
            session = conn.session(),
            next_seq = conn.next_expected_sequence(),
            "sandbox_smoke: logged in"
        );

        let mut got = 0u32;
        while got < 10 {
            match conn.next_message().await {
                Some(Ok(msg)) => {
                    got += 1;
                    tracing::info!(?msg, n = got, "sandbox_smoke: received");
                }
                Some(Err(CompressedError::Soup(itch_soup::SoupError::SessionEnded))) => {
                    tracing::info!("sandbox_smoke: session ended early");
                    break;
                }
                Some(Err(err)) => {
                    tracing::warn!(?err, "sandbox_smoke: non-fatal error; continuing");
                }
                None => {
                    tracing::warn!("sandbox_smoke: stream ended without EOS");
                    break;
                }
            }
        }

        tracing::info!(received = got, "sandbox_smoke: disconnecting via logout");
        conn.logout().await.expect("graceful logout");
        got
    })
    .await;

    let got = outcome.expect("sandbox_smoke timed out after 30 s");
    assert!(
        got > 0,
        "sandbox_smoke received no messages — verify host / credentials / market hours"
    );
}
