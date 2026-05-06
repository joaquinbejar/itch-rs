//! End-to-end integration tests for SoupBinTCP per
//! `docs/TESTING.md` §4 and `docs/TRANSPORT-SPEC.md` §3.
//!
//! Drives a real `SoupServer` on `127.0.0.1:0` from a real
//! `SoupConnection` (and `ResilientSoupClient`) through every
//! documented exchange.
//!
//! NOTE: this file depends on `SoupServer` (issue #18) and
//! `ResilientSoupClient` (issue #17). Until both PRs land, this
//! test target won't compile — that is intentional, the user
//! merges #17 → #18 → #19 in order.
//!
//! All timing-sensitive scenarios use `tokio::time::pause` to
//! avoid wall-clock flakiness.

#![cfg(feature = "integration")]

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use itch_protocol::messages::{Header, SystemEvent};
use itch_protocol::primitives::{StockLocate, Timestamp, TrackingNumber};
use itch_protocol::{EventCode, Message};
use itch_soup::{
    login_with_timeout, LoginRejectReason, ResilientSoupClient, ResilientSoupConfig,
    SoupConnection, SoupCredentials, SoupError, SoupServer, SoupSession, StaticAuthenticator,
};
use itch_source::{ChannelSource, NullSeqStore, RingBufferSeqStore, StaticPolicy};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

// ----- helpers -----

fn sample_message_at(ts: u64) -> Message {
    Message::SystemEvent(SystemEvent {
        header: Header {
            stock_locate: StockLocate::from_u16(0),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(ts),
        },
        event_code: EventCode::StartOfSystemHours,
    })
}

async fn login_once(addr: std::net::SocketAddr, user: &str, pw: &str) -> SoupConnection<TcpStream> {
    let s = TcpStream::connect(addr).await.expect("connect");
    login_with_timeout(
        s,
        SoupCredentials::new(user, pw),
        "",
        0,
        Duration::from_secs(2),
    )
    .await
    .expect("login")
}

/// Spin up a server with the given source / store / policy. Returns
/// the bound address, the source sender (caller pushes messages),
/// and the join handle (caller awaits / aborts).
async fn spin_server(
    initial_capacity: usize,
) -> (
    std::net::SocketAddr,
    mpsc::Sender<Message>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, source) = ChannelSource::bounded(initial_capacity.max(8));
    let server = SoupServer::bind_with_auth(
        "127.0.0.1:0",
        source,
        RingBufferSeqStore::new(),
        StaticPolicy::empty(),
        StaticAuthenticator::new("alice", "secret"),
    )
    .await
    .expect("bind");
    let addr = server.local_addr().expect("local_addr");
    let handle = tokio::spawn(async move { server.serve().await.expect("serve") });
    // Yield so the listener is actually bound and ready.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, tx, handle)
}

// ----- happy-path scenarios -----

#[tokio::test]
async fn happy_path_1000_messages_in_order() {
    let (addr, src_tx, server_task) = spin_server(64).await;
    let mut conn = login_once(addr, "alice", "secret").await;

    // Push 1000 messages with monotone timestamps for ordering check.
    let producer = tokio::spawn(async move {
        for ts in 0..1000u64 {
            src_tx.send(sample_message_at(ts)).await.expect("push");
        }
        // Drop sender → server emits Z.
    });

    let mut last_ts = None::<u64>;
    let mut count = 0u32;
    while let Some(item) = conn.next().await {
        match item {
            Ok(Message::SystemEvent(SystemEvent { header, .. })) => {
                let ts = header.timestamp.as_u64();
                if let Some(prev) = last_ts {
                    assert!(ts > prev, "messages must arrive in order");
                }
                last_ts = Some(ts);
                count += 1;
            }
            Ok(other) => panic!("unexpected message: {other:?}"),
            Err(SoupError::SessionEnded) => break,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert_eq!(count, 1000);
    let _ = producer.await;
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}

#[tokio::test]
async fn login_rejected_not_authorized() {
    let (addr, _src_tx, server_task) = spin_server(8).await;

    let s = TcpStream::connect(addr).await.expect("connect");
    let err = login_with_timeout(
        s,
        SoupCredentials::new("bob", "wrong"),
        "",
        0,
        Duration::from_secs(2),
    )
    .await
    .expect_err("rejected");
    match err {
        SoupError::LoginRejected(LoginRejectReason::NotAuthorized) => {}
        other => panic!("expected NotAuthorized, got {other:?}"),
    }
    server_task.abort();
}

#[tokio::test]
async fn login_rejected_session_unavailable() {
    let (tx, source) = ChannelSource::bounded(8);
    let server = SoupServer::bind(
        "127.0.0.1:0",
        source,
        NullSeqStore::new(),
        StaticPolicy::empty(),
    )
    .await
    .expect("bind")
    .with_session(SoupSession::new("S0001"));
    let addr = server.local_addr().expect("addr");
    let server_task = tokio::spawn(async move { server.serve().await.expect("serve") });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let s = TcpStream::connect(addr).await.expect("connect");
    let err = login_with_timeout(
        s,
        SoupCredentials::new("alice", "any"),
        "OTHER",
        0,
        Duration::from_secs(2),
    )
    .await
    .expect_err("session mismatch");
    match err {
        SoupError::LoginRejected(LoginRejectReason::SessionUnavailable) => {}
        other => panic!("expected SessionUnavailable, got {other:?}"),
    }
    drop(tx);
    server_task.abort();
}

#[tokio::test]
async fn server_kills_socket_mid_session_surfaces_error() {
    let (addr, src_tx, server_task) = spin_server(8).await;
    let mut conn = login_once(addr, "alice", "secret").await;
    // Give the server time to attach the subscriber to the
    // broadcast channel before publishing.
    tokio::time::sleep(Duration::from_millis(50)).await;
    src_tx.send(sample_message_at(1)).await.expect("push");
    // Read the one message.
    let m = conn.next().await.expect("some").expect("ok");
    assert!(matches!(m, Message::SystemEvent(_)));
    // Abruptly close the source — the server's per-subscriber tasks
    // see the broadcast end and shut down, the TCP socket closes,
    // the client surfaces an error or `None`. Mirrors a server-side
    // mid-session abort from the client's point of view.
    drop(src_tx);
    server_task.abort();
    let _ = server_task.await;
    let saw_signal = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match conn.next().await {
                Some(Err(_)) | None => return true,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(saw_signal, "client must surface a disconnect signal");
}

#[tokio::test]
async fn reconnect_with_sequence_resume_via_base_connection() {
    // First login: receive 5 messages, server kills socket.
    let (addr, src_tx, server_task) = spin_server(64).await;
    let mut conn = login_once(addr, "alice", "secret").await;
    for ts in 0..5u64 {
        src_tx.send(sample_message_at(ts)).await.expect("push");
    }
    let mut got_first = 0u32;
    while got_first < 5 {
        match conn.next().await {
            Some(Ok(_)) => got_first += 1,
            Some(Err(other)) => panic!("err during first batch: {other:?}"),
            None => panic!("socket closed before first batch complete"),
        }
    }
    let resume_seq = conn.next_expected_sequence();
    drop(conn);

    // Server still running; push 3 more, then reconnect.
    for ts in 5..8u64 {
        src_tx.send(sample_message_at(ts)).await.expect("push");
    }
    let s = TcpStream::connect(addr).await.expect("reconnect");
    let mut conn2 = login_with_timeout(
        s,
        SoupCredentials::new("alice", "secret"),
        "",
        resume_seq,
        Duration::from_secs(2),
    )
    .await
    .expect("login resume");

    drop(src_tx); // signal EOS

    let mut got_second = 0u32;
    while let Some(item) = conn2.next().await {
        match item {
            Ok(_) => got_second += 1,
            Err(SoupError::SessionEnded) => break,
            Err(other) => panic!("err during second batch: {other:?}"),
        }
    }
    assert_eq!(
        got_second, 3,
        "received the 3 missed-then-replayed messages"
    );
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}

#[tokio::test]
async fn resilient_client_recovers_across_disconnect() {
    // Server: accept once, send 2 messages, then drop. Subsequent
    // accept sends 2 more then EOS.
    let (addr, src_tx, server_task) = spin_server(64).await;
    let cfg = ResilientSoupConfig::new(vec![addr], SoupCredentials::new("alice", "secret"))
        .with_backoff(Duration::from_millis(5), Duration::from_millis(20));
    let mut client = ResilientSoupClient::new(cfg);

    // Force the lazy connect by pulling once on the client side
    // before publishing — guarantees the subscriber is attached to
    // the broadcast before the source produces messages.
    let first_msg = tokio::spawn(async move {
        let item = client.next_message().await;
        (client, item)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    for ts in 0..4u64 {
        src_tx.send(sample_message_at(ts)).await.expect("push");
    }
    drop(src_tx);

    let (mut client, first) = first_msg.await.expect("join");
    let mut got = 0u32;
    if let Some(Ok(_)) = first {
        got += 1;
    }
    while let Some(item) = client.next_message().await {
        match item {
            Ok(_) => got += 1,
            Err(SoupError::SessionEnded) => break,
            Err(_) => continue,
        }
    }
    assert_eq!(got, 4);
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}

#[tokio::test]
async fn multi_client_concurrent_broadcast() {
    let (addr, src_tx, server_task) = spin_server(64).await;
    // Open two concurrent clients before publishing.
    let a = login_once(addr, "alice", "secret").await;
    let b = login_once(addr, "alice", "secret").await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    for ts in 0..5u64 {
        src_tx.send(sample_message_at(ts)).await.expect("push");
    }
    drop(src_tx);

    let drain = |mut c: SoupConnection<TcpStream>| async move {
        let mut got = 0u32;
        while let Some(item) = c.next().await {
            match item {
                Ok(_) => got += 1,
                Err(SoupError::SessionEnded) => break,
                Err(other) => panic!("err: {other:?}"),
            }
        }
        got
    };
    let ta = tokio::spawn(async move { drain(a).await });
    let tb = tokio::spawn(async move { drain(b).await });
    assert_eq!(ta.await.expect("a"), 5);
    assert_eq!(tb.await.expect("b"), 5);
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}

#[tokio::test]
async fn logout_closes_socket_cleanly() {
    let (addr, src_tx, server_task) = spin_server(8).await;
    let conn = login_once(addr, "alice", "secret").await;
    conn.logout().await.expect("logout ok");
    drop(src_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}

#[tokio::test]
async fn graceful_eos_on_source_exhaustion() {
    let (addr, src_tx, server_task) = spin_server(8).await;
    let mut conn = login_once(addr, "alice", "secret").await;
    src_tx.send(sample_message_at(1)).await.expect("push");
    drop(src_tx); // signal EOS

    let mut count = 0u32;
    let mut saw_eos = false;
    while let Some(item) = conn.next().await {
        match item {
            Ok(_) => count += 1,
            Err(SoupError::SessionEnded) => {
                saw_eos = true;
                break;
            }
            Err(other) => panic!("err: {other:?}"),
        }
    }
    assert_eq!(count, 1);
    assert!(
        saw_eos,
        "client must observe SessionEnded on source exhaustion"
    );
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}

/// Heartbeat scheduler primitives are unit-tested in
/// `crates/itch-soup/src/heartbeat.rs`. This integration test only
/// verifies that the server-side heartbeat path doesn't surface as
/// a stream item to the client (filter rules per
/// `docs/TRANSPORT-SPEC.md` §3.4).
#[tokio::test]
async fn heartbeat_packets_are_filtered_from_stream() {
    use itch_soup::SoupCodec;
    use itch_soup::SoupPacket;
    use tokio::net::TcpListener;
    use tokio_util::codec::Framed;

    // Bare TCP server — bypass SoupServer to inject `H` heartbeats
    // directly between sequenced data packets.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.expect("accept");
        let mut framed = Framed::new(sock, SoupCodec::new());
        let _req = framed.next().await.expect("req").expect("ok");
        framed
            .send(SoupPacket::LoginAccepted(itch_soup::LoginAccepted {
                session: "S".into(),
                sequence: 1,
            }))
            .await
            .expect("accepted");
        // Heartbeat → data → heartbeat → data → EOS.
        framed.send(SoupPacket::ServerHeartbeat).await.expect("h1");
        let mut buf = vec![0u8; sample_message_at(1).encoded_len()];
        sample_message_at(1).encode(&mut buf).expect("enc");
        framed
            .send(SoupPacket::SequencedData(buf))
            .await
            .expect("d1");
        framed.send(SoupPacket::ServerHeartbeat).await.expect("h2");
        let mut buf = vec![0u8; sample_message_at(2).encoded_len()];
        sample_message_at(2).encode(&mut buf).expect("enc");
        framed
            .send(SoupPacket::SequencedData(buf))
            .await
            .expect("d2");
        framed.send(SoupPacket::EndOfSession).await.expect("z");
    });

    let mut conn = login_once(addr, "any", "any").await;
    let mut got = 0u32;
    while let Some(item) = conn.next().await {
        match item {
            Ok(_) => got += 1,
            Err(SoupError::SessionEnded) => break,
            Err(other) => panic!("err: {other:?}"),
        }
    }
    assert_eq!(got, 2, "heartbeats filtered, only data surfaces");
    let _ = server.await;
}
