//! Integration tests for `itch_tcp::Server`.
//!
//! Each test brings up a real `Server::bind` on `127.0.0.1:0`,
//! connects one or more clients via `itch_tcp::connect`, and
//! asserts the wire-level message ordering / behavior.
//!
//! The tests use [`ChannelSource`] for the live data so the
//! producer can pace itself — that way the client has time to
//! connect before the source exhausts and the broadcast closes.
//! Late-attach (subscribing after the source has ended) is *not*
//! supported by `itch-tcp` 0.2 — full reconnect-resume lands with
//! `itch-soup` per issue #18.

use std::time::Duration;

use futures::StreamExt;
use itch_protocol::{enums::EventCode, messages::SystemEvent, Header, Message};
use itch_source::{ChannelSource, RingBufferSeqStore, StaticPolicy};
use itch_tcp::{connect, Server};

fn fixture(n: u8) -> Message {
    let codes = [
        EventCode::StartOfMessages,
        EventCode::StartOfSystemHours,
        EventCode::StartOfMarketHours,
        EventCode::EndOfMarketHours,
        EventCode::EndOfSystemHours,
        EventCode::EndOfMessages,
    ];
    Message::SystemEvent(SystemEvent {
        header: Header::default(),
        event_code: codes[(n as usize) % codes.len()],
    })
}

async fn drain_until_close(mut framed: itch_tcp::ItchConnection) -> Vec<Message> {
    let mut got: Vec<Message> = Vec::new();
    let deadline = tokio::time::sleep(Duration::from_secs(3));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            _ = &mut deadline => break,
            item = framed.next() => match item {
                Some(Ok(msg)) => got.push(msg),
                Some(Err(err)) => panic!("client error: {err:?}"),
                None => break,
            }
        }
    }
    got
}

/// Bring up a server backed by a bounded channel, returning
/// `(addr, sender, server_handle)`. The caller drives the sender
/// to feed messages on its own schedule.
async fn spawn_server() -> (
    std::net::SocketAddr,
    tokio::sync::mpsc::Sender<Message>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let (tx, source) = ChannelSource::bounded(64);
    let store = RingBufferSeqStore::new();
    let policy = StaticPolicy::empty();

    let server = Server::bind("127.0.0.1:0", source, store, policy)
        .await
        .expect("bind");
    let addr = server.local_addr().expect("local_addr");
    let handle = tokio::spawn(server.serve());
    // Brief tick so the accept loop is up before the test connects.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, tx, handle)
}

/// Sleep long enough for the server-side `accept` arm to complete
/// the subscribe-and-spawn after a client TCP-level `connect`. The
/// kernel completes the three-way handshake before the user-space
/// `accept().await` resumes, so the test can race the producer
/// against the broadcast subscribe — sleeping bridges that.
async fn wait_for_subscribe() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn single_client_receives_every_message_in_order() {
    let (addr, tx, server_handle) = spawn_server().await;

    let client = connect(addr).await.expect("client connect");
    wait_for_subscribe().await;
    // Once the client is attached, push 10 messages.
    let messages: Vec<Message> = (0..10).map(fixture).collect();
    for m in &messages {
        tx.send(*m).await.expect("send");
    }
    drop(tx); // closes the source → server drains and closes

    let received = drain_until_close(client).await;
    assert_eq!(received.len(), messages.len());
    for (got, want) in received.iter().zip(messages.iter()) {
        assert_eq!(got, want);
    }

    let _ = server_handle.await;
}

#[tokio::test]
async fn two_clients_concurrently_receive_every_message() {
    let (addr, tx, server_handle) = spawn_server().await;

    let a = connect(addr).await.expect("client A");
    let b = connect(addr).await.expect("client B");
    wait_for_subscribe().await;

    let messages: Vec<Message> = (0..10).map(fixture).collect();
    for m in &messages {
        tx.send(*m).await.expect("send");
    }
    drop(tx);

    let (got_a, got_b) = tokio::join!(drain_until_close(a), drain_until_close(b));
    assert_eq!(got_a, messages, "client A");
    assert_eq!(got_b, messages, "client B");

    let _ = server_handle.await;
}

#[tokio::test]
async fn warmup_messages_arrive_before_live() {
    let warmup_msgs = vec![fixture(0), fixture(1)];
    let (tx, source) = ChannelSource::bounded(64);
    let store = RingBufferSeqStore::new();
    let policy = StaticPolicy::new(warmup_msgs.clone());

    let server = Server::bind("127.0.0.1:0", source, store, policy)
        .await
        .expect("bind");
    let addr = server.local_addr().expect("local_addr");
    let server_handle = tokio::spawn(server.serve());
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = connect(addr).await.expect("client connect");
    wait_for_subscribe().await;

    let live = vec![fixture(2), fixture(3)];
    for m in &live {
        tx.send(*m).await.expect("send");
    }
    drop(tx);

    let received = drain_until_close(client).await;
    let mut expected = warmup_msgs.clone();
    expected.extend(live.iter().copied());
    assert_eq!(received, expected, "warmup must precede live");

    let _ = server_handle.await;
}
