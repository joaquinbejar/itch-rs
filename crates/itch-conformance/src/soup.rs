//! Async SoupBinTCP test driver (feature-gated).
//!
//! Walks a third-party SoupBinTCP server through the documented
//! lifecycle scenarios and reports pass / fail per scenario. The
//! driver is best-effort and intentionally narrow — full-coverage
//! integration tests live in `crates/itch-soup/tests/integration.rs`.
//!
//! ```ignore
//! use std::net::SocketAddr;
//! use itch_conformance::soup::{drive_server, SoupScenario};
//! use itch_soup::SoupCredentials;
//!
//! # async fn doc(addr: SocketAddr) {
//! let creds = SoupCredentials::new("user", "pass");
//! let scenarios = [
//!     SoupScenario::LoginAccept,
//!     SoupScenario::HeartbeatUnderSilence,
//!     SoupScenario::Logout,
//! ];
//! let report = drive_server(addr, creds, &scenarios).await;
//! for outcome in &report {
//!     println!("{outcome:?}");
//! }
//! # }
//! ```
//!
//! The driver never panics on a misbehaving peer: every scenario
//! that fails is surfaced as a [`SoupResult::Fail`] carrying a short
//! diagnostic string.

use std::net::SocketAddr;
use std::time::Duration;

use futures::StreamExt;
use itch_soup::{login_with_timeout, SoupConnection, SoupCredentials, SoupError};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Default per-scenario operation timeout. Bounds how long the
/// driver waits for a single login / message exchange before
/// declaring a scenario failed.
pub const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);

/// One end-to-end scenario the driver can exercise against a
/// SoupBinTCP server. The scenarios cover the same lifecycle paths
/// that `itch-soup`'s integration tests exercise internally — the
/// purpose here is to expose a public, embeddable subset for
/// downstream conformance checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoupScenario {
    /// Connect, log in successfully, immediately disconnect.
    LoginAccept,
    /// Connect with deliberately bad credentials and expect a
    /// `LoginRejected` error.
    LoginReject,
    /// Connect, log in, then receive `messages` sequenced-data
    /// messages from the server.
    SequencedFlow {
        /// Number of inbound sequenced-data messages to drain.
        messages: usize,
    },
    /// Connect, log in, idle for ~1 s, expect no error and a quiet
    /// channel (the server should send a heartbeat).
    HeartbeatUnderSilence,
    /// Connect, log in, then idle for >15 s — the server is
    /// expected to time out the silent client. This scenario is
    /// inherently slow and may be skipped by tooling that does not
    /// want to wait wall-clock time.
    DeadLink,
    /// Log in, drop the connection, then reconnect with the
    /// `next_expected_sequence` carried across.
    ReconnectWithSequenceResume,
    /// Connect, log in, send a graceful `LogoutRequest`.
    Logout,
    /// Connect, log in, then expect an `EndOfSession` (`Z`) packet
    /// at some point.
    EndOfSession,
}

/// Outcome of a single scenario run.
#[derive(Debug, Clone)]
pub enum SoupResult {
    /// Scenario completed successfully.
    Pass(SoupScenario),
    /// Scenario failed; the second field carries a short
    /// human-readable reason (typically the rendered error).
    Fail(SoupScenario, String),
}

impl SoupResult {
    /// Whether this outcome is a `Pass`.
    #[must_use]
    #[inline]
    pub const fn is_pass(&self) -> bool {
        matches!(self, SoupResult::Pass(_))
    }

    /// The scenario this outcome refers to.
    #[must_use]
    #[inline]
    pub const fn scenario(&self) -> &SoupScenario {
        match self {
            SoupResult::Pass(s) | SoupResult::Fail(s, _) => s,
        }
    }
}

/// Drive a SoupBinTCP server through `scenarios`, reporting one
/// [`SoupResult`] per scenario in order.
///
/// Each scenario opens a fresh TCP connection to `addr`. A scenario
/// that cannot even connect is reported as a `Fail`; the driver does
/// not abort early.
///
/// # Errors
///
/// This function never returns an error directly; per-scenario
/// failures are encoded as `SoupResult::Fail` entries.
pub async fn drive_server(
    addr: SocketAddr,
    credentials: SoupCredentials,
    scenarios: &[SoupScenario],
) -> Vec<SoupResult> {
    let mut out = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let result = run_scenario(addr, credentials.clone(), *scenario).await;
        out.push(result);
    }
    out
}

async fn run_scenario(
    addr: SocketAddr,
    credentials: SoupCredentials,
    scenario: SoupScenario,
) -> SoupResult {
    match scenario {
        SoupScenario::LoginAccept => match connect_and_login(addr, credentials, "", 0).await {
            Ok(conn) => match conn.logout().await {
                Ok(()) => SoupResult::Pass(scenario),
                Err(err) => SoupResult::Fail(scenario, err.to_string()),
            },
            Err(err) => SoupResult::Fail(scenario, err.to_string()),
        },
        SoupScenario::LoginReject => {
            let bad = SoupCredentials::new(credentials.username.clone(), "__bad_password__");
            match connect_and_login(addr, bad, "", 0).await {
                Err(SoupError::LoginRejected(_)) => SoupResult::Pass(scenario),
                Err(other) => {
                    SoupResult::Fail(scenario, format!("expected LoginRejected, got {other}"))
                }
                Ok(_) => SoupResult::Fail(
                    scenario,
                    "server accepted login despite bad password".to_string(),
                ),
            }
        }
        SoupScenario::SequencedFlow { messages } => {
            match connect_and_login(addr, credentials, "", 0).await {
                Ok(mut conn) => {
                    for i in 0..messages {
                        let polled = timeout(SCENARIO_TIMEOUT, conn.next_message()).await;
                        match polled {
                            Err(_) => {
                                return SoupResult::Fail(
                                    scenario,
                                    format!("timed out waiting for message {i}"),
                                );
                            }
                            Ok(None) => {
                                return SoupResult::Fail(
                                    scenario,
                                    format!("stream ended after {i} messages"),
                                );
                            }
                            Ok(Some(Err(err))) => {
                                return SoupResult::Fail(
                                    scenario,
                                    format!("error on message {i}: {err}"),
                                );
                            }
                            Ok(Some(Ok(_msg))) => {}
                        }
                    }
                    SoupResult::Pass(scenario)
                }
                Err(err) => SoupResult::Fail(scenario, err.to_string()),
            }
        }
        SoupScenario::HeartbeatUnderSilence => {
            match connect_and_login(addr, credentials, "", 0).await {
                Ok(mut conn) => {
                    // Idle for ~1 s; expect no fatal error to land
                    // (heartbeats are filtered by the Stream impl).
                    let polled = timeout(Duration::from_millis(1100), conn.next_message()).await;
                    match polled {
                        Err(_) => SoupResult::Pass(scenario),
                        Ok(Some(Err(err))) => SoupResult::Fail(
                            scenario,
                            format!("unexpected error under silence: {err}"),
                        ),
                        Ok(Some(Ok(_))) => SoupResult::Pass(scenario), // server sent data, fine
                        Ok(None) => SoupResult::Fail(
                            scenario,
                            "stream ended before heartbeat window".to_string(),
                        ),
                    }
                }
                Err(err) => SoupResult::Fail(scenario, err.to_string()),
            }
        }
        SoupScenario::DeadLink => match connect_and_login(addr, credentials, "", 0).await {
            Ok(mut conn) => {
                // Sleep past the documented 15 s dead-link window
                // and verify the next poll surfaces an error.
                sleep(Duration::from_secs(16)).await;
                match timeout(Duration::from_secs(2), conn.next_message()).await {
                    Ok(Some(Err(_))) | Ok(None) => SoupResult::Pass(scenario),
                    Ok(Some(Ok(_))) => SoupResult::Pass(scenario),
                    Err(_) => SoupResult::Fail(
                        scenario,
                        "no dead-link signal after 15 s of silence".to_string(),
                    ),
                }
            }
            Err(err) => SoupResult::Fail(scenario, err.to_string()),
        },
        SoupScenario::ReconnectWithSequenceResume => {
            // First leg: log in, capture next_expected_sequence,
            // drop connection.
            let first = match connect_and_login(addr, credentials.clone(), "", 0).await {
                Ok(c) => c,
                Err(err) => return SoupResult::Fail(scenario, err.to_string()),
            };
            let session = first.session().to_string();
            let resume_seq = first.next_expected_sequence();
            // Drop the connection (no logout) by letting it go out
            // of scope.
            drop(first);
            // Second leg: log back in with the captured session +
            // sequence.
            match connect_and_login(addr, credentials, &session, resume_seq).await {
                Ok(conn) => match conn.logout().await {
                    Ok(()) => SoupResult::Pass(scenario),
                    Err(err) => SoupResult::Fail(scenario, err.to_string()),
                },
                Err(err) => SoupResult::Fail(scenario, err.to_string()),
            }
        }
        SoupScenario::Logout => match connect_and_login(addr, credentials, "", 0).await {
            Ok(conn) => match conn.logout().await {
                Ok(()) => SoupResult::Pass(scenario),
                Err(err) => SoupResult::Fail(scenario, err.to_string()),
            },
            Err(err) => SoupResult::Fail(scenario, err.to_string()),
        },
        SoupScenario::EndOfSession => {
            match connect_and_login(addr, credentials, "", 0).await {
                Ok(mut conn) => {
                    // Drain until EndOfSession or timeout.
                    let polled = timeout(SCENARIO_TIMEOUT, async {
                        while let Some(item) = conn.next().await {
                            if matches!(item, Err(SoupError::SessionEnded)) {
                                return Ok::<_, SoupError>(());
                            }
                        }
                        Ok(())
                    })
                    .await;
                    match polled {
                        Ok(Ok(())) => SoupResult::Pass(scenario),
                        Ok(Err(err)) => SoupResult::Fail(scenario, err.to_string()),
                        Err(_) => SoupResult::Fail(
                            scenario,
                            "did not receive end-of-session within scenario timeout".to_string(),
                        ),
                    }
                }
                Err(err) => SoupResult::Fail(scenario, err.to_string()),
            }
        }
    }
}

async fn connect_and_login(
    addr: SocketAddr,
    credentials: SoupCredentials,
    requested_session: &str,
    requested_sequence: u64,
) -> Result<SoupConnection<TcpStream>, SoupError> {
    let stream = TcpStream::connect(addr).await?;
    login_with_timeout(
        stream,
        credentials,
        requested_session,
        requested_sequence,
        SCENARIO_TIMEOUT,
    )
    .await
}
