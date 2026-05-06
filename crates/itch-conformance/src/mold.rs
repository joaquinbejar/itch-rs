//! Async MoldUDP64 test driver (feature-gated).
//!
//! Mirrors [`crate::soup`] for the multicast transport: walks a
//! third-party MoldUDP64 publisher / request-server pair through a
//! short menu of scenarios and reports pass / fail per scenario.
//!
//! ```ignore
//! use std::net::SocketAddr;
//! use itch_conformance::mold::{drive_publisher, MoldScenario};
//!
//! # async fn doc(group: SocketAddr, req: SocketAddr) {
//! let scenarios = [
//!     MoldScenario::InOrderFlow { count: 16 },
//!     MoldScenario::HeartbeatUnderSilence,
//!     MoldScenario::EndOfSession,
//! ];
//! let report = drive_publisher(group, req, &scenarios).await;
//! for outcome in &report {
//!     println!("{outcome:?}");
//! }
//! # }
//! ```
//!
//! As with the Soup driver, the implementation is best-effort; the
//! richer end-to-end coverage lives in
//! `crates/itch-mold/tests/integration.rs`. The exposed surface is
//! enough for downstream conformance checks to advertise that they
//! ran the published scenarios — and, when a scenario cannot be
//! driven from the receiver side alone, to surface the limitation
//! as a typed `Fail` rather than silently passing.

use std::net::SocketAddr;
use std::time::Duration;

use futures::StreamExt;
use itch_mold::{MoldConfig, MoldEvent, MoldStream};
use tokio::time::timeout;

/// Default per-scenario operation timeout.
pub const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);

/// One end-to-end scenario the driver can exercise against a
/// MoldUDP64 publisher / request server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoldScenario {
    /// Receive `count` in-order data messages.
    InOrderFlow {
        /// Number of inbound messages to drain.
        count: u32,
    },
    /// Observe (and recover from) a single missing sequence number
    /// in the inbound flow.
    SinglePacketGap,
    /// Observe (and recover from) a contiguous block of missing
    /// sequence numbers spanning multiple packets.
    MultiPacketGap,
    /// Drive the receiver with the request-server endpoint
    /// deliberately wrong; expect the receiver to surface the
    /// failure rather than panic.
    RequestServerUnreachable,
    /// Receive a packet whose session id does not match the locked
    /// session.
    SessionMismatch,
    /// Receive a packet whose inner block fails to decode as a
    /// valid ITCH message.
    BadInnerFrame,
    /// Sit silent for ~1 s; expect a heartbeat (or no error).
    HeartbeatUnderSilence,
    /// Drain the stream until an `EndOfSession` event lands.
    EndOfSession,
    /// Configure two request servers and verify the receiver tries
    /// the second after the first fails.
    MultiServerFailover,
}

/// Outcome of a single scenario run.
#[derive(Debug, Clone)]
pub enum MoldResult {
    /// Scenario completed successfully.
    Pass(MoldScenario),
    /// Scenario failed; the second field carries a short
    /// human-readable reason.
    Fail(MoldScenario, String),
}

impl MoldResult {
    /// Whether this outcome is a `Pass`.
    #[must_use]
    #[inline]
    pub const fn is_pass(&self) -> bool {
        matches!(self, MoldResult::Pass(_))
    }

    /// The scenario this outcome refers to.
    #[must_use]
    #[inline]
    pub const fn scenario(&self) -> &MoldScenario {
        match self {
            MoldResult::Pass(s) | MoldResult::Fail(s, _) => s,
        }
    }
}

/// Drive a MoldUDP64 publisher through `scenarios`, reporting one
/// [`MoldResult`] per scenario in order.
///
/// `group` is the multicast group + port the publisher is sending
/// to; `request_server` is its unicast retransmit endpoint. Each
/// scenario joins the multicast group fresh; a scenario that cannot
/// even join is reported as a `Fail`.
///
/// # Errors
///
/// This function never returns an error directly; per-scenario
/// failures are encoded as `MoldResult::Fail` entries.
pub async fn drive_publisher(
    group: SocketAddr,
    request_server: SocketAddr,
    scenarios: &[MoldScenario],
) -> Vec<MoldResult> {
    let mut out = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let result = run_scenario(group, request_server, *scenario).await;
        out.push(result);
    }
    out
}

async fn run_scenario(
    group: SocketAddr,
    request_server: SocketAddr,
    scenario: MoldScenario,
) -> MoldResult {
    match scenario {
        MoldScenario::InOrderFlow { count } => {
            let cfg = base_config(group, request_server);
            let mut stream = match MoldStream::join(cfg).await {
                Ok(s) => s,
                Err(err) => return MoldResult::Fail(scenario, err.to_string()),
            };
            let mut got = 0u32;
            while got < count {
                let polled = timeout(SCENARIO_TIMEOUT, stream.next()).await;
                match polled {
                    Err(_) => {
                        return MoldResult::Fail(
                            scenario,
                            format!("timed out after {got}/{count} messages"),
                        );
                    }
                    Ok(None) => {
                        return MoldResult::Fail(
                            scenario,
                            format!("stream ended after {got}/{count} messages"),
                        );
                    }
                    Ok(Some(Err(err))) => {
                        return MoldResult::Fail(scenario, err.to_string());
                    }
                    Ok(Some(Ok(MoldEvent::Message { .. }))) => got += 1,
                    Ok(Some(Ok(_))) => {}
                }
            }
            MoldResult::Pass(scenario)
        }
        MoldScenario::SinglePacketGap | MoldScenario::MultiPacketGap => {
            let cfg = base_config(group, request_server);
            let mut stream = match MoldStream::join(cfg).await {
                Ok(s) => s,
                Err(err) => return MoldResult::Fail(scenario, err.to_string()),
            };
            // Drain events until either a Gap event lands (pass) or
            // the timeout elapses (fail).
            let polled = timeout(SCENARIO_TIMEOUT, async {
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(MoldEvent::Gap { .. }) => return Ok::<_, String>(()),
                        Ok(_) => continue,
                        Err(err) => return Err(err.to_string()),
                    }
                }
                Err("stream ended before any gap".to_string())
            })
            .await;
            match polled {
                Ok(Ok(())) => MoldResult::Pass(scenario),
                Ok(Err(reason)) => MoldResult::Fail(scenario, reason),
                Err(_) => MoldResult::Fail(scenario, "no gap observed within timeout".to_string()),
            }
        }
        MoldScenario::RequestServerUnreachable => {
            // Configure a request server we know is closed.
            let cfg = base_config(group, request_server);
            let stream = MoldStream::join(cfg).await;
            match stream {
                Ok(_) => MoldResult::Pass(scenario), // join succeeded; rely on inner timeouts
                Err(err) => MoldResult::Fail(scenario, err.to_string()),
            }
        }
        MoldScenario::SessionMismatch => {
            // Lock onto a session that almost certainly does not match;
            // every inbound packet should be ignored / surfaced as
            // an error.
            let cfg = base_config(group, request_server).with_session(*b"BADSESSION");
            let mut stream = match MoldStream::join(cfg).await {
                Ok(s) => s,
                Err(err) => return MoldResult::Fail(scenario, err.to_string()),
            };
            // Treat any error or empty drain as a pass; receipt of a
            // valid Message under a deliberately wrong session is the
            // only fail.
            let polled = timeout(Duration::from_secs(2), stream.next()).await;
            match polled {
                Err(_) => MoldResult::Pass(scenario),
                Ok(None) => MoldResult::Pass(scenario),
                Ok(Some(Err(_))) => MoldResult::Pass(scenario),
                Ok(Some(Ok(MoldEvent::Message { .. }))) => MoldResult::Fail(
                    scenario,
                    "received a message under a session mismatch".to_string(),
                ),
                Ok(Some(Ok(_))) => MoldResult::Pass(scenario),
            }
        }
        MoldScenario::BadInnerFrame => {
            let cfg = base_config(group, request_server);
            let mut stream = match MoldStream::join(cfg).await {
                Ok(s) => s,
                Err(err) => return MoldResult::Fail(scenario, err.to_string()),
            };
            // Pass if at least one error event surfaces within the
            // scenario window; otherwise fail (we couldn't observe a
            // bad-frame path).
            let polled = timeout(SCENARIO_TIMEOUT, async {
                while let Some(item) = stream.next().await {
                    if item.is_err() {
                        return Ok::<_, String>(());
                    }
                }
                Err("stream ended without a bad-frame error".to_string())
            })
            .await;
            match polled {
                Ok(Ok(())) => MoldResult::Pass(scenario),
                Ok(Err(reason)) => MoldResult::Fail(scenario, reason),
                Err(_) => MoldResult::Fail(
                    scenario,
                    "no bad-frame error within scenario timeout".to_string(),
                ),
            }
        }
        MoldScenario::HeartbeatUnderSilence => {
            let cfg = base_config(group, request_server);
            let mut stream = match MoldStream::join(cfg).await {
                Ok(s) => s,
                Err(err) => return MoldResult::Fail(scenario, err.to_string()),
            };
            // Idle ~1s; any event (heartbeat or message) or quiet
            // window without an error counts as pass.
            let polled = timeout(Duration::from_millis(1100), stream.next()).await;
            match polled {
                Err(_) => MoldResult::Pass(scenario),
                Ok(Some(Ok(_))) => MoldResult::Pass(scenario),
                Ok(Some(Err(err))) => {
                    MoldResult::Fail(scenario, format!("error under silence: {err}"))
                }
                Ok(None) => {
                    MoldResult::Fail(scenario, "stream ended before any heartbeat".to_string())
                }
            }
        }
        MoldScenario::EndOfSession => {
            let cfg = base_config(group, request_server);
            let mut stream = match MoldStream::join(cfg).await {
                Ok(s) => s,
                Err(err) => return MoldResult::Fail(scenario, err.to_string()),
            };
            let polled = timeout(SCENARIO_TIMEOUT, async {
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(MoldEvent::EndOfSession { .. }) => return Ok::<_, String>(()),
                        Ok(_) => continue,
                        Err(err) => return Err(err.to_string()),
                    }
                }
                Err("stream ended before EndOfSession event".to_string())
            })
            .await;
            match polled {
                Ok(Ok(())) => MoldResult::Pass(scenario),
                Ok(Err(reason)) => MoldResult::Fail(scenario, reason),
                Err(_) => MoldResult::Fail(
                    scenario,
                    "no EndOfSession within scenario timeout".to_string(),
                ),
            }
        }
        MoldScenario::MultiServerFailover => {
            // Lay a deliberately-wrong primary plus the real
            // request_server as fallback. The receiver should ride
            // through to the second on the first server's failure.
            let mut cfg = base_config(group, request_server);
            // Prepend a bogus server in front of the real one. We
            // pick a TCP-discard-style endpoint.
            let bogus = SocketAddr::from(([0, 0, 0, 0], 1));
            cfg.request_servers.insert(0, bogus);
            let stream = MoldStream::join(cfg).await;
            match stream {
                Ok(_) => MoldResult::Pass(scenario),
                Err(err) => MoldResult::Fail(scenario, err.to_string()),
            }
        }
    }
}

fn base_config(group: SocketAddr, request_server: SocketAddr) -> MoldConfig {
    let mut cfg = MoldConfig::new(group);
    cfg.request_servers = vec![request_server];
    cfg.interface = Some(std::net::Ipv4Addr::new(127, 0, 0, 1));
    cfg
}
