//! AMQP 0-9-1 connection loop with content framing state machine.
//!
//! This handles:
//! - Protocol header negotiation + handshake (via amqp_connection)
//! - Frame reading with content framing (METHOD → HEADER → BODY*)
//! - Heartbeat send/receive
//! - Dispatch to AMQP method handlers
//!
//! Accepts any async stream (plain TCP or TLS) — TLS sits below AMQP.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use crate::core::amqp_codec::*;
use crate::core::method::*;
use crate::core::properties::BasicProperties;
use crate::core::validation;
use crate::server::amqp_connection;
use crate::server::handler::amqp_basic;
use crate::server::handler::amqp_dispatch;
use crate::state::{Broker, ConnectionState};

/// Content framing state: tracks partially-received publish operations.
struct ContentState {
    exchange: String,
    routing_key: String,
    mandatory: bool,
    properties: BasicProperties,
    body_size: u64,
    body_received: Vec<u8>,
    channel: u16,
}

/// Spawn a new AMQP 0-9-1 connection handler on a plain TCP stream.
pub fn spawn_amqp(stream: TcpStream, addr: SocketAddr, broker: Broker) {
    let boxed: Box<dyn crate::server::AsyncStream> = Box::new(stream);
    spawn_amqp_on_stream(boxed, addr, broker);
}

/// Spawn a new AMQP 0-9-1 connection handler on any async stream.
/// TLS connections use this after the TLS handshake completes.
pub fn spawn_amqp_on_stream(
    stream: Box<dyn crate::server::AsyncStream>,
    addr: SocketAddr,
    broker: Broker,
) {
    tokio::spawn(async move {
        let conn_id = broker.alloc_conn_id();

        // Create AMQP delivery channel — server pushes raw frame bytes here
        let (amqp_tx, mut amqp_rx) =
            tokio::sync::mpsc::channel::<Vec<u8>>(crate::config::DELIVERY_CHANNEL_CAPACITY);

        broker.connections.insert(
            conn_id,
            crate::state::ConnHandle {
                id: conn_id,
                addr,
                amqp_tx,
            },
        );
        broker.conn_state.insert(conn_id, ConnectionState::new());

        info!(conn_id, %addr, "AMQP connection accepted");

        let (reader, writer) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        // Handshake
        if amqp_connection::perform_handshake(conn_id, addr, &mut reader, &mut writer, &broker)
            .await
            .is_err()
        {
            broker.remove_connection(conn_id);
            info!(conn_id, "handshake failed, disconnected");
            return;
        }

        // Get negotiated heartbeat interval
        let heartbeat_secs = broker
            .conn_state
            .get(&conn_id)
            .map(|cs| cs.heartbeat)
            .unwrap_or(DEFAULT_HEARTBEAT);

        let heartbeat_interval = if heartbeat_secs > 0 {
            Duration::from_secs(heartbeat_secs as u64)
        } else {
            Duration::from_secs(crate::config::FALLBACK_HEARTBEAT_SECS)
        };
        let heartbeat_timeout = heartbeat_interval * 2;

        let mut hb_ticker = tokio::time::interval(heartbeat_interval);
        hb_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        hb_ticker.tick().await; // skip first immediate tick

        let mut last_activity = Instant::now();
        let mut content_state: Option<ContentState> = None;

        // ── Main frame loop ───────────────────────────
        loop {
            // Get negotiated limits for validation
            let (neg_frame_max, neg_channel_max) = broker
                .conn_state
                .get(&conn_id)
                .map(|cs| (cs.frame_max, cs.channel_max))
                .unwrap_or((DEFAULT_FRAME_MAX, DEFAULT_CHANNEL_MAX));

            tokio::select! {
                // ── Incoming frames from client ───────
                result = amqp_connection::read_amqp_frame(&mut reader) => {
                    let frame = match result {
                        Ok(f) => f,
                        Err(_) => break,
                    };
                    last_activity = Instant::now();
                    debug!(conn_id, frame_type = frame.frame_type, channel = frame.channel, payload_len = frame.payload.len(), "FRAME_IN");

                    // ── Frame-level validation ────────────
                    if let Some(err) = validation::validate_frame_type(frame.frame_type) {
                        warn!(conn_id, err, frame_type = frame.frame_type, "frame validation failed");
                        send_connection_close(&mut writer, UNEXPECTED_FRAME, err).await;
                        break;
                    }
                    if let Some(err) = validation::validate_frame_size(frame.payload.len(), neg_frame_max) {
                        warn!(conn_id, err, "frame too large");
                        send_connection_close(&mut writer, FRAME_ERROR, err).await;
                        break;
                    }
                    if let Some(err) = validation::validate_channel_number(frame.channel, neg_channel_max) {
                        warn!(conn_id, err, channel = frame.channel, "channel number invalid");
                        send_connection_close(&mut writer, CHANNEL_ERROR, err).await;
                        break;
                    }

                    match frame.frame_type {
                        FRAME_METHOD => {
                            let method = match decode_method(&frame.payload) {
                                Ok(m) => m,
                                Err(e) => {
                                    warn!(conn_id, error = %e, "bad method frame");
                                    send_connection_close(&mut writer, SYNTAX_ERROR, "bad method frame").await;
                                    break;
                                }
                            };

                            // Validate channel/class relationship
                            if let Some(err) = validation::validate_channel(frame.channel, method.class_id) {
                                warn!(conn_id, err, channel = frame.channel, class_id = method.class_id, "channel/class mismatch");
                                send_connection_close(&mut writer, COMMAND_INVALID, err).await;
                                break;
                            }

                            // Basic.Publish is special: it starts content framing
                            if method.class_id == CLASS_BASIC && method.method_id == METHOD_BASIC_PUBLISH {
                                let (exchange, routing_key, mandatory, _immediate) =
                                    amqp_basic::parse_publish_args(&method.arguments);
                                debug!(conn_id, exchange = exchange.as_str(), routing_key = routing_key.as_str(), "publish method received, waiting for content");
                                content_state = Some(ContentState {
                                    exchange,
                                    routing_key,
                                    mandatory,
                                    properties: BasicProperties::default(),
                                    body_size: 0,
                                    body_received: Vec::new(),
                                    channel: frame.channel,
                                });
                                continue;
                            }

                            // All other methods go through the dispatcher
                            let keep = amqp_dispatch::dispatch_method(
                                conn_id, frame.channel, &method, &mut writer, &broker,
                            ).await;
                            if !keep { break; }
                        }

                        FRAME_HEADER => {
                            if let Some(ref mut cs) = content_state {
                                match decode_content_header(&frame.payload) {
                                    Ok(header) => {
                                        debug!(conn_id, body_size = header.body_size, "header frame received");
                                        cs.body_size = header.body_size;
                                        cs.properties = header.properties;
                                        cs.body_received.reserve(header.body_size as usize);

                                        // Zero-length body: deliver immediately
                                        if header.body_size == 0 {
                                            let state = content_state.take().unwrap();
                                            amqp_basic::handle_publish(
                                                conn_id, state.channel,
                                                &state.exchange, &state.routing_key, state.mandatory,
                                                &state.properties, &state.body_received,
                                                &mut writer, &broker,
                                            ).await;
                                        }
                                    }
                                    Err(e) => {
                                        warn!(conn_id, error = %e, "bad content header");
                                        content_state = None;
                                    }
                                }
                            } else {
                                warn!(conn_id, "content header without publish");
                            }
                        }

                        FRAME_BODY => {
                            if let Some(ref mut cs) = content_state {
                                cs.body_received.extend_from_slice(&frame.payload);
                                if cs.body_received.len() as u64 >= cs.body_size {
                                    let state = content_state.take().unwrap();
                                    amqp_basic::handle_publish(
                                        conn_id, state.channel,
                                        &state.exchange, &state.routing_key, state.mandatory,
                                        &state.properties, &state.body_received,
                                        &mut writer, &broker,
                                    ).await;
                                }
                            } else {
                                warn!(conn_id, "body frame without content state");
                            }
                        }

                        FRAME_HEARTBEAT => {
                            if let Some(err) = validation::validate_heartbeat(frame.channel, frame.payload.len()) {
                                warn!(conn_id, err, "invalid heartbeat");
                                send_connection_close(&mut writer, FRAME_ERROR, err).await;
                                break;
                            }
                            debug!(conn_id, "heartbeat received");
                        }

                        _ => {
                            warn!(conn_id, frame_type = frame.frame_type, "unknown frame type");
                        }
                    }
                }

                // ── Heartbeat ─────────────────────────
                _ = hb_ticker.tick() => {
                    if last_activity.elapsed() > heartbeat_timeout {
                        warn!(conn_id, "heartbeat timeout");
                        break;
                    }
                    let hb = encode_heartbeat();
                    if writer.write_all(&hb).await.is_err() { break; }
                    if writer.flush().await.is_err() { break; }
                }

                // ── Outgoing delivery frames ──────────
                Some(frame_bytes) = amqp_rx.recv() => {
                    if writer.write_all(&frame_bytes).await.is_err() { break; }
                    // Drain any additional queued frames without blocking
                    while let Ok(more) = amqp_rx.try_recv() {
                        if writer.write_all(&more).await.is_err() { break; }
                    }
                    if writer.flush().await.is_err() { break; }
                }
            }
        }

        broker.remove_connection(conn_id);
        info!(conn_id, "AMQP connection closed");
    });
}

/// Send a Connection.Close frame for fatal protocol errors.
async fn send_connection_close(writer: &mut crate::server::AmqpWriter, code: u16, text: &str) {
    let close = amqp_connection::build_connection_close(code, text, 0, 0);
    let frame = encode_method_frame(0, CLASS_CONNECTION, METHOD_CONNECTION_CLOSE, &close);
    let _ = writer.write_all(&frame).await;
    let _ = writer.flush().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_state_accumulates_body() {
        let mut cs = ContentState {
            exchange: "ex".into(),
            routing_key: "rk".into(),
            mandatory: false,
            properties: BasicProperties::default(),
            body_size: 10,
            body_received: Vec::new(),
            channel: 1,
        };
        cs.body_received.extend_from_slice(&[1, 2, 3, 4, 5]);
        assert_eq!(cs.body_received.len(), 5);
        assert!(cs.body_received.len() < cs.body_size as usize);
        cs.body_received.extend_from_slice(&[6, 7, 8, 9, 10]);
        assert!(cs.body_received.len() as u64 >= cs.body_size);
    }

    #[test]
    fn zero_body_publish() {
        let cs = ContentState {
            exchange: "amq.direct".into(),
            routing_key: "test".into(),
            mandatory: true,
            properties: BasicProperties::default(),
            body_size: 0,
            body_received: Vec::new(),
            channel: 1,
        };
        assert_eq!(cs.body_size, 0);
        assert!(cs.body_received.is_empty());
    }
}
