//! AMQP 0-9-1 connection loop with content framing state machine.
//!
//! This replaces the legacy RQ protocol loop. It handles:
//! - Protocol header negotiation + handshake (via amqp_connection)
//! - Frame reading with content framing (METHOD → HEADER → BODY*)
//! - Heartbeat send/receive
//! - Dispatch to AMQP method handlers
//!
//! Usage: call `spawn_amqp(stream, addr, broker)` instead of the old `spawn`.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use crate::core::amqp_codec::*;
use crate::core::method::*;
use crate::core::properties::BasicProperties;
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

/// Spawn a new AMQP 0-9-1 connection handler.
pub fn spawn_amqp(stream: TcpStream, addr: SocketAddr, broker: Broker) {
    tokio::spawn(async move {
        let conn_id = broker.alloc_conn_id();
        broker.connections.insert(
            conn_id,
            crate::state::ConnHandle {
                id: conn_id,
                addr,
                tx: tokio::sync::mpsc::channel(1).0, // placeholder, not used in AMQP mode
            },
        );
        broker.conn_state.insert(conn_id, ConnectionState::new());

        info!(conn_id, %addr, "AMQP connection accepted");

        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        // Handshake
        if amqp_connection::perform_handshake(conn_id, &mut reader, &mut writer, &broker)
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
            Duration::from_secs(60)
        };
        let heartbeat_timeout = heartbeat_interval * 2;

        let mut hb_ticker = tokio::time::interval(heartbeat_interval);
        hb_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        hb_ticker.tick().await; // skip first immediate tick

        let mut last_activity = Instant::now();
        let mut content_state: Option<ContentState> = None;

        // ── Main frame loop ───────────────────────────
        loop {
            tokio::select! {
                result = amqp_connection::read_amqp_frame(&mut reader) => {
                    let frame = match result {
                        Ok(f) => f,
                        Err(_) => break,
                    };
                    last_activity = Instant::now();

                    match frame.frame_type {
                        FRAME_METHOD => {
                            let method = match decode_method(&frame.payload) {
                                Ok(m) => m,
                                Err(e) => {
                                    warn!(conn_id, error = %e, "bad method frame");
                                    break;
                                }
                            };

                            // Basic.Publish is special: it starts content framing
                            if method.class_id == CLASS_BASIC && method.method_id == METHOD_BASIC_PUBLISH {
                                let (exchange, routing_key, mandatory, _immediate) =
                                    amqp_basic::parse_publish_args(&method.arguments);
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
                            debug!(conn_id, "heartbeat received");
                        }

                        _ => {
                            warn!(conn_id, frame_type = frame.frame_type, "unknown frame type");
                        }
                    }
                }

                _ = hb_ticker.tick() => {
                    if last_activity.elapsed() > heartbeat_timeout {
                        warn!(conn_id, "heartbeat timeout");
                        break;
                    }
                    // Send heartbeat
                    let hb = encode_heartbeat();
                    if writer.write_all(&hb).await.is_err() { break; }
                    if writer.flush().await.is_err() { break; }
                }
            }
        }

        broker.remove_connection(conn_id);
        info!(conn_id, "AMQP connection closed");
    });
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
