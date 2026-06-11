// Copyright (c) 2026 Edilson Pateguana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: Edilson Pateguana
// Year: 2026
// File: amqp_loop.rs
// Description: Main event loop for processing an individual client connection.

//! AMQP 0-9-1 connection loop with content framing state machine.
//!
//! This handles:
//! - Protocol header negotiation + handshake (via amqp_connection)
//! - Frame reading with content framing (METHOD → HEADER → BODY*)
//! - Heartbeat send/receive
//! - Dispatch to AMQP method handlers
//!
//! Accepts any async stream (plain TCP or TLS) — TLS sits below AMQP.

// TODO: There are long functions and deep nested code

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use crate::protocol::amqp::codec::*;
use crate::protocol::amqp::connection as amqp_connection;
use crate::protocol::amqp::handlers::basic as amqp_basic;
use crate::protocol::amqp::handlers::dispatch as amqp_dispatch;
use crate::protocol::amqp::method::*;
use crate::protocol::amqp::properties::BasicProperties;
use crate::protocol::amqp::session::ConnectionState;
use crate::protocol::amqp::validation;
use crate::state::Broker;

struct ContentState {
    exchange: String,
    routing_key: String,
    mandatory: bool,
    properties: BasicProperties,
    body_size: u64,
    body_received: Vec<u8>,
    channel: u16,
}

/// Spawns a green thread / tokio task to handle a plain AMQP connection.
/// Spawns a new async task to handle an incoming plain-TCP AMQP
/// connection from the given stream and address.
pub fn spawn_amqp(stream: TcpStream, addr: SocketAddr, broker: Broker) {
    let boxed: Box<dyn crate::protocol::AsyncStream> = Box::new(stream);
    spawn_amqp_on_stream(boxed, addr, broker);
}

pub fn spawn_amqp_on_stream(
    stream: Box<dyn crate::protocol::AsyncStream>,
    addr: SocketAddr,
    broker: Broker,
) {
    tokio::spawn(async move {
        let conn_id = broker.alloc_conn_id();

        let (amqp_tx, mut amqp_rx) =
            tokio::sync::mpsc::channel::<Vec<u8>>(crate::config::delivery_channel_capacity());

        broker.connections.insert(
            conn_id,
            crate::state::ConnHandle {
                id: conn_id,
                addr,
                sink: std::sync::Arc::new(crate::protocol::MpscDeliverySink::new(amqp_tx)),
            },
        );

        broker
            .conn_state
            .insert(conn_id, Box::new(ConnectionState::new()));
        crate::metrics::record_conn_opened();

        info!(conn_id, %addr, "AMQP connection accepted");

        let (reader, writer) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        if amqp_connection::perform_handshake(conn_id, addr, &mut reader, &mut writer, &broker)
            .await
            .is_err()
        {
            broker.remove_connection(conn_id);
            info!(conn_id, "handshake failed, disconnected");
            return;
        }

        let heartbeat_secs =
            crate::protocol::amqp::session::with_conn_state_ref(&broker, conn_id, |cs| {
                cs.heartbeat
            })
            .unwrap_or(DEFAULT_HEARTBEAT);

        let heartbeat_interval = if heartbeat_secs > 0 {
            Duration::from_secs(heartbeat_secs as u64)
        } else {
            Duration::from_secs(crate::config::fallback_heartbeat_secs())
        };
        let heartbeat_timeout = heartbeat_interval * 2;

        let mut hb_ticker = tokio::time::interval(heartbeat_interval);
        hb_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        hb_ticker.tick().await;

        let mut last_activity = Instant::now();
        let mut content_state: Option<ContentState> = None;

        loop {
            let (neg_frame_max, neg_channel_max) =
                crate::protocol::amqp::session::with_conn_state_ref(&broker, conn_id, |cs| {
                    (cs.frame_max, cs.channel_max)
                })
                .unwrap_or((DEFAULT_FRAME_MAX, DEFAULT_CHANNEL_MAX));

            tokio::select! {
                result = amqp_connection::read_amqp_frame(&mut reader) => {
                    let frame = match result {
                        Ok(f) => f,
                        Err(_) => break,
                    };
                    last_activity = Instant::now();

                    debug!(conn_id, frame_type = frame.frame_type, channel = frame.channel, payload_len = frame.payload.len(), "FRAME_IN");

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
                            let action = process_method_frame(
                                conn_id, frame.channel, &frame.payload,
                                &mut content_state, &mut writer, &broker,
                            ).await;
                            match action {
                                FrameAction::Continue => {}
                                FrameAction::Close(code, msg) => {
                                    send_connection_close(&mut writer, code, &msg).await;
                                    break;
                                }
                                FrameAction::Disconnect => break,
                            }
                        }

                        FRAME_HEADER => {
                            handle_content_header_frame(
                                conn_id, &frame.payload, &mut content_state, &mut writer, &broker,
                            ).await;
                        }

                        FRAME_BODY => {
                            handle_content_body_frame(
                                conn_id, &frame.payload, &mut content_state, &mut writer, &broker,
                            ).await;
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

                _ = hb_ticker.tick() => {
                    if last_activity.elapsed() > heartbeat_timeout {
                        warn!(conn_id, "heartbeat timeout");
                        break;
                    }
                    // Zero-alloc: use static heartbeat frame slice
                    if writer.write_all(crate::protocol::amqp::codec::heartbeat_bytes()).await.is_err() { break; }
                    if writer.flush().await.is_err() { break; }
                }

                Some(frame_bytes) = amqp_rx.recv() => {
                    if writer.write_all(&frame_bytes).await.is_err() { break; }
                    while let Ok(more) = amqp_rx.try_recv() {
                        if writer.write_all(&more).await.is_err() { break; }
                    }
                    if writer.flush().await.is_err() { break; }
                }
            }
        }

        broker.remove_connection(conn_id);
        crate::metrics::record_conn_closed();
        info!(conn_id, "AMQP connection closed");
    });
}

/// Outcome of processing a single AMQP method frame.
enum FrameAction {
    /// Continue the connection loop normally.
    Continue,
    /// Send a connection.close with the given code/text, then disconnect.
    Close(u16, String),
    /// Disconnect immediately (e.g. the handler returned keep=false).
    Disconnect,
}

/// Decodes and dispatches a single AMQP method frame.
/// If the method is basic.publish, it initializes content_state for the
/// multi-frame publish flow and returns Continue.
async fn process_method_frame(
    conn_id: u64,
    channel: u16,
    payload: &[u8],
    content_state: &mut Option<ContentState>,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) -> FrameAction {
    let method = match decode_method(payload) {
        Ok(m) => m,
        Err(e) => {
            warn!(conn_id, error = %e, "bad method frame");
            return FrameAction::Close(SYNTAX_ERROR, "bad method frame".into());
        }
    };

    if let Some(err) = validation::validate_channel(channel, method.class_id) {
        warn!(
            conn_id,
            err,
            channel,
            class_id = method.class_id,
            "channel/class mismatch"
        );
        return FrameAction::Close(COMMAND_INVALID, err.into());
    }

    if method.class_id == CLASS_BASIC && method.method_id == METHOD_BASIC_PUBLISH {
        let (exchange, routing_key, mandatory, _immediate) =
            amqp_basic::parse_publish_args(&method.arguments);
        debug!(
            conn_id,
            exchange = exchange.as_str(),
            routing_key = routing_key.as_str(),
            "publish method received, waiting for content"
        );
        *content_state = Some(ContentState {
            exchange,
            routing_key,
            mandatory,
            properties: BasicProperties::default(),
            body_size: 0,
            body_received: Vec::new(),
            channel,
        });
        return FrameAction::Continue;
    }

    let keep = amqp_dispatch::dispatch_method(conn_id, channel, &method, writer, broker).await;
    if keep {
        FrameAction::Continue
    } else {
        FrameAction::Disconnect
    }
}

/// Processes an AMQP content header frame, updating the content state
/// and invoking handle_publish when body_size == 0.
async fn handle_content_header_frame(
    conn_id: u64,
    payload: &[u8],
    content_state: &mut Option<ContentState>,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let Some(cs) = content_state.as_mut() else {
        warn!(conn_id, "content header without publish");
        return;
    };

    let header = match decode_content_header(payload) {
        Ok(h) => h,
        Err(e) => {
            warn!(conn_id, error = %e, "bad content header");
            *content_state = None;
            return;
        }
    };

    debug!(
        conn_id,
        body_size = header.body_size,
        "header frame received"
    );
    cs.body_size = header.body_size;
    cs.properties = header.properties;
    cs.body_received.reserve(header.body_size as usize);

    if header.body_size == 0 {
        flush_publish(conn_id, content_state, writer, broker).await;
    }
}

/// Processes an AMQP body frame, appending data to the content state
/// and invoking handle_publish when all bytes have been received.
async fn handle_content_body_frame(
    conn_id: u64,
    payload: &[u8],
    content_state: &mut Option<ContentState>,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let Some(cs) = content_state.as_mut() else {
        warn!(conn_id, "body frame without content state");
        return;
    };
    cs.body_received.extend_from_slice(payload);
    if cs.body_received.len() as u64 >= cs.body_size {
        flush_publish(conn_id, content_state, writer, broker).await;
    }
}

/// Takes the completed content state and dispatches the full publish.
async fn flush_publish(
    conn_id: u64,
    content_state: &mut Option<ContentState>,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let state = content_state.take().unwrap();
    amqp_basic::handle_publish(
        conn_id,
        state.channel,
        &state.exchange,
        &state.routing_key,
        state.mandatory,
        &state.properties,
        &state.body_received,
        writer,
        broker,
    )
    .await;
}

async fn send_connection_close(
    writer: &mut crate::protocol::amqp::AmqpWriter,
    code: u16,
    text: &str,
) {
    let close = amqp_connection::build_connection_close(code, text, 0, 0);
    let frame = encode_method_frame(0, CLASS_CONNECTION, METHOD_CONNECTION_CLOSE, &close);
    let _ = writer.write_all(&frame).await;
    let _ = writer.flush().await;
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
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
