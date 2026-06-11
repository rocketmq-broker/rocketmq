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
// File: amqp_tx.rs
// Description: AMQP Transaction (Tx) class method handlers for lightweight commits/rollbacks.

//! AMQP 0-9-1 Tx class (class 90) and Confirm class (class 85).

use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

use crate::protocol::amqp::codec::*;
use crate::protocol::amqp::method::*;

use crate::protocol::amqp::session::PendingOp;
use crate::state::Broker;

use super::auth_check::send_channel_error;

pub async fn handle_tx_select(
    conn_id: u64,
    channel: u16,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    crate::protocol::amqp::session::with_conn_state(broker, conn_id, |cs| {
        cs.tx_mode = true;
        cs.tx_buffer.clear();
    });
    info!(conn_id, channel, "tx mode enabled");
    let reply = encode_method_frame(channel, CLASS_TX, METHOD_TX_SELECT_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

pub async fn process_tx_commit(
    conn_id: u64,
    channel: u16,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let ops = match drain_tx_buffer(conn_id, channel, writer, broker, METHOD_TX_COMMIT).await {
        Some(ops) => ops,
        None => return,
    };

    if !validate_tx_schemas(conn_id, channel, writer, broker, &ops).await {
        return;
    }

    apply_tx_ops(conn_id, broker, &ops);
    info!(conn_id, channel, ops = ops.len(), "tx committed");
    let reply = encode_method_frame(channel, CLASS_TX, METHOD_TX_COMMIT_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

/// Drains the tx_buffer from the ConnectionState, returning the pending ops.
/// Sends a channel error and returns `None` if not in tx mode.
async fn drain_tx_buffer(
    conn_id: u64,
    channel: u16,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
    method_id: u16,
) -> Option<Vec<PendingOp>> {
    let result = crate::protocol::amqp::session::with_conn_state(broker, conn_id, |cs| {
        if !cs.tx_mode {
            return None;
        }
        Some(std::mem::take(&mut cs.tx_buffer))
    })
    .flatten();

    if result.is_none() {
        warn!(conn_id, "tx operation without tx_select");
        send_channel_error(
            writer,
            channel,
            PRECONDITION_FAILED,
            "PRECONDITION_FAILED - not in tx mode",
            CLASS_TX,
            method_id,
        )
        .await;
    }
    result
}

/// Validates all publish ops in the tx buffer against queue schemas.
/// Returns `false` and sends a channel error on the first validation failure.
async fn validate_tx_schemas(
    conn_id: u64,
    channel: u16,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
    ops: &[PendingOp],
) -> bool {
    for op in ops {
        let PendingOp::Publish {
            routing_key, body, ..
        } = op
        else {
            continue;
        };
        let schema_err = broker
            .queues
            .get(routing_key.as_ref())
            .and_then(|q| q.schema.clone())
            .and_then(|s| crate::schema::validate::validate_message(&s, body).err());

        let Some(err) = schema_err else { continue };

        warn!(
            conn_id,
            queue = routing_key.as_ref(),
            "transactional schema validation failed: {}",
            err
        );
        crate::metrics::record_schema_validation_failed(routing_key);
        let broker_err = crate::schema::error::to_broker_error(routing_key, &err);
        send_channel_error(
            writer,
            channel,
            PRECONDITION_FAILED,
            &broker_err.to_reply_text(),
            CLASS_TX,
            METHOD_TX_COMMIT,
        )
        .await;
        return false;
    }
    true
}

/// Applies committed tx operations (publish + ack) to the broker.
fn apply_tx_ops(conn_id: u64, broker: &Broker, ops: &[PendingOp]) {
    for op in ops {
        match op {
            PendingOp::Publish {
                exchange,
                routing_key,
                headers,
                body,
            } => {
                let msg_id = broker.alloc_msg_id();
                if let Some(mut queue) = broker.queues.get_mut(routing_key.as_ref()) {
                    let msg = crate::queue::Message::new_routed(
                        msg_id,
                        headers.clone(),
                        body.clone(),
                        exchange.clone(),
                        routing_key.clone(),
                    );
                    queue
                        .messages
                        .push_back(crate::queue::message::QueueMessage::Full(msg));
                }
            }
            PendingOp::Ack { msg_id } => {
                apply_tx_ack(conn_id, broker, *msg_id);
            }
        }
    }
}

/// Applies a single transactional ack to the broker.
fn apply_tx_ack(conn_id: u64, broker: &Broker, msg_id: u64) {
    // OPT-1: O(1) lookup via delivery_index
    let queue_name = broker.delivery_index.remove(&msg_id).map(|(_, v)| v);
    let Some(qn) = queue_name else { return };
    let Some(mut entry) = broker.queues.get_mut(qn.as_ref()) else {
        return;
    };
    if entry.inflight.remove(&msg_id).is_none() {
        return;
    }
    let _ = broker.wal().log_ack(msg_id);
    if let Some(mut deliveries) = broker.conn_deliveries.get_mut(&conn_id) {
        deliveries.retain(|(_, tag)| *tag != msg_id);
    }
}

pub async fn process_tx_rollback(
    conn_id: u64,
    channel: u16,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let ops = match drain_tx_buffer(conn_id, channel, writer, broker, METHOD_TX_ROLLBACK).await {
        Some(ops) => ops,
        None => return,
    };

    info!(conn_id, channel, discarded = ops.len(), "tx rolled back");
    let reply = encode_method_frame(channel, CLASS_TX, METHOD_TX_ROLLBACK_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

pub async fn handle_confirm_select(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let no_wait = args.first().copied().unwrap_or(0) & 0x01 != 0;

    crate::protocol::amqp::session::with_channel(broker, conn_id, channel, |ch| {
        ch.confirm_mode = true;
        ch.next_delivery_tag = 1;
    });

    info!(conn_id, channel, "confirm mode enabled");
    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_CONFIRM, METHOD_CONFIRM_SELECT_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use crate::protocol::amqp::session::ConnectionState;

    #[test]
    fn tx_select_ok_frame() {
        let frame = encode_method_frame(1, CLASS_TX, METHOD_TX_SELECT_OK, &[]);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_TX);
        assert_eq!(m.method_id, METHOD_TX_SELECT_OK);
    }

    #[test]
    fn tx_commit_ok_frame() {
        let frame = encode_method_frame(1, CLASS_TX, METHOD_TX_COMMIT_OK, &[]);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_TX);
        assert_eq!(m.method_id, METHOD_TX_COMMIT_OK);
    }

    #[test]
    fn tx_rollback_ok_frame() {
        let frame = encode_method_frame(1, CLASS_TX, METHOD_TX_ROLLBACK_OK, &[]);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_TX);
        assert_eq!(m.method_id, METHOD_TX_ROLLBACK_OK);
    }

    #[test]
    fn confirm_select_ok_frame() {
        let frame = encode_method_frame(1, CLASS_CONFIRM, METHOD_CONFIRM_SELECT_OK, &[]);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_CONFIRM);
        assert_eq!(m.method_id, METHOD_CONFIRM_SELECT_OK);
    }
    fn test_broker() -> Broker {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_tx_wal");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(format!("test_{}.wal", id));
        let wal = std::sync::Arc::new(crate::storage::wal::Wal::open(&path).unwrap());
        crate::state::BrokerState::new(wal).into()
    }

    /// Dedicated unit test verification for `process_tx_commit` function with schema validation.
    #[tokio::test]
    async fn test_coverage_for_handle_tx_commit() {
        let broker: Broker = test_broker();

        let mut conn_state = ConnectionState::new();
        conn_state.username = "guest".to_string();
        conn_state.vhost = "/".to_string();
        conn_state.tx_mode = true;

        let mut q = crate::queue::QueueState::new();
        let schema_content = b"syntax = \"proto3\"; message User { string name = 1; }";
        let compiled = crate::schema::compile_proto(schema_content, "User").unwrap();
        q.schema = Some(std::sync::Arc::new(compiled));
        broker.queues.insert("schema-queue".to_string(), q);

        let (mut rx_stream, tx_stream) = tokio::io::duplex(65536);
        let boxed: Box<dyn crate::protocol::AsyncStream> = Box::new(tx_stream);
        let (_read_half, write_half) = tokio::io::split(boxed);
        let mut writer = tokio::io::BufWriter::new(write_half);

        let properties_json = crate::protocol::amqp::properties::BasicProperties {
            content_type: Some("application/json".to_string()),
            ..Default::default()
        };
        let mut prop_bytes = Vec::new();
        properties_json.encode(&mut prop_bytes).unwrap();

        conn_state.tx_buffer.push(PendingOp::Publish {
            exchange: "amq.direct".to_string().into(),
            routing_key: "schema-queue".to_string().into(),
            headers: prop_bytes.into(),
            body: b"{}".to_vec().into(),
        });
        broker.conn_state.insert(1, Box::new(conn_state));

        process_tx_commit(1, 1, &mut writer, &broker).await;

        {
            let queue = broker.queues.get("schema-queue").unwrap();
            assert_eq!(queue.messages.len(), 0);
        }

        let mut buf = vec![0u8; 4096];
        use tokio::io::AsyncReadExt;
        let n = rx_stream.read(&mut buf).await.unwrap();
        let mut got_channel_error = false;
        let mut offset = 0;
        while offset < n {
            if let Ok((decoded, consumed)) = decode_frame(&buf[offset..n]) {
                offset += consumed;
                if decoded.frame_type == FRAME_METHOD {
                    if let Ok(m) = decode_method(&decoded.payload) {
                        if m.class_id == CLASS_CHANNEL && m.method_id == 40 {
                            got_channel_error = true;
                        }
                    }
                }
            } else {
                break;
            }
        }
        assert!(got_channel_error);

        let mut cs_guard = broker.conn_state.get_mut(&1).unwrap();
        let conn_state = cs_guard
            .value_mut()
            .as_any_mut()
            .downcast_mut::<ConnectionState>()
            .unwrap();
        conn_state.tx_buffer.clear();
        let properties_proto = crate::protocol::amqp::properties::BasicProperties {
            content_type: Some("application/x-protobuf".to_string()),
            ..Default::default()
        };
        let mut prop_bytes_proto = Vec::new();
        properties_proto.encode(&mut prop_bytes_proto).unwrap();

        let mut valid_body = vec![0x0A, 5];
        valid_body.extend_from_slice(b"Alice");

        conn_state.tx_buffer.push(PendingOp::Publish {
            exchange: "amq.direct".to_string().into(),
            routing_key: "schema-queue".to_string().into(),
            headers: prop_bytes_proto.into(),
            body: valid_body.into(),
        });
        drop(cs_guard);

        let (mut rx_stream2, tx_stream2) = tokio::io::duplex(65536);
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut buf = vec![0u8; 1024];
            while let Ok(n) = rx_stream2.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
        });
        let boxed2: Box<dyn crate::protocol::AsyncStream> = Box::new(tx_stream2);
        let (_read_half2, write_half2) = tokio::io::split(boxed2);
        let mut writer2 = tokio::io::BufWriter::new(write_half2);

        process_tx_commit(1, 1, &mut writer2, &broker).await;

        {
            let queue = broker.queues.get("schema-queue").unwrap();
            assert_eq!(queue.messages.len(), 1);
        }
    }
}
