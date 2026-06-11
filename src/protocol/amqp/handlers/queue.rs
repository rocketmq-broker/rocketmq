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
// File: amqp_queue.rs
// Description: AMQP Queue class method handlers (declare, bind, purge, delete).

//! AMQP 0-9-1 Queue class handlers (class 50).

use std::io::Cursor;
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::protocol::amqp::codec::*;
use crate::protocol::amqp::method::*;
use crate::protocol::amqp::types::*;
use crate::queue::{QueueOptions, QueueState};
use crate::routing::exchange::Binding;
use crate::state::Broker;

use super::auth_check::send_channel_error;

pub async fn handle_declare(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let mut name = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let passive = flags & 0x01 != 0;
    let durable = flags & 0x02 != 0;
    let exclusive = flags & 0x04 != 0;
    let auto_delete = flags & 0x08 != 0;
    let no_wait = flags & 0x10 != 0;
    let arguments = read_field_table(&mut r).unwrap_or_default();

    if name.is_empty() {
        name = format!("amq.gen-{}", broker.alloc_msg_id());
    } else if !passive && name.starts_with("amq.") && !name.starts_with("amq.gen-") {
        send_channel_error(
            writer,
            channel,
            ACCESS_REFUSED,
            "ACCESS_REFUSED - queue names starting with 'amq.' are reserved",
            CLASS_QUEUE,
            METHOD_QUEUE_DECLARE,
        )
        .await;
        return;
    }

    if !passive
        && super::auth_check::check_configure(
            conn_id,
            channel,
            &name,
            CLASS_QUEUE,
            METHOD_QUEUE_DECLARE,
            writer,
            broker,
        )
        .await
    {
        return;
    }

    if passive {
        let Some(q) = broker.queues.get(&name) else {
            send_channel_error(
                writer,
                channel,
                NOT_FOUND,
                "NOT_FOUND - no such queue",
                CLASS_QUEUE,
                METHOD_QUEUE_DECLARE,
            )
            .await;
            return;
        };

        let (msg_count, consumer_count) = (q.messages.len() as u32, q.consumer_tags.len() as u32);
        if !no_wait {
            send_declare_ok(channel, &name, msg_count, consumer_count, writer).await;
        }
        return;
    }

    let locked = broker
        .queues
        .get(&name)
        .is_some_and(|q| q.options.exclusive && q.owner_conn_id != Some(conn_id));
    if locked {
        send_channel_error(
            writer,
            channel,
            RESOURCE_LOCKED,
            "RESOURCE_LOCKED - exclusive queue",
            CLASS_QUEUE,
            METHOD_QUEUE_DECLARE,
        )
        .await;
        return;
    }

    let mut opts = QueueOptions {
        durable,
        exclusive,
        auto_delete,
        ..QueueOptions::default()
    };

    // Parse x-queue-type for quorum/stream support
    if let Some(FieldValue::LongString(v)) = arguments.get("x-queue-type") {
        let type_str = String::from_utf8_lossy(v);
        opts.queue_type = crate::queue::options::QueueType::from_amqp_arg(Some(&type_str));
        if type_str == "stream" {
            opts.stream_mode = true;
        }
    }
    if let Some(FieldValue::LongInt(v)) = arguments.get("x-quorum-initial-group-size") {
        opts.quorum_group_size = *v as u32;
    }
    // Quorum queues are always durable (Raft requires persistence)
    if opts.queue_type == crate::queue::options::QueueType::Quorum {
        opts.durable = true;
    }

    if let Some(FieldValue::LongInt(v)) = arguments.get("x-message-ttl") {
        opts.message_ttl = Some(std::time::Duration::from_millis(*v as u64));
    }
    if let Some(FieldValue::LongInt(v)) = arguments.get("x-max-length") {
        opts.max_length = Some(*v as usize);
    }
    if let Some(FieldValue::LongString(v)) = arguments.get("x-dead-letter-exchange") {
        opts.dead_letter_exchange = Some(String::from_utf8_lossy(v).to_string());
    }
    if let Some(FieldValue::LongString(v)) = arguments.get("x-schema") {
        opts.schema = Some(v.clone());
    }
    if let Some(FieldValue::LongString(v)) = arguments.get("x-schema-type") {
        opts.schema_type = Some(String::from_utf8_lossy(v).to_string());
    }
    if let Some(FieldValue::LongString(v)) = arguments.get("x-schema-message") {
        opts.schema_message = Some(String::from_utf8_lossy(v).to_string());
    }

    let schema_override = arguments.contains_key("x-schema-override");
    let schema_delete = arguments.contains_key("x-schema-delete");

    let mut compiled_schema = None;
    if let Some(raw) = &opts.schema {
        let _schema_type = match &opts.schema_type {
            Some(t) if t == "protobuf" => t.clone(),
            Some(_) => {
                let err = crate::schema::error::BrokerError {
                    code: crate::schema::error::ErrorCode::SchemaUnsupportedType,
                    queue: name.clone(),
                    fields: vec![],
                    truncated: false,
                };
                send_channel_error(
                    writer,
                    channel,
                    PRECONDITION_FAILED,
                    &err.to_reply_text(),
                    CLASS_QUEUE,
                    METHOD_QUEUE_DECLARE,
                )
                .await;
                return;
            }
            None => {
                let err = crate::schema::error::BrokerError {
                    code: crate::schema::error::ErrorCode::MissingArgument,
                    queue: name.clone(),
                    fields: vec![],
                    truncated: false,
                };
                send_channel_error(
                    writer,
                    channel,
                    PRECONDITION_FAILED,
                    &err.to_reply_text(),
                    CLASS_QUEUE,
                    METHOD_QUEUE_DECLARE,
                )
                .await;
                return;
            }
        };

        let message_name = match &opts.schema_message {
            Some(m) => m.clone(),
            None => {
                let err = crate::schema::error::BrokerError {
                    code: crate::schema::error::ErrorCode::MissingArgument,
                    queue: name.clone(),
                    fields: vec![],
                    truncated: false,
                };
                send_channel_error(
                    writer,
                    channel,
                    PRECONDITION_FAILED,
                    &err.to_reply_text(),
                    CLASS_QUEUE,
                    METHOD_QUEUE_DECLARE,
                )
                .await;
                return;
            }
        };

        match crate::schema::compile_proto(raw, &message_name) {
            Ok(compiled) => {
                compiled_schema = Some(std::sync::Arc::new(compiled));
            }
            Err(_) => {
                let err = crate::schema::error::BrokerError {
                    code: crate::schema::error::ErrorCode::SchemaCompileFailed,
                    queue: name.clone(),
                    fields: vec![],
                    truncated: false,
                };
                send_channel_error(
                    writer,
                    channel,
                    PRECONDITION_FAILED,
                    &err.to_reply_text(),
                    CLASS_QUEUE,
                    METHOD_QUEUE_DECLARE,
                )
                .await;
                return;
            }
        }
    }

    let is_new = !broker.queues.contains_key(&name);

    // Capture before `opts` is consumed by or_insert_with closure
    let queue_type_str = opts.queue_type.as_str().to_string();
    let group_size = opts.quorum_group_size;
    let is_quorum = opts.queue_type == crate::queue::options::QueueType::Quorum;

    // Scoped to drop the DashMap RefMut before auto_bind_default_exchange,
    // which also accesses broker.queues and would deadlock otherwise.
    {
        let mut entry = broker.queues.entry(name.clone()).or_insert_with(|| {
            let mut q = QueueState::with_options(opts);
            q.name_arc = std::sync::Arc::from(name.as_str());
            if exclusive {
                q.owner_conn_id = Some(conn_id);
            }
            q
        });

        if exclusive && is_new {
            broker.register_exclusive_queue(conn_id, &name);
        }

        // x-schema-delete removes the schema binding from the queue.
        if schema_delete {
            if entry.schema.is_some() {
                tracing::info!(
                    conn_id,
                    channel,
                    queue = name.as_str(),
                    "schema removed via x-schema-delete"
                );
                entry.schema = None;
            }
        } else if let Some(ref new_schema) = compiled_schema {
            // Reject conflicting schema re-declarations unless x-schema-override is set.
            let schema_conflict = if !schema_override {
                entry.schema.as_ref().and_then(|ex| {
                    crate::schema::validate::check_schema_conflict(ex, new_schema).err()
                })
            } else {
                None
            };
            if let Some(e) = schema_conflict {
                tracing::warn!(
                    conn_id,
                    channel,
                    queue = name.as_str(),
                    error = %e,
                    "schema conflict on re-declaration"
                );
                let broker_err = crate::schema::error::to_broker_error(&name, &e);
                send_channel_error(
                    writer,
                    channel,
                    406,
                    &broker_err.to_reply_text(),
                    CLASS_QUEUE,
                    METHOD_QUEUE_DECLARE,
                )
                .await;
                return;
            }
            entry.schema = compiled_schema.clone();
        }
    }

    broker.auto_bind_default_exchange(&name);

    if is_new {
        // For quorum queues: create Raft group and set replica info
        if is_quorum {
            if let Some(c) = broker.cluster() {
                let node_id = c.node_id;
                let replicas = c.create_queue_raft_group(&name, group_size);
                if let Some(mut q) = broker.queues.get_mut(&name) {
                    q.leader_node = Some(node_id);
                    q.replica_nodes = replicas;
                }
            } else {
                // Single-node: set self as leader
                if let Some(mut q) = broker.queues.get_mut(&name) {
                    q.leader_node = Some(crate::config::get_node_id());
                    q.replica_nodes = vec![crate::config::get_node_id()];
                }
            }
        }

        if let Some(c) = broker.cluster() {
            let c = c.clone();
            let name_clone = name.clone();
            tokio::spawn(async move {
                c.broadcast(crate::cluster::ClusterFrame::DeclareQueue {
                    name: name_clone,
                    durable,
                    exclusive,
                    auto_delete,
                    queue_type: queue_type_str,
                    group_size,
                })
                .await;
            });
        }
    }

    if durable && is_new {
        let wal = broker.wal();
        let _ = wal.log_declare_queue(&name, true);
        if let Some(ref schema) = compiled_schema {
            let _ = wal.log_set_queue_schema(
                schema.id,
                &name,
                &schema.raw,
                &schema.descriptor_set_bytes,
                schema.message_descriptor.full_name(),
            );
        }
    }

    crate::metrics::record_queue_declared();
    if is_new {
        crate::metrics::record_queue_created();
    }
    info!(conn_id, channel, queue = name.as_str(), "queue declared");
    if !no_wait {
        let (msg_count, consumer_count) = broker
            .queues
            .get(&name)
            .map(|q| (q.messages.len() as u32, q.consumer_tags.len() as u32))
            .unwrap_or((0, 0));
        send_declare_ok(channel, &name, msg_count, consumer_count, writer).await;
    }
}

pub async fn handle_delete(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let name = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let if_unused = flags & 0x01 != 0;
    let if_empty = flags & 0x02 != 0;
    let no_wait = flags & 0x04 != 0;

    if !broker.queues.contains_key(&name) {
        send_channel_error(
            writer,
            channel,
            NOT_FOUND,
            "NOT_FOUND - no such queue",
            CLASS_QUEUE,
            METHOD_QUEUE_DELETE,
        )
        .await;
        return;
    }

    if let Some(q) = broker.queues.get(&name) {
        if if_unused && !q.consumer_tags.is_empty() {
            send_channel_error(
                writer,
                channel,
                PRECONDITION_FAILED,
                "PRECONDITION_FAILED - queue in use",
                CLASS_QUEUE,
                METHOD_QUEUE_DELETE,
            )
            .await;
            return;
        }
        if if_empty && !q.messages.is_empty() {
            send_channel_error(
                writer,
                channel,
                PRECONDITION_FAILED,
                "PRECONDITION_FAILED - queue not empty",
                CLASS_QUEUE,
                METHOD_QUEUE_DELETE,
            )
            .await;
            return;
        }
    }

    let msg_count = broker
        .queues
        .remove(&name)
        .map(|(_, q)| q.messages.len() as u32)
        .unwrap_or(0);

    broker.deregister_exclusive_queue(conn_id, &name);

    if let Some(c) = broker.cluster() {
        let c = c.clone();
        let name_clone = name.clone();
        tokio::spawn(async move {
            c.broadcast(crate::cluster::ClusterFrame::DeleteQueue { name: name_clone })
                .await;
        });
    }

    crate::metrics::record_queue_deleted();
    info!(
        conn_id,
        channel,
        queue = name.as_str(),
        msg_count,
        "queue deleted"
    );
    if !no_wait {
        let mut reply_args = Vec::new();
        write_long(&mut reply_args, msg_count).unwrap();
        let reply = encode_method_frame(channel, CLASS_QUEUE, METHOD_QUEUE_DELETE_OK, &reply_args);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

pub async fn handle_purge(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let name = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let no_wait = flags & 0x01 != 0;

    if !broker.queues.contains_key(&name) {
        send_channel_error(
            writer,
            channel,
            NOT_FOUND,
            "NOT_FOUND - no such queue",
            CLASS_QUEUE,
            METHOD_QUEUE_PURGE,
        )
        .await;
        return;
    }

    let msg_count = if let Some(mut q) = broker.queues.get_mut(&name) {
        let count = q.messages.len() as u32;
        q.messages.clear();
        if let Some(c) = broker.cluster() {
            let c = c.clone();
            let name_clone = name.clone();
            tokio::spawn(async move {
                c.broadcast(crate::cluster::ClusterFrame::PurgeQueue { name: name_clone })
                    .await;
            });
        }
        count
    } else {
        0
    };

    info!(
        conn_id,
        channel,
        queue = name.as_str(),
        msg_count,
        "queue purged"
    );
    if !no_wait {
        let mut reply_args = Vec::new();
        write_long(&mut reply_args, msg_count).unwrap();
        let reply = encode_method_frame(channel, CLASS_QUEUE, METHOD_QUEUE_PURGE_OK, &reply_args);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

pub async fn handle_bind(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let queue = read_shortstr(&mut r).unwrap_or_default();
    let exchange = read_shortstr(&mut r).unwrap_or_default();
    let routing_key = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let no_wait = flags & 0x01 != 0;
    let _arguments = read_field_table(&mut r).unwrap_or_default();

    if !broker.queues.contains_key(&queue) {
        send_channel_error(
            writer,
            channel,
            NOT_FOUND,
            "NOT_FOUND - no such queue",
            CLASS_QUEUE,
            METHOD_QUEUE_BIND,
        )
        .await;
        return;
    }

    {
        let mut exchanges = broker.exchanges.write().await;
        let Some(ex) = exchanges.get_mut(&exchange) else {
            send_channel_error(
                writer,
                channel,
                NOT_FOUND,
                "NOT_FOUND - no such exchange",
                CLASS_QUEUE,
                METHOD_QUEUE_BIND,
            )
            .await;
            return;
        };

        ex.add_binding(Binding {
            queue_name: queue.clone().into(),
            routing_key: routing_key.clone().into(),
            headers_match: None,
        });

        if let Some(c) = broker.cluster() {
            let c = c.clone();
            let exchange_clone = exchange.clone();
            let queue_clone = queue.clone();
            let routing_key_clone = routing_key.clone();
            tokio::spawn(async move {
                c.broadcast(crate::cluster::ClusterFrame::BindQueue {
                    exchange: exchange_clone,
                    queue: queue_clone,
                    routing_key: routing_key_clone,
                })
                .await;
            });
        }
    }

    info!(
        conn_id,
        exchange = exchange.as_str(),
        queue = queue.as_str(),
        routing_key = routing_key.as_str(),
        "queue bound"
    );

    {
        let wal = broker.wal();
        let _ = wal.log_bind(&exchange, &queue, &routing_key);
    }

    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_QUEUE, METHOD_QUEUE_BIND_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

pub async fn handle_unbind(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let queue = read_shortstr(&mut r).unwrap_or_default();
    let exchange = read_shortstr(&mut r).unwrap_or_default();
    let routing_key = read_shortstr(&mut r).unwrap_or_default();

    if !broker.queues.contains_key(&queue) {
        send_channel_error(
            writer,
            channel,
            NOT_FOUND,
            "NOT_FOUND - no such queue",
            CLASS_QUEUE,
            METHOD_QUEUE_UNBIND,
        )
        .await;
        return;
    }

    {
        let mut exchanges = broker.exchanges.write().await;
        let Some(ex) = exchanges.get_mut(&exchange) else {
            send_channel_error(
                writer,
                channel,
                NOT_FOUND,
                "NOT_FOUND - no such exchange",
                CLASS_QUEUE,
                METHOD_QUEUE_UNBIND,
            )
            .await;
            return;
        };
        ex.remove_binding(&queue, &routing_key);
    }

    info!(
        conn_id,
        exchange = exchange.as_str(),
        queue = queue.as_str(),
        "queue unbound"
    );
    let reply = encode_method_frame(channel, CLASS_QUEUE, METHOD_QUEUE_UNBIND_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

async fn send_declare_ok(
    channel: u16,
    name: &str,
    msg_count: u32,
    consumer_count: u32,
    writer: &mut crate::protocol::amqp::AmqpWriter,
) {
    let mut args = Vec::new();
    write_shortstr(&mut args, name).unwrap();
    write_long(&mut args, msg_count).unwrap();
    write_long(&mut args, consumer_count).unwrap();
    let reply = encode_method_frame(channel, CLASS_QUEUE, METHOD_QUEUE_DECLARE_OK, &args);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    fn make_declare_args(name: &str, flags: u8) -> Vec<u8> {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, name).unwrap();
        write_octet(&mut args, flags).unwrap();
        write_field_table(&mut args, &FieldTable::new()).unwrap();
        args
    }

    #[test]
    fn declare_args_parse() {
        let args = make_declare_args("test.q", 0x06);
        let mut r = Cursor::new(&args);
        let _ = read_short(&mut r).unwrap();
        assert_eq!(read_shortstr(&mut r).unwrap(), "test.q");
        let flags = read_octet(&mut r).unwrap();
        assert_eq!(flags & 0x02, 0x02);
        assert_eq!(flags & 0x04, 0x04);
    }

    #[test]
    fn declare_ok_frame() {
        let mut args = Vec::new();
        write_shortstr(&mut args, "my.queue").unwrap();
        write_long(&mut args, 5).unwrap();
        write_long(&mut args, 2).unwrap();
        let frame = encode_method_frame(1, CLASS_QUEUE, METHOD_QUEUE_DECLARE_OK, &args);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_QUEUE);
        assert_eq!(m.method_id, METHOD_QUEUE_DECLARE_OK);
        let mut r = Cursor::new(&m.arguments);
        assert_eq!(read_shortstr(&mut r).unwrap(), "my.queue");
        assert_eq!(read_long(&mut r).unwrap(), 5);
        assert_eq!(read_long(&mut r).unwrap(), 2);
    }

    #[test]
    fn delete_ok_frame() {
        let mut args = Vec::new();
        write_long(&mut args, 42).unwrap();
        let frame = encode_method_frame(1, CLASS_QUEUE, METHOD_QUEUE_DELETE_OK, &args);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_QUEUE);
        let mut r = Cursor::new(&m.arguments);
        assert_eq!(read_long(&mut r).unwrap(), 42);
    }

    #[test]
    fn purge_ok_frame() {
        let mut args = Vec::new();
        write_long(&mut args, 10).unwrap();
        let frame = encode_method_frame(1, CLASS_QUEUE, METHOD_QUEUE_PURGE_OK, &args);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_QUEUE);
        assert_eq!(m.method_id, METHOD_QUEUE_PURGE_OK);
    }

    #[test]
    fn bind_args_parse() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, "q1").unwrap();
        write_shortstr(&mut args, "amq.direct").unwrap();
        write_shortstr(&mut args, "rk1").unwrap();
        write_octet(&mut args, 0).unwrap();
        write_field_table(&mut args, &FieldTable::new()).unwrap();

        let mut r = Cursor::new(&args);
        let _ = read_short(&mut r).unwrap();
        assert_eq!(read_shortstr(&mut r).unwrap(), "q1");
        assert_eq!(read_shortstr(&mut r).unwrap(), "amq.direct");
        assert_eq!(read_shortstr(&mut r).unwrap(), "rk1");
    }

    #[test]
    fn unbind_args_parse() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, "q1").unwrap();
        write_shortstr(&mut args, "amq.topic").unwrap();
        write_shortstr(&mut args, "*.stock").unwrap();
        write_field_table(&mut args, &FieldTable::new()).unwrap();

        let mut r = Cursor::new(&args);
        let _ = read_short(&mut r).unwrap();
        assert_eq!(read_shortstr(&mut r).unwrap(), "q1");
        assert_eq!(read_shortstr(&mut r).unwrap(), "amq.topic");
        assert_eq!(read_shortstr(&mut r).unwrap(), "*.stock");
    }

    fn test_broker() -> Broker {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_queue_handler_wal");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(format!("test_{}.wal", id));
        let wal = std::sync::Arc::new(crate::storage::wal::Wal::open(&path).unwrap());
        crate::state::BrokerState::new(wal).into()
    }

    /// Dedicated unit test verification for `handle_declare` function.
    #[tokio::test]
    async fn test_coverage_for_handle_declare() {
        let broker: Broker = test_broker();

        let mut conn_state = crate::protocol::amqp::session::ConnectionState::new();
        conn_state.username = "guest".to_string();
        conn_state.vhost = "/".to_string();
        broker.conn_state.insert(1, Box::new(conn_state));

        let (mut rx_stream, tx_stream) = tokio::io::duplex(65536);

        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut buf = vec![0u8; 1024];
            while let Ok(n) = rx_stream.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
        });

        let boxed: Box<dyn crate::protocol::AsyncStream> = Box::new(tx_stream);
        let (_read_half, write_half) = tokio::io::split(boxed);
        let mut writer = tokio::io::BufWriter::new(write_half);

        let schema_content = b"syntax = \"proto3\"; message User { string name = 1; }";
        let mut arguments = FieldTable::new();
        arguments.insert(
            "x-schema".to_string(),
            FieldValue::LongString(schema_content.to_vec()),
        );
        arguments.insert(
            "x-schema-type".to_string(),
            FieldValue::LongString(b"protobuf".to_vec()),
        );
        arguments.insert(
            "x-schema-message".to_string(),
            FieldValue::LongString(b"User".to_vec()),
        );

        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, "schema-queue").unwrap();
        write_octet(&mut args, 0).unwrap();
        write_field_table(&mut args, &arguments).unwrap();

        handle_declare(1, 1, &args, &mut writer, &broker).await;

        {
            let queue = broker.queues.get("schema-queue").unwrap();
            assert!(queue.schema.is_some());
            let compiled = queue.schema.as_ref().unwrap();
            assert_eq!(compiled.message_descriptor.name(), "User");
        }

        handle_declare(1, 1, &args, &mut writer, &broker).await;

        let diff_schema = b"syntax = \"proto3\"; message User { string name = 1; int32 age = 2; }";
        let mut diff_arguments = FieldTable::new();
        diff_arguments.insert(
            "x-schema".to_string(),
            FieldValue::LongString(diff_schema.to_vec()),
        );
        diff_arguments.insert(
            "x-schema-type".to_string(),
            FieldValue::LongString(b"protobuf".to_vec()),
        );
        diff_arguments.insert(
            "x-schema-message".to_string(),
            FieldValue::LongString(b"User".to_vec()),
        );

        let mut diff_args = Vec::new();
        write_short(&mut diff_args, 0).unwrap();
        write_shortstr(&mut diff_args, "schema-queue").unwrap();
        write_octet(&mut diff_args, 0).unwrap();
        write_field_table(&mut diff_args, &diff_arguments).unwrap();

        handle_declare(1, 1, &diff_args, &mut writer, &broker).await;

        {
            // Schema conflict rejected — original schema preserved (1 field)
            let queue = broker.queues.get("schema-queue").unwrap();
            let compiled = queue.schema.as_ref().unwrap();
            assert_eq!(compiled.message_descriptor.fields().count(), 1);
        }

        // Re-declare with x-schema-override — should succeed and update
        diff_arguments.insert("x-schema-override".to_string(), FieldValue::Boolean(true));
        let mut override_args = Vec::new();
        write_short(&mut override_args, 0).unwrap();
        write_shortstr(&mut override_args, "schema-queue").unwrap();
        write_octet(&mut override_args, 0).unwrap();
        write_field_table(&mut override_args, &diff_arguments).unwrap();

        handle_declare(1, 1, &override_args, &mut writer, &broker).await;

        {
            let queue = broker.queues.get("schema-queue").unwrap();
            let compiled = queue.schema.as_ref().unwrap();
            assert_eq!(compiled.message_descriptor.fields().count(), 2);
        }
    }
}
