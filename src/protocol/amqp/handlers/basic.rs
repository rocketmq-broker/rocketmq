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
// File: amqp_basic.rs
// Description: AMQP Basic class method handlers (publish, consume, ack, nack, deliver).

//! AMQP 0-9-1 Basic class handlers (class 60).
//!
//! Handles Basic.Publish (with content framing), Basic.Consume, Basic.Cancel,
//! Basic.Ack, Basic.Nack, Basic.Reject, Basic.Get, Basic.Qos, Basic.Recover,
//! and Basic.Return.

use std::collections::HashMap;
use std::io::Cursor;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

use crate::protocol::amqp::codec::*;
use crate::protocol::amqp::method::*;
use crate::protocol::amqp::properties::BasicProperties;
use crate::protocol::amqp::types::*;

use super::auth_check::send_channel_error;
use crate::protocol::amqp::session::PendingOp;
use crate::queue::Message;
use crate::state::Broker;

#[inline]
pub fn parse_publish_args(args: &[u8]) -> (String, String, bool, bool) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let exchange = read_shortstr(&mut r).unwrap_or_default();
    let routing_key = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let mandatory = flags & 0x01 != 0;
    let immediate = flags & 0x02 != 0;
    (exchange, routing_key, mandatory, immediate)
}

pub async fn handle_publish(
    conn_id: u64,
    channel: u16,
    exchange_name: &str,
    routing_key: &str,
    mandatory: bool,
    properties: &BasicProperties,
    body: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    if super::auth_check::check_write(
        conn_id,
        channel,
        exchange_name,
        CLASS_BASIC,
        METHOD_BASIC_PUBLISH,
        writer,
        broker,
    )
    .await
    {
        return;
    }

    let confirm_tag = alloc_confirm_tag(conn_id, channel, broker);

    if buffer_tx_publish(
        conn_id,
        exchange_name,
        routing_key,
        properties,
        body,
        broker,
    ) {
        return;
    }

    let msg_headers = resolve_publish_headers(properties);
    let target_queues = match route_to_exchanges(
        conn_id,
        channel,
        exchange_name,
        routing_key,
        &msg_headers,
        confirm_tag,
        writer,
        broker,
    )
    .await
    {
        Some(qs) => qs,
        None => return,
    };

    if target_queues.is_empty() {
        handle_no_route(
            conn_id,
            channel,
            exchange_name,
            routing_key,
            mandatory,
            properties,
            body,
            confirm_tag,
            writer,
        )
        .await;
        return;
    }

    if validate_schemas(
        conn_id,
        channel,
        &target_queues,
        body,
        confirm_tag,
        writer,
        broker,
    )
    .await
    {
        return;
    }

    let published = enqueue_to_targets(
        conn_id,
        exchange_name,
        routing_key,
        properties,
        body,
        &target_queues,
        confirm_tag,
        broker,
    );

    replicate_confirmed_publishes(channel, body, confirm_tag, &published, writer, broker).await;
}

/// Buffers publish into tx_buffer when the connection is in transaction mode.
/// Returns true if the message was buffered (caller should return early).
fn buffer_tx_publish(
    conn_id: u64,
    exchange_name: &str,
    routing_key: &str,
    properties: &BasicProperties,
    body: &[u8],
    broker: &Broker,
) -> bool {
    let tx_mode =
        crate::protocol::amqp::session::with_conn_state_ref(broker, conn_id, |cs| cs.tx_mode)
            .unwrap_or(false);
    if !tx_mode {
        return false;
    }
    let mut prop_bytes = Vec::new();
    properties.encode(&mut prop_bytes).unwrap_or_default();
    crate::protocol::amqp::session::with_conn_state(broker, conn_id, |cs| {
        cs.tx_buffer.push(PendingOp::Publish {
            exchange: exchange_name.into(),
            routing_key: routing_key.into(),
            headers: prop_bytes.into(),
            body: body.to_vec().into(),
        });
    });
    true
}

/// Extracts a flat HashMap from BasicProperties headers for exchange routing.
fn resolve_publish_headers(properties: &BasicProperties) -> HashMap<String, String> {
    properties
        .headers
        .as_ref()
        .map(|h| {
            h.iter()
                .map(|(k, v)| (k.clone(), format!("{:?}", v)))
                .collect()
        })
        .unwrap_or_default()
}

/// Resolves the target queues via the exchange routing table.
/// Returns None if the exchange was not found (confirm ack already sent).
async fn route_to_exchanges(
    conn_id: u64,
    channel: u16,
    exchange_name: &str,
    routing_key: &str,
    msg_headers: &HashMap<String, String>,
    confirm_tag: Option<u64>,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) -> Option<Vec<std::sync::Arc<str>>> {
    let exchanges = broker.exchanges.read().await;
    let exchange = match exchanges.get(exchange_name) {
        Some(ex) => ex,
        None => {
            warn!(conn_id, exchange = exchange_name, "exchange not found");
            if let Some(tag) = confirm_tag {
                send_confirm_ack(channel, tag, writer).await;
            }
            return None;
        }
    };
    let mut qs = Vec::new();
    exchange.route_each(routing_key, msg_headers, |q| qs.push(q.clone()));
    // Drop exchange RwLock guard before touching queues
    drop(exchanges);
    debug!(conn_id, exchange = exchange_name, routing_key, targets = ?qs, "routed");
    Some(qs)
}

/// Handles the case where routing produced zero target queues.
async fn handle_no_route(
    conn_id: u64,
    channel: u16,
    exchange_name: &str,
    routing_key: &str,
    mandatory: bool,
    properties: &BasicProperties,
    body: &[u8],
    confirm_tag: Option<u64>,
    writer: &mut crate::protocol::amqp::AmqpWriter,
) {
    if mandatory {
        send_basic_return(
            channel,
            NO_ROUTE,
            "NO_ROUTE",
            exchange_name,
            routing_key,
            properties,
            body,
            writer,
        )
        .await;
    }
    warn!(
        conn_id,
        exchange = exchange_name,
        routing_key,
        "no matching bindings"
    );
    if let Some(tag) = confirm_tag {
        send_confirm_ack(channel, tag, writer).await;
    }
}

/// Validates message body against each target queue's schema.
/// Returns true if validation failed (caller should return early).
async fn validate_schemas(
    conn_id: u64,
    channel: u16,
    target_queues: &[std::sync::Arc<str>],
    body: &[u8],
    confirm_tag: Option<u64>,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) -> bool {
    for queue_name in target_queues {
        let schema_err = broker
            .queues
            .get(queue_name.as_ref())
            .and_then(|q| q.schema.clone())
            .and_then(|s| crate::schema::validate::validate_message(&s, body).err());

        let Some(err) = schema_err else { continue };

        warn!(
            conn_id,
            queue = queue_name.as_ref(),
            "schema validation failed: {}",
            err
        );
        crate::metrics::record_schema_validation_failed(queue_name.as_ref());
        let broker_err = crate::schema::error::to_broker_error(queue_name.as_ref(), &err);
        send_channel_error(
            writer,
            channel,
            PRECONDITION_FAILED,
            &broker_err.to_reply_text(),
            CLASS_BASIC,
            METHOD_BASIC_PUBLISH,
        )
        .await;
        if let Some(tag) = confirm_tag {
            send_confirm_nack(channel, tag, writer).await;
        }
        return true;
    }
    false
}

/// Context shared across the enqueue loop to avoid re-encoding per queue.
struct EnqueueCtx {
    exchange_arc: std::sync::Arc<str>,
    rk_arc: std::sync::Arc<str>,
    prop_bytes: bytes::Bytes,
    body_bytes: bytes::Bytes,
    is_persistent: bool,
    priority: u8,
    per_msg_ttl: Option<Duration>,
}

/// Enqueues the message into each target queue. Returns (queue, msg_id) pairs
/// for downstream cluster replication.
fn enqueue_to_targets(
    conn_id: u64,
    exchange_name: &str,
    routing_key: &str,
    properties: &BasicProperties,
    body: &[u8],
    target_queues: &[std::sync::Arc<str>],
    confirm_tag: Option<u64>,
    broker: &Broker,
) -> Vec<(std::sync::Arc<str>, u64)> {
    // OPT-3: encode properties once before the queue loop
    let mut raw_props = Vec::new();
    properties.encode(&mut raw_props).unwrap_or_default();

    let ctx = EnqueueCtx {
        exchange_arc: exchange_name.into(),
        rk_arc: routing_key.into(),
        prop_bytes: raw_props.into(),
        body_bytes: body.to_vec().into(),
        is_persistent: properties.delivery_mode == Some(2),
        priority: properties.priority.unwrap_or(0),
        per_msg_ttl: properties
            .expiration
            .as_ref()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis),
    };

    let mut published = Vec::new();
    for queue_name in target_queues {
        let msg_id = broker.alloc_msg_id();
        published.push((queue_name.clone(), msg_id));
        enqueue_single(conn_id, queue_name, msg_id, &ctx, confirm_tag, broker);
    }
    published
}

/// Enqueues a single message into one queue, handling TTL, priority capping,
/// max-length eviction, WAL persistence, and fire-and-forget replication.
fn enqueue_single(
    conn_id: u64,
    queue_name: &std::sync::Arc<str>,
    msg_id: u64,
    ctx: &EnqueueCtx,
    confirm_tag: Option<u64>,
    broker: &Broker,
) {
    let Some(mut queue_ref) = broker.queues.get_mut(queue_name.as_ref()) else {
        return;
    };
    let queue = queue_ref.value_mut();

    let expiration = resolve_ttl(queue.options.message_ttl, ctx.per_msg_ttl);
    let effective_priority = cap_priority(ctx.priority, queue.options.max_priority);
    evict_overflow(queue);

    let disk_ref = persist_if_durable(queue, queue_name, msg_id, ctx, broker);
    let msg = build_queue_message(msg_id, effective_priority, expiration, disk_ref, ctx);

    queue.last_activity = Instant::now();
    queue.messages.push_back(msg);
    queue.stat_published += 1;
    debug!(
        conn_id,
        msg_id,
        queue = queue_name.as_ref(),
        "queued via AMQP"
    );
    crate::metrics::record_published(queue_name);

    if confirm_tag.is_none() {
        fire_and_forget_replicate(broker, queue_name, msg_id, &ctx.body_bytes);
    }
}

fn resolve_ttl(queue_ttl: Option<Duration>, msg_ttl: Option<Duration>) -> Option<Instant> {
    match (queue_ttl, msg_ttl) {
        (Some(qt), Some(pt)) => Some(Instant::now() + qt.min(pt)),
        (Some(t), None) | (None, Some(t)) => Some(Instant::now() + t),
        (None, None) => None,
    }
}

fn cap_priority(requested: u8, max_priority: u8) -> u8 {
    if max_priority > 0 {
        requested.min(max_priority)
    } else {
        0
    }
}

fn evict_overflow(queue: &mut crate::queue::state::QueueState) {
    if let Some(max_len) = queue.options.max_length {
        while queue.messages.len() >= max_len {
            if queue.messages.pop_oldest().is_none() {
                break;
            }
        }
    }
}

fn persist_if_durable(
    queue: &crate::queue::state::QueueState,
    queue_name: &std::sync::Arc<str>,
    msg_id: u64,
    ctx: &EnqueueCtx,
    broker: &Broker,
) -> Option<(u64, u64, u32)> {
    if !queue.options.durable || !ctx.is_persistent {
        return None;
    }
    broker
        .wal()
        .log_enqueue(
            queue_name.as_ref(),
            msg_id,
            &ctx.exchange_arc,
            &ctx.rk_arc,
            &ctx.prop_bytes,
            &ctx.body_bytes,
        )
        .ok()
}

fn build_queue_message(
    msg_id: u64,
    priority: u8,
    expiration: Option<Instant>,
    disk_ref: Option<(u64, u64, u32)>,
    ctx: &EnqueueCtx,
) -> crate::queue::message::QueueMessage {
    if let Some((segment_id, offset, length)) = disk_ref {
        crate::queue::message::QueueMessage::Ref(crate::queue::message::MessageRef {
            id: msg_id,
            segment_id,
            offset,
            length,
            priority,
            expiration,
            redelivered: false,
            delivery_count: 0,
            exchange: ctx.exchange_arc.clone(),
            routing_key: ctx.rk_arc.clone(),
        })
    } else {
        crate::queue::message::QueueMessage::Full(Message {
            id: msg_id,
            headers: ctx.prop_bytes.clone(),
            body: ctx.body_bytes.clone(),
            priority,
            expiration,
            redelivered: false,
            delivery_count: 0,
            exchange: ctx.exchange_arc.clone(),
            routing_key: ctx.rk_arc.clone(),
        })
    }
}

fn fire_and_forget_replicate(
    broker: &Broker,
    queue_name: &std::sync::Arc<str>,
    msg_id: u64,
    body: &bytes::Bytes,
) {
    if let Some(c) = broker.cluster() {
        let c = c.clone();
        let q_name = queue_name.clone();
        let body_vec = body.to_vec();
        tokio::spawn(async move {
            let _ = c.replicate_publish(&q_name, msg_id, &body_vec).await;
        });
    }
}

async fn replicate_confirmed_publishes(
    channel: u16,
    body: &[u8],
    confirm_tag: Option<u64>,
    published: &[(std::sync::Arc<str>, u64)],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let Some(tag) = confirm_tag else { return };
    if let Some(c) = broker.cluster() {
        for (queue_name, msg_id) in published {
            let _ = c.replicate_publish(queue_name, *msg_id, body).await;
        }
    }
    send_confirm_ack(channel, tag, writer).await;
}

fn alloc_confirm_tag(conn_id: u64, channel: u16, broker: &Broker) -> Option<u64> {
    crate::protocol::amqp::session::with_channel(broker, conn_id, channel, |ch| {
        if !ch.confirm_mode {
            return None;
        }
        let tag = ch.next_delivery_tag;
        ch.next_delivery_tag += 1;
        Some(tag)
    })
    .flatten()
}

async fn send_confirm_ack(
    channel: u16,
    delivery_tag: u64,
    writer: &mut crate::protocol::amqp::AmqpWriter,
) {
    let mut args = Vec::new();
    write_longlong(&mut args, delivery_tag).unwrap();
    write_octet(&mut args, 0).unwrap();
    let frame = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_ACK, &args);
    let _ = writer.write_all(&frame).await;
    let _ = writer.flush().await;
}

async fn send_confirm_nack(
    channel: u16,
    delivery_tag: u64,
    writer: &mut crate::protocol::amqp::AmqpWriter,
) {
    let mut args = Vec::new();
    write_longlong(&mut args, delivery_tag).unwrap();
    write_octet(&mut args, 0).unwrap();
    write_octet(&mut args, 0).unwrap();
    let frame = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_NACK, &args);
    let _ = writer.write_all(&frame).await;
    let _ = writer.flush().await;
}

async fn send_basic_return(
    channel: u16,
    reply_code: u16,
    reply_text: &str,
    exchange: &str,
    routing_key: &str,
    properties: &BasicProperties,
    body: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
) {
    let mut args = Vec::new();
    write_short(&mut args, reply_code).unwrap();
    write_shortstr(&mut args, reply_text).unwrap();
    write_shortstr(&mut args, exchange).unwrap();
    write_shortstr(&mut args, routing_key).unwrap();
    let method = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_RETURN, &args);
    let header = encode_content_header(channel, CLASS_BASIC, body.len() as u64, properties);

    let _ = writer.write_all(&method).await;
    let _ = writer.write_all(&header).await;
    if !body.is_empty() {
        let body_frame = encode_body_frame(channel, body);
        let _ = writer.write_all(&body_frame).await;
    }
    let _ = writer.flush().await;
}

pub async fn handle_consume(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let queue_name = read_shortstr(&mut r).unwrap_or_default();
    let consumer_tag_arg = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let _no_local = flags & 0x01 != 0;
    let no_ack = flags & 0x02 != 0;
    let exclusive = flags & 0x04 != 0;
    let no_wait = flags & 0x08 != 0;

    // AMQP 0-9-1 basic.consume includes an arguments field-table after flags.
    let arguments = read_field_table(&mut r).unwrap_or_default();
    let consumer_tag = if consumer_tag_arg.is_empty() {
        None
    } else {
        Some(consumer_tag_arg)
    };

    if super::auth_check::check_read(
        conn_id,
        channel,
        &queue_name,
        CLASS_BASIC,
        METHOD_BASIC_CONSUME,
        writer,
        broker,
    )
    .await
    {
        return;
    }

    if reject_exclusive_conflict(channel, &queue_name, exclusive, writer, broker).await {
        return;
    }

    if validate_consumer_schema(conn_id, channel, &queue_name, &arguments, writer, broker).await {
        return;
    }

    let assigned_tag = match broker.queues.get_mut(&queue_name) {
        Some(mut queue) => queue.add_consumer(conn_id, channel, consumer_tag, None, no_ack),
        None => {
            send_channel_error(
                writer,
                channel,
                NOT_FOUND,
                "NOT_FOUND - no such queue",
                CLASS_BASIC,
                METHOD_BASIC_CONSUME,
            )
            .await;
            return;
        }
    };

    // OPT-2: index consumer_tag → queue for O(1) cancel lookup
    broker.register_consumer(conn_id, channel, &queue_name, &assigned_tag);

    info!(
        conn_id,
        channel,
        queue = queue_name.as_str(),
        tag = assigned_tag.as_str(),
        "consumer started"
    );
    if !no_wait {
        let mut reply_args = Vec::new();
        write_shortstr(&mut reply_args, &assigned_tag).unwrap();
        let reply = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_CONSUME_OK, &reply_args);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

/// Rejects a basic.consume if the queue already has consumers and exclusive is set.
async fn reject_exclusive_conflict(
    channel: u16,
    queue_name: &str,
    exclusive: bool,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) -> bool {
    let has_consumers = broker
        .queues
        .get(queue_name)
        .is_some_and(|q| !q.consumer_tags.is_empty());
    if !exclusive || !has_consumers {
        return false;
    }
    send_channel_error(
        writer,
        channel,
        ACCESS_REFUSED,
        "ACCESS_REFUSED - exclusive consumer exists",
        CLASS_BASIC,
        METHOD_BASIC_CONSUME,
    )
    .await;
    true
}

/// Validates consumer-supplied schema against the queue's schema if x-consumer-schema is present.
/// Returns true if validation failed (caller should return early).
async fn validate_consumer_schema(
    conn_id: u64,
    channel: u16,
    queue_name: &str,
    arguments: &std::collections::BTreeMap<String, FieldValue>,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) -> bool {
    let Some(FieldValue::LongString(raw_schema)) = arguments.get("x-consumer-schema") else {
        return false;
    };

    let message_name =
        match arguments.get("x-consumer-schema-message") {
            Some(FieldValue::LongString(v)) => String::from_utf8_lossy(v).to_string(),
            _ => {
                send_channel_error(
                writer, channel, PRECONDITION_FAILED,
                "PRECONDITION_FAILED - x-consumer-schema-message required with x-consumer-schema",
                CLASS_BASIC, METHOD_BASIC_CONSUME,
            )
            .await;
                return true;
            }
        };

    let consumer_compiled = match crate::schema::compile_proto(raw_schema, &message_name) {
        Ok(c) => c,
        Err(_) => {
            let err = crate::schema::error::BrokerError {
                code: crate::schema::error::ErrorCode::SchemaCompileFailed,
                queue: queue_name.to_string(),
                fields: vec![],
                truncated: false,
            };
            send_channel_error(
                writer,
                channel,
                PRECONDITION_FAILED,
                &err.to_reply_text(),
                CLASS_BASIC,
                METHOD_BASIC_CONSUME,
            )
            .await;
            return true;
        }
    };

    let subset_err = broker
        .queues
        .get(queue_name)
        .and_then(|q| q.schema.clone())
        .and_then(|s| crate::schema::validate::check_consumer_subset(&s, &consumer_compiled).err());

    let Some(err) = subset_err else {
        return false;
    };

    warn!(
        conn_id,
        queue = queue_name,
        "consumer schema not a subset of queue schema: {}",
        err
    );
    let broker_err = crate::schema::error::to_broker_error(queue_name, &err);
    send_channel_error(
        writer,
        channel,
        PRECONDITION_FAILED,
        &broker_err.to_reply_text(),
        CLASS_BASIC,
        METHOD_BASIC_CONSUME,
    )
    .await;
    true
}

pub async fn handle_cancel(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let consumer_tag = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let no_wait = flags & 0x01 != 0;

    // OPT-2: O(1) lookup via consumer_index instead of scanning all queues
    broker.deregister_consumer(conn_id, &consumer_tag);

    info!(
        conn_id,
        channel,
        tag = consumer_tag.as_str(),
        "consumer cancelled"
    );
    if !no_wait {
        let mut reply_args = Vec::new();
        write_shortstr(&mut reply_args, &consumer_tag).unwrap();
        let reply = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_CANCEL_OK, &reply_args);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

/// Decrements the unacked count for a channel. O(1), zero-clone.
fn decrement_unacked(broker: &Broker, conn_id: u64, channel: u16) {
    crate::protocol::amqp::session::with_channel(broker, conn_id, channel, |ch| {
        if ch.unacked_count > 0 {
            ch.unacked_count -= 1;
        }
    });
}

/// Removes a delivery from conn_deliveries tracking.
fn untrack_delivery(broker: &Broker, conn_id: u64, delivery_tag: u64) {
    if let Some(mut deliveries) = broker.conn_deliveries.get_mut(&conn_id) {
        deliveries.retain(|(_, tag)| *tag != delivery_tag);
    }
}

pub async fn handle_ack(conn_id: u64, channel: u16, args: &[u8], broker: &Broker) {
    let mut r = Cursor::new(args);
    let delivery_tag = read_longlong(&mut r).unwrap_or(0);
    let flags = read_octet(&mut r).unwrap_or(0);
    let _multiple = flags & 0x01 != 0;

    decrement_unacked(broker, conn_id, channel);

    // OPT-1: O(1) lookup via delivery_index instead of scanning all queues
    let queue_name = broker.delivery_index.remove(&delivery_tag).map(|(_, v)| v);
    let Some(qn) = queue_name else {
        warn!(conn_id, delivery_tag, "ack for unknown delivery tag");
        return;
    };
    let Some(mut entry) = broker.queues.get_mut(qn.as_ref()) else {
        warn!(conn_id, delivery_tag, "ack for unknown delivery tag");
        return;
    };
    if entry.inflight.remove(&delivery_tag).is_none() {
        warn!(conn_id, delivery_tag, "ack for unknown delivery tag");
        return;
    }

    let _ = broker.wal().log_ack(delivery_tag);
    entry.stat_acked += 1;
    crate::metrics::record_acked();
    info!(conn_id, delivery_tag, "acked");
    untrack_delivery(broker, conn_id, delivery_tag);
    spawn_replicate_ack(broker, &qn, delivery_tag);
}

pub async fn handle_reject(conn_id: u64, channel: u16, args: &[u8], broker: &Broker) {
    let mut r = Cursor::new(args);
    let delivery_tag = read_longlong(&mut r).unwrap_or(0);
    let flags = read_octet(&mut r).unwrap_or(0);
    let requeue = flags & 0x01 != 0;

    decrement_unacked(broker, conn_id, channel);
    resolve_negative_delivery(broker, conn_id, delivery_tag, requeue, "rejected");
}

pub async fn handle_nack(conn_id: u64, channel: u16, args: &[u8], broker: &Broker) {
    let mut r = Cursor::new(args);
    let delivery_tag = read_longlong(&mut r).unwrap_or(0);
    let flags = read_octet(&mut r).unwrap_or(0);
    let _multiple = flags & 0x01 != 0;
    let requeue = flags & 0x02 != 0;

    decrement_unacked(broker, conn_id, channel);
    resolve_negative_delivery(broker, conn_id, delivery_tag, requeue, "nacked");
}

/// Shared logic for reject/nack: looks up the inflight message by delivery
/// tag, optionally requeues it, and cleans up tracking state.
fn resolve_negative_delivery(
    broker: &Broker,
    conn_id: u64,
    delivery_tag: u64,
    requeue: bool,
    verb: &str,
) {
    // OPT-1: O(1) lookup via delivery_index
    let queue_name = broker.delivery_index.remove(&delivery_tag).map(|(_, v)| v);
    let Some(qn) = queue_name else {
        warn!(conn_id, delivery_tag, "{}  for unknown delivery tag", verb);
        return;
    };
    let Some(mut entry) = broker.queues.get_mut(qn.as_ref()) else {
        warn!(conn_id, delivery_tag, "{} for unknown delivery tag", verb);
        return;
    };
    let Some(mut msg) = entry.inflight.remove(&delivery_tag) else {
        warn!(conn_id, delivery_tag, "{} for unknown delivery tag", verb);
        return;
    };

    untrack_delivery(broker, conn_id, delivery_tag);
    if requeue {
        msg.redelivered = true;
        msg.delivery_count += 1;
        entry
            .messages
            .push_front(crate::queue::message::QueueMessage::Full(msg));
        info!(conn_id, delivery_tag, "{}+requeued", verb);
    } else {
        info!(conn_id, delivery_tag, "{}+discarded", verb);
    }
}

/// Spawns a cluster ack replication task if clustering is enabled.
fn spawn_replicate_ack(broker: &Broker, queue_name: &std::sync::Arc<str>, delivery_tag: u64) {
    if let Some(c) = broker.cluster() {
        let c = c.clone();
        let q_name = queue_name.to_string();
        tokio::spawn(async move {
            let _ = c.replicate_ack(&q_name, delivery_tag).await;
        });
    }
}

pub async fn handle_get(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let queue_name = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let no_ack = flags & 0x01 != 0;

    // OPT-12: single get_mut instead of contains_key + get_mut (two DashMap probes, TOCTOU race)
    let q_msg = match broker.queues.get_mut(&queue_name) {
        Some(mut q) => q.value_mut().messages.pop_front(),
        None => {
            send_channel_error(
                writer,
                channel,
                NOT_FOUND,
                "NOT_FOUND - no such queue",
                CLASS_BASIC,
                METHOD_BASIC_GET,
            )
            .await;
            return;
        }
    };

    let Some(q_msg) = q_msg else {
        send_get_empty(channel, writer).await;
        return;
    };

    let msg = match q_msg.resolve(broker.wal()) {
        Ok(m) => m,
        Err(e) => {
            warn!("Failed to resolve basic.get message: {}", e);
            send_get_empty(channel, writer).await;
            return;
        }
    };

    send_get_ok(channel, &queue_name, &msg, writer, broker).await;

    if !no_ack {
        track_get_delivery(conn_id, &queue_name, msg, broker);
    }
}

async fn send_get_empty(channel: u16, writer: &mut crate::protocol::amqp::AmqpWriter) {
    let mut reply_args = Vec::new();
    write_shortstr(&mut reply_args, "").unwrap();
    let reply = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_GET_EMPTY, &reply_args);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

async fn send_get_ok(
    channel: u16,
    queue_name: &str,
    msg: &Message,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let msg_count = broker
        .queues
        .get(queue_name)
        .map(|q| q.messages.len() as u32)
        .unwrap_or(0);

    let mut reply_args = Vec::new();
    write_longlong(&mut reply_args, msg.id).unwrap();
    write_octet(&mut reply_args, if msg.redelivered { 1 } else { 0 }).unwrap();
    write_shortstr(&mut reply_args, &msg.exchange).unwrap();
    write_shortstr(&mut reply_args, &msg.routing_key).unwrap();
    write_long(&mut reply_args, msg_count).unwrap();

    let method = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_GET_OK, &reply_args);
    let props = BasicProperties::default();
    let header = encode_content_header(channel, CLASS_BASIC, msg.body.len() as u64, &props);

    let _ = writer.write_all(&method).await;
    let _ = writer.write_all(&header).await;
    if !msg.body.is_empty() {
        let body_frame = encode_body_frame(channel, &msg.body);
        let _ = writer.write_all(&body_frame).await;
    }
    let _ = writer.flush().await;
}

/// Tracks a basic.get delivery for O(1) ack lookup.
fn track_get_delivery(conn_id: u64, queue_name: &str, msg: Message, broker: &Broker) {
    let delivery_tag = msg.id;
    // OPT-1: index this delivery for O(1) ack lookup
    broker
        .delivery_index
        .insert(delivery_tag, queue_name.into());
    broker
        .conn_deliveries
        .entry(conn_id)
        .or_default()
        .push((queue_name.into(), delivery_tag));
    if let Some(mut q) = broker.queues.get_mut(queue_name) {
        q.inflight.insert(delivery_tag, msg);
    }
}

pub async fn handle_qos(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _prefetch_size = read_long(&mut r).unwrap_or(0);
    let prefetch_count = read_short(&mut r).unwrap_or(0);
    let flags = read_octet(&mut r).unwrap_or(0);
    let global = flags & 0x01 != 0;

    crate::protocol::amqp::session::with_conn_state(broker, conn_id, |cs| {
        if global {
            for ch in cs.channels.values_mut() {
                ch.prefetch_count = prefetch_count;
            }
        } else if let Some(ch) = cs.channels.get_mut(&channel) {
            ch.prefetch_count = prefetch_count;
        }
    });

    info!(conn_id, channel, prefetch_count, global, "qos set");
    let reply = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_QOS_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

pub async fn handle_recover(
    conn_id: u64,
    channel: u16,
    _args: &[u8],
    writer: &mut crate::protocol::amqp::AmqpWriter,
    _broker: &Broker,
) {
    info!(conn_id, channel, "recover");
    let reply = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_RECOVER_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

#[inline]
pub fn build_deliver_args(
    consumer_tag: &str,
    delivery_tag: u64,
    redelivered: bool,
    exchange: &str,
    routing_key: &str,
) -> Vec<u8> {
    let mut args = Vec::new();
    write_shortstr(&mut args, consumer_tag).unwrap();
    write_longlong(&mut args, delivery_tag).unwrap();
    write_octet(&mut args, if redelivered { 1 } else { 0 }).unwrap();
    write_shortstr(&mut args, exchange).unwrap();
    write_shortstr(&mut args, routing_key).unwrap();
    args
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use crate::protocol::amqp::session::ConnectionState;

    #[test]
    fn publish_args_parse() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, "amq.direct").unwrap();
        write_shortstr(&mut args, "my.key").unwrap();
        write_octet(&mut args, 0x01).unwrap();
        let (ex, rk, mandatory, immediate) = parse_publish_args(&args);
        assert_eq!(ex, "amq.direct");
        assert_eq!(rk, "my.key");
        assert!(mandatory);
        assert!(!immediate);
    }

    #[test]
    fn deliver_args_build() {
        let args = build_deliver_args("ctag-1", 42, false, "amq.direct", "key1");
        let mut r = Cursor::new(&args);
        assert_eq!(read_shortstr(&mut r).unwrap(), "ctag-1");
        assert_eq!(read_longlong(&mut r).unwrap(), 42);
        assert_eq!(read_octet(&mut r).unwrap(), 0);
        assert_eq!(read_shortstr(&mut r).unwrap(), "amq.direct");
        assert_eq!(read_shortstr(&mut r).unwrap(), "key1");
    }

    #[test]
    fn deliver_args_redelivered() {
        let args = build_deliver_args("t", 1, true, "", "");
        let mut r = Cursor::new(&args);
        let _ = read_shortstr(&mut r).unwrap();
        let _ = read_longlong(&mut r).unwrap();
        assert_eq!(read_octet(&mut r).unwrap(), 1);
    }

    #[test]
    fn consume_args_parse() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, "test.queue").unwrap();
        write_shortstr(&mut args, "my-tag").unwrap();
        write_octet(&mut args, 0x02).unwrap();
        let mut r = Cursor::new(&args);
        let _ = read_short(&mut r).unwrap();
        assert_eq!(read_shortstr(&mut r).unwrap(), "test.queue");
        assert_eq!(read_shortstr(&mut r).unwrap(), "my-tag");
        let flags = read_octet(&mut r).unwrap();
        assert_eq!(flags & 0x02, 0x02);
    }

    #[test]
    fn ack_args_parse() {
        let mut args = Vec::new();
        write_longlong(&mut args, 99).unwrap();
        write_octet(&mut args, 0x01).unwrap();
        let mut r = Cursor::new(&args);
        assert_eq!(read_longlong(&mut r).unwrap(), 99);
        assert_eq!(read_octet(&mut r).unwrap(), 0x01);
    }

    #[test]
    fn reject_args_parse() {
        let mut args = Vec::new();
        write_longlong(&mut args, 7).unwrap();
        write_octet(&mut args, 0x01).unwrap();
        let mut r = Cursor::new(&args);
        assert_eq!(read_longlong(&mut r).unwrap(), 7);
        assert_eq!(read_octet(&mut r).unwrap() & 0x01, 0x01);
    }

    #[test]
    fn qos_args_parse() {
        let mut args = Vec::new();
        write_long(&mut args, 0).unwrap();
        write_short(&mut args, 10).unwrap();
        write_octet(&mut args, 0x01).unwrap();
        let mut r = Cursor::new(&args);
        assert_eq!(read_long(&mut r).unwrap(), 0);
        assert_eq!(read_short(&mut r).unwrap(), 10);
        assert_eq!(read_octet(&mut r).unwrap() & 0x01, 0x01);
    }

    #[test]
    fn get_args_parse() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, "q1").unwrap();
        write_octet(&mut args, 0x01).unwrap();
        let mut r = Cursor::new(&args);
        let _ = read_short(&mut r).unwrap();
        assert_eq!(read_shortstr(&mut r).unwrap(), "q1");
        assert_eq!(read_octet(&mut r).unwrap() & 0x01, 0x01);
    }

    #[test]
    fn basic_return_frame_structure() {
        let mut args = Vec::new();
        write_short(&mut args, NO_ROUTE).unwrap();
        write_shortstr(&mut args, "NO_ROUTE").unwrap();
        write_shortstr(&mut args, "amq.direct").unwrap();
        write_shortstr(&mut args, "key").unwrap();
        let frame = encode_method_frame(1, CLASS_BASIC, METHOD_BASIC_RETURN, &args);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_BASIC);
        assert_eq!(m.method_id, METHOD_BASIC_RETURN);
    }
    fn test_broker() -> Broker {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_basic_handler_wal");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(format!("test_{}.wal", id));
        let wal = std::sync::Arc::new(crate::storage::wal::Wal::open(&path).unwrap());
        crate::state::BrokerState::new(wal).into()
    }

    /// Dedicated unit test verification for `handle_publish` function with schema validation.
    #[tokio::test]
    async fn test_coverage_for_handle_publish() {
        let broker: Broker = test_broker();

        let mut conn_state = ConnectionState::new();
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

        let mut q = crate::queue::QueueState::new();
        let schema_content = b"syntax = \"proto3\"; message User { string name = 1; }";
        let compiled = crate::schema::compile_proto(schema_content, "User").unwrap();
        q.schema = Some(std::sync::Arc::new(compiled));
        broker.queues.insert("schema-queue".to_string(), q);

        {
            let mut exchanges = broker.exchanges.write().await;
            if let Some(ex) = exchanges.get_mut("amq.direct") {
                ex.bindings.push(crate::routing::exchange::Binding {
                    queue_name: "schema-queue".to_string().into(),
                    routing_key: "schema-queue".to_string().into(),
                    headers_match: None,
                });
            }
        }

        let properties_no_ct = BasicProperties::default();
        let body_garbage = b"invalid payload but no content-type";
        handle_publish(
            1,
            1,
            "amq.direct",
            "schema-queue",
            false,
            &properties_no_ct,
            body_garbage,
            &mut writer,
            &broker,
        )
        .await;

        {
            let queue = broker.queues.get("schema-queue").unwrap();
            assert_eq!(queue.messages.len(), 0);
        }

        let non_schema_q = crate::queue::QueueState::new();
        broker
            .queues
            .insert("non-schema-queue".to_string(), non_schema_q);
        {
            let mut exchanges = broker.exchanges.write().await;
            if let Some(ex) = exchanges.get_mut("amq.direct") {
                ex.bindings.push(crate::routing::exchange::Binding {
                    queue_name: "non-schema-queue".to_string().into(),
                    routing_key: "non-schema-queue".to_string().into(),
                    headers_match: None,
                });
            }
        }
        handle_publish(
            1,
            1,
            "amq.direct",
            "non-schema-queue",
            false,
            &properties_no_ct,
            body_garbage,
            &mut writer,
            &broker,
        )
        .await;

        {
            let queue = broker.queues.get("non-schema-queue").unwrap();
            assert_eq!(queue.messages.len(), 1);
        }

        let properties_json = BasicProperties {
            content_type: Some("application/json".to_string()),
            ..Default::default()
        };
        handle_publish(
            1,
            1,
            "amq.direct",
            "schema-queue",
            false,
            &properties_json,
            b"{}",
            &mut writer,
            &broker,
        )
        .await;

        {
            let queue = broker.queues.get("schema-queue").unwrap();
            assert_eq!(queue.messages.len(), 0);
        }

        let properties_proto = BasicProperties {
            content_type: Some("application/x-protobuf".to_string()),
            ..Default::default()
        };
        handle_publish(
            1,
            1,
            "amq.direct",
            "schema-queue",
            false,
            &properties_proto,
            b"bad encoded protobuf",
            &mut writer,
            &broker,
        )
        .await;

        {
            let queue = broker.queues.get("schema-queue").unwrap();
            assert_eq!(queue.messages.len(), 0);
        }

        let mut valid_body = vec![0x0A, 5];
        valid_body.extend_from_slice(b"Alice");

        handle_publish(
            1,
            1,
            "amq.direct",
            "schema-queue",
            false,
            &properties_proto,
            &valid_body,
            &mut writer,
            &broker,
        )
        .await;

        {
            let queue = broker.queues.get("schema-queue").unwrap();
            assert_eq!(queue.messages.len(), 1);
        }
    }

    /// Integration test verifying publisher confirm ACKs/NACKs on schema validation success/failure.
    #[tokio::test]
    async fn test_schema_publisher_confirms() {
        let broker: Broker = test_broker();

        let mut conn_state = ConnectionState::new();
        conn_state.username = "guest".to_string();
        conn_state.vhost = "/".to_string();

        let mut ch = crate::protocol::amqp::session::ChannelState::new(1);
        ch.confirm_mode = true;
        ch.next_delivery_tag = 1;
        conn_state.channels.insert(1, ch);
        broker.conn_state.insert(1, Box::new(conn_state));

        let (mut rx_stream, tx_stream) = tokio::io::duplex(65536);
        let boxed: Box<dyn crate::protocol::AsyncStream> = Box::new(tx_stream);
        let (_read_half, write_half) = tokio::io::split(boxed);
        let mut writer = tokio::io::BufWriter::new(write_half);

        let mut q = crate::queue::QueueState::new();
        let schema_content = b"syntax = \"proto3\"; message User { string name = 1; }";
        let compiled = crate::schema::compile_proto(schema_content, "User").unwrap();
        q.schema = Some(std::sync::Arc::new(compiled));
        broker.queues.insert("schema-queue".to_string(), q);

        {
            let mut exchanges = broker.exchanges.write().await;
            if let Some(ex) = exchanges.get_mut("amq.direct") {
                ex.bindings.push(crate::routing::exchange::Binding {
                    queue_name: "schema-queue".to_string().into(),
                    routing_key: "schema-queue".to_string().into(),
                    headers_match: None,
                });
            }
        }

        let properties_proto = BasicProperties {
            content_type: Some("application/x-protobuf".to_string()),
            ..Default::default()
        };
        handle_publish(
            1,
            1,
            "amq.direct",
            "schema-queue",
            false,
            &properties_proto,
            b"invalid",
            &mut writer,
            &broker,
        )
        .await;

        let mut buf = vec![0u8; 4096];
        use tokio::io::AsyncReadExt;
        let n = rx_stream.read(&mut buf).await.unwrap();

        let mut offset = 0;
        let mut got_nack = false;
        let mut got_channel_error = false;
        while offset < n {
            if let Ok((decoded, consumed)) = decode_frame(&buf[offset..n]) {
                offset += consumed;
                if decoded.frame_type == FRAME_METHOD {
                    if let Ok(m) = decode_method(&decoded.payload) {
                        if m.class_id == CLASS_BASIC && m.method_id == METHOD_BASIC_NACK {
                            got_nack = true;
                        }
                        if m.class_id == CLASS_CHANNEL && m.method_id == 40 {
                            got_channel_error = true;
                        }
                    }
                }
            } else {
                break;
            }
        }
        assert!(got_nack);
        assert!(got_channel_error);
    }
}
