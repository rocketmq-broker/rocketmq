//! AMQP 0-9-1 Basic class handlers (class 60).
//!
//! Handles Basic.Publish (with content framing), Basic.Consume, Basic.Cancel,
//! Basic.Ack, Basic.Nack, Basic.Reject, Basic.Get, Basic.Qos, Basic.Recover,
//! and Basic.Return.

use std::collections::HashMap;
use std::io::Cursor;
use std::time::{Duration, Instant};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tracing::{debug, info, warn};

use crate::core::amqp_codec::*;
use crate::core::method::*;
use crate::core::properties::BasicProperties;
use crate::core::types::*;
use crate::queue::Message;
use crate::state::Broker;

// ─── Basic.Publish ────────────────────────────────────

/// Parse Basic.Publish arguments: ticket(short) exchange(shortstr) routing_key(shortstr) flags(octet)
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

/// Handle a fully assembled publish (method args + content header + body).
/// Called after content framing is complete.
pub async fn handle_publish(
    conn_id: u64,
    channel: u16,
    exchange_name: &str,
    routing_key: &str,
    mandatory: bool,
    properties: &BasicProperties,
    body: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    // Permission check: write permission needed to publish to an exchange
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

    let priority = properties.priority.unwrap_or(0);
    let per_msg_ttl = properties
        .expiration
        .as_ref()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis);

    // Route through exchange
    let target_queues = {
        let exchanges = broker.exchanges.read().await;
        let exchange = match exchanges.get(exchange_name) {
            Some(ex) => ex,
            None => {
                warn!(conn_id, exchange = exchange_name, "exchange not found");
                return;
            }
        };
        let msg_headers: HashMap<String, String> = properties
            .headers
            .as_ref()
            .map(|h| {
                h.iter()
                    .map(|(k, v)| (k.clone(), format!("{:?}", v)))
                    .collect()
            })
            .unwrap_or_default();
        let targets = exchange.route(routing_key, &msg_headers);
        debug!(conn_id, exchange = exchange_name, routing_key, targets = ?targets, "routed");
        targets
    };

    if target_queues.is_empty() {
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
        return;
    }

    for queue_name in &target_queues {
        let msg_id = broker.alloc_msg_id();
        let mut queue_ref = match broker.queues.get_mut(queue_name.as_str()) {
            Some(q) => q,
            None => continue,
        };
        let queue = queue_ref.value_mut();

        let expiration = match (queue.options.message_ttl, per_msg_ttl) {
            (Some(qt), Some(pt)) => Some(Instant::now() + qt.min(pt)),
            (Some(t), None) | (None, Some(t)) => Some(Instant::now() + t),
            (None, None) => None,
        };

        let effective_priority = if queue.options.max_priority > 0 {
            priority.min(queue.options.max_priority)
        } else {
            0
        };

        // Overflow eviction
        if let Some(max_len) = queue.options.max_length {
            while queue.messages.len() >= max_len {
                if queue.messages.pop_oldest().is_none() {
                    break;
                }
            }
        }

        // Encode properties back to bytes for storage
        let mut prop_bytes = Vec::new();
        properties.encode(&mut prop_bytes).unwrap_or_default();

        let msg = Message {
            id: msg_id,
            headers: prop_bytes,
            body: body.to_vec(),
            priority: effective_priority,
            expiration,
            redelivered: false,
            delivery_count: 0,
        };

        queue.last_activity = Instant::now();
        queue.messages.push_back(msg);
        debug!(
            conn_id,
            msg_id,
            queue = queue_name.as_str(),
            "queued via AMQP"
        );
    }
}

// ─── Basic.Return ─────────────────────────────────────

async fn send_basic_return(
    channel: u16,
    reply_code: u16,
    reply_text: &str,
    exchange: &str,
    routing_key: &str,
    properties: &BasicProperties,
    body: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
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

// ─── Basic.Consume ────────────────────────────────────

pub async fn handle_consume(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let queue_name = read_shortstr(&mut r).unwrap_or_default();
    let consumer_tag_arg = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let _no_local = flags & 0x01 != 0;
    let _no_ack = flags & 0x02 != 0;
    let exclusive = flags & 0x04 != 0;
    let no_wait = flags & 0x08 != 0;

    let consumer_tag = if consumer_tag_arg.is_empty() {
        None
    } else {
        Some(consumer_tag_arg)
    };

    // Permission check: read permission needed to consume from a queue
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

    // Check exclusive
    if exclusive {
        if let Some(q) = broker.queues.get(&queue_name) {
            if !q.consumer_tags.is_empty() {
                let close = build_channel_close(
                    ACCESS_REFUSED,
                    "ACCESS_REFUSED - exclusive consumer exists",
                    CLASS_BASIC,
                    METHOD_BASIC_CONSUME,
                );
                let _ = writer
                    .write_all(&encode_method_frame(
                        channel,
                        CLASS_CHANNEL,
                        METHOD_CHANNEL_CLOSE,
                        &close,
                    ))
                    .await;
                let _ = writer.flush().await;
                return;
            }
        }
    }

    let assigned_tag = match broker.queues.get_mut(&queue_name) {
        Some(mut queue) => queue.add_consumer(conn_id, channel, consumer_tag, None),
        None => {
            let close = build_channel_close(
                NOT_FOUND,
                "NOT_FOUND - no such queue",
                CLASS_BASIC,
                METHOD_BASIC_CONSUME,
            );
            let _ = writer
                .write_all(&encode_method_frame(
                    channel,
                    CLASS_CHANNEL,
                    METHOD_CHANNEL_CLOSE,
                    &close,
                ))
                .await;
            let _ = writer.flush().await;
            return;
        }
    };

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

// ─── Basic.Cancel ─────────────────────────────────────

pub async fn handle_cancel(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let consumer_tag = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let no_wait = flags & 0x01 != 0;

    for mut entry in broker.queues.iter_mut() {
        if entry.value_mut().cancel_consumer(&consumer_tag) {
            break;
        }
    }

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

// ─── Basic.Ack ────────────────────────────────────────

pub async fn handle_ack(conn_id: u64, channel: u16, args: &[u8], broker: &Broker) {
    let mut r = Cursor::new(args);
    let delivery_tag = read_longlong(&mut r).unwrap_or(0);
    let flags = read_octet(&mut r).unwrap_or(0);
    let _multiple = flags & 0x01 != 0;

    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        if let Some(ch) = cs.channels.get_mut(&channel) {
            if ch.unacked_count > 0 {
                ch.unacked_count -= 1;
            }
        }
    }

    for mut entry in broker.queues.iter_mut() {
        if entry.value_mut().inflight.remove(&delivery_tag).is_some() {
            if let Some(wal) = broker.wal() {
                let _ = wal.log_ack(delivery_tag);
            }
            info!(conn_id, delivery_tag, "acked");
            return;
        }
    }
    warn!(conn_id, delivery_tag, "ack for unknown delivery tag");
}

// ─── Basic.Reject ─────────────────────────────────────

pub async fn handle_reject(conn_id: u64, channel: u16, args: &[u8], broker: &Broker) {
    let mut r = Cursor::new(args);
    let delivery_tag = read_longlong(&mut r).unwrap_or(0);
    let flags = read_octet(&mut r).unwrap_or(0);
    let requeue = flags & 0x01 != 0;

    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        if let Some(ch) = cs.channels.get_mut(&channel) {
            if ch.unacked_count > 0 {
                ch.unacked_count -= 1;
            }
        }
    }

    for mut entry in broker.queues.iter_mut() {
        if let Some(mut msg) = entry.value_mut().inflight.remove(&delivery_tag) {
            if requeue {
                msg.redelivered = true;
                msg.delivery_count += 1;
                entry.value_mut().messages.push_front(msg);
                info!(conn_id, delivery_tag, "rejected+requeued");
            } else {
                info!(conn_id, delivery_tag, "rejected+discarded");
            }
            return;
        }
    }
    warn!(conn_id, delivery_tag, "reject for unknown delivery tag");
}

// ─── Basic.Nack ───────────────────────────────────────

pub async fn handle_nack(conn_id: u64, channel: u16, args: &[u8], broker: &Broker) {
    let mut r = Cursor::new(args);
    let delivery_tag = read_longlong(&mut r).unwrap_or(0);
    let flags = read_octet(&mut r).unwrap_or(0);
    let _multiple = flags & 0x01 != 0;
    let requeue = flags & 0x02 != 0;

    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        if let Some(ch) = cs.channels.get_mut(&channel) {
            if ch.unacked_count > 0 {
                ch.unacked_count -= 1;
            }
        }
    }

    for mut entry in broker.queues.iter_mut() {
        if let Some(mut msg) = entry.value_mut().inflight.remove(&delivery_tag) {
            if requeue {
                msg.redelivered = true;
                msg.delivery_count += 1;
                entry.value_mut().messages.push_front(msg);
                info!(conn_id, delivery_tag, "nacked+requeued");
            } else {
                info!(conn_id, delivery_tag, "nacked+discarded");
            }
            return;
        }
    }
    warn!(conn_id, delivery_tag, "nack for unknown delivery tag");
}

// ─── Basic.Get ────────────────────────────────────────

pub async fn handle_get(
    _conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let queue_name = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let no_ack = flags & 0x01 != 0;

    let msg = broker
        .queues
        .get_mut(&queue_name)
        .and_then(|mut q| q.value_mut().messages.pop_front());

    match msg {
        Some(msg) => {
            let delivery_tag = msg.id;
            let msg_count = broker
                .queues
                .get(&queue_name)
                .map(|q| q.messages.len() as u32)
                .unwrap_or(0);

            // Basic.GetOk args
            let mut reply_args = Vec::new();
            write_longlong(&mut reply_args, delivery_tag).unwrap();
            write_octet(&mut reply_args, if msg.redelivered { 1 } else { 0 }).unwrap();
            write_shortstr(&mut reply_args, "").unwrap(); // exchange
            write_shortstr(&mut reply_args, "").unwrap(); // routing_key
            write_long(&mut reply_args, msg_count).unwrap();

            let method =
                encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_GET_OK, &reply_args);
            let props = BasicProperties::default();
            let header = encode_content_header(channel, CLASS_BASIC, msg.body.len() as u64, &props);

            let _ = writer.write_all(&method).await;
            let _ = writer.write_all(&header).await;
            if !msg.body.is_empty() {
                let body_frame = encode_body_frame(channel, &msg.body);
                let _ = writer.write_all(&body_frame).await;
            }
            let _ = writer.flush().await;

            if !no_ack {
                if let Some(mut q) = broker.queues.get_mut(&queue_name) {
                    q.inflight.insert(delivery_tag, msg);
                }
            }
        }
        None => {
            // Basic.GetEmpty
            let mut reply_args = Vec::new();
            write_shortstr(&mut reply_args, "").unwrap(); // cluster-id (deprecated)
            let reply =
                encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_GET_EMPTY, &reply_args);
            let _ = writer.write_all(&reply).await;
            let _ = writer.flush().await;
        }
    }
}

// ─── Basic.Qos ────────────────────────────────────────

pub async fn handle_qos(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _prefetch_size = read_long(&mut r).unwrap_or(0);
    let prefetch_count = read_short(&mut r).unwrap_or(0);
    let flags = read_octet(&mut r).unwrap_or(0);
    let global = flags & 0x01 != 0;

    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        if global {
            for ch in cs.channels.values_mut() {
                ch.prefetch_count = prefetch_count;
            }
        } else if let Some(ch) = cs.channels.get_mut(&channel) {
            ch.prefetch_count = prefetch_count;
        }
    }

    info!(conn_id, channel, prefetch_count, global, "qos set");
    let reply = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_QOS_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

// ─── Basic.Recover ────────────────────────────────────

pub async fn handle_recover(
    conn_id: u64,
    channel: u16,
    _args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    _broker: &Broker,
) {
    // Requeue unacked — simplified: just send RecoverOk
    info!(conn_id, channel, "recover");
    let reply = encode_method_frame(channel, CLASS_BASIC, METHOD_BASIC_RECOVER_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

// ─── Helpers ──────────────────────────────────────────

fn build_channel_close(code: u16, text: &str, class_id: u16, method_id: u16) -> Vec<u8> {
    let mut buf = Vec::new();
    write_short(&mut buf, code).unwrap();
    write_shortstr(&mut buf, text).unwrap();
    write_short(&mut buf, class_id).unwrap();
    write_short(&mut buf, method_id).unwrap();
    buf
}

/// Build a Basic.Deliver method frame (server→client).
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
    use super::*;

    #[test]
    fn publish_args_parse() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, "amq.direct").unwrap();
        write_shortstr(&mut args, "my.key").unwrap();
        write_octet(&mut args, 0x01).unwrap(); // mandatory=true
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
        write_octet(&mut args, 0x02).unwrap(); // no_ack
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
        write_octet(&mut args, 0x01).unwrap(); // multiple
        let mut r = Cursor::new(&args);
        assert_eq!(read_longlong(&mut r).unwrap(), 99);
        assert_eq!(read_octet(&mut r).unwrap(), 0x01);
    }

    #[test]
    fn reject_args_parse() {
        let mut args = Vec::new();
        write_longlong(&mut args, 7).unwrap();
        write_octet(&mut args, 0x01).unwrap(); // requeue
        let mut r = Cursor::new(&args);
        assert_eq!(read_longlong(&mut r).unwrap(), 7);
        assert_eq!(read_octet(&mut r).unwrap() & 0x01, 0x01);
    }

    #[test]
    fn qos_args_parse() {
        let mut args = Vec::new();
        write_long(&mut args, 0).unwrap();
        write_short(&mut args, 10).unwrap();
        write_octet(&mut args, 0x01).unwrap(); // global
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
        write_octet(&mut args, 0x01).unwrap(); // no_ack
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
}
