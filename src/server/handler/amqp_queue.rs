//! AMQP 0-9-1 Queue class handlers (class 50).

use std::io::Cursor;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tracing::info;

use crate::core::amqp_codec::*;
use crate::core::method::*;
use crate::core::types::*;
use crate::queue::{QueueOptions, QueueState};
use crate::routing::exchange::Binding;
use crate::state::Broker;

/// Queue.Declare: queue(shortstr) passive(bit) durable(bit) exclusive(bit)
///   auto_delete(bit) no_wait(bit) arguments(table)
pub async fn handle_declare(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
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

    // Server-named queue
    if name.is_empty() {
        name = format!("amq.gen-{}", broker.alloc_msg_id());
    }

    // Permission check: configure permission needed for declare (skip for passive)
    if !passive {
        if super::auth_check::check_configure(
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
    }

    if passive {
        if let Some(q) = broker.queues.get(&name) {
            let (msg_count, consumer_count) =
                (q.messages.len() as u32, q.consumer_tags.len() as u32);
            if !no_wait {
                send_declare_ok(channel, &name, msg_count, consumer_count, writer).await;
            }
        } else {
            let close = build_channel_close(
                NOT_FOUND,
                "NOT_FOUND - no such queue",
                CLASS_QUEUE,
                METHOD_QUEUE_DECLARE,
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
        }
        return;
    }

    // Check exclusive ownership
    if let Some(existing) = broker.queues.get(&name) {
        if existing.options.exclusive && existing.owner_conn_id != Some(conn_id) {
            let close = build_channel_close(
                RESOURCE_LOCKED,
                "RESOURCE_LOCKED - exclusive queue",
                CLASS_QUEUE,
                METHOD_QUEUE_DECLARE,
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

    let mut opts = QueueOptions {
        durable,
        exclusive,
        auto_delete,
        ..QueueOptions::default()
    };

    // Parse x-message-ttl, x-max-length, x-dead-letter-exchange from arguments table
    if let Some(FieldValue::LongInt(v)) = arguments.get("x-message-ttl") {
        opts.message_ttl = Some(std::time::Duration::from_millis(*v as u64));
    }
    if let Some(FieldValue::LongInt(v)) = arguments.get("x-max-length") {
        opts.max_length = Some(*v as usize);
    }
    if let Some(FieldValue::LongString(v)) = arguments.get("x-dead-letter-exchange") {
        opts.dead_letter_exchange = Some(String::from_utf8_lossy(v).to_string());
    }

    broker.queues.entry(name.clone()).or_insert_with(|| {
        let mut q = QueueState::with_options(opts);
        if exclusive {
            q.owner_conn_id = Some(conn_id);
        }
        q
    });

    broker.auto_bind_default_exchange(&name);

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

/// Queue.Delete: queue(shortstr) if_unused(bit) if_empty(bit) no_wait(bit)
pub async fn handle_delete(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let name = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let if_unused = flags & 0x01 != 0;
    let if_empty = flags & 0x02 != 0;
    let no_wait = flags & 0x04 != 0;

    // Pre-checks
    if let Some(q) = broker.queues.get(&name) {
        if if_unused && !q.consumer_tags.is_empty() {
            let close = build_channel_close(
                PRECONDITION_FAILED,
                "PRECONDITION_FAILED - queue in use",
                CLASS_QUEUE,
                METHOD_QUEUE_DELETE,
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
        if if_empty && !q.messages.is_empty() {
            let close = build_channel_close(
                PRECONDITION_FAILED,
                "PRECONDITION_FAILED - queue not empty",
                CLASS_QUEUE,
                METHOD_QUEUE_DELETE,
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

    let msg_count = broker
        .queues
        .remove(&name)
        .map(|(_, q)| q.messages.len() as u32)
        .unwrap_or(0);

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

/// Queue.Purge: queue(shortstr) no_wait(bit)
pub async fn handle_purge(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let name = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let no_wait = flags & 0x01 != 0;

    let msg_count = if let Some(mut q) = broker.queues.get_mut(&name) {
        let count = q.messages.len() as u32;
        q.messages.clear();
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

/// Queue.Bind: queue(shortstr) exchange(shortstr) routing_key(shortstr) no_wait(bit) arguments(table)
pub async fn handle_bind(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
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

    {
        let mut exchanges = broker.exchanges.write().await;
        if let Some(ex) = exchanges.get_mut(&exchange) {
            ex.add_binding(Binding {
                queue_name: queue.clone(),
                routing_key: routing_key.clone(),
                headers_match: None,
            });
        } else {
            let close = build_channel_close(
                NOT_FOUND,
                "NOT_FOUND - no such exchange",
                CLASS_QUEUE,
                METHOD_QUEUE_BIND,
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

    info!(
        conn_id,
        exchange = exchange.as_str(),
        queue = queue.as_str(),
        routing_key = routing_key.as_str(),
        "queue bound"
    );
    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_QUEUE, METHOD_QUEUE_BIND_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

/// Queue.Unbind: queue(shortstr) exchange(shortstr) routing_key(shortstr) arguments(table)
pub async fn handle_unbind(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let queue = read_shortstr(&mut r).unwrap_or_default();
    let exchange = read_shortstr(&mut r).unwrap_or_default();
    let routing_key = read_shortstr(&mut r).unwrap_or_default();

    {
        let mut exchanges = broker.exchanges.write().await;
        if let Some(ex) = exchanges.get_mut(&exchange) {
            ex.remove_binding(&queue, &routing_key);
        }
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

// ─── Helpers ──────────────────────────────────────────

async fn send_declare_ok(
    channel: u16,
    name: &str,
    msg_count: u32,
    consumer_count: u32,
    writer: &mut BufWriter<OwnedWriteHalf>,
) {
    let mut args = Vec::new();
    write_shortstr(&mut args, name).unwrap();
    write_long(&mut args, msg_count).unwrap();
    write_long(&mut args, consumer_count).unwrap();
    let reply = encode_method_frame(channel, CLASS_QUEUE, METHOD_QUEUE_DECLARE_OK, &args);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

fn build_channel_close(reply_code: u16, text: &str, class_id: u16, method_id: u16) -> Vec<u8> {
    let mut buf = Vec::new();
    write_short(&mut buf, reply_code).unwrap();
    write_shortstr(&mut buf, text).unwrap();
    write_short(&mut buf, class_id).unwrap();
    write_short(&mut buf, method_id).unwrap();
    buf
}

#[cfg(test)]
mod tests {
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
        let args = make_declare_args("test.q", 0x06); // durable + exclusive
        let mut r = Cursor::new(&args);
        let _ = read_short(&mut r).unwrap();
        assert_eq!(read_shortstr(&mut r).unwrap(), "test.q");
        let flags = read_octet(&mut r).unwrap();
        assert_eq!(flags & 0x02, 0x02); // durable
        assert_eq!(flags & 0x04, 0x04); // exclusive
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
}
