//! AMQP 0-9-1 Exchange class handlers (class 40).

use std::io::Cursor;
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::core::amqp_codec::*;
use crate::core::method::*;
use crate::core::types::*;
use crate::routing::exchange::{Binding, Exchange, ExchangeType};
use crate::state::Broker;

/// Exchange.Declare: exchange(shortstr) type(shortstr) passive(bit) durable(bit)
///   auto_delete(bit) internal(bit) no_wait(bit) arguments(table)
pub async fn handle_declare(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let name = read_shortstr(&mut r).unwrap_or_default();
    let kind_str = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let passive = flags & 0x01 != 0;
    let durable = flags & 0x02 != 0;
    let _auto_delete = flags & 0x04 != 0;
    let _internal = flags & 0x08 != 0;
    let no_wait = flags & 0x10 != 0;
    let _arguments = read_field_table(&mut r).unwrap_or_default();

    // Permission check: configure permission needed for declare (skip for passive)
    if !passive {
        if super::auth_check::check_configure(
            conn_id,
            channel,
            &name,
            CLASS_EXCHANGE,
            METHOD_EXCHANGE_DECLARE,
            writer,
            broker,
        )
        .await
        {
            return;
        }
    }

    if passive {
        let exists = broker.exchanges.read().await.contains_key(&name);
        if !exists {
            let close = build_channel_close(
                NOT_FOUND,
                "NOT_FOUND - no such exchange",
                CLASS_EXCHANGE,
                METHOD_EXCHANGE_DECLARE,
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
    } else {
        let kind = ExchangeType::from_str(&kind_str).unwrap_or(ExchangeType::Direct);
        let kind_byte = kind.to_byte();
        let mut exchanges = broker.exchanges.write().await;
        let is_new = !exchanges.contains_key(&name);
        exchanges
            .entry(name.clone())
            .or_insert_with(|| Exchange::new(name.clone(), kind, durable));

        // WAL: persist durable exchange declarations
        if durable && is_new {
            if let Some(wal) = broker.wal() {
                let _ = wal.log_declare_exchange(&name, kind_byte, true);
            }
        }
    }

    info!(
        conn_id,
        channel,
        exchange = name.as_str(),
        "exchange declared"
    );
    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

/// Exchange.Delete: exchange(shortstr) if_unused(bit) no_wait(bit)
pub async fn handle_delete(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let name = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let _if_unused = flags & 0x01 != 0;
    let no_wait = flags & 0x02 != 0;

    if name.starts_with("amq.") {
        let close = build_channel_close(
            ACCESS_REFUSED,
            "ACCESS_REFUSED - cannot delete default exchange",
            CLASS_EXCHANGE,
            METHOD_EXCHANGE_DELETE,
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

    broker.exchanges.write().await.remove(&name);
    info!(
        conn_id,
        channel,
        exchange = name.as_str(),
        "exchange deleted"
    );
    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_EXCHANGE, METHOD_EXCHANGE_DELETE_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

/// Exchange.Bind (RabbitMQ extension)
pub async fn handle_bind(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) {
    // Not to be confused with Queue.Bind — this is exchange-to-exchange binding
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let destination = read_shortstr(&mut r).unwrap_or_default();
    let source = read_shortstr(&mut r).unwrap_or_default();
    let routing_key = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let no_wait = flags & 0x01 != 0;

    // For now, treat as queue bind on the source exchange
    {
        let mut exchanges = broker.exchanges.write().await;
        if let Some(ex) = exchanges.get_mut(&source) {
            ex.add_binding(Binding {
                queue_name: destination.clone(),
                routing_key: routing_key.clone(),
                headers_match: None,
            });
        }
    }

    info!(
        conn_id,
        source = source.as_str(),
        dest = destination.as_str(),
        "exchange bound"
    );
    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_EXCHANGE, METHOD_EXCHANGE_BIND_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

/// Exchange.Unbind
pub async fn handle_unbind(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let destination = read_shortstr(&mut r).unwrap_or_default();
    let source = read_shortstr(&mut r).unwrap_or_default();
    let routing_key = read_shortstr(&mut r).unwrap_or_default();

    {
        let mut exchanges = broker.exchanges.write().await;
        if let Some(ex) = exchanges.get_mut(&source) {
            ex.remove_binding(&destination, &routing_key);
        }
    }

    info!(
        conn_id,
        source = source.as_str(),
        dest = destination.as_str(),
        "exchange unbound"
    );
    let reply = encode_method_frame(channel, CLASS_EXCHANGE, METHOD_EXCHANGE_UNBIND_OK, &[]);
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

    #[test]
    fn exchange_declare_args_encode() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap(); // ticket
        write_shortstr(&mut args, "test.ex").unwrap();
        write_shortstr(&mut args, "direct").unwrap();
        write_octet(&mut args, 0x02).unwrap(); // durable
        write_field_table(&mut args, &FieldTable::new()).unwrap();

        let mut r = Cursor::new(&args);
        assert_eq!(read_short(&mut r).unwrap(), 0);
        assert_eq!(read_shortstr(&mut r).unwrap(), "test.ex");
        assert_eq!(read_shortstr(&mut r).unwrap(), "direct");
        assert_eq!(read_octet(&mut r).unwrap(), 0x02);
    }

    #[test]
    fn exchange_delete_args_encode() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, "my.exchange").unwrap();
        write_octet(&mut args, 0).unwrap();

        let mut r = Cursor::new(&args);
        let _ = read_short(&mut r).unwrap();
        assert_eq!(read_shortstr(&mut r).unwrap(), "my.exchange");
    }

    #[test]
    fn channel_close_error_builds() {
        let args = build_channel_close(
            NOT_FOUND,
            "NOT_FOUND",
            CLASS_EXCHANGE,
            METHOD_EXCHANGE_DECLARE,
        );
        let mut r = Cursor::new(&args);
        assert_eq!(read_short(&mut r).unwrap(), 404);
        assert_eq!(read_shortstr(&mut r).unwrap(), "NOT_FOUND");
        assert_eq!(read_short(&mut r).unwrap(), CLASS_EXCHANGE);
        assert_eq!(read_short(&mut r).unwrap(), METHOD_EXCHANGE_DECLARE);
    }
}
