//! AMQP 0-9-1 method dispatcher.
//!
//! Routes incoming method frames by class_id/method_id to the appropriate handler.

use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::tcp::OwnedWriteHalf;
use tracing::{info, warn};

use crate::core::amqp_codec::*;
use crate::core::method::*;
use crate::core::types::*;
use crate::state::{Broker, ChannelState, ConnectionState};

/// Dispatch a decoded AMQP method frame to the correct handler.
/// Returns `false` if the connection should be closed.
pub async fn dispatch_method(
    conn_id: u64,
    channel: u16,
    method: &MethodFrame,
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) -> bool {
    match (method.class_id, method.method_id) {
        // ── Connection class (channel 0 only) ─────────
        (CLASS_CONNECTION, METHOD_CONNECTION_CLOSE) => {
            info!(conn_id, "client requested Connection.Close");
            let reply = encode_method_frame(0, CLASS_CONNECTION, METHOD_CONNECTION_CLOSE_OK, &[]);
            let _ = writer.write_all(&reply).await;
            let _ = writer.flush().await;
            false // signal to close
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_CLOSE_OK) => {
            info!(conn_id, "received Connection.CloseOk");
            false
        }

        // ── Channel class ─────────────────────────────
        (CLASS_CHANNEL, METHOD_CHANNEL_OPEN) => {
            handle_channel_open(conn_id, channel, writer, broker).await;
            true
        }
        (CLASS_CHANNEL, METHOD_CHANNEL_CLOSE) => {
            handle_channel_close(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_CHANNEL, METHOD_CHANNEL_CLOSE_OK) => {
            // Acknowledgement of our close — nothing to do
            true
        }
        (CLASS_CHANNEL, METHOD_CHANNEL_FLOW) => {
            handle_channel_flow(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }

        // ── Exchange class ────────────────────────────
        (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE) => {
            super::amqp_exchange::handle_declare(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_EXCHANGE, METHOD_EXCHANGE_DELETE) => {
            super::amqp_exchange::handle_delete(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_EXCHANGE, METHOD_EXCHANGE_BIND) => {
            super::amqp_exchange::handle_bind(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_EXCHANGE, METHOD_EXCHANGE_UNBIND) => {
            super::amqp_exchange::handle_unbind(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }

        // ── Queue class ───────────────────────────────
        (CLASS_QUEUE, METHOD_QUEUE_DECLARE) => {
            super::amqp_queue::handle_declare(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_QUEUE, METHOD_QUEUE_DELETE) => {
            super::amqp_queue::handle_delete(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_QUEUE, METHOD_QUEUE_PURGE) => {
            super::amqp_queue::handle_purge(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_QUEUE, METHOD_QUEUE_BIND) => {
            super::amqp_queue::handle_bind(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_QUEUE, METHOD_QUEUE_UNBIND) => {
            super::amqp_queue::handle_unbind(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }

        // ── Basic class ───────────────────────────────
        // Note: Basic.Publish is handled via content framing in the connection loop,
        // not here. The connection reads method+header+body then calls handle_publish.
        (CLASS_BASIC, METHOD_BASIC_CONSUME) => {
            super::amqp_basic::handle_consume(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_BASIC, METHOD_BASIC_CANCEL) => {
            super::amqp_basic::handle_cancel(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_BASIC, METHOD_BASIC_ACK) => {
            super::amqp_basic::handle_ack(conn_id, channel, &method.arguments, broker).await;
            true
        }
        (CLASS_BASIC, METHOD_BASIC_REJECT) => {
            super::amqp_basic::handle_reject(conn_id, channel, &method.arguments, broker).await;
            true
        }
        (CLASS_BASIC, METHOD_BASIC_NACK) => {
            super::amqp_basic::handle_nack(conn_id, channel, &method.arguments, broker).await;
            true
        }
        (CLASS_BASIC, METHOD_BASIC_GET) => {
            super::amqp_basic::handle_get(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_BASIC, METHOD_BASIC_QOS) => {
            super::amqp_basic::handle_qos(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }
        (CLASS_BASIC, METHOD_BASIC_RECOVER | METHOD_BASIC_RECOVER_ASYNC) => {
            super::amqp_basic::handle_recover(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }

        // ── Tx class ──────────────────────────────────
        (CLASS_TX, METHOD_TX_SELECT) => {
            super::amqp_tx::handle_tx_select(conn_id, channel, writer, broker).await;
            true
        }
        (CLASS_TX, METHOD_TX_COMMIT) => {
            super::amqp_tx::handle_tx_commit(conn_id, channel, writer, broker).await;
            true
        }
        (CLASS_TX, METHOD_TX_ROLLBACK) => {
            super::amqp_tx::handle_tx_rollback(conn_id, channel, writer, broker).await;
            true
        }

        // ── Confirm class ─────────────────────────────
        (CLASS_CONFIRM, METHOD_CONFIRM_SELECT) => {
            super::amqp_tx::handle_confirm_select(conn_id, channel, &method.arguments, writer, broker).await;
            true
        }

        // ── Unknown ───────────────────────────────────
        _ => {
            warn!(
                conn_id,
                channel,
                class_id = method.class_id,
                method_id = method.method_id,
                "unhandled AMQP method"
            );
            true
        }
    }
}

// ─── Channel handlers ─────────────────────────────────

async fn handle_channel_open(
    conn_id: u64,
    channel: u16,
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    broker
        .conn_state
        .entry(conn_id)
        .or_insert_with(ConnectionState::new)
        .channels
        .entry(channel)
        .or_insert_with(|| ChannelState::new(channel));

    info!(conn_id, channel, "channel opened");

    // Channel.OpenOk: reserved(longstr) — send empty
    let mut args = Vec::new();
    write_longstr(&mut args, b"").unwrap();
    let reply = encode_method_frame(channel, CLASS_CHANNEL, METHOD_CHANNEL_OPEN_OK, &args);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

async fn handle_channel_close(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    // Parse: reply_code(short) reply_text(shortstr) class_id(short) method_id(short)
    if args.len() >= 4 {
        let code = u16::from_be_bytes([args[0], args[1]]);
        if code != REPLY_SUCCESS {
            warn!(conn_id, channel, code, "channel closed with error");
        }
    }

    if let Some(mut conn_state) = broker.conn_state.get_mut(&conn_id) {
        conn_state.channels.remove(&channel);
    }

    info!(conn_id, channel, "channel closed");
    let reply = encode_method_frame(channel, CLASS_CHANNEL, METHOD_CHANNEL_CLOSE_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

async fn handle_channel_flow(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) {
    let active = args.first().copied().unwrap_or(1) != 0;

    if let Some(mut conn_state) = broker.conn_state.get_mut(&conn_id) {
        if let Some(ch) = conn_state.channels.get_mut(&channel) {
            ch.flow_active = active;
        }
    }

    info!(conn_id, channel, active, "channel flow");
    let mut reply_args = Vec::new();
    write_octet(&mut reply_args, if active { 1 } else { 0 }).unwrap();
    let reply = encode_method_frame(channel, CLASS_CHANNEL, METHOD_CHANNEL_FLOW_OK, &reply_args);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn channel_open_ok_frame_valid() {
        let mut args = Vec::new();
        write_longstr(&mut args, b"").unwrap();
        let frame = encode_method_frame(1, CLASS_CHANNEL, METHOD_CHANNEL_OPEN_OK, &args);
        let (decoded, _) = decode_frame(&frame).unwrap();
        assert_eq!(decoded.frame_type, FRAME_METHOD);
        assert_eq!(decoded.channel, 1);
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_CHANNEL);
        assert_eq!(m.method_id, METHOD_CHANNEL_OPEN_OK);
    }

    #[test]
    fn channel_close_ok_frame_valid() {
        let frame = encode_method_frame(1, CLASS_CHANNEL, METHOD_CHANNEL_CLOSE_OK, &[]);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_CHANNEL);
        assert_eq!(m.method_id, METHOD_CHANNEL_CLOSE_OK);
    }

    #[test]
    fn channel_flow_ok_active() {
        let mut args = Vec::new();
        write_octet(&mut args, 1).unwrap();
        let frame = encode_method_frame(1, CLASS_CHANNEL, METHOD_CHANNEL_FLOW_OK, &args);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.class_id, CLASS_CHANNEL);
        assert_eq!(m.arguments, vec![1]);
    }

    #[test]
    fn channel_flow_ok_inactive() {
        let mut args = Vec::new();
        write_octet(&mut args, 0).unwrap();
        let frame = encode_method_frame(1, CLASS_CHANNEL, METHOD_CHANNEL_FLOW_OK, &args);
        let (decoded, _) = decode_frame(&frame).unwrap();
        let m = decode_method(&decoded.payload).unwrap();
        assert_eq!(m.arguments, vec![0]);
    }
}
