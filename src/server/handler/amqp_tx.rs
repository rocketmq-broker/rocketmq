//! AMQP 0-9-1 Tx class (class 90) and Confirm class (class 85).

use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tracing::{info, warn};

use crate::core::amqp_codec::*;
use crate::core::method::*;
use crate::core::types::*;
use crate::state::Broker;
use crate::state::broker::PendingOp;

// ─── Tx.Select ────────────────────────────────────────

pub async fn handle_tx_select(
    conn_id: u64, channel: u16,
    writer: &mut BufWriter<OwnedWriteHalf>, broker: &Broker,
) {
    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        cs.tx_mode = true;
        cs.tx_buffer.clear();
    }
    info!(conn_id, channel, "tx mode enabled");
    let reply = encode_method_frame(channel, CLASS_TX, METHOD_TX_SELECT_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

// ─── Tx.Commit ────────────────────────────────────────

pub async fn handle_tx_commit(
    conn_id: u64, channel: u16,
    writer: &mut BufWriter<OwnedWriteHalf>, broker: &Broker,
) {
    let ops = {
        match broker.conn_state.get_mut(&conn_id) {
            Some(mut cs) => {
                if !cs.tx_mode {
                    warn!(conn_id, "tx_commit without tx_select");
                    let close = build_channel_close(PRECONDITION_FAILED, "PRECONDITION_FAILED - not in tx mode", CLASS_TX, METHOD_TX_COMMIT);
                    let _ = writer.write_all(&encode_method_frame(channel, CLASS_CHANNEL, METHOD_CHANNEL_CLOSE, &close)).await;
                    let _ = writer.flush().await;
                    return;
                }
                std::mem::take(&mut cs.tx_buffer)
            }
            None => return,
        }
    };

    for op in &ops {
        match op {
            PendingOp::Publish { routing_key, body, .. } => {
                let msg_id = broker.alloc_msg_id();
                if let Some(mut queue) = broker.queues.get_mut(routing_key.as_str()) {
                    let msg = crate::queue::Message::new(msg_id, Vec::new(), body.clone());
                    queue.messages.push_back(msg);
                }
            }
            PendingOp::Ack { msg_id } => {
                for mut entry in broker.queues.iter_mut() {
                    if entry.value_mut().inflight.remove(msg_id).is_some() {
                        if let Some(wal) = broker.wal() { let _ = wal.log_ack(*msg_id); }
                        break;
                    }
                }
            }
        }
    }

    info!(conn_id, channel, ops = ops.len(), "tx committed");
    let reply = encode_method_frame(channel, CLASS_TX, METHOD_TX_COMMIT_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

// ─── Tx.Rollback ──────────────────────────────────────

pub async fn handle_tx_rollback(
    conn_id: u64, channel: u16,
    writer: &mut BufWriter<OwnedWriteHalf>, broker: &Broker,
) {
    let discarded = {
        match broker.conn_state.get_mut(&conn_id) {
            Some(mut cs) => {
                if !cs.tx_mode {
                    warn!(conn_id, "tx_rollback without tx_select");
                    let close = build_channel_close(PRECONDITION_FAILED, "PRECONDITION_FAILED - not in tx mode", CLASS_TX, METHOD_TX_ROLLBACK);
                    let _ = writer.write_all(&encode_method_frame(channel, CLASS_CHANNEL, METHOD_CHANNEL_CLOSE, &close)).await;
                    let _ = writer.flush().await;
                    return;
                }
                let count = cs.tx_buffer.len();
                cs.tx_buffer.clear();
                count
            }
            None => return,
        }
    };

    info!(conn_id, channel, discarded, "tx rolled back");
    let reply = encode_method_frame(channel, CLASS_TX, METHOD_TX_ROLLBACK_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

// ─── Confirm.Select ───────────────────────────────────

pub async fn handle_confirm_select(
    conn_id: u64, channel: u16, args: &[u8],
    writer: &mut BufWriter<OwnedWriteHalf>, broker: &Broker,
) {
    let no_wait = args.first().copied().unwrap_or(0) & 0x01 != 0;

    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        cs.confirm_mode = true;
    }

    info!(conn_id, channel, "confirm mode enabled");
    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_CONFIRM, METHOD_CONFIRM_SELECT_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

// ─── Helper ───────────────────────────────────────────

fn build_channel_close(code: u16, text: &str, class_id: u16, method_id: u16) -> Vec<u8> {
    let mut buf = Vec::new();
    write_short(&mut buf, code).unwrap();
    write_shortstr(&mut buf, text).unwrap();
    write_short(&mut buf, class_id).unwrap();
    write_short(&mut buf, method_id).unwrap();
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
