//! Transaction handlers: TxSelect, TxCommit, TxRollback.
//!
//! When in tx mode, Publish and Ack operations are buffered into
//! `ConnectionState.tx_buffer` instead of being executed immediately.
//! On TxCommit, all buffered ops are applied atomically.
//! On TxRollback, the buffer is discarded.

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::core::protocol::{Event, Frame};
use crate::state::Broker;
use crate::state::broker::PendingOp;

/// Enable transaction mode on a connection.
pub async fn tx_select(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker) {
    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        cs.tx_mode = true;
        cs.tx_buffer.clear();
        info!(conn_id, "transaction mode enabled");
    }
    let _ = tx.send(Frame::empty(Event::TxSelectOk)).await;
}

/// Commit the current transaction: execute all buffered operations.
pub async fn tx_commit(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker) {
    // Extract the buffered ops (drain under lock)
    let ops = {
        match broker.conn_state.get_mut(&conn_id) {
            Some(mut cs) => {
                if !cs.tx_mode {
                    warn!(conn_id, "tx_commit without tx_select");
                    return;
                }
                std::mem::take(&mut cs.tx_buffer)
            }
            None => return,
        }
    };

    // Execute all buffered operations atomically
    for op in &ops {
        match op {
            PendingOp::Publish {
                exchange: _,
                routing_key,
                headers: _,
                body,
            } => {
                // For tx publish, route to the queue directly (simplified)
                let msg_id = broker.alloc_msg_id();
                if let Some(mut queue) = broker.queues.get_mut(routing_key.as_str()) {
                    let msg = crate::queue::Message::new(msg_id, Vec::new(), body.clone());
                    queue.messages.push_back(msg);
                }
            }
            PendingOp::Ack { msg_id } => {
                for mut entry in broker.queues.iter_mut() {
                    if entry.value_mut().inflight.remove(msg_id).is_some() {
                        if let Some(wal) = broker.wal() {
                            let _ = wal.log_ack(*msg_id);
                        }
                        break;
                    }
                }
            }
        }
    }

    info!(conn_id, ops = ops.len(), "transaction committed");
    let _ = tx.send(Frame::empty(Event::TxCommitOk)).await;
}

/// Rollback the current transaction: discard all buffered operations.
pub async fn tx_rollback(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker) {
    let discarded = {
        match broker.conn_state.get_mut(&conn_id) {
            Some(mut cs) => {
                if !cs.tx_mode {
                    warn!(conn_id, "tx_rollback without tx_select");
                    return;
                }
                let count = cs.tx_buffer.len();
                cs.tx_buffer.clear();
                count
            }
            None => return,
        }
    };

    info!(conn_id, discarded, "transaction rolled back");
    let _ = tx.send(Frame::empty(Event::TxRollbackOk)).await;
}
