//! Integration tests for the transaction and vhost handlers.
//!
//! These tests exercise the handler functions directly with a real BrokerState
//! and mpsc channel pair, verifying that the correct response frames are sent
//! and that state is mutated appropriately.

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tokio::sync::mpsc;

    use crate::core::protocol::{Event, Frame};
    use crate::queue::QueueState;
    use crate::state::{BrokerState, ConnectionState, VHost};
    use crate::state::broker::PendingOp;
    use crate::server::handler::transaction;
    use crate::server::handler::connection;

    fn setup_broker() -> (Arc<BrokerState>, mpsc::Sender<Frame>, mpsc::Receiver<Frame>) {
        let broker = Arc::new(BrokerState::new());
        let (tx, rx) = mpsc::channel(16);
        (broker, tx, rx)
    }

    fn setup_broker_with_conn() -> (Arc<BrokerState>, u64, mpsc::Sender<Frame>, mpsc::Receiver<Frame>) {
        let broker = Arc::new(BrokerState::new());
        let conn_id = broker.alloc_conn_id();
        broker.conn_state.insert(conn_id, ConnectionState::new());
        let (tx, rx) = mpsc::channel(16);
        (broker, conn_id, tx, rx)
    }

    // ──────────────────────────────────────────────
    // VHost Open handler tests
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn vhost_open_default() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        connection::vhost_open(conn_id, &tx, &broker, b"/").await;

        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::VHostOpenOk);
        assert_eq!(std::str::from_utf8(&frame.payload).unwrap(), "/");

        let cs = broker.conn_state.get(&conn_id).unwrap();
        assert_eq!(cs.vhost, "/");
    }

    #[tokio::test]
    async fn vhost_open_custom() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        // Create the vhost first
        broker.vhosts.insert("/staging".to_string(), VHost::new("/staging".to_string()));

        connection::vhost_open(conn_id, &tx, &broker, b"/staging").await;

        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::VHostOpenOk);
        assert_eq!(std::str::from_utf8(&frame.payload).unwrap(), "/staging");

        let cs = broker.conn_state.get(&conn_id).unwrap();
        assert_eq!(cs.vhost, "/staging");
    }

    #[tokio::test]
    async fn vhost_open_nonexistent_no_response() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        connection::vhost_open(conn_id, &tx, &broker, b"/nonexistent").await;

        // No response frame should be sent
        assert!(rx.try_recv().is_err());

        // vhost should remain at default
        let cs = broker.conn_state.get(&conn_id).unwrap();
        assert_eq!(cs.vhost, "/");
    }

    #[tokio::test]
    async fn vhost_open_switches_connection() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        broker.vhosts.insert("/v1".to_string(), VHost::new("/v1".to_string()));
        broker.vhosts.insert("/v2".to_string(), VHost::new("/v2".to_string()));

        // Open v1
        connection::vhost_open(conn_id, &tx, &broker, b"/v1").await;
        let _ = rx.recv().await;
        assert_eq!(broker.conn_state.get(&conn_id).unwrap().vhost, "/v1");

        // Switch to v2
        connection::vhost_open(conn_id, &tx, &broker, b"/v2").await;
        let _ = rx.recv().await;
        assert_eq!(broker.conn_state.get(&conn_id).unwrap().vhost, "/v2");
    }

    #[tokio::test]
    async fn vhost_open_with_whitespace_trim() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        connection::vhost_open(conn_id, &tx, &broker, b"/ ").await;

        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::VHostOpenOk);
    }

    // ──────────────────────────────────────────────
    // TxSelect handler tests
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn tx_select_enables_tx_mode() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        transaction::tx_select(conn_id, &tx, &broker).await;

        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::TxSelectOk);

        let cs = broker.conn_state.get(&conn_id).unwrap();
        assert!(cs.tx_mode);
        assert!(cs.tx_buffer.is_empty());
    }

    #[tokio::test]
    async fn tx_select_clears_existing_buffer() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        // Pre-populate buffer
        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_mode = true;
            cs.tx_buffer.push(PendingOp::Ack { msg_id: 1 });
        }

        transaction::tx_select(conn_id, &tx, &broker).await;
        let _ = rx.recv().await;

        let cs = broker.conn_state.get(&conn_id).unwrap();
        assert!(cs.tx_buffer.is_empty());
    }

    // ──────────────────────────────────────────────
    // TxCommit handler tests
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn tx_commit_executes_publish() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        // Setup: create queue and enable tx mode
        broker.queues.insert("q1".into(), QueueState::new());
        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_mode = true;
            cs.tx_buffer.push(PendingOp::Publish {
                exchange: "".to_string(),
                routing_key: "q1".to_string(),
                headers: vec![],
                body: b"transacted".to_vec(),
            });
        }

        transaction::tx_commit(conn_id, &tx, &broker).await;

        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::TxCommitOk);

        // Message should be in the queue now
        let q = broker.queues.get("q1").unwrap();
        assert_eq!(q.messages.len(), 1);
    }

    #[tokio::test]
    async fn tx_commit_executes_ack() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        // Setup: put message in inflight
        broker.queues.insert("q1".into(), QueueState::new());
        {
            let msg = crate::queue::Message::new(42, vec![], b"test".to_vec());
            broker.queues.get_mut("q1").unwrap().inflight.insert(42, msg);
        }

        // Enable tx mode and buffer an ack
        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_mode = true;
            cs.tx_buffer.push(PendingOp::Ack { msg_id: 42 });
        }

        transaction::tx_commit(conn_id, &tx, &broker).await;

        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::TxCommitOk);

        // Inflight should be cleared
        let q = broker.queues.get("q1").unwrap();
        assert!(q.inflight.is_empty());
    }

    #[tokio::test]
    async fn tx_commit_multiple_ops() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        broker.queues.insert("q1".into(), QueueState::new());
        broker.queues.insert("q2".into(), QueueState::new());

        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_mode = true;
            cs.tx_buffer.push(PendingOp::Publish {
                exchange: "".to_string(),
                routing_key: "q1".to_string(),
                headers: vec![],
                body: b"msg1".to_vec(),
            });
            cs.tx_buffer.push(PendingOp::Publish {
                exchange: "".to_string(),
                routing_key: "q2".to_string(),
                headers: vec![],
                body: b"msg2".to_vec(),
            });
        }

        transaction::tx_commit(conn_id, &tx, &broker).await;
        let _ = rx.recv().await;

        assert_eq!(broker.queues.get("q1").unwrap().messages.len(), 1);
        assert_eq!(broker.queues.get("q2").unwrap().messages.len(), 1);
    }

    #[tokio::test]
    async fn tx_commit_without_tx_mode_is_noop() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        // tx_mode is false by default
        transaction::tx_commit(conn_id, &tx, &broker).await;

        // No response frame
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn tx_commit_empty_buffer() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        broker.conn_state.get_mut(&conn_id).unwrap().tx_mode = true;

        transaction::tx_commit(conn_id, &tx, &broker).await;

        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::TxCommitOk);
    }

    // ──────────────────────────────────────────────
    // TxRollback handler tests
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn tx_rollback_clears_buffer() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_mode = true;
            cs.tx_buffer.push(PendingOp::Publish {
                exchange: "".to_string(),
                routing_key: "q1".to_string(),
                headers: vec![],
                body: b"discarded".to_vec(),
            });
        }

        transaction::tx_rollback(conn_id, &tx, &broker).await;

        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::TxRollbackOk);

        let cs = broker.conn_state.get(&conn_id).unwrap();
        assert!(cs.tx_buffer.is_empty());
        // tx_mode stays enabled
        assert!(cs.tx_mode);
    }

    #[tokio::test]
    async fn tx_rollback_does_not_apply_ops() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        broker.queues.insert("q1".into(), QueueState::new());

        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_mode = true;
            cs.tx_buffer.push(PendingOp::Publish {
                exchange: "".to_string(),
                routing_key: "q1".to_string(),
                headers: vec![],
                body: b"should_not_appear".to_vec(),
            });
        }

        transaction::tx_rollback(conn_id, &tx, &broker).await;
        let _ = rx.recv().await;

        // Queue should still be empty
        assert_eq!(broker.queues.get("q1").unwrap().messages.len(), 0);
    }

    #[tokio::test]
    async fn tx_rollback_without_tx_mode_is_noop() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        transaction::tx_rollback(conn_id, &tx, &broker).await;

        // No response frame
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn tx_rollback_empty_buffer() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();

        broker.conn_state.get_mut(&conn_id).unwrap().tx_mode = true;

        transaction::tx_rollback(conn_id, &tx, &broker).await;

        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::TxRollbackOk);
    }

    // ──────────────────────────────────────────────
    // Full transaction lifecycle tests
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn tx_lifecycle_select_commit() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();
        broker.queues.insert("q1".into(), QueueState::new());

        // 1. tx_select
        transaction::tx_select(conn_id, &tx, &broker).await;
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::TxSelectOk);

        // 2. Buffer a publish
        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_buffer.push(PendingOp::Publish {
                exchange: "".to_string(),
                routing_key: "q1".to_string(),
                headers: vec![],
                body: b"lifecycle-msg".to_vec(),
            });
        }

        // 3. tx_commit
        transaction::tx_commit(conn_id, &tx, &broker).await;
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::TxCommitOk);

        // 4. Verify message is in queue
        assert_eq!(broker.queues.get("q1").unwrap().messages.len(), 1);
    }

    #[tokio::test]
    async fn tx_lifecycle_select_rollback() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();
        broker.queues.insert("q1".into(), QueueState::new());

        // 1. tx_select
        transaction::tx_select(conn_id, &tx, &broker).await;
        let _ = rx.recv().await;

        // 2. Buffer a publish
        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_buffer.push(PendingOp::Publish {
                exchange: "".to_string(),
                routing_key: "q1".to_string(),
                headers: vec![],
                body: b"rolled-back-msg".to_vec(),
            });
        }

        // 3. tx_rollback
        transaction::tx_rollback(conn_id, &tx, &broker).await;
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.event, Event::TxRollbackOk);

        // 4. Queue should be empty
        assert_eq!(broker.queues.get("q1").unwrap().messages.len(), 0);
    }

    #[tokio::test]
    async fn tx_lifecycle_select_commit_select_again() {
        let (broker, conn_id, tx, mut rx) = setup_broker_with_conn();
        broker.queues.insert("q1".into(), QueueState::new());

        // First transaction
        transaction::tx_select(conn_id, &tx, &broker).await;
        let _ = rx.recv().await;

        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_buffer.push(PendingOp::Publish {
                exchange: "".to_string(),
                routing_key: "q1".to_string(),
                headers: vec![],
                body: b"first".to_vec(),
            });
        }

        transaction::tx_commit(conn_id, &tx, &broker).await;
        let _ = rx.recv().await;

        // Second transaction via re-select
        transaction::tx_select(conn_id, &tx, &broker).await;
        let _ = rx.recv().await;

        {
            let mut cs = broker.conn_state.get_mut(&conn_id).unwrap();
            cs.tx_buffer.push(PendingOp::Publish {
                exchange: "".to_string(),
                routing_key: "q1".to_string(),
                headers: vec![],
                body: b"second".to_vec(),
            });
        }

        transaction::tx_commit(conn_id, &tx, &broker).await;
        let _ = rx.recv().await;

        assert_eq!(broker.queues.get("q1").unwrap().messages.len(), 2);
    }
}
