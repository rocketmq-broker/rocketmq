use tokio::sync::mpsc;
use tracing::info;

use crate::state::Broker;
use crate::core::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker) {
    if let Some(mut conn_state) = broker.conn_state.get_mut(&conn_id) {
        conn_state.confirm_mode = true;
    }

    info!(conn_id, "confirm mode enabled");
    let _ = tx.send(Frame::empty(Event::ConfirmSelectOk)).await;
}
