use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::broker::Broker;
use crate::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let channel_id: u16 = if body.len() >= 2 {
        u16::from_be_bytes([body[0], body[1]])
    } else {
        warn!(conn_id, "missing channel_id in channel_close");
        return;
    };

    if let Some(mut conn_state) = broker.conn_state.get_mut(&conn_id) {
        conn_state.channels.remove(&channel_id);
    }

    info!(conn_id, channel_id, "channel closed");
    let _ = tx.send(Frame::empty(Event::ChannelCloseOk)).await;
}
