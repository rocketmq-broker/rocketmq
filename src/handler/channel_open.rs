use tokio::sync::mpsc;
use tracing::info;

use crate::broker::{Broker, ChannelState, ConnectionState};
use crate::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let channel_id: u16 = if body.len() >= 2 {
        u16::from_be_bytes([body[0], body[1]])
    } else {
        broker
            .conn_state
            .get(&conn_id)
            .map(|cs| cs.channels.keys().max().copied().unwrap_or(0) + 1)
            .unwrap_or(1)
    };

    broker
        .conn_state
        .entry(conn_id)
        .or_insert_with(ConnectionState::new)
        .channels
        .entry(channel_id)
        .or_insert_with(|| ChannelState::new(channel_id));

    info!(conn_id, channel_id, "channel opened");

    let reply = format!("channel_id:{}\r\n", channel_id);
    let _ = tx
        .send(Frame::with_body(Event::ChannelOpenOk, reply.into_bytes()))
        .await;
}
