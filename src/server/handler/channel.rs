use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::core::protocol::{Event, Frame};
use crate::state::{Broker, ChannelState, ConnectionState};

pub async fn channel_open(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
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

pub async fn channel_close(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
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

pub async fn qos(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let body_str = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid qos body");
            return;
        }
    };

    let mut prefetch_count: u16 = 0;
    for line in body_str.split("\r\n") {
        if let Some((k, v)) = line.split_once(':') {
            if k == "prefetch_count" {
                prefetch_count = v.parse().unwrap_or(0);
            }
        }
    }

    if let Some(mut conn_state) = broker.conn_state.get_mut(&conn_id) {
        for ch in conn_state.channels.values_mut() {
            ch.prefetch_count = prefetch_count;
        }
    }

    info!(conn_id, prefetch_count, "qos set");
    let _ = tx.send(Frame::empty(Event::QosOk)).await;
}

pub async fn confirm_select(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker) {
    if let Some(mut conn_state) = broker.conn_state.get_mut(&conn_id) {
        conn_state.confirm_mode = true;
    }

    info!(conn_id, "confirm mode enabled");
    let _ = tx.send(Frame::empty(Event::ConfirmSelectOk)).await;
}

pub async fn channel_flow(conn_id: u64, channel_id: u16, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let active = body.first().copied().unwrap_or(1) != 0;

    if let Some(mut conn_state) = broker.conn_state.get_mut(&conn_id) {
        if let Some(ch) = conn_state.channels.get_mut(&channel_id) {
            ch.flow_active = active;
        }
    }

    info!(conn_id, channel_id, active, "channel flow set");
    let reply = if active { vec![1] } else { vec![0] };
    let _ = tx.send(Frame::with_body(Event::ChannelFlowOk, reply)).await;
}
