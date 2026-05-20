pub mod channel;
pub mod connection;
pub mod exchange;
pub mod message;
pub mod queue;

use tokio::sync::mpsc;
use tracing::warn;

use crate::core::protocol::{Event, Frame, Header};
use crate::state::Broker;

pub fn parse_msg_id(headers: &[u8]) -> Option<u64> {
    let s = std::str::from_utf8(headers).ok()?;
    for line in s.split("\r\n") {
        if let Some(rest) = line.strip_prefix("id:") {
            return rest.parse::<u64>().ok();
        }
    }
    None
}

pub async fn dispatch(
    conn_id: u64,
    tx: &mpsc::Sender<Frame>,
    broker: &Broker,
    header: &Header,
    payload: &[u8],
) {
    let body = &payload[header.bodyoff as usize..];
    let inline_headers = &payload[..header.bodyoff as usize];

    match header.event {
        Event::Nop => connection::nop(conn_id).await,
        Event::Heartbeat => connection::heartbeat(tx).await,
        Event::AssertQueue => queue::assert_queue(conn_id, tx, broker, body).await,
        Event::Listen => queue::listen(conn_id, header.channel_id, tx, broker, body).await,
        Event::Publish => message::publish(conn_id, header.channel_id, broker, inline_headers, body).await,
        Event::Ack => message::ack(conn_id, header.channel_id, broker, inline_headers).await,
        Event::Nack => message::nack(conn_id, header.channel_id, broker, inline_headers).await,
        Event::DeclareExchange => {
            exchange::declare_exchange(conn_id, tx, broker, inline_headers).await
        }
        Event::DeleteExchange => {
            exchange::delete_exchange(conn_id, tx, broker, inline_headers).await
        }
        Event::Bind => exchange::bind(conn_id, tx, broker, inline_headers).await,
        Event::Unbind => exchange::unbind(conn_id, tx, broker, inline_headers).await,
        Event::ChannelOpen => channel::channel_open(conn_id, tx, broker, body).await,
        Event::ChannelClose => channel::channel_close(conn_id, tx, broker, body).await,
        Event::Qos => channel::qos(conn_id, tx, broker, body).await,
        Event::ConfirmSelect => channel::confirm_select(conn_id, tx, broker).await,
        Event::ChannelFlow => channel::channel_flow(conn_id, header.channel_id, tx, broker, body).await,
        Event::BasicCancel => queue::basic_cancel(conn_id, tx, broker, body).await,
        other => warn!(conn_id, ?other, "unexpected event"),
    }
}
