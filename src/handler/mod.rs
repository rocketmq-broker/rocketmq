mod ack;
mod assert_queue;
mod bind;
mod channel_close;
mod channel_open;
mod confirm_select;
mod declare_exchange;
mod delete_exchange;
mod heartbeat;
mod listen;
mod nack;
mod nop;
mod publish;
mod qos;
mod unbind;

use tokio::sync::mpsc;
use tracing::warn;

use crate::broker::Broker;
use crate::core::protocol::{Event, Frame, Header};

pub use ack::handle as ack;
pub use assert_queue::handle as assert_queue;
pub use bind::handle as bind;
pub use channel_close::handle as channel_close;
pub use channel_open::handle as channel_open;
pub use confirm_select::handle as confirm_select;
pub use declare_exchange::handle as declare_exchange;
pub use delete_exchange::handle as delete_exchange;
pub use heartbeat::handle as heartbeat;
pub use listen::handle as listen;
pub use nack::handle as nack;
pub use nop::handle as nop;
pub use publish::handle as publish;
pub use qos::handle as qos;
pub use unbind::handle as unbind;

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
        Event::Nop => nop(conn_id).await,
        Event::Heartbeat => heartbeat(tx).await,
        Event::AssertQueue => assert_queue(conn_id, tx, broker, body).await,
        Event::Listen => listen(conn_id, tx, broker, body).await,
        Event::Publish => publish(conn_id, broker, inline_headers, body).await,
        Event::Ack => ack(conn_id, broker, inline_headers).await,
        Event::Nack => nack(conn_id, broker, inline_headers).await,
        Event::DeclareExchange => declare_exchange(conn_id, tx, broker, inline_headers).await,
        Event::DeleteExchange => delete_exchange(conn_id, tx, broker, inline_headers).await,
        Event::Bind => bind(conn_id, tx, broker, inline_headers).await,
        Event::Unbind => unbind(conn_id, tx, broker, inline_headers).await,
        Event::ChannelOpen => channel_open(conn_id, tx, broker, body).await,
        Event::ChannelClose => channel_close(conn_id, tx, broker, body).await,
        Event::Qos => qos(conn_id, tx, broker, body).await,
        Event::ConfirmSelect => confirm_select(conn_id, tx, broker).await,
        other => warn!(conn_id, ?other, "unexpected event"),
    }
}
