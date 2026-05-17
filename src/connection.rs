//! Per-connection reader/writer tasks and connection lifecycle.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::state::{Broker, ConnHandle, ConnectionState};
use crate::core::error::Error;
use crate::handler;
use crate::core::protocol::{Frame, HEADER_SIZE, Header, MAX_BODY};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);
const CHANNEL_CAPACITY: usize = 64;

pub fn spawn(stream: TcpStream, addr: SocketAddr, broker: Broker) {
    tokio::spawn(async move {
        let (tx, rx) = mpsc::channel::<Frame>(CHANNEL_CAPACITY);

        let conn_id = broker.alloc_conn_id();
        broker.connections.insert(
            conn_id,
            ConnHandle {
                id: conn_id,
                addr,
                tx: tx.clone(),
            },
        );
        broker
            .conn_state
            .insert(conn_id, ConnectionState::new());

        info!(conn_id, %addr, "connected");

        let (reader, writer) = stream.into_split();
        tokio::spawn(writer_task(conn_id, writer, rx));
        reader_task(conn_id, reader, tx, broker.clone()).await;

        broker.remove_connection(conn_id);
        info!(conn_id, "disconnected");
    });
}

async fn writer_task(conn_id: u64, writer: OwnedWriteHalf, mut rx: mpsc::Receiver<Frame>) {
    let mut writer = BufWriter::new(writer);
    while let Some(frame) = rx.recv().await {
        let header_bytes = frame.header.to_bytes();
        if writer.write_all(&header_bytes).await.is_err() {
            break;
        }
        if !frame.payload.is_empty() && writer.write_all(&frame.payload).await.is_err() {
            break;
        }
        if writer.flush().await.is_err() {
            break;
        }
    }
    debug!(conn_id, "writer closed");
}

async fn reader_task(
    conn_id: u64,
    reader: OwnedReadHalf,
    tx: mpsc::Sender<Frame>,
    broker: Broker,
) {
    let mut reader = BufReader::new(reader);
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut last_activity = Instant::now();

    loop {
        tokio::select! {
            result = read_frame(&mut reader) => {
                match result {
                    Ok((header, payload)) => {
                        last_activity = Instant::now();
                        handler::dispatch(conn_id, &tx, &broker, &header, &payload).await;
                    }
                    Err(_) => break,
                }
            }
            _ = heartbeat.tick() => {
                if last_activity.elapsed() > HEARTBEAT_TIMEOUT {
                    warn!(conn_id, "heartbeat timeout");
                    break;
                }
                if tx.send(Frame::empty(crate::core::protocol::Event::Heartbeat)).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn read_frame(reader: &mut BufReader<OwnedReadHalf>) -> Result<(Header, Vec<u8>), Error> {
    let mut buf = [0u8; HEADER_SIZE];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|_| Error::Disconnected)?;

    let header = Header::from_bytes(&buf)?;
    if header.bodylen as usize > MAX_BODY {
        return Err(Error::BadPayload);
    }

    let payload = if header.bodylen > 0 {
        let mut body = vec![0u8; header.bodylen as usize];
        reader.read_exact(&mut body).await?;
        body
    } else {
        Vec::new()
    };

    Ok((header, payload))
}
