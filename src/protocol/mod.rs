// Copyright (c) 2026 Edilson Pateguana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Protocol abstraction layer.

use std::sync::Arc;

pub struct ChannelMeta {
    pub id: u16,
    pub prefetch_count: u16,
    pub unacked_count: u16,
    pub confirm_mode: bool,
    pub flow_active: bool,
}

pub trait ConnectionMeta: Send + Sync {
    fn username(&self) -> String;
    fn vhost(&self) -> String;
    fn channels_count(&self) -> usize;
    fn get_channels(&self) -> Vec<ChannelMeta>;
    fn heartbeat(&self) -> u16;
    fn frame_max(&self) -> u32;
    fn channel_max(&self) -> u16;
    fn tx_mode(&self) -> bool;
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub mod amqp;

/// Defines behavioral capabilities for async stream.
pub trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncStream for T {}

/// Trait representing a generic delivery sink for sending frames.
pub trait DeliverySink: Send + Sync + 'static {
    /// Attempt to deliver the given frames payload. Returns true if successful.
    fn try_deliver(&self, frames: Vec<u8>) -> bool;
    /// Check if the sink is closed.
    fn is_closed(&self) -> bool;
}

/// Concrete Mpsc channel delivery sink.
pub struct MpscDeliverySink {
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl MpscDeliverySink {
    /// Create a new MpscDeliverySink wrapping a tokio mpsc Sender.
    pub fn new(tx: tokio::sync::mpsc::Sender<Vec<u8>>) -> Self {
        Self { tx }
    }
}

impl DeliverySink for MpscDeliverySink {
    fn try_deliver(&self, frames: Vec<u8>) -> bool {
        self.tx.try_send(frames).is_ok()
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// A pluggable protocol adapter via enum dispatch.
#[derive(Clone)]
pub enum ProtocolAdapter {
    Amqp(crate::protocol::amqp::AmqpProtocol),
}

impl ProtocolAdapter {
    pub fn create_amqp() -> Self {
        ProtocolAdapter::Amqp(crate::protocol::amqp::AmqpProtocol::new())
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Amqp(p) => p.name(),
        }
    }

    pub async fn start(
        self,
        broker: Arc<crate::state::BrokerState>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Amqp(p) => p.start(broker).await,
        }
    }

    pub fn handle_stream(
        self,
        stream: Box<dyn crate::protocol::AsyncStream>,
        addr: std::net::SocketAddr,
        broker: Arc<crate::state::BrokerState>,
    ) {
        match self {
            Self::Amqp(p) => p.handle_stream(stream, addr, broker),
        }
    }
}

/// Initializes and starts all configured protocol adapters (e.g., AMQP, MQTT).
pub async fn start_adapters(
    broker: std::sync::Arc<crate::state::BrokerState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let amqp_adapter = ProtocolAdapter::create_amqp();

    // Start background protocol tasks
    amqp_adapter.clone().start(broker.clone()).await?;

    // Setup plain TCP AMQP
    let amqp_addr = crate::config::get_amqp_listen_addr();
    crate::server::transport::spawn_listener(
        &amqp_addr,
        None,
        amqp_adapter.clone(),
        broker.clone(),
    )
    .await?;

    // Setup TLS AMQPS
    let amqps_addr = crate::config::get_amqps_listen_addr();
    let tls_acceptor = match crate::server::tls::build_tls_acceptor(
        &crate::config::get_tls_cert_path(),
        &crate::config::get_tls_key_path(),
    ) {
        Ok(acc) => Some(acc),
        Err(e) => {
            tracing::warn!(error = %e, "TLS setup failed — AMQPS disabled, plain AMQP only");
            None
        }
    };

    if tls_acceptor.is_some() {
        crate::server::transport::spawn_listener(
            &amqps_addr,
            tls_acceptor,
            amqp_adapter,
            broker.clone(),
        )
        .await?;
    }

    Ok(())
}
