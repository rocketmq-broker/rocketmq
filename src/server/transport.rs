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

use crate::protocol::ProtocolAdapter;
use crate::state::BrokerState;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

pub async fn spawn_listener(
    addr: &str,
    tls_acceptor: Option<TlsAcceptor>,
    adapter: ProtocolAdapter,
    broker: Arc<BrokerState>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let protocol_name = adapter.name();

    if let Some(acceptor) = tls_acceptor {
        info!("{} (TLS) on {}", protocol_name, addr);
        spawn_tls_accept_loop(listener, acceptor, adapter, broker);
    } else {
        info!("{} on {}", protocol_name, addr);
        spawn_plain_accept_loop(listener, adapter, broker);
    }

    Ok(())
}

fn spawn_tls_accept_loop(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    adapter: ProtocolAdapter,
    broker: Arc<BrokerState>,
) {
    tokio::spawn(async move {
        loop {
            let (tcp_stream, addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to accept TLS connection");
                    continue;
                }
            };
            let acceptor = acceptor.clone();
            let broker = broker.clone();
            let adapter = adapter.clone();
            tokio::spawn(async move {
                match acceptor.accept(tcp_stream).await {
                    Ok(tls_stream) => {
                        info!(%addr, "TLS handshake complete");
                        adapter.handle_stream(Box::new(tls_stream), addr, broker);
                    }
                    Err(e) => warn!(%addr, error = %e, "TLS handshake failed"),
                }
            });
        }
    });
}

fn spawn_plain_accept_loop(
    listener: TcpListener,
    adapter: ProtocolAdapter,
    broker: Arc<BrokerState>,
) {
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    adapter
                        .clone()
                        .handle_stream(Box::new(stream), addr, broker.clone());
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to accept connection");
                }
            }
        }
    });
}
