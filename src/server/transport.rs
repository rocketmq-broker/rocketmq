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

    // TODO: too nested code
    if let Some(acceptor) = tls_acceptor {
        info!("{} (TLS) on {}", protocol_name, addr);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, addr)) => {
                        let acceptor_clone = acceptor.clone();
                        let broker_clone = broker.clone();
                        let adapter_clone = adapter.clone();
                        tokio::spawn(async move {
                            match acceptor_clone.accept(tcp_stream).await {
                                Ok(tls_stream) => {
                                    info!(%addr, "TLS handshake complete");
                                    let boxed = Box::new(tls_stream);
                                    adapter_clone.handle_stream(boxed, addr, broker_clone);
                                }
                                Err(e) => {
                                    warn!(%addr, error = %e, "TLS handshake failed");
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to accept TLS connection");
                    }
                }
            }
        });
    } else {
        info!("{} on {}", protocol_name, addr);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let boxed = Box::new(stream);
                        adapter.clone().handle_stream(boxed, addr, broker.clone());
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to accept connection");
                    }
                }
            }
        });
    }

    Ok(())
}
