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
//
// Author: Edilson Pateguana
// Year: 2026
// File: main.rs
// Description: Main entry point for the RocketMQ AMQP broker application.

#![allow(clippy::too_many_arguments, clippy::field_reassign_with_default)]

#[allow(dead_code)]
mod auth;
#[allow(dead_code)]
mod cluster;
#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod core;
#[allow(dead_code)]
mod management;
#[allow(dead_code)]
mod metrics;
#[allow(dead_code)]
mod queue;
#[allow(dead_code)]
mod routing;
#[allow(dead_code)]
mod server;
#[allow(dead_code)]
mod state;
#[allow(dead_code)]
mod storage;

use std::sync::Arc;
use tracing::{info, warn};

use config::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_log_filter().parse().unwrap()),
        )
        .init();

    let _meter_provider = metrics::init_meter_provider();

    let broker: state::Broker = Arc::new(state::BrokerState::new());

    let wal = storage::init_with_recovery(&broker)?;

    broker.set_wal(wal);

    let node_id = get_node_id();
    let cluster_addr = get_cluster_addr();
    let seed_nodes = get_cluster_seeds();

    info!(
        node_id,
        cluster_addr,
        ?seed_nodes,
        "initializing cluster management"
    );

    let cluster_manager = Arc::new(cluster::ClusterManager::new(node_id, cluster_addr.clone()));
    broker.set_cluster(cluster_manager.clone());

    cluster::start_cluster_listener(broker.clone(), cluster_manager.clone(), cluster_addr).await?;
    cluster::start_peer_connector(broker.clone(), cluster_manager.clone(), seed_nodes).await;

    server::tasks::spawn_all(broker.clone());

    server::amqp_delivery::spawn_delivery_task(broker.clone());

    let mgmt_broker = broker.clone();
    tokio::spawn(async move {
        if let Err(e) = management::serve(mgmt_broker).await {
            tracing::error!(error = %e, "Management HTTP API failed");
        }
    });

    let amqp_addr = get_amqp_listen_addr();
    let amqp_listener = tokio::net::TcpListener::bind(&amqp_addr).await?;
    info!("AMQP 0-9-1 on {}", amqp_addr);

    let amqps_addr = get_amqps_listen_addr();
    let tls_acceptor =
        match server::tls::build_tls_acceptor(&get_tls_cert_path(), &get_tls_key_path()) {
            Ok(acc) => {
                let amqps_listener = tokio::net::TcpListener::bind(&amqps_addr).await?;
                info!("AMQPS (TLS) on {}", amqps_addr);
                Some((amqps_listener, acc))
            }
            Err(e) => {
                warn!(error = %e, "TLS setup failed — AMQPS disabled, plain AMQP only");
                None
            }
        };

    let plain_broker = broker.clone();
    tokio::spawn(async move {
        loop {
            match amqp_listener.accept().await {
                Ok((stream, addr)) => {
                    server::amqp_loop::spawn_amqp(stream, addr, plain_broker.clone());
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to accept plain AMQP connection");
                }
            }
        }
    });

    if let Some((tls_listener, acceptor)) = tls_acceptor {
        tokio::spawn(handle_tls_accept(tls_listener, acceptor, broker.clone()));
    }

    std::future::pending::<()>().await;
    Ok(())
}

async fn handle_tls_accept(
    tls_listener: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    broker: Arc<state::broker::BrokerState>,
) {
    loop {
        match tls_listener.accept().await {
            Ok((tcp_stream, addr)) => {
                let acceptor = acceptor.clone();
                let broker = broker.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_tls_handshake(tcp_stream, addr, acceptor, broker).await {
                        warn!(%addr, error = %e, "TLS handshake failed");
                    }
                });
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to accept TLS AMQPS connection");
            }
        }
    }
}

async fn handle_tls_handshake(
    tcp_stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    acceptor: tokio_rustls::TlsAcceptor,
    broker: Arc<state::broker::BrokerState>,
) -> Result<(), std::io::Error> {
    let tls_stream = acceptor.accept(tcp_stream).await?;
    info!(%addr, "TLS handshake complete");
    let boxed: Box<dyn server::AsyncStream> = Box::new(tls_stream);
    server::amqp_loop::spawn_amqp_on_stream(boxed, addr, broker);
    Ok(())
}
