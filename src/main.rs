#[allow(dead_code)]
mod auth;
#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod core;
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
                .unwrap_or_else(|_| DEFAULT_LOG_FILTER.parse().unwrap()),
        )
        .init();

    let broker: state::Broker = Arc::new(state::BrokerState::new());

    // Initialize WAL and replay any existing entries (crash recovery)
    let wal = storage::init_with_recovery(&broker)?;

    // Store WAL handle in broker for handlers to use
    broker.set_wal(wal);

    // Spawn background maintenance tasks (queue TTL, message TTL, dedup eviction)
    server::tasks::spawn_all(broker.clone());

    // Spawn AMQP delivery pipeline (pushes messages to consumers)
    server::amqp_delivery::spawn_delivery_task(broker.clone());

    // ── Plain AMQP listener (port 5672) ──────────────
    let amqp_listener = tokio::net::TcpListener::bind(AMQP_LISTEN_ADDR).await?;
    info!("AMQP 0-9-1 on {}", AMQP_LISTEN_ADDR);

    // ── AMQPS (TLS) listener (port 5671) ─────────────
    let tls_acceptor = match server::tls::build_tls_acceptor(TLS_CERT_PATH, TLS_KEY_PATH) {
        Ok(acc) => {
            let amqps_listener = tokio::net::TcpListener::bind(AMQPS_LISTEN_ADDR).await?;
            info!("AMQPS (TLS) on {}", AMQPS_LISTEN_ADDR);
            Some((amqps_listener, acc))
        }
        Err(e) => {
            warn!(error = %e, "TLS setup failed — AMQPS disabled, plain AMQP only");
            None
        }
    };

    // ── Accept loop: plain + TLS ─────────────────────
    loop {
        tokio::select! {
            // Plain AMQP connections
            result = amqp_listener.accept() => {
                let (stream, addr) = result?;
                server::amqp_loop::spawn_amqp(stream, addr, broker.clone());
            }

            // TLS AMQP connections (if TLS is configured)
            result = async {
                match &tls_acceptor {
                    Some((listener, _)) => listener.accept().await,
                    None => std::future::pending().await,
                }
            } => {
                let (tcp_stream, addr) = result?;
                if let Some((_, ref acceptor)) = tls_acceptor {
                    let acceptor = acceptor.clone();
                    let broker = broker.clone();
                    tokio::spawn(async move {
                        match acceptor.accept(tcp_stream).await {
                            Ok(tls_stream) => {
                                info!(%addr, "TLS handshake complete");
                                let boxed: Box<dyn server::AsyncStream> = Box::new(tls_stream);
                                server::amqp_loop::spawn_amqp_on_stream(boxed, addr, broker);
                            }
                            Err(e) => {
                                warn!(%addr, error = %e, "TLS handshake failed");
                            }
                        }
                    });
                }
            }
        }
    }
}
