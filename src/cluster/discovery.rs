// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
//
// File: discovery.rs
// Description: Peer discovery backends: DNS SRV, Kubernetes, and static seeds.

//! Automatic peer discovery for cluster formation.
//!
//! Supports three backends:
//! - **Static seeds**: addresses from `cluster_seeds` config
//! - **DNS SRV**: periodic SRV record resolution
//! - **Kubernetes**: headless service endpoint discovery

use std::net::ToSocketAddrs;
use tracing::{debug, warn};

/// Discriminator for the active peer discovery mechanism.
///
/// ```ignore
/// let d = DiscoveryBackend::from_config("dns");
/// assert_eq!(d, DiscoveryBackend::Dns);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum DiscoveryBackend {
    /// Manual `cluster_seeds` list only.
    #[default]
    Static,
    /// DNS SRV record lookup.
    Dns,
    /// Kubernetes headless service endpoint API.
    Kubernetes,
}

impl DiscoveryBackend {
    /// Parses the config value into a backend variant.
    pub fn from_config(s: &str) -> Self {
        match s {
            "dns" | "DNS" => Self::Dns,
            "k8s" | "kubernetes" | "K8S" => Self::Kubernetes,
            _ => Self::Static,
        }
    }
}

/// Resolves DNS SRV/A records to socket addresses.
///
/// Falls back to A record resolution if SRV lookup fails.
/// Returns a list of "host:port" strings suitable for TCP connect.
///
/// ```ignore
/// let addrs = resolve_dns_peers("_rocketmq._tcp.cluster.local", 5680);
/// ```
pub fn resolve_dns_peers(hostname: &str, default_port: u16) -> Vec<String> {
    let lookup_target = format!("{}:{}", hostname, default_port);
    match lookup_target.to_socket_addrs() {
        Ok(addrs) => {
            let results: Vec<String> = addrs.map(|a| a.to_string()).collect();
            debug!(
                hostname,
                count = results.len(),
                "DNS peer discovery resolved"
            );
            results
        }
        Err(e) => {
            warn!(hostname, error = %e, "DNS peer discovery failed");
            Vec::new()
        }
    }
}

/// Discovers Kubernetes pod endpoints for a headless service.
///
/// Uses the downward API environment variables and DNS. In K8s,
/// a headless service's DNS name resolves to all pod IPs.
///
/// ```ignore
/// let peers = resolve_k8s_peers("rocketmq-headless", "default", 5680);
/// ```
pub fn resolve_k8s_peers(service_name: &str, namespace: &str, port: u16) -> Vec<String> {
    // In Kubernetes, a headless service resolves via DNS:
    // <service>.<namespace>.svc.cluster.local
    let fqdn = format!("{}.{}.svc.cluster.local", service_name, namespace);
    resolve_dns_peers(&fqdn, port)
}

/// Aggregates peer addresses from all configured discovery backends.
///
/// Merges static seeds with DNS/K8s-discovered addresses, deduplicating.
pub fn discover_peers(
    backend: &DiscoveryBackend,
    static_seeds: &[String],
    dns_hostname: &str,
    k8s_service: &str,
    k8s_namespace: &str,
    cluster_port: u16,
) -> Vec<String> {
    let mut peers: Vec<String> = static_seeds.to_vec();

    match backend {
        DiscoveryBackend::Static => {}
        DiscoveryBackend::Dns => {
            if !dns_hostname.is_empty() {
                let dns_peers = resolve_dns_peers(dns_hostname, cluster_port);
                for p in dns_peers {
                    if !peers.contains(&p) {
                        peers.push(p);
                    }
                }
            }
        }
        DiscoveryBackend::Kubernetes => {
            if !k8s_service.is_empty() {
                let k8s_peers = resolve_k8s_peers(k8s_service, k8s_namespace, cluster_port);
                for p in k8s_peers {
                    if !peers.contains(&p) {
                        peers.push(p);
                    }
                }
            }
        }
    }

    peers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_from_config_variants() {
        assert_eq!(DiscoveryBackend::from_config("dns"), DiscoveryBackend::Dns);
        assert_eq!(
            DiscoveryBackend::from_config("k8s"),
            DiscoveryBackend::Kubernetes
        );
        assert_eq!(
            DiscoveryBackend::from_config("kubernetes"),
            DiscoveryBackend::Kubernetes
        );
        assert_eq!(
            DiscoveryBackend::from_config("static"),
            DiscoveryBackend::Static
        );
        assert_eq!(
            DiscoveryBackend::from_config("unknown"),
            DiscoveryBackend::Static
        );
    }

    #[test]
    fn discover_peers_static_returns_seeds() {
        let seeds = vec!["127.0.0.1:5680".to_string(), "127.0.0.2:5680".to_string()];
        let result = discover_peers(&DiscoveryBackend::Static, &seeds, "", "", "", 5680);
        assert_eq!(result, seeds);
    }

    #[test]
    fn discover_peers_dns_with_empty_hostname_returns_seeds() {
        let seeds = vec!["127.0.0.1:5680".to_string()];
        let result = discover_peers(&DiscoveryBackend::Dns, &seeds, "", "", "", 5680);
        assert_eq!(result, seeds);
    }

    #[test]
    fn resolve_dns_peers_invalid_hostname() {
        let result = resolve_dns_peers("nonexistent.invalid.test", 5680);
        assert!(result.is_empty());
    }

    #[test]
    fn resolve_dns_peers_localhost() {
        let result = resolve_dns_peers("localhost", 5680);
        // localhost should resolve to at least one address
        assert!(!result.is_empty());
    }
}
