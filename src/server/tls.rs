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
// File: tls.rs
// Description: Transport Layer Security (TLS) configuration and acceptor builder.

//! TLS support for AMQPS (port 5671).
//!
//! On first run, self-signed certificates are generated and saved to
//! the data directory. For production, replace with real CA-signed certs.

use std::fs;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::{info, warn};

pub fn build_tls_acceptor(
    cert_path: &str,
    key_path: &str,
) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    if !Path::new(cert_path).exists() || !Path::new(key_path).exists() {
        generate_self_signed(cert_path, key_path)?;
    }

    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    info!(cert = cert_path, key = key_path, "TLS configured");
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Loads PEM-encoded X.509 certificates from the given file path.
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, Box<dyn std::error::Error>> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .filter_map(|r| r.ok())
        .collect();
    if certs.is_empty() {
        return Err(format!("no certificates found in {}", path).into());
    }
    info!(count = certs.len(), path, "loaded TLS certificates");
    Ok(certs)
}

/// Loads a PEM-encoded private key from the given file path.
fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);

    // Try PKCS8 first, then RSA, then EC
    for item in rustls_pemfile::read_all(&mut reader) {
        match item {
            Ok(rustls_pemfile::Item::Pkcs8Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Ok(rustls_pemfile::Item::Pkcs1Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Ok(rustls_pemfile::Item::Sec1Key(key)) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            _ => continue,
        }
    }
    Err(format!("no private key found in {}", path).into())
}

/// Generates a self-signed TLS certificate and private key for
/// local development, writing them to the configured data directory.
fn generate_self_signed(cert_path: &str, key_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    warn!("generating self-signed TLS certificate (for development only)");

    let mut params =
        rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;

    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String("RocketMQ Dev".to_string()),
    );
    params.distinguished_name.push(
        rcgen::DnType::OrganizationName,
        rcgen::DnValue::Utf8String("RocketMQ".to_string()),
    );

    // Valid for 365 days
    params.not_after = rcgen::date_time_ymd(2027, 12, 31);

    // Add SAN for IP addresses
    params.subject_alt_names = vec![
        rcgen::SanType::DnsName("localhost".try_into()?),
        rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))),
    ];

    let key_pair = rcgen::KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    // Ensure data directory exists
    if let Some(parent) = Path::new(cert_path).parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(cert_path, cert.pem())?;
    fs::write(key_path, key_pair.serialize_pem())?;

    info!(
        cert = cert_path,
        key = key_path,
        "self-signed certificate generated"
    );
    Ok(())
}
