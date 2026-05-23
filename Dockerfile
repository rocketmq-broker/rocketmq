# Multi-stage Dockerfile for Rust broker
FROM rust:1.82-slim AS builder

WORKDIR /usr/src/rocketmq

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy src/main.rs to build dependencies and cache them
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy real source code
COPY src ./src

# Rebuild real application
RUN touch src/main.rs && cargo build --release

# Runner stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies (like OpenSSL / CA-certificates if TLS is used)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled binary from builder
COPY --from=builder /usr/src/rocketmq/target/release/rocketmq /usr/bin/rocketmq

# Copy built-in management UI static assets
COPY --from=builder /usr/src/rocketmq/src/management/www /app/src/management/www

# Create data directory
RUN mkdir -p data

# Default ports: AMQP 5672, AMQPS 5671, Mgmt 15672, Cluster 5680
EXPOSE 5672 5671 15672 5680

ENTRYPOINT ["/usr/bin/rocketmq"]
