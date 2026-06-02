# Multi-stage Dockerfile for Rust broker using native cross-compilation
# Builder and runtime both use bookworm to ensure glibc compatibility.
FROM --platform=$BUILDPLATFORM rust:1.95-slim-bookworm AS builder

# Re-declare build platforms/targets
ARG BUILDARCH
ARG TARGETARCH

WORKDIR /usr/src/rocketmq

# Install cross-compilation toolchains if building for a foreign architecture
RUN apt-get update && \
    if [ "$TARGETARCH" = "arm64" ] && [ "$BUILDARCH" != "arm64" ]; then \
    apt-get install -y gcc-aarch64-linux-gnu libc6-dev-arm64-cross; \
    elif [ "$TARGETARCH" = "amd64" ] && [ "$BUILDARCH" != "amd64" ]; then \
    apt-get install -y gcc-x86-64-linux-gnu libc6-dev-amd64-cross; \
    fi && \
    rm -rf /var/lib/apt/lists/*

# Add Rust targets
RUN if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
    case "$TARGETARCH" in \
    "amd64") TARGET_TRIPLE="x86_64-unknown-linux-gnu" ;; \
    "arm64") TARGET_TRIPLE="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported target architecture: $TARGETARCH"; exit 1 ;; \
    esac && \
    rustup target add "$TARGET_TRIPLE"; \
    fi

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy src/main.rs to build dependencies and cache them
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    if [ "$TARGETARCH" = "$BUILDARCH" ]; then \
    cargo build --release; \
    else \
    case "$TARGETARCH" in \
    "amd64") \
    TARGET_TRIPLE="x86_64-unknown-linux-gnu" \
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    ;; \
    "arm64") \
    TARGET_TRIPLE="aarch64-unknown-linux-gnu" \
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    ;; \
    esac && \
    cargo build --release --target "$TARGET_TRIPLE"; \
    fi && \
    rm -rf src

# Copy real source code
COPY src ./src

# Rebuild real application
RUN touch src/main.rs && \
    if [ "$TARGETARCH" = "$BUILDARCH" ]; then \
        cargo build --release; \
    else \
        case "$TARGETARCH" in \
            "amd64") \
                TARGET_TRIPLE="x86_64-unknown-linux-gnu" \
                export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
                ;; \
            "arm64") \
                TARGET_TRIPLE="aarch64-unknown-linux-gnu" \
                export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
                ;; \
        esac && \
        cargo build --release --target "$TARGET_TRIPLE" && \
        mkdir -p target/release && \
        cp target/"$TARGET_TRIPLE"/release/rocketmq target/release/rocketmq; \
    fi && \
    mkdir -p data
# Runner stage
FROM debian:bookworm-slim

WORKDIR /app

# Copy compiled binary from builder
COPY --from=builder /usr/src/rocketmq/target/release/rocketmq /usr/bin/rocketmq

# Copy built-in management UI static assets
COPY --from=builder /usr/src/rocketmq/src/management/www /app/src/management/www

# Copy empty data directory created in builder
COPY --from=builder /usr/src/rocketmq/data /app/data

# Bind to all interfaces so Docker port forwarding works
ENV ROCKETMQ_BIND_HOST=0.0.0.0

# Default ports: AMQP 5672, AMQPS 5671, Mgmt 15672, Cluster 5680
EXPOSE 5672 5671 15672 5680

ENTRYPOINT ["/usr/bin/rocketmq"]
