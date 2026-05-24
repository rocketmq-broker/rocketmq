# RocketMQ

A lightweight, high-performance message broker written from scratch in Rust, implementing the **AMQP 0-9-1** protocol. RocketMQ is fully wire-compatible with RabbitMQ, allowing existing AMQP clients (such as `amqplib`, `pika`, etc.) to connect seamlessly.

---

## ⚡ Core Features

- **Protocol Negotiation**: Full AMQP connection and channel handshake lifecycle.
- **Exchange Routing**: Direct, Fanout, Topic (wildcard `*` and `#`), and Headers exchanges.
- **Queue Engine**: Durability, exclusivity, message TTL, and priority-level queues.
- **Guarantees**: Publisher confirms, QoS prefetch limits, and full transaction (`Tx`) blocks.
- **Storage**: Segmented Write-Ahead Log (WAL) with CRC32 integrity checks for crash recovery.
- **TLS Security**: AMQPS support on port `5671` via `tokio-rustls`.
- **Management UI**: Native RabbitMQ Management Dashboard compatibility on port `15672`.

---

## 🚀 Getting Started

Ensure you have the Rust toolchain installed (2024 edition).

### 1. Build and Run
```bash
cargo build --release
cargo run --release
```

### 2. Default Ports
- **AMQP (Plain)**: `127.0.0.1:5672`
- **AMQPS (TLS)**: `127.0.0.1:5671`
- **Management HTTP API**: `127.0.0.1:15672` (default credentials: `guest` / `guest`)

---

## ⚙️ Configuration

Tune the broker dynamically via environment variables:

- `ROCKETMQ_NODE_ID` — Node identifier (default: `1`)
- `ROCKETMQ_BIND_HOST` — Network bind address (default: `127.0.0.1`)
- `ROCKETMQ_AMQP_PORT` — AMQP port (default: `5672`)
- `ROCKETMQ_AMQPS_PORT` — AMQPS port (default: `5671`)
- `ROCKETMQ_MGMT_PORT` — Management UI port (default: `15672`)
- `ROCKETMQ_DATA_DIR` — Path to WAL and user database directory (default: `data`)

---

## 🧪 Testing

Execute the comprehensive unit and integration test suite:

```bash
cargo test
```

---

## 📄 License

Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
