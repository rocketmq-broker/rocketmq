# RocketMQ

A lightweight, high-performance message broker written from scratch in Rust, implementing the **AMQP 0-9-1** protocol. RocketMQ is fully wire-compatible with RabbitMQ, allowing existing AMQP clients (such as `amqplib`, `pika`, etc.) to connect seamlessly.

---

## ⚡ Features

- **AMQP 0-9-1 Compliance** — Direct, Fanout, Topic (wildcard `*` and `#`), and Headers exchanges.
- **Robust Queue Engine** — Supports durability, exclusivity, message TTL, and priority levels.
- **Guaranteed Delivery** — Publisher confirms, QoS prefetch limits, and full transactions (`Tx` blocks).
- **Resilient Storage** — Segmented Write-Ahead Log (WAL) with CRC32 integrity checks for crash recovery.
- **Secure by Default** — Built-in TLS support via `tokio-rustls`.
- **Management UI** — Native RabbitMQ Management Dashboard compatibility.
- **Observability** — OpenTelemetry integration with a Prometheus exporter for metrics.

---

## 🚀 Getting Started

### Prerequisites

Ensure you have the Rust toolchain installed (Rust 2024 edition).

### 1. Build and Run

```bash
# Build and run the broker in release mode
cargo run --release
```

### 2. Default Ports

* **AMQP (Plain)**: `127.0.0.1:5672`
* **AMQPS (TLS)**: `127.0.0.1:5671`
* **Management UI**: `127.0.0.1:15672` (default: `guest` / `guest`)

---

## ⚙️ Configuration

RocketMQ can be configured via environment variables or a `rocketmq.conf` file:

| Variable | Config Key | Default | Description |
|----------|------------|---------|-------------|
| `ROCKETMQ_NODE_ID` | `node_id` | `1` | Node identifier |
| `ROCKETMQ_BIND_HOST` | `bind_host` | `127.0.0.1` | Network bind address |
| `ROCKETMQ_AMQP_PORT` | `amqp_port` | `5672` | AMQP port |
| `ROCKETMQ_AMQPS_PORT` | `amqps_port` | `5671` | AMQPS port |
| `ROCKETMQ_MGMT_PORT` | `mgmt_port` | `15672` | Management UI port |
| `ROCKETMQ_DATA_DIR` | `data_dir` | `data` | WAL and user database directory |
| `ROCKETMQ_CLUSTER_ENABLED` | `cluster_enabled` | `false` | Enable cluster mode |
| `ROCKETMQ_CLUSTER_ADDR` | `cluster_addr` | `127.0.0.1:5680` | Cluster listen address |
| `ROCKETMQ_CLUSTER_SEEDS` | `cluster_seeds` | (empty) | Comma-separated peer addresses |

---

## 🧪 Testing

Run the full suite of unit and integration tests:

```bash
cargo test
```

---

## 📄 License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
