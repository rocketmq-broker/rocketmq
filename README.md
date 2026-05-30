# RocketMQ

A lightweight, high-performance message broker written from scratch in Rust, implementing the **AMQP 0-9-1** protocol. RocketMQ is fully wire-compatible with RabbitMQ, allowing existing AMQP clients (such as `amqplib`, `pika`, etc.) to connect seamlessly.

---

## Core Features

- **Protocol Negotiation**: Full AMQP connection and channel handshake lifecycle.
- **Exchange Routing**: Direct, Fanout, Topic (wildcard `*` and `#`), and Headers exchanges.
- **Queue Engine**: Durability, exclusivity, message TTL, and priority-level queues.
- **Guarantees**: Publisher confirms, QoS prefetch limits, and full transaction (`Tx`) blocks.
- **Storage**: Segmented Write-Ahead Log (WAL) with CRC32 integrity checks for crash recovery.
- **TLS Security**: AMQPS support on port `5671` via `tokio-rustls`.
- **Management UI**: Native RabbitMQ Management Dashboard compatibility on port `15672`.
- **Built-in Schema Registry**: Confluent-compatible schema registry with versioning, compatibility enforcement, and wire-format validation — no external service required.
- **OpenTelemetry Metrics**: Full OTel instrumentation with Prometheus exporter for AMQP operations, system resources, and schema registry activity.
- **Cross-platform**: Runs on Linux, macOS, and Windows.

---

## Built-in Schema Registry

RocketMQ ships a **native, Confluent-compatible Schema Registry** directly inside the broker. Schemas are versioned, compatibility-checked, and enforced at publish time — zero external dependencies.

### How it works

1. **Register a schema** via the REST API:
```bash
curl -X POST http://localhost:15672/api/schemas/subjects/orders-value/versions \
  -H 'Content-Type: application/json' \
  -d '{
    "schema": "syntax = \"proto3\"; message Order { string id = 1; int32 qty = 2; }",
    "schemaType": "PROTOBUF",
    "messageName": "Order"
  }'
# → {"id": 1}
```

2. **Declare a queue** with `x-schema-subject`:
```python
channel.queue_declare("orders", arguments={"x-schema-subject": "orders-value"})
```

3. **Publish with wire prefix** — producers prepend `[0x00, schema_id_be32]` to the protobuf payload. The broker validates the prefix, resolves the schema, verifies subject ownership, and validates the message before accepting it.

### Compatibility Modes

| Mode | Behaviour |
|------|-----------|
| `BACKWARD` (default) | New schema can read data written by the previous version |
| `FORWARD` | Previous schema can read data written by the new version |
| `FULL` | Both backward and forward compatible |
| `NONE` | No compatibility check |

### REST API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/schemas/subjects` | List all subjects |
| `POST` | `/api/schemas/subjects/:subject/versions` | Register a new schema version |
| `GET` | `/api/schemas/subjects/:subject/versions` | List versions for a subject |
| `GET` | `/api/schemas/subjects/:subject/versions/:version` | Get a specific version |
| `GET` | `/api/schemas/subjects/:subject/versions/latest` | Get the latest version |
| `GET` | `/api/schemas/ids/:id` | Lookup schema by global ID |
| `DELETE` | `/api/schemas/subjects/:subject` | Soft-delete all versions |
| `DELETE` | `/api/schemas/subjects/:subject/versions/:version` | Soft-delete a specific version |
| `GET` | `/api/schemas/config` | Get global compatibility level |
| `PUT` | `/api/schemas/config` | Set global compatibility level |
| `GET` | `/api/schemas/config/:subject` | Get subject-level compatibility |
| `PUT` | `/api/schemas/config/:subject` | Set subject-level compatibility |

---

## Getting Started

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

## Configuration

Tune the broker via environment variables or `rocketmq.conf`:

| Variable | Conf Key | Default | Description |
|----------|----------|---------|-------------|
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

## Testing

Execute the comprehensive unit and integration test suite:

```bash
cargo test
```

---

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
