# Protocol — Implementation Plan

> Covers 2 partial and 16 missing protocol features.

## Phase 1 — Wire Protocol Maturity (Sprint 12)

### 1.1 Channel Scoping 🟡→✅
- All handlers extract `header.channel_id` and look up `ChannelState`
- Scope `prefetch`, `confirm_mode`, `unacked_count` per channel
- Channel 0 = control channel (heartbeats, connection.close)

### 1.2 Client Identification 🟡→✅
- Add `ConnectionStart/StartOk` handshake events (`0x40`/`0x41`)
- Client sends `client_name`, `product`, `version`
- Store in `ConnHandle` for logging

### 1.3 Protocol/Version/Capability Negotiation ❌
- Handshake: client sends `AMQP\x00\x01\x00\x00`, server replies with `ConnectionStart`
- `ConnectionTune` (`0x42`/`0x43`) negotiates `channel_max`, `frame_max`, `heartbeat`
- Capabilities advertised as comma-separated list, only mutual features activate

## Phase 2 — Security (Sprint 13)

### 2.1 Authentication ❌
- SASL PLAIN mechanism in `ConnectionStartOk.response`
- `auth.rs` with `UserStore` backed by `data/users.toml`
- Failed auth → `ConnectionClose` with `ACCESS_REFUSED`

### 2.2 Authorization ❌
- Per-user permissions: `configure`, `write`, `read` (regex on resource names)
- Checked in `assert_queue`, `publish`, `listen`, `bind`

### 2.3 TLS/SSL ❌
- `tokio-rustls` dependency, port 8443 for TLS
- Config: `data/tls/cert.pem` + `key.pem`
- TS client: `new RocketMQ({ tls: true })`

### 2.4 SASL ❌
- Mechanisms: `PLAIN` (over TLS), `EXTERNAL` (client cert), future `SCRAM-SHA-256`

## Phase 3 — Advanced Protocol (Sprint 14)

### 3.1 Compression ❌
- Negotiate in capabilities: `compression:zstd,lz4,none`
- Header field `compression: u8`, only compress payloads > 256B
- `zstd` crate dependency

### 3.2 Chunked Bodies / Large Message Streaming ❌
- `flags: u8` in header, bit 0 = `MORE_FRAMES`
- Split payloads > `frame_max`, reassemble on receiver

### 3.3 Fragmentation / Reassembly ❌
- `reassembly_buffer` per connection, append chunks until complete

### 3.4 Partial Frame Handling ❌
- Streaming parser with `ReadState::Header` / `ReadState::Body` enum
- Replace `read_exact` with incremental reads

### 3.5 Connection Recovery ❌ (client)
- Auto-reconnect with exponential backoff (1s → 30s cap)
- Track subscriptions/assertions for re-establishment on reconnect

### 3.6 Session Recovery ❌
- Server assigns `session_id` during handshake
- On reconnect, client sends `session_id`, server restores state
- Unacked messages redelivered with `redelivered: true`

### 3.7 Protocol Extensions ❌
- `x-` prefixed capabilities, unknown ones ignored

### 3.8 Schema Evolution ❌
- `flags: u8` in header for forward compat, unknown flags ignored
- Unknown events → warn + skip frame

## New Events

| Event | Code | Phase |
|---|---|---|
| `ChannelFlow/FlowOk` | `0x2E`/`0x2F` | 1 |
| `ConnectionStart/StartOk` | `0x40`/`0x41` | 1 |
| `ConnectionTune/TuneOk` | `0x42`/`0x43` | 1 |
| `ConnectionOpen/OpenOk` | `0x44`/`0x45` | 1 |
| `ConnectionClose/CloseOk` | `0x46`/`0x47` | 1 |
