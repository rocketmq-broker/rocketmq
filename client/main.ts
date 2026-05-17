/**
 * rocketmq://admin:pass@127.0.0.1:8080
 *
 * rocketmq Protocol specification
 *
 * Fixed Header (13 bytes):
 * | 2 Bytes | magic ([82, 81] = "RQ")
 * | 1 Byte  | version
 * | 1 Byte  | tag
 * | 1 Byte  | event
 * | 4 Bytes | body length  (big-endian u32) — total bytes after fixed header (dynamic headers + body content)
 * | 4 Bytes | body offset  (big-endian u32) — bytes of inline headers; content starts at this offset
 *
 * Payload (bodylen bytes):
 * | bodyoff Bytes           | inline headers (key:value pairs)
 * | bodylen - bodyoff Bytes | actual body content
 */

// ── Constants ───────────────────────────────────────────────────────────

const ROCKETMQ_VERSION = 1;
const ROCKETMQ_MAGIC: readonly [number, number] = [82, 81] as const;
const ROCKETMQ_HEADER_SIZE = 13;

const RocketmqEvent = {
  Nop: 0x00,
  AssertQueue: 0x01,
  AssertQueueOk: 0x02,
  Listen: 0x03,
  ListenOk: 0x04,
  Publish: 0x05,
  Deliver: 0x06,
  Ack: 0x07,
  Nack: 0x08,
  Heartbeat: 0x09,
} as const;

type RocketmqEventValue = (typeof RocketmqEvent)[keyof typeof RocketmqEvent];

// ── Binary helpers ──────────────────────────────────────────────────────

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function encodeString(value: string): Uint8Array {
  return encoder.encode(value);
}

function decodeString(buf: Uint8Array): string {
  return decoder.decode(buf);
}

/** Allocate a buffer and return both the raw bytes and a DataView over it. */
function allocBuffer(size: number): { bytes: Uint8Array; view: DataView } {
  const bytes = new Uint8Array(size);
  const view = new DataView(bytes.buffer);
  return { bytes, view };
}

/** Write a big-endian u32 at the given byte offset. */
function writeU32BE(view: DataView, offset: number, value: number): void {
  view.setUint32(offset, value, false);
}

/** Read a big-endian u32 at the given byte offset. */
function readU32BE(view: DataView, offset: number): number {
  return view.getUint32(offset, false);
}

/** Write a single u8 at the given byte offset. */
function writeU8(view: DataView, offset: number, value: number): void {
  view.setUint8(offset, value);
}

/** Read a single u8 at the given byte offset. */
function readU8(view: DataView, offset: number): number {
  return view.getUint8(offset);
}

/** Read exactly `n` bytes from a Deno.Conn. Throws on short read / EOF. */
async function readExact(conn: Deno.Conn, n: number): Promise<Uint8Array> {
  const buf = new Uint8Array(n);
  let filled = 0;
  while (filled < n) {
    const nread = await conn.read(buf.subarray(filled));
    if (nread === null) throw new Error("unexpected EOF");
    filled += nread;
  }
  return buf;
}

// ── Protocol ────────────────────────────────────────────────────────────

type HeaderOptions = {
  event: RocketmqEventValue;
  tag?: number;
  headers?: Record<string, string>;
  contentLen: number;
};

type ParsedHeader = {
  magic: [number, number];
  version: number;
  tag: number;
  event: number;
  bodylen: number;
  bodyoff: number;
};

function encodeHeaders(headers: Record<string, string>): Uint8Array {
  const str = Object.entries(headers)
    .map(([k, v]) => `${k}:${v}`)
    .join("\r\n");
  return encodeString(str);
}

/** Build the 13-byte fixed header for sending. */
function makeHeader(opts: HeaderOptions): {
  header: Uint8Array;
  inlineHeaders: Uint8Array | null;
} {
  const inlineHeaders =
    opts.headers && Object.keys(opts.headers).length > 0
      ? encodeHeaders(opts.headers)
      : null;

  const bodyOff = inlineHeaders?.length ?? 0;
  const bodyLen = bodyOff + opts.contentLen;

  const { bytes, view } = allocBuffer(ROCKETMQ_HEADER_SIZE);

  bytes.set(ROCKETMQ_MAGIC);
  writeU8(view, 2, ROCKETMQ_VERSION);
  writeU8(view, 3, opts.tag ?? 0);
  writeU8(view, 4, opts.event);
  writeU32BE(view, 5, bodyLen);
  writeU32BE(view, 9, bodyOff);

  return { header: bytes, inlineHeaders };
}

/** Parse a 13-byte fixed header from raw bytes. */
function parseHeader(buf: Uint8Array): ParsedHeader {
  if (buf.length < ROCKETMQ_HEADER_SIZE) {
    throw new Error(
      `header too short: expected ${ROCKETMQ_HEADER_SIZE}, got ${buf.length}`,
    );
  }

  if (buf[0] !== ROCKETMQ_MAGIC[0] || buf[1] !== ROCKETMQ_MAGIC[1]) {
    throw new Error("invalid magic");
  }

  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

  return {
    magic: [buf[0], buf[1]],
    version: readU8(view, 2),
    tag: readU8(view, 3),
    event: readU8(view, 4),
    bodylen: readU32BE(view, 5),
    bodyoff: readU32BE(view, 9),
  };
}

// ── Types ───────────────────────────────────────────────────────────────

type ConnectionOptions = {
  host: string;
  port: number;
};

// ── Connection ──────────────────────────────────────────────────────────

type RocketmqResponse = {
  header: ParsedHeader;
  body: string;
};

class Connection {
  private readonly conn: Deno.Conn;

  constructor(conn: Deno.Conn) {
    this.conn = conn;
  }

  /** Send the fixed header, optional inline headers, and optional body content. */
  private async send(opts: HeaderOptions, content?: Uint8Array): Promise<void> {
    const { header, inlineHeaders } = makeHeader(opts);

    await this.conn.write(header);
    if (inlineHeaders) await this.conn.write(inlineHeaders);
    if (content) await this.conn.write(content);
  }

  /** Read a response: fixed header + body (skipping dynamic headers via bodyoff). */
  private async recv(): Promise<RocketmqResponse> {
    const headerBuf = await readExact(this.conn, ROCKETMQ_HEADER_SIZE);
    const header = parseHeader(headerBuf);

    let body = "";
    if (header.bodylen > 0) {
      const payload = await readExact(this.conn, header.bodylen);
      const content = payload.subarray(header.bodyoff);
      body = decodeString(content);
    }

    return { header, body };
  }

  async assertQueue(qname: string): Promise<RocketmqResponse> {
    const content = encodeString(qname);
    await this.send(
      { event: RocketmqEvent.AssertQueue, contentLen: content.length },
      content,
    );
    return this.recv();
  }

  async nop(): Promise<void> {
    await this.send({ event: RocketmqEvent.Nop, contentLen: 0 });
  }

  close(): void {
    this.conn.close();
  }
}

async function connect(options: ConnectionOptions): Promise<Connection> {
  const conn = await Deno.connect({
    hostname: options.host,
    port: options.port,
    transport: "tcp",
  });
  return new Connection(conn);
}

// ── Main ────────────────────────────────────────────────────────────────

async function main() {
  const conn = await connect({ host: "localhost", port: 8080 });

  const response = await conn.assertQueue("users_queue");
  console.log("event:", response.header.event);
  console.log("body:", response.body);

  // conn.close();
}

if (import.meta.main) {
  await main();
}
