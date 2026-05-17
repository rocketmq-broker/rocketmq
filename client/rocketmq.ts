/**
 * RocketMQ client library for Deno.
 *
 * Wire protocol (14-byte fixed header):
 *   [0..1]   magic "RQ"
 *   [2]      version
 *   [3..4]   channel_id  (BE u16)
 *   [5]      event
 *   [6..9]   bodylen     (BE u32)
 *   [10..13] bodyoff     (BE u32)
 */

const VERSION = 1;
const MAGIC: readonly [number, number] = [82, 81] as const;
const HEADER_SIZE = 14;

export const Event = {
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

  DeclareExchange: 0x10,
  DeclareExchangeOk: 0x11,
  DeleteExchange: 0x12,
  DeleteExchangeOk: 0x13,
  Bind: 0x14,
  BindOk: 0x15,
  Unbind: 0x16,
  UnbindOk: 0x17,

  ChannelOpen: 0x20,
  ChannelOpenOk: 0x21,
  ChannelClose: 0x22,
  ChannelCloseOk: 0x23,

  Qos: 0x28,
  QosOk: 0x29,
  ConfirmSelect: 0x2a,
  ConfirmSelectOk: 0x2b,
  PublishAck: 0x2c,
  PublishNack: 0x2d,
} as const;

export type EventValue = (typeof Event)[keyof typeof Event];

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function encode(s: string): Uint8Array {
  return encoder.encode(s);
}

function decode(buf: Uint8Array): string {
  return decoder.decode(buf);
}

function alloc(n: number): { bytes: Uint8Array; view: DataView } {
  const bytes = new Uint8Array(n);
  return { bytes, view: new DataView(bytes.buffer) };
}

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

// ── Header ──────────────────────────────────────────────────────────────

export type ParsedHeader = {
  event: number;
  channelId: number;
  bodylen: number;
  bodyoff: number;
};

function buildHeader(
  event: EventValue,
  bodylen: number,
  bodyoff: number,
  channelId = 0,
): Uint8Array {
  const { bytes, view } = alloc(HEADER_SIZE);
  bytes.set(MAGIC);
  view.setUint8(2, VERSION);
  view.setUint16(3, channelId, false);
  view.setUint8(5, event);
  view.setUint32(6, bodylen, false);
  view.setUint32(10, bodyoff, false);
  return bytes;
}

function parseHeader(buf: Uint8Array): ParsedHeader {
  if (buf[0] !== MAGIC[0] || buf[1] !== MAGIC[1]) throw new Error("bad magic");
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  return {
    event: view.getUint8(5),
    channelId: view.getUint16(3, false),
    bodylen: view.getUint32(6, false),
    bodyoff: view.getUint32(10, false),
  };
}

// ── Response ────────────────────────────────────────────────────────────

export type Response = {
  header: ParsedHeader;
  payload: Uint8Array;
  body: Uint8Array;
};

// ── Connection ──────────────────────────────────────────────────────────

export interface Message<T = Uint8Array> {
  id: bigint;
  headers: Record<string, string>;
  content: T;
}

export type MessageHandler = (
  msgId: bigint,
  headers: Record<string, string>,
  body: Uint8Array,
) => void;

export class Connection {
  private conn: Deno.Conn;
  private onDeliver: MessageHandler | null = null;
  private _listening = false;

  get listening(): boolean {
    return this._listening;
  }

  set listening(value: boolean) {
    this._listening = value;
  }

  private pendingRequests: Array<(resp: Response) => void> = [];

  constructor(conn: Deno.Conn) {
    this.conn = conn;
    this.listening = true;
    this.readLoop().catch(console.error);
  }

  /** Send a raw frame: header + payload bytes. */
  async sendFrame(
    event: EventValue,
    payload: Uint8Array,
    bodyoff = 0,
  ): Promise<void> {
    const header = buildHeader(event, payload.length, bodyoff);
    // Combine into single write to prevent frame interleaving
    const frame = new Uint8Array(header.length + payload.length);
    frame.set(header);
    if (payload.length > 0) frame.set(payload, header.length);
    await this.conn.write(frame);
  }

  async request(
    event: EventValue,
    payload: Uint8Array,
    bodyoff = 0,
  ): Promise<Response> {
    return new Promise((resolve) => {
      this.pendingRequests.push(resolve);
      this.sendFrame(event, payload, bodyoff).catch(console.error);
    });
  }

  private parseHeaders(raw: Uint8Array): {
    msgId: bigint;
    headers: Record<string, string>;
  } {
    const lines = decode(raw).split("\r\n");
    let msgId = 0n;
    const headers: Record<string, string> = {};

    for (const line of lines) {
      if (!line) continue;
      const idx = line.indexOf(":");
      if (idx === -1) continue;
      const key = line.slice(0, idx);
      const val = line.slice(idx + 1);

      if (key === "id") {
        msgId = BigInt(val);
      } else {
        headers[key] = val;
      }
    }
    return { msgId, headers };
  }

  private readonly frameHandlers: Record<
    number,
    (
      header: ParsedHeader,
      payload: Uint8Array,
      body: Uint8Array,
    ) => void | Promise<void>
  > = (() => {
    const resolve = (_h: ParsedHeader, payload: Uint8Array, body: Uint8Array) => {
      this.pendingRequests.shift()?.({ header: _h, payload, body });
    };

    return {
      [Event.Heartbeat]: async () => {
        await this.sendFrame(Event.Heartbeat, new Uint8Array(0));
      },
      [Event.Deliver]: (header: ParsedHeader, payload: Uint8Array, body: Uint8Array) => {
        if (this.onDeliver) {
          const { msgId, headers } = this.parseHeaders(
            payload.subarray(0, header.bodyoff),
          );
          this.onDeliver(msgId, headers, body);
        }
      },
      [Event.AssertQueueOk]: resolve,
      [Event.ListenOk]: resolve,
      [Event.DeclareExchangeOk]: resolve,
      [Event.DeleteExchangeOk]: resolve,
      [Event.BindOk]: resolve,
      [Event.UnbindOk]: resolve,
      [Event.ChannelOpenOk]: resolve,
      [Event.ChannelCloseOk]: resolve,
      [Event.QosOk]: resolve,
      [Event.ConfirmSelectOk]: resolve,
      [Event.PublishAck]: resolve,
      [Event.PublishNack]: resolve,
    };
  })();

  private async handleFrame(
    header: ParsedHeader,
    payload: Uint8Array,
    body: Uint8Array,
  ): Promise<void> {
    const handler = this.frameHandlers[header.event];
    if (handler) {
      await handler(header, payload, body);
    }
  }

  private async readLoop(): Promise<void> {
    while (this.listening) {
      try {
        const headerBuf = await readExact(this.conn, HEADER_SIZE);
        const header = parseHeader(headerBuf);
        const payload =
          header.bodylen > 0
            ? await readExact(this.conn, header.bodylen)
            : new Uint8Array(0);

        await this.handleFrame(
          header,
          payload,
          payload.subarray(header.bodyoff),
        );
      } catch {
        this.listening = false;
        break;
      }
    }
  }

  onMessage(handler: MessageHandler): void {
    this.onDeliver = handler;
  }

  async createChannel(): Promise<Channel> {
    return new Channel(this);
  }

  close(): void {
    this.listening = false;
    try {
      this.conn.close();
    } catch {
      /* already closed */
    }
  }
}

export type ExchangeType = "direct" | "fanout" | "topic" | "headers";

export interface ExchangeOptions {
  durable?: boolean;
}

export interface QueueOptions {
  durable?: boolean;
  exclusive?: boolean;
  autoDelete?: boolean;
  maxPriority?: number;
  messageTtl?: number;
  maxLength?: number;
  deadLetterExchange?: string;
  deadLetterRoutingKey?: string;
}

export interface PublishOptions {
  headers?: Record<string, string>;
  priority?: number;
  expiration?: number;
}

export class Channel {
  constructor(private conn: Connection) {}

  async assertQueue(name: string, options?: QueueOptions): Promise<void> {
    if (options) {
      let headerStr = `name:${name}\r\n`;
      if (options.durable) headerStr += `durable:true\r\n`;
      if (options.exclusive) headerStr += `exclusive:true\r\n`;
      if (options.autoDelete) headerStr += `auto_delete:true\r\n`;
      if (options.maxPriority) headerStr += `max_priority:${options.maxPriority}\r\n`;
      if (options.messageTtl != null) headerStr += `message_ttl:${options.messageTtl}\r\n`;
      if (options.maxLength != null) headerStr += `max_length:${options.maxLength}\r\n`;
      if (options.deadLetterExchange) headerStr += `x-dead-letter-exchange:${options.deadLetterExchange}\r\n`;
      if (options.deadLetterRoutingKey) headerStr += `x-dead-letter-routing-key:${options.deadLetterRoutingKey}\r\n`;
      await this.conn.request(Event.AssertQueue, encode(headerStr));
    } else {
      await this.conn.request(Event.AssertQueue, encode(name));
    }
  }

  async prefetch(count: number): Promise<void> {
    const body = encode(`prefetch_count:${count}\r\n`);
    await this.conn.request(Event.Qos, body);
  }

  async confirmSelect(): Promise<void> {
    await this.conn.request(Event.ConfirmSelect, new Uint8Array(0));
  }

  async assertExchange(
    name: string,
    type: ExchangeType = "direct",
    options?: ExchangeOptions,
  ): Promise<void> {
    const durable = options?.durable ?? false;
    const headerStr = `name:${name}\r\ntype:${type}\r\ndurable:${durable}\r\n`;
    const prefix = encode(headerStr);
    await this.conn.request(Event.DeclareExchange, prefix, prefix.length);
  }

  async deleteExchange(name: string): Promise<void> {
    const headerStr = `name:${name}\r\n`;
    const prefix = encode(headerStr);
    await this.conn.request(Event.DeleteExchange, prefix, prefix.length);
  }

  async bindQueue(
    queue: string,
    exchange: string,
    routingKey = "",
  ): Promise<void> {
    const headerStr = `exchange:${exchange}\r\nqueue:${queue}\r\nrouting_key:${routingKey}\r\n`;
    const prefix = encode(headerStr);
    await this.conn.request(Event.Bind, prefix, prefix.length);
  }

  async unbindQueue(
    queue: string,
    exchange: string,
    routingKey = "",
  ): Promise<void> {
    const headerStr = `exchange:${exchange}\r\nqueue:${queue}\r\nrouting_key:${routingKey}\r\n`;
    const prefix = encode(headerStr);
    await this.conn.request(Event.Unbind, prefix, prefix.length);
  }

  async publish(
    exchange: string,
    routingKey: string,
    content: Uint8Array,
    options?: PublishOptions,
  ): Promise<void> {
    let headerStr = `exchange:${exchange}\r\nrouting_key:${routingKey}\r\n`;
    if (options?.priority != null) headerStr += `priority:${options.priority}\r\n`;
    if (options?.expiration != null) headerStr += `expiration:${options.expiration}\r\n`;
    if (options?.headers) {
      for (const [k, v] of Object.entries(options.headers)) {
        headerStr += `${k}:${v}\r\n`;
      }
    }
    const prefix = encode(headerStr);
    const payload = new Uint8Array(prefix.length + content.length);
    payload.set(prefix);
    payload.set(content, prefix.length);
    await this.conn.sendFrame(Event.Publish, payload, prefix.length);
  }

  async sendToQueue(
    queueName: string,
    content: Uint8Array,
    options?: PublishOptions,
  ): Promise<void> {
    await this.publish("", queueName, content, options);
  }

  async consume<T = Uint8Array>(
    queueName: string,
    onMessage: (
      body: T,
      headers: Record<string, string>,
      msg: Message<T>,
    ) => void | Promise<void>,
  ): Promise<void> {
    this.conn.onMessage((msgId, headers, rawBody) => {
      let content: any = rawBody;
      try {
        const text = new TextDecoder().decode(rawBody);
        content = JSON.parse(text);
      } catch {
        // Fallback to raw Uint8Array if it's not valid JSON
      }

      const msg: Message<T> = { id: msgId, headers, content: content as T };
      onMessage(content as T, headers, msg);
    });
    await this.conn.request(Event.Listen, encode(queueName));
  }

  async ack(msg: Message<any>): Promise<void> {
    const prefix = encode(`id:${msg.id}\r\n`);
    await this.conn.sendFrame(Event.Ack, prefix, prefix.length);
  }

  async nack(
    msg: Message<any>,
    options?: { requeue?: boolean },
  ): Promise<void> {
    const requeue = options?.requeue ?? false;
    const prefix = encode(`id:${msg.id}\r\nrequeue:${requeue}\r\n`);
    await this.conn.sendFrame(Event.Nack, prefix, prefix.length);
  }
}

export async function connect(
  host = "localhost",
  port = 8080,
): Promise<Connection> {
  const conn = await Deno.connect({ hostname: host, port, transport: "tcp" });
  return new Connection(conn);
}
