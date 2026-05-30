/**
 * RocketMQ TypeScript client — schema-aware AMQP wrapper.
 *
 * Wraps amqplib with automatic schema registration so you can write:
 *
 *   const rocket = await RocketMQ.connect("amqp://localhost");
 *   await rocket.assertQueue("orders", OrderSchema, { durable: true });
 *   await rocket.publish("orders", { id: "abc", qty: 5 });
 */

import amqp from "amqplib";
import { toProto } from "./schema";

export { Field, Schema } from "./schema";

interface RocketOptions {
  /** AMQP connection URL. Default: amqp://guest:guest@localhost:5672 */
  url?: string;
  /** Management API base URL. Default: http://localhost:15672 */
  mgmtUrl?: string;
  /** Management API credentials. Default: guest:guest */
  mgmtAuth?: { user: string; pass: string };
}

export class RocketMQ {
  private schemaIds = new Map<string, number>();

  private constructor(
    public readonly conn: amqp.Connection,
    public readonly ch: amqp.Channel,
    private mgmtUrl: string,
    private authHeader: string,
  ) {}

  /** Opens a connection + channel ready for schema-aware operations. */
  static async connect(opts: RocketOptions = {}): Promise<RocketMQ> {
    const url = opts.url ?? "amqp://guest:guest@localhost:5672";
    const mgmt = opts.mgmtUrl ?? "http://localhost:15672";
    const auth = opts.mgmtAuth ?? { user: "guest", pass: "guest" };
    const authHeader =
      "Basic " + Buffer.from(`${auth.user}:${auth.pass}`).toString("base64");
    const conn = await amqp.connect(url);
    const ch = await conn.createChannel();
    return new RocketMQ(conn, ch, mgmt, authHeader);
  }

  /**
   * Declares a queue with an optional schema class.
   *
   * When a schema is provided:
   *   1. Generates a proto3 definition from the decorated class
   *   2. Registers it with the broker's built-in schema registry
   *   3. Binds the queue to the schema subject via x-schema-subject
   */
  async assertQueue(
    name: string,
    schema?: Function,
    opts?: amqp.Options.AssertQueue,
  ): Promise<amqp.Replies.AssertQueue> {
    if (!schema) {
      return this.ch.assertQueue(name, opts);
    }

    const subject = `${name}-value`;
    const proto = toProto(schema);
    const schemaId = await this.registerSchema(subject, proto, schema.name);
    this.schemaIds.set(name, schemaId);

    return this.ch.assertQueue(name, {
      ...opts,
      arguments: { ...opts?.arguments, "x-schema-subject": subject },
    });
  }

  /** Declares an exchange (passthrough to amqplib). */
  async assertExchange(
    name: string,
    type: string,
    opts?: amqp.Options.AssertExchange,
  ): Promise<amqp.Replies.AssertExchange> {
    return this.ch.assertExchange(name, type, opts);
  }

  /** Binds a queue to an exchange (passthrough to amqplib). */
  async bindQueue(
    queue: string,
    exchange: string,
    routingKey: string,
  ): Promise<amqp.Replies.Empty> {
    return this.ch.bindQueue(queue, exchange, routingKey);
  }

  /**
   * Publishes a JSON message. If the queue has a registered schema,
   * the Confluent wire-format prefix [0x00, schema_id_be32] is
   * prepended automatically.
   */
  publish(
    exchange: string,
    routingKey: string,
    payload: Record<string, unknown>,
    opts?: amqp.Options.Publish,
  ): boolean {
    const json = Buffer.from(JSON.stringify(payload));

    return this.ch.publish(exchange, routingKey, json, {
      contentType: "application/json",
      persistent: true,
      ...opts,
    });
  }

  /** Subscribes to a queue with automatic JSON parsing. */
  async consume(
    queue: string,
    handler: (msg: Record<string, unknown>, raw: amqp.ConsumeMessage) => void,
    opts?: amqp.Options.Consume,
  ): Promise<amqp.Replies.Consume> {
    return this.ch.consume(
      queue,
      (msg) => {
        if (!msg) return;
        const body = JSON.parse(msg.content.toString());
        handler(body, msg);
      },
      opts,
    );
  }

  /** Acknowledges a message. */
  ack(msg: amqp.ConsumeMessage): void {
    this.ch.ack(msg);
  }

  /** Sets prefetch count on the channel. */
  async prefetch(count: number): Promise<void> {
    await this.ch.prefetch(count);
  }

  /** Closes channel and connection. */
  async close(): Promise<void> {
    await this.ch.close();
    await this.conn.close();
  }

  private async registerSchema(
    subject: string,
    proto: string,
    messageName: string,
  ): Promise<number> {
    const url = `${this.mgmtUrl}/api/schemas/subjects/${encodeURIComponent(subject)}/versions`;
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: this.authHeader,
      },
      body: JSON.stringify({
        schema: proto,
        schemaType: "PROTOBUF",
        messageName,
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(
        `Schema registration failed for '${subject}': ${res.status} ${text}`,
      );
    }

    const body = (await res.json()) as { id: number };
    return body.id;
  }
}
