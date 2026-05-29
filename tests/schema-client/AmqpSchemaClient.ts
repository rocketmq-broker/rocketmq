import amqp from "amqplib";
import protobuf from "protobufjs";
import { generateProtoFromClass } from "./decorators";

/**
 * Robust TypeScript client wrapper for RocketMQ AMQP Schema Validation.
 * Hides all Protobuf serialization, dynamic parsing, and content-type details.
 */
export class AmqpSchemaClient {
  private connection: any;
  private channel: any = null;
  // Map of queueName -> compiled Protobuf Type
  private schemaCache: Map<
    string,
    { type: protobuf.Type; protoSrc: string; messageName: string }
  > = new Map();

  private constructor(connection: any) {
    this.connection = connection;
  }

  /**
   * Connect to the RocketMQ AMQP Broker and return a new instance of AmqpSchemaClient.
   */
  public static async connect(
    url: string = "amqp://guest:guest@localhost:5672",
  ): Promise<AmqpSchemaClient> {
    const connection = await amqp.connect(url);
    const client = new AmqpSchemaClient(connection);
    client.channel = await connection.createChannel();
    return client;
  }

  /**
   * Declare a queue enforced with a Protobuf schema using a decorated TypeScript class.
   *
   * The class must use `@ProtoMessage` and `@ProtoField` decorators.
   * The `.proto` source is generated automatically from decorator metadata —
   * the client never touches raw Protobuf syntax.
   *
   * @example
   *   @ProtoMessage("events", "UserCreated")
   *   class UserCreated {
   *     @ProtoField(1, "string") name: string;
   *     @ProtoField(2, "int32")  age: number;
   *   }
   *
   *   await client.declareSchemaQueue("user-events", UserCreated);
   */
  public async declareSchemaQueue(queueName: string, schemaClass: any): Promise<void> {
    const { protoSrc, fullMessageName } = generateProtoFromClass(schemaClass);
    await this.declareQueue(queueName, protoSrc, fullMessageName);
  }

  /**
   * Declare a queue enforced with a Protobuf schema from raw `.proto` source.
   * Prefer `declareSchemaQueue` with a decorated class for a cleaner API.
   */
  public async declareQueue(
    queueName: string,
    protoSrc: string,
    messageName: string,
  ): Promise<void> {
    if (!this.channel) throw new Error("Channel is not initialized");

    // 1. Compile schema locally to verify validity and cache it
    try {
      const root = protobuf.parse(protoSrc).root;
      const type = root.lookupType(messageName);
      this.schemaCache.set(queueName, { type, protoSrc, messageName });
    } catch (err) {
      throw new Error(
        `Failed to compile schema locally: ${(err as Error).message}`,
      );
    }

    // 2. Declare the queue to the broker with the x-schema header arguments
    await this.channel.assertQueue(queueName, {
      durable: true,
      arguments: {
        "x-schema": protoSrc,
        "x-schema-type": "protobuf",
        "x-schema-message": messageName,
      },
    });
  }

  /**
   * Publish a plain JS object to a schema-enforced queue.
   * Performs automatic Protobuf encoding and sets the correct content-type header.
   */
  public async publish<T = any>(queueName: string, payload: T): Promise<boolean> {
    if (!this.channel) throw new Error("Channel is not initialized");

    const schema = this.schemaCache.get(queueName);
    if (!schema) {
      throw new Error(
        `No local schema found for queue "${queueName}". Call declareQueue first.`,
      );
    }

    try {
      // Create and encode the Protobuf message dynamically
      const msgError = schema.type.verify(payload as any);
      if (msgError) throw new Error(`Validation failed: ${msgError}`);

      const messageInstance = schema.type.create(payload as any);
      const binaryBuffer = schema.type.encode(messageInstance).finish();

      // Publish with content-type to enable the broker's schema validation gate
      return this.channel.sendToQueue(queueName, Buffer.from(binaryBuffer), {
        contentType: "application/x-protobuf",
      });
    } catch (err) {
      throw new Error(
        `Serialization error for queue "${queueName}": ${(err as Error).message}`,
      );
    }
  }

  /**
   * Consume from a schema-enforced queue.
   * Automatically intercepts incoming binary Protobuf messages, decodes them to plain JS objects,
   * and handles any invalid format errors gracefully.
   */
  public async consume<T = any>(
    queueName: string,
    onMessage: (msg: T, raw: any) => void,
  ): Promise<string> {
    if (!this.channel) throw new Error("Channel is not initialized");

    const schema = this.schemaCache.get(queueName);
    if (!schema) {
      throw new Error(
        `No local schema found for queue "${queueName}". Call declareQueue first.`,
      );
    }

    const consumeResult = await this.channel.consume(queueName, (msg: any) => {
      if (msg === null) return;

      // Handle non-protobuf or legacy malformed messages gracefully
      if (msg.properties.contentType !== "application/x-protobuf") {
        console.warn(
          `[AmqpSchemaClient] Received message without application/x-protobuf content-type on queue "${queueName}". Acknowledging and discarding.`,
        );
        this.channel?.ack(msg);
        return;
      }

      try {
        const decoded = schema.type.decode(msg.content);
        // Convert to plain JS object to hide all protobuf structure classes
        const plainObject = schema.type.toObject(decoded, {
          longs: String,
          enums: String,
          bytes: String,
          defaults: true,
        });

        // Pass clean plain object to user callback
        onMessage(plainObject as T, msg);
      } catch (err) {
        console.error(
          `[AmqpSchemaClient] Failed to decode incoming message on queue "${queueName}" using schema "${schema.messageName}": ${(err as Error).message}`,
        );
        console.warn(
          "[AmqpSchemaClient] Automatically acknowledging corrupt message to clear the queue.",
        );
        this.channel?.ack(msg);
      }
    });

    return consumeResult.consumerTag;
  }

  /**
   * Purges all messages from a queue. Useful for clean test environments.
   */
  public async purgeQueue(queueName: string): Promise<void> {
    if (!this.channel) throw new Error("Channel is not initialized");
    await this.channel.purgeQueue(queueName);
  }

  /**
   * Acknowledge a message.
   */
  public ack(message: any): void {
    this.channel?.ack(message);
  }

  /**
   * Close the channel and connection.
   */
  public async close(): Promise<void> {
    if (this.channel) {
      await this.channel.close();
    }
    await this.connection.close();
  }

  /**
   * Create a raw channel to easily inspect low-level errors (e.g. captured closure)
   */
  public async createRawChannel(): Promise<any> {
    return this.connection.createChannel();
  }
}
