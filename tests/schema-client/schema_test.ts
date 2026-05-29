import { AmqpSchemaClient } from "./AmqpSchemaClient";
import { ProtoMessage, ProtoField } from "./decorators";

const AMQP_URL = process.env.AMQP_URL || "amqp://guest:guest@localhost:5672";
const QUEUE_NAME = "ts-schema-queue";

// ── Schema definition: pure TypeScript, zero Protobuf syntax ────────

@ProtoMessage("test", "Point")
class Point {
  @ProtoField(1, "int32")
  x: number;

  @ProtoField(2, "int32")
  y: number;
}

// ── Test scenarios ──────────────────────────────────────────────────

async function main() {
  console.log("--- AMQP Schema Validation TS Test Client ---");

  // 1. Connect
  console.log(`Connecting to RocketMQ AMQP broker at ${AMQP_URL}...`);
  const client = await AmqpSchemaClient.connect(AMQP_URL);
  console.log("✓ Connected successfully via AmqpSchemaClient!");

  // 2. Declare schema queue — just pass the class, no .proto source needed!
  console.log(`Declaring schema queue: "${QUEUE_NAME}"...`);
  await client.declareSchemaQueue(QUEUE_NAME, Point);
  console.log("✓ Queue declared and schema compiled successfully.");

  // 3. Purge any leftover messages from previous runs
  await client.purgeQueue(QUEUE_NAME);
  console.log("✓ Queue purged.");

  // 4. Scenario 1: Publish a valid Point object
  console.log("\n--- Scenario 1: Publishing a Valid Point Object ---");
  const validPoint: Point = { x: 42, y: 84 };
  console.log("Publishing plain object:", validPoint);
  await client.publish<Point>(QUEUE_NAME, validPoint);
  console.log("✓ Published successfully.");

  // 5. Scenario 2: Consume and decode
  console.log("\n--- Scenario 2: Consuming and Decoding the Valid Point ---");
  await new Promise<void>((resolve, reject) => {
    client
      .consume<Point>(QUEUE_NAME, (msg, raw) => {
        console.log("✓ Successfully received and decoded message!");
        console.log("  Decoded Object:", msg);
        console.log(`  Parsed Values: x=${msg.x}, y=${msg.y}`);
        client.ack(raw);
        resolve();
      })
      .catch(reject);
  });

  // 6. Scenario 3: Publish invalid bytes to trigger broker validation gate
  console.log(
    "\n--- Scenario 3: Publishing an Invalid Payload to Trigger validation ---",
  );
  const rawChannel = await client.createRawChannel();

  const errorPromise = new Promise<void>((resolve) => {
    rawChannel.on("error", (err: any) => {
      console.log("✓ Captured expected channel closure from RocketMQ Broker!");
      console.log(`  Error Details: "${err.message}"`);
      resolve();
    });
  });

  const invalidPayload = Buffer.from([0x99, 0x88, 0x77]);
  console.log(
    "Publishing invalid binary bytes directly to raw AMQP channel...",
  );
  rawChannel.sendToQueue(QUEUE_NAME, invalidPayload, {
    contentType: "application/x-protobuf",
  });

  await errorPromise;

  console.log("\n--- TS Schema Wrapper Tests Complete ---");
  await client.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Test client wrapper failed:", err);
  process.exit(1);
});
