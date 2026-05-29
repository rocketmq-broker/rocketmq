const amqp = require("amqplib");
const protobuf = require("protobufjs");

const AMQP_URL = process.env.AMQP_URL || "amqp://guest:guest@localhost:5672";
const QUEUE_NAME = "js-schema-queue";

const PROTO_SRC = `
syntax = "proto3";
package test;

message Point {
  int32 x = 1;
  int32 y = 2;
}
`;

async function main() {
  console.log("--- AMQP Schema Validation JS Test Client ---");

  // 1. Compile the Protobuf schema locally to generate test payloads
  const root = protobuf.parse(PROTO_SRC).root;
  const Point = root.lookupType("test.Point");
  console.log("✓ Locally parsed and compiled test.Point schema");

  // 2. Connect to the AMQP Broker
  console.log(`Connecting to broker at ${AMQP_URL}...`);
  const conn = await amqp.connect(AMQP_URL);
  console.log("✓ Connected successfully!");

  // 3. Declare Schema Queue
  console.log(`Declaring schema queue: "${QUEUE_NAME}"...`);
  const ch = await conn.createChannel();
  await ch.assertQueue(QUEUE_NAME, {
    durable: true,
    arguments: {
      "x-schema": PROTO_SRC,
      "x-schema-type": "protobuf",
      "x-schema-message": "test.Point",
    },
  });
  console.log("✓ Queue declared with schema arguments successfully!");

  // 4. Test Scenario 1: Publish a valid Point message
  console.log("\n--- Scenario 1: Publishing a Valid Point ---");
  const validPoint = { x: 42, y: 84 };
  const validPayload = Point.encode(Point.create(validPoint)).finish();

  console.log(
    `Publishing valid point: x=${validPoint.x}, y=${validPoint.y}...`,
  );
  ch.sendToQueue(QUEUE_NAME, validPayload, {
    contentType: "application/x-protobuf",
  });
  console.log("✓ Sent valid message.");

  // 5. Test Scenario 2: Consume the valid message and decode it
  console.log("\n--- Scenario 2: Consuming and Decoding the Valid Point ---");
  await new Promise((resolve, reject) => {
    ch.consume(QUEUE_NAME, (msg) => {
      if (msg !== null) {
        try {
          const decoded = Point.decode(msg.content);
          console.log(`✓ Successfully received and decoded message!`);
          console.log(`  Decoded Values: x=${decoded.x}, y=${decoded.y}`);
          ch.ack(msg);
          resolve();
        } catch (err) {
          reject(err);
        }
      }
    });
  });

  // Clean up the first channel
  await ch.close();

  // 6. Test Scenario 3: Publish an invalid message (expecting channel closure / precondition failed)
  console.log("\n--- Scenario 3: Publishing an Invalid Payload ---");
  const invalidCh = await conn.createChannel();

  // Set up channel error listener
  const errorPromise = new Promise((resolve) => {
    invalidCh.on("error", (err) => {
      console.log("✓ Captured expected channel error from broker!");
      console.log(`  Error Message: "${err.message}"`);
      resolve();
    });
  });

  const invalidPayload = Buffer.from([0x99, 0x88, 0x77]); // random invalid bytes
  console.log(
    "Publishing invalid bytes with application/x-protobuf content-type...",
  );
  invalidCh.sendToQueue(QUEUE_NAME, invalidPayload, {
    contentType: "application/x-protobuf",
  });

  await errorPromise;

  console.log("\n--- JS Schema Tests Complete ---");
  await conn.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Test client failed:", err);
  process.exit(1);
});
