import { RocketMQ } from "./rocketmq";

async function main() {
  const mq = await RocketMQ.connect();

  await mq.assertQueue("pending-notifications", undefined, { durable: true });
  await mq.prefetch(1);

  console.log(`[sub] waiting for notifications…`);

  await mq.consume("pending-notifications", (msg, raw) => {
    console.log("[sub] received:", msg);
    mq.ack(raw);
  });
}

main().catch((err) => {
  console.error("[sub] fatal:", err);
  process.exit(1);
});
