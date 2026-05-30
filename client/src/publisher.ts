import { Field, RocketMQ, Schema } from "./rocketmq";

@Schema()
class Notification {
  @Field()
  id!: string;

  @Field()
  content!: string;

  @Field({ type: "int64" })
  timestamp!: number;
}

async function main() {
  const mq = await RocketMQ.connect();

  await mq.assertExchange("notifications", "direct", { durable: true });
  await mq.assertQueue("pending-notifications", Notification, {
    durable: true,
  });
  await mq.bindQueue("pending-notifications", "notifications", "notify");

  for (let i = 1; i <= 5; i++) {
    mq.publish("notifications", "notify", {
      id: `notif-${i}`,
      content: `Hello from notification #${i}`,
      timestamp: Date.now(),
    });
    console.log(`[pub] sent notification #${i}`);
  }

  await mq.close();
  console.log("[pub] done");
}

main().catch((err) => {
  console.error("[pub] fatal:", err);
  process.exit(1);
});
