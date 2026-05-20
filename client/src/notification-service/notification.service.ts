import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL, QUEUE_NOTIFICATIONS_SEND, QUEUE_DLQ } from '../amqp';
import { NotificationEvent } from '../amqp/interfaces';

@Injectable()
export class NotificationService implements OnModuleInit {
  private readonly logger = new Logger('NotificationService');
  private stats = {
    sent: 0,
    orderCompleted: 0,
    paymentFailed: 0,
    inventoryFailed: 0,
  };

  constructor(@Inject(AMQP_CHANNEL) private readonly channel: amqp.Channel) {}

  async onModuleInit() {
    this.logger.log(`👂 Consuming from ${QUEUE_NOTIFICATIONS_SEND}`);
    this.logger.log(`👂 Consuming from ${QUEUE_DLQ} (dead letters)`);

    // ── Main notification consumer ────────────────
    await this.channel.consume(QUEUE_NOTIFICATIONS_SEND, (msg) => {
      if (!msg) return;
      try {
        this.handleNotification(msg);
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.error(`💥 Notification error: ${message}`);
        this.channel.nack(msg, false, false);
      }
    });

    // ── Dead letter consumer ──────────────────────
    await this.channel.consume(QUEUE_DLQ, (msg) => {
      if (!msg) return;
      this.logger.error(
        `☠️ [DLQ] Dead letter received:\n` +
          `   Exchange: ${msg.fields.exchange}\n` +
          `   Routing Key: ${msg.fields.routingKey}\n` +
          `   Body: ${msg.content.toString().substring(0, 200)}`,
      );
      this.channel.ack(msg);
    });
  }

  private handleNotification(msg: amqp.ConsumeMessage): void {
    const event = JSON.parse(msg.content.toString()) as NotificationEvent;
    this.stats.sent++;

    switch (event.type) {
      case 'ORDER_COMPLETED':
        this.stats.orderCompleted++;
        this.logger.log(
          `🎉 [EMAIL] Order ${event.orderId} completed!\n` +
            `   Customer: ${event.customerId}\n` +
            `   Amount: $${event.amount?.toFixed(2)}\n` +
            `   Items: ${event.items?.length}\n` +
            `   Transaction: ${event.transactionId}\n` +
            `   Ship to: ${event.shippingAddress}`,
        );
        break;

      case 'PAYMENT_FAILED':
        this.stats.paymentFailed++;
        this.logger.warn(
          `⚠️ [EMAIL] Payment failed for order ${event.orderId}\n` +
            `   Customer: ${event.customerId}\n` +
            `   Reason: ${event.reason}`,
        );
        break;

      case 'INVENTORY_FAILED':
        this.stats.inventoryFailed++;
        this.logger.warn(
          `⚠️ [EMAIL] Inventory failed for order ${event.orderId}\n` +
            `   Customer: ${event.customerId}\n` +
            `   Errors: ${event.errors?.join(', ')}`,
        );
        break;

      default:
        this.logger.log(`📨 Unknown notification type: ${event.type}`);
    }

    this.logger.log(
      `📊 Stats: ${this.stats.sent} sent, ` +
        `${this.stats.orderCompleted} completed, ` +
        `${this.stats.paymentFailed} payment failures, ` +
        `${this.stats.inventoryFailed} inventory failures`,
    );

    this.channel.ack(msg);
  }
}
