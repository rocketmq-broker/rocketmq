import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import {
  AMQP_CHANNEL,
  EXCHANGE_NOTIFICATIONS,
  QUEUE_INVENTORY_RESERVE,
} from '../amqp';
import { InventoryRequest, NotificationEvent } from '../amqp/interfaces';

// In-memory stock — simulates a real inventory database
const STOCK: Record<string, number> = {
  'PROD-001': 50,
  'PROD-002': 30,
  'PROD-003': 100,
  'PROD-004': 10,
  'PROD-005': 0, // out of stock
};

@Injectable()
export class InventoryService implements OnModuleInit {
  private readonly logger = new Logger('InventoryService');

  constructor(@Inject(AMQP_CHANNEL) private readonly channel: amqp.Channel) {}

  async onModuleInit() {
    this.logger.log(`👂 Consuming from ${QUEUE_INVENTORY_RESERVE}`);
    this.logger.log(`📊 Initial stock: ${JSON.stringify(STOCK)}`);

    await this.channel.consume(QUEUE_INVENTORY_RESERVE, (msg) => {
      if (!msg) return;
      try {
        this.handleReservation(msg);
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.error(`💥 Inventory error: ${message}`);
        this.channel.nack(msg, false, false);
      }
    });
  }

  private handleReservation(msg: amqp.ConsumeMessage): void {
    const request = JSON.parse(msg.content.toString()) as InventoryRequest;
    this.logger.log(`📦 Reserving inventory for order ${request.orderId}`);

    // ── Check & reserve stock ─────────────────
    const reservationErrors: string[] = [];
    const reserved: { productId: string; quantity: number }[] = [];

    for (const item of request.items) {
      const available = STOCK[item.productId] ?? 0;
      if (available < item.quantity) {
        reservationErrors.push(
          `${item.productId}: need ${item.quantity}, have ${available}`,
        );
      } else {
        reserved.push({ productId: item.productId, quantity: item.quantity });
      }
    }

    if (reservationErrors.length > 0) {
      this.logger.warn(
        `❌ Inventory reservation FAILED for order ${request.orderId}: ${reservationErrors.join('; ')}`,
      );

      const failEvent: NotificationEvent = {
        type: 'INVENTORY_FAILED',
        orderId: request.orderId,
        customerId: request.customerId,
        transactionId: request.transactionId,
        errors: reservationErrors,
        timestamp: new Date().toISOString(),
      };

      this.channel.publish(
        EXCHANGE_NOTIFICATIONS,
        '',
        Buffer.from(JSON.stringify(failEvent)),
        { persistent: true, contentType: 'application/json' },
      );

      this.channel.ack(msg);
      return;
    }

    // ── Deduct stock ──────────────────────────
    for (const item of reserved) {
      STOCK[item.productId] -= item.quantity;
      this.logger.log(
        `  📉 ${item.productId}: ${STOCK[item.productId] + item.quantity} → ${STOCK[item.productId]}`,
      );
    }

    // ── Notify success ────────────────────────
    const successEvent: NotificationEvent = {
      type: 'ORDER_COMPLETED',
      orderId: request.orderId,
      customerId: request.customerId,
      transactionId: request.transactionId,
      items: reserved,
      amount: request.amount,
      shippingAddress: request.shippingAddress,
      completedAt: new Date().toISOString(),
    };

    this.channel.publish(
      EXCHANGE_NOTIFICATIONS,
      '',
      Buffer.from(JSON.stringify(successEvent)),
      { persistent: true, contentType: 'application/json' },
    );

    this.logger.log(
      `✅ Inventory reserved for order ${request.orderId} (${reserved.length} items) → notifications`,
    );
    this.channel.ack(msg);
  }
}
