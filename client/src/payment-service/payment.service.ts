import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import {
  AMQP_CHANNEL,
  EXCHANGE_INVENTORY,
  EXCHANGE_NOTIFICATIONS,
  QUEUE_PAYMENTS_PROCESS,
  RK_INVENTORY_RESERVE,
} from '../amqp';
import {
  InventoryRequest,
  NotificationEvent,
  PaymentRequest,
} from '../amqp/interfaces';

@Injectable()
export class PaymentService implements OnModuleInit {
  private readonly logger = new Logger('PaymentService');

  constructor(@Inject(AMQP_CHANNEL) private readonly channel: amqp.Channel) {}

  async onModuleInit() {
    this.logger.log(`👂 Consuming from ${QUEUE_PAYMENTS_PROCESS}`);

    await this.channel.consume(QUEUE_PAYMENTS_PROCESS, (msg) => {
      if (!msg) return;
      void this.handlePayment(msg).catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.error(`💥 Payment processing error: ${message}`);
        this.channel.nack(msg, false, false);
      });
    });
  }

  private async handlePayment(msg: amqp.ConsumeMessage): Promise<void> {
    const payment = JSON.parse(msg.content.toString()) as PaymentRequest;
    this.logger.log(
      `💳 Processing payment for order ${payment.orderId} ($${payment.amount?.toFixed(2)})`,
    );

    // ── Simulate payment processing ───────────
    const processingTimeMs = 100 + Math.random() * 200;
    await new Promise((resolve) => setTimeout(resolve, processingTimeMs));

    // Simulate: 90% success, 10% failure
    const success = Math.random() < 0.9;

    if (!success) {
      this.logger.warn(`❌ Payment DECLINED for order ${payment.orderId}`);

      const failEvent: NotificationEvent = {
        type: 'PAYMENT_FAILED',
        orderId: payment.orderId,
        customerId: payment.customerId,
        reason: 'Insufficient funds',
        timestamp: new Date().toISOString(),
      };

      this.channel.publish(
        EXCHANGE_NOTIFICATIONS,
        '', // fanout ignores routing key
        Buffer.from(JSON.stringify(failEvent)),
        { persistent: true, contentType: 'application/json' },
      );

      this.channel.ack(msg);
      return;
    }

    // ── Payment succeeded → reserve inventory ─
    const transactionId = `TXN-${Date.now()}`;
    const inventoryRequest: InventoryRequest = {
      orderId: payment.orderId,
      customerId: payment.customerId,
      transactionId,
      items: payment.items,
      amount: payment.amount,
      shippingAddress: payment.shippingAddress,
      paidAt: new Date().toISOString(),
    };

    this.channel.publish(
      EXCHANGE_INVENTORY,
      RK_INVENTORY_RESERVE,
      Buffer.from(JSON.stringify(inventoryRequest)),
      { persistent: true, contentType: 'application/json' },
    );

    this.logger.log(
      `✅ Payment ${transactionId} approved → ${EXCHANGE_INVENTORY}/${RK_INVENTORY_RESERVE}`,
    );
    this.channel.ack(msg);
  }
}
