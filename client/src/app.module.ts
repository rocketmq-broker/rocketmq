import { Module } from '@nestjs/common';
import { AmqpModule } from './amqp';
import { InventoryServiceModule } from './inventory-service/inventory-service.module';
import { NotificationServiceModule } from './notification-service/notification-service.module';
import { OrderGatewayModule } from './order-gateway/order-gateway.module';
import { OrderServiceModule } from './order-service/order-service.module';
import { PaymentServiceModule } from './payment-service/payment-service.module';

@Module({
  imports: [
    // Shared AMQP connection + topology declaration
    AmqpModule,

    // ── Microservices ───────────────────────────
    OrderGatewayModule, // HTTP API → publishes to orders.exchange
    OrderServiceModule, // orders.created → validates → payments.process
    PaymentServiceModule, // payments.process → charges → inventory.reserve
    InventoryServiceModule, // inventory.reserve → reserves stock → notifications
    NotificationServiceModule, // notifications.send → emails/logs + DLQ monitor
  ],
})
export class AppModule {}
