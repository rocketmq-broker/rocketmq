import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('Bootstrap');

  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();

  const port = process.env.PORT || 3000;
  await app.listen(port);

  logger.log(`
╔══════════════════════════════════════════════════════════╗
║  E-Commerce Order Pipeline                               ║
║                                                          ║
║  HTTP API:  http://127.0.0.1:${port}/orders               ║
║  AMQP:     amqp://127.0.0.1:5672                         ║
║                                                          ║
║  Services running:                                       ║
║    1. OrderGateway    → POST /orders (HTTP → AMQP)       ║
║    2. OrderService    → validates orders                  ║
║    3. PaymentService  → processes payments                ║
║    4. InventoryService→ reserves stock                    ║
║    5. NotificationSvc → delivers notifications + DLQ     ║
║                                                          ║
║  Flow:                                                   ║
║    HTTP → orders.created → payments.process →            ║
║    inventory.reserve → notifications.send                ║
╚══════════════════════════════════════════════════════════╝
  `);
}

void bootstrap();
