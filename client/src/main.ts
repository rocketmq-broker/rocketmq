import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('Bootstrap');

  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();

  const port = process.env.PORT || 3001;
  await app.listen(port);

  logger.log(`
╔══════════════════════════════════════════════════════════╗
║  Game Telemetry & Real-Time Anti-Cheat Analytics        ║
║                                                          ║
║  HTTP Monitor:  http://127.0.0.1:${port}/                  ║
║  AMQP Cluster:  amqp://127.0.0.1:5672                    ║
║                                                          ║
║  Cooperative Microservices Booted (8/8):                 ║
║    1. GameSessionGenerator   → Tick Loop                 ║
║    2. TelemetryProcessor     → Cheat Detection           ║
║    3. LiveAlertGateway       → Broadcast Gateway         ║
║    4. MatchmakingService     → Lobby Placement           ║
║    5. CheatBusterEnforcer    → Cluster Banhammer         ║
║    6. LeaderboardService     → Scoring & Rankings        ║
║    7. SystemMetricsEviction  → Performance & DLQ Tester  ║
║    8. LobbyMaintenanceSvc    → Sweeper & Pinger          ║
╚══════════════════════════════════════════════════════════╝
  `);
}

void bootstrap();
