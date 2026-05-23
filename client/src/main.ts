/**
 * Copyright (c) 2026 Edilson Pateguana
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Author: Edilson Pateguana
 * Year: 2026
 * File: main.ts
 * Description: Source file for the main component.
 */

import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

/**
 * Executes the standard bootstrap lifecycle step.
 *
 * Performs client execution steps for bootstrap.
 */
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
