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
 * File: app.module.ts
 * Description: Source file for the app.module component.
 */

import { Module } from '@nestjs/common';
import { AmqpModule } from './amqp';
import { GameEngineModule } from './game-engine/game-engine.module';
import { LiveAlertModule } from './live-alert/live-alert.module';
import { TelemetryProcessorModule } from './telemetry-processor/telemetry-processor.module';

// New Clustered Advanced Telemetry Microservices
import { AntiCheatModule } from './anti-cheat/anti-cheat.module';
import { LeaderboardModule } from './leaderboard/leaderboard.module';
import { LobbyMaintenanceModule } from './lobby-maintenance/lobby-maintenance.module';
import { MatchmakingModule } from './matchmaking/matchmaking.module';
import { SystemMonitorModule } from './system-monitor/system-monitor.module';

@Module({
  imports: [
    // Shared AMQP connection + topology declaration
    AmqpModule,

    // ── Real-Time Game Telemetry & Analytics Platform ───────────────────
    GameEngineModule, // Publishes ticks and actions forever
    TelemetryProcessorModule, // Analytics & speedhack cheat detection
    LiveAlertModule, // Warning dashboard, matchmaking gateway, and metric logger

    // ── Clustered Topology Testing Microservices ────────────────────────
    MatchmakingModule, // Groups matches and emits lobby.created
    AntiCheatModule, // Broadcasts bans over security.broadcast fanout
    LeaderboardModule, // Computes real-time rankings and broadcasts updates
    SystemMonitorModule, // Triggers DLQ dead-lettering for system anomalies
    LobbyMaintenanceModule, // Monitors lobby pings and sweeps old rooms
  ],
})
/**
 * Service class managing app module operations.
 *
 * Defines schemas, types, or services for app module inside the NestJS client.
 */
export class AppModule {}
