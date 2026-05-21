import { Module } from '@nestjs/common';
import { AmqpModule } from './amqp';
import { GameEngineModule } from './game-engine/game-engine.module';
import { LiveAlertModule } from './live-alert/live-alert.module';
import { TelemetryProcessorModule } from './telemetry-processor/telemetry-processor.module';

// New Clustered Advanced Telemetry Microservices
import { MatchmakingModule } from './matchmaking/matchmaking.module';
import { AntiCheatModule } from './anti-cheat/anti-cheat.module';
import { LeaderboardModule } from './leaderboard/leaderboard.module';
import { SystemMonitorModule } from './system-monitor/system-monitor.module';
import { LobbyMaintenanceModule } from './lobby-maintenance/lobby-maintenance.module';

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
export class AppModule {}
