import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import {
  QUEUE_ANTI_CHEAT,
  QUEUE_MATCHMAKING,
  QUEUE_METRICS_LOGS,
} from '../amqp/constants';

@Injectable()
export class LiveAlertService implements OnModuleInit {
  private readonly logger = new Logger('LiveAlertGateway');

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  onModuleInit() {
    this.logger.log('Starting Live Alert and Broadcast Gateway (listening)...');
    this.consumeMatchmaking();
    this.consumeAntiCheatAlerts();
    this.consumeSystemMetrics();
  }

  private async consumeMatchmaking() {
    await this.ch.consume(QUEUE_MATCHMAKING, (msg) => {
      if (!msg) return;

      try {
        const content = JSON.parse(msg.content.toString());
        this.logger.log(
          `[MATCHMAKER] successfully allocated region, matched player: "${content.player.username}" (Skill Rating: ${content.player.skillRating}, Ping: ${content.player.ping}ms)`,
        );
        this.ch.ack(msg);
      } catch (err) {
        this.logger.error('Failed to parse matchmaker event:', err.message);
        this.ch.ack(msg); // Ack anyway for mock purposes
      }
    });
  }

  private async consumeAntiCheatAlerts() {
    await this.ch.consume(QUEUE_ANTI_CHEAT, (msg) => {
      if (!msg) return;

      try {
        const content = JSON.parse(msg.content.toString());

        console.log('');
        console.log(
          '🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨',
        );
        console.log(
          `[ALERT GATEWAY] SECURITY PROTOCOL ACTIVATED IN "${content.map.toUpperCase()}"!`,
        );
        console.log(`SUSPECT: ${content.suspect.name}`);
        console.log(
          `VIOLATION: Speed hack threshold exceeded (${content.suspect.velocity.toFixed(2)} units/sec)`,
        );
        console.log(
          `COORDINATES: X: ${content.suspect.coordinates.x.toFixed(1)}, Y: ${content.suspect.coordinates.y.toFixed(1)}`,
        );
        console.log(`ACTION ENFORCED: ${content.actionRecommended}`);
        console.log(
          '🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨',
        );
        console.log('');

        this.ch.ack(msg);
      } catch (err) {
        this.ch.ack(msg);
      }
    });
  }

  private async consumeSystemMetrics() {
    await this.ch.consume(QUEUE_METRICS_LOGS, (msg) => {
      if (!msg) return;

      try {
        const content = JSON.parse(msg.content.toString());
        this.logger.log(
          `[LIVE TELEMETRY] Host: ${content.hostname} | CPU: ${content.cpuUtilization.toFixed(1)}% | Ticks: ${content.telemetryRates.ticksProcessed} | Actions: ${content.telemetryRates.actionsProcessed}`,
        );
        this.ch.ack(msg);
      } catch (err) {
        this.ch.ack(msg);
      }
    });
  }
}
