import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import {
  EXCHANGE_ANALYTICS,
  EXCHANGE_METRICS,
  QUEUE_TELEMETRY,
  RK_ANTI_CHEAT_ALERT,
} from '../amqp/constants';

@Injectable()
export class TelemetryProcessorService implements OnModuleInit {
  private readonly logger = new Logger('TelemetryProcessor');
  private totalTicks = 0;
  private totalActions = 0;

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  onModuleInit() {
    this.logger.log(
      'Initializing Real-Time Telemetry and Anti-Cheat pipeline...',
    );
    this.consumeTelemetry();

    // Periodically export aggregated system logs to metrics fanout exchange
    setInterval(() => {
      this.publishPlatformMetrics();
    }, 2000);
  }

  private async consumeTelemetry() {
    await this.ch.consume(QUEUE_TELEMETRY, (msg) => {
      if (!msg) return;

      try {
        const content = JSON.parse(msg.content.toString());
        const routingKey = msg.fields.routingKey;

        if (routingKey.endsWith('.tick')) {
          this.totalTicks++;
          this.processTickEvent(content);
        } else if (routingKey.endsWith('.action')) {
          this.totalActions++;
          this.logger.log(
            `[ACTION LOG] ${content.player.name} in session ${content.sessionId} performed action: ${content.action}`,
          );
        }

        this.ch.ack(msg);
      } catch (err) {
        this.logger.error(
          'Failed to process telemetry event, sending to DLQ:',
          err.message,
        );
        this.ch.nack(msg, false, false); // Nack directly to Dead Letter Exchange
      }
    });
  }

  private processTickEvent(tickData: any) {
    const players = tickData.players || [];

    // Check speedhack threshold
    for (const p of players) {
      if (p.velocity > 22.0) {
        this.logger.warn(
          `[ANTI-CHEAT ALERT] High velocity anomaly: ${p.name} at ${p.velocity.toFixed(2)} units/sec in ${tickData.sessionId}!`,
        );

        const alertPayload = {
          timestamp: Date.now(),
          sessionId: tickData.sessionId,
          map: tickData.map,
          suspect: {
            id: p.id,
            name: p.name,
            velocity: p.velocity,
            coordinates: { x: p.x, y: p.y },
          },
          severity: 'CRITICAL',
          actionRecommended: 'KICK_PLAYER',
        };

        // Publish to anti-cheat direct queue
        this.ch.publish(
          EXCHANGE_ANALYTICS,
          RK_ANTI_CHEAT_ALERT,
          Buffer.from(JSON.stringify(alertPayload)),
        );
      }
    }
  }

  private async publishPlatformMetrics() {
    const metricsPayload = {
      timestamp: Date.now(),
      hostname: 'game-engine-server-1a',
      telemetryRates: {
        ticksProcessed: this.totalTicks,
        actionsProcessed: this.totalActions,
      },
      cpuUtilization: 14.5 + Math.random() * 5.0,
      memoryUsageBytes:
        256 * 1024 * 1024 + Math.floor(Math.random() * 20 * 1024 * 1024),
    };

    this.ch.publish(
      EXCHANGE_METRICS,
      '', // Fanout routing key is ignored
      Buffer.from(JSON.stringify(metricsPayload)),
    );
  }
}
