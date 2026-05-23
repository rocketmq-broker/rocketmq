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
 * File: telemetry-processor.service.ts
 * Description: Telemetry event processor, database sink, and analytics consumer.
 */

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
/**
 * Processes incoming game telemetry events and persists them to long-term storage.
 *
 * Processes incoming game telemetry events and persists them to long-term storage.
 */
export class TelemetryProcessorService implements OnModuleInit {
  private readonly logger = new Logger('TelemetryProcessor');
  private totalTicks = 0;
  private totalActions = 0;

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  /**
   * Executes the standard on module init lifecycle step.
   *
   * Performs client execution steps for on module init.
   */
  onModuleInit() {
    this.logger.log(
      'Initializing Real-Time Telemetry and Anti-Cheat pipeline...',
    );
    this.consumeTelemetry();

    // Periodically export aggregated system logs to metrics fanout exchange
    /**
     * Executes the standard set interval lifecycle step.
     *
     * Performs client execution steps for set interval.
     *
     * @param ( - The ( configuration payload.
     */
    setInterval(() => {
      this.publishPlatformMetrics();
    }, 2000);
  }

  /**
   * Executes the standard consume telemetry lifecycle step.
   *
   * Performs client execution steps for consume telemetry.
   */
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

  /**
   * Executes the standard process tick event lifecycle step.
   *
   * Performs client execution steps for process tick event.
   *
   * @param tickData - Parsed request data transfer object. (Type: any)
   */
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

  /**
   * Executes the standard publish platform metrics lifecycle step.
   *
   * Performs client execution steps for publish platform metrics.
   */
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
