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
 * File: live-alert.service.ts
 * Description: Real-time system alert broadcast and notification dispatch service.
 */

import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import {
  QUEUE_ANTI_CHEAT,
  QUEUE_MATCHMAKING,
  QUEUE_METRICS_LOGS,
} from '../amqp/constants';

@Injectable()
/**
 * Dispatches system alerts and real-time status updates via WebSocket or AMQP.
 *
 * Dispatches system alerts and real-time status updates via WebSocket or AMQP.
 */
export class LiveAlertService implements OnModuleInit {
  private readonly logger = new Logger('LiveAlertGateway');

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  /**
   * Executes the standard on module init lifecycle step.
   *
   * Performs client execution steps for on module init.
   */
  onModuleInit() {
    this.logger.log('Starting Live Alert and Broadcast Gateway (listening)...');
    this.consumeMatchmaking();
    this.consumeAntiCheatAlerts();
    this.consumeSystemMetrics();
  }

  /**
   * Executes the standard consume matchmaking lifecycle step.
   *
   * Performs client execution steps for consume matchmaking.
   */
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

  /**
   * Executes the standard consume anti cheat alerts lifecycle step.
   *
   * Performs client execution steps for consume anti cheat alerts.
   */
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

  /**
   * Executes the standard consume system metrics lifecycle step.
   *
   * Performs client execution steps for consume system metrics.
   */
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
