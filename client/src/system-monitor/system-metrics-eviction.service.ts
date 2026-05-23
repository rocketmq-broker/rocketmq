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
 * File: system-metrics-eviction.service.ts
 * Description: System resource telemetry, JVM/process metric gathering, and cache eviction.
 */

import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import { QUEUE_METRICS_LOGS } from '../amqp/constants';

@Injectable()
/**
 * Monitors system resources and evicts expired telemetry to prevent out-of-memory errors.
 *
 * Monitors system resources and evicts expired telemetry to prevent out-of-memory errors.
 */
export class SystemMetricsEvictionService implements OnModuleInit {
  private readonly logger = new Logger('SystemMetricsEviction');

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  /**
   * Executes the standard on module init lifecycle step.
   *
   * Performs client execution steps for on module init.
   */
  onModuleInit() {
    this.logger.log('Starting System Metrics anomaly & DLQ inspector...');
    this.ch.consume(QUEUE_METRICS_LOGS, (msg) => {
      if (!msg) return;

      try {
        const metrics = JSON.parse(msg.content.toString());
        const cpu = metrics.cpuUtilization || 0;

        if (cpu > 18.0) {
          // Anomaly detected! Let's reject this message so it propagates to the Dead Letter Queue!
          this.logger.warn(
            `⚠️ CPU Utilization Alert (${cpu.toFixed(1)}%). Releasing telemetry payload directly to the Dead Letter Exchange!`,
          );

          // Nack with requeue=false -> forces dead-lettering
          this.ch.nack(msg, false, false);
        } else {
          this.logger.log(`[MONITOR] Telemetry OK (CPU: ${cpu.toFixed(1)}%).`);
          this.ch.ack(msg);
        }
      } catch (err) {
        this.ch.nack(msg, false, false);
      }
    });
  }
}
