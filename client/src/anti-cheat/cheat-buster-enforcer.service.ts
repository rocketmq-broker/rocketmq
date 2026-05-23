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
 * File: cheat-buster-enforcer.service.ts
 * Description: Anti-cheat enforcement, player validation, and telemetry analysis.
 */

import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import { EXCHANGE_SECURITY, QUEUE_ANTI_CHEAT } from '../amqp/constants';

@Injectable()
/**
 * Service class managing cheat buster enforcer service operations.
 *
 * Defines schemas, types, or services for cheat buster enforcer service inside the NestJS client.
 */
export class CheatBusterEnforcerService implements OnModuleInit {
  private readonly logger = new Logger('CheatBusterEnforcer');
  private bannedUsersList = new Set<string>();

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  /**
   * Executes the standard on module init lifecycle step.
   *
   * Performs client execution steps for on module init.
   */
  onModuleInit() {
    this.logger.log('Anti-Cheat Mitigation Enforcer active (subscribing)...');
    this.ch.consume(QUEUE_ANTI_CHEAT, (msg) => {
      if (!msg) return;

      try {
        const alert = JSON.parse(msg.content.toString());
        const suspectName = alert.suspect.name;

        if (!this.bannedUsersList.has(suspectName)) {
          this.bannedUsersList.add(suspectName);
          this.logger.warn(
            `🔨 BANHAMMER: Flagging player "${suspectName}" for automatic cluster eviction.`,
          );

          const banPayload = {
            event: 'PLAYER_BAN',
            player: suspectName,
            reason: `Speedhack detected (${alert.suspect.velocity.toFixed(1)} units/sec)`,
            timestamp: Date.now(),
          };

          // Broadcast ban over security fanout exchange
          this.ch.publish(
            EXCHANGE_SECURITY,
            '',
            Buffer.from(JSON.stringify(banPayload)),
          );
        }

        this.ch.ack(msg);
      } catch (err) {
        this.ch.ack(msg);
      }
    });
  }
}
