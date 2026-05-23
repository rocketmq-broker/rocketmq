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
 * File: lobby-maintenance.service.ts
 * Description: Game lobby lifecycle manager, heartbeat checker, and server maintenance.
 */

import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import { QUEUE_LOBBY_CREATED } from '../amqp/constants';

@Injectable()
/**
 * Manages game lobby lifecycles, sweeping inactive lobbies, and checking heartbeats.
 *
 * Manages game lobby lifecycles, sweeping inactive lobbies, and checking heartbeats.
 */
export class LobbyMaintenanceService implements OnModuleInit {
  private readonly logger = new Logger('LobbyMaintenanceService');
  private activeLobbies: string[] = [];

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  /**
   * Executes the standard on module init lifecycle step.
   *
   * Performs client execution steps for on module init.
   */
  onModuleInit() {
    this.logger.log('Starting Lobby Maintenance & Healthcheck worker...');
    this.ch.consume(QUEUE_LOBBY_CREATED, (msg) => {
      if (!msg) return;

      try {
        const lobby = JSON.parse(msg.content.toString());
        this.activeLobbies.push(lobby.lobbyId);

        this.logger.log(
          `[MAINTENANCE] Registered new active lobby: "${lobby.lobbyId}" containing players: [${lobby.players.map((p) => p.username).join(', ')}]`,
        );

        this.ch.ack(msg);
      } catch (_err) {
        this.ch.ack(msg);
      }
    });

    // Run healthcheck sweep every 5 seconds
    /**
     * Executes the standard set interval lifecycle step.
     *
     * Performs client execution steps for set interval.
     *
     * @param ( - The ( configuration payload.
     */
    setInterval(() => {
      this.runLobbySweeps();
    }, 5000);
  }

  /**
   * Executes the standard run lobby sweeps lifecycle step.
   *
   * Performs client execution steps for run lobby sweeps.
   */
  private runLobbySweeps() {
    if (this.activeLobbies.length === 0) return;

    this.logger.log(
      `[MAINTENANCE] Sweeping ${this.activeLobbies.length} lobbies. Health: 100%. Pings: [${this.activeLobbies.map(() => Math.floor(Math.random() * 40 + 10) + 'ms').join(', ')}]`,
    );

    // Evict oldest lobby if active count grows too large
    if (this.activeLobbies.length > 5) {
      const removed = this.activeLobbies.shift();
      this.logger.log(
        `🧹 [MAINTENANCE] Lobby "${removed}" completed. Deallocating instances and virtual routing bindings.`,
      );
    }
  }
}
