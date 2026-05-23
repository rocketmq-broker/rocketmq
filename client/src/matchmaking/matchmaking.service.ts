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
 * File: matchmaking.service.ts
 * Description: Client side matchmaking service and game queue manager.
 */

import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import {
  EXCHANGE_MATCHMAKING,
  QUEUE_MATCHMAKING,
  RK_LOBBY_CREATED,
} from '../amqp/constants';

@Injectable()
/**
 * Manages the player matchmaking queue, pairing players, and creating game sessions.
 *
 * Manages the player matchmaking queue, pairing players, and creating game sessions.
 */
export class MatchmakingService implements OnModuleInit {
  private readonly logger = new Logger('MatchmakingService');
  private matchQueue: any[] = [];

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  /**
   * Executes the standard on module init lifecycle step.
   *
   * Performs client execution steps for on module init.
   */
  onModuleInit() {
    this.logger.log('Starting Matchmaker consumer...');
    this.ch.consume(QUEUE_MATCHMAKING, (msg) => {
      if (!msg) return;

      try {
        const candidate = JSON.parse(msg.content.toString());
        this.logger.log(
          `Enqueuing candidate: ${candidate.player.username} into match pool.`,
        );

        this.matchQueue.push(candidate);

        // If we have 3 players in queue, match them into a new lobby!
        if (this.matchQueue.length >= 3) {
          const matchedGroup = this.matchQueue.splice(0, 3);
          this.createLobby(matchedGroup);
        }

        this.ch.ack(msg);
      } catch (err) {
        this.ch.ack(msg);
      }
    });
  }

  /**
   * Executes the standard create lobby lifecycle step.
   *
   * Performs client execution steps for create lobby.
   *
   * @param players - The players configuration payload. (Type: any[])
   */
  private createLobby(players: any[]) {
    const lobbyId = `matched-lobby-${Math.floor(Math.random() * 90000 + 10000)}`;
    this.logger.log(`✨ LOBBY CREATED: Grouped 3 players into "${lobbyId}"`);

    const lobbyPayload = {
      lobbyId,
      timestamp: Date.now(),
      players: players.map((p) => p.player),
      region: 'us-east-1',
    };

    // Publish to matchmaking exchange
    this.ch.publish(
      EXCHANGE_MATCHMAKING,
      RK_LOBBY_CREATED,
      Buffer.from(JSON.stringify(lobbyPayload)),
    );
  }
}
