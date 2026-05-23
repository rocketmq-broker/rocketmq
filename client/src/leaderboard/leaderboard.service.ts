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
 * File: leaderboard.service.ts
 * Description: Leaderboard ranking and player score tracking service.
 */

import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import {
  EXCHANGE_LEADERBOARD,
  QUEUE_SESSION_ACTIONS,
  RK_LEADERBOARD_GLOBAL,
} from '../amqp/constants';

@Injectable()
/**
 * Tracks and updates player scores, ranks, and leaderboard standings.
 *
 * Tracks and updates player scores, ranks, and leaderboard standings.
 */
export class LeaderboardService implements OnModuleInit {
  private readonly logger = new Logger('LeaderboardService');
  private scores: { [username: string]: number } = {
    CyberStriker: 15,
    PixelNinja: 32,
    VoidWalker: 8,
    NovaBlast: 45,
    ZeroG: 50,
  };

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  /**
   * Executes the standard on module init lifecycle step.
   *
   * Performs client execution steps for on module init.
   */
  onModuleInit() {
    this.logger.log('Starting Leaderboard rankings processor...');

    // Consume actions to dynamically update ranking scores
    this.ch.consume(QUEUE_SESSION_ACTIONS, (msg) => {
      if (!msg) return;

      try {
        const actionEvent = JSON.parse(msg.content.toString());
        const pName = actionEvent.player.name;
        const act = actionEvent.action;

        if (pName) {
          if (!this.scores[pName]) this.scores[pName] = 0;

          if (act === 'Frag') {
            this.scores[pName] += 10;
          } else if (act === 'Shoot') {
            this.scores[pName] += 1;
          }
        }

        this.ch.ack(msg);
      } catch (err) {
        this.ch.ack(msg);
      }
    });

    // Periodically broadcast updated global rankings
    /**
     * Executes the standard set interval lifecycle step.
     *
     * Performs client execution steps for set interval.
     *
     * @param ( - The ( configuration payload.
     */
    setInterval(() => {
      this.broadcastRankings();
    }, 3500);
  }

  /**
   * Executes the standard broadcast rankings lifecycle step.
   *
   * Performs client execution steps for broadcast rankings.
   */
  private broadcastRankings() {
    const sortedRankings = Object.entries(this.scores)
      .map(([name, score]) => ({ name, score }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 5);

    this.logger.log(
      `🏆 Broadcast high scores: 1st: ${sortedRankings[0]?.name} (${sortedRankings[0]?.score})`,
    );

    const leaderboardPayload = {
      timestamp: Date.now(),
      rankings: sortedRankings,
    };

    // Publish to topic exchange with routing key leaderboard.global.rankings
    this.ch.publish(
      EXCHANGE_LEADERBOARD,
      RK_LEADERBOARD_GLOBAL,
      Buffer.from(JSON.stringify(leaderboardPayload)),
    );
  }
}
