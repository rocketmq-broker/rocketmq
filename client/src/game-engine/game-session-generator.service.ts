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
 * File: game-session-generator.service.ts
 * Description: Core game session state generator and simulation orchestrator.
 */

import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import {
  EXCHANGE_ANALYTICS,
  EXCHANGE_GAME,
  RK_MATCHMAKING_LOBBY,
} from '../amqp/constants';

/**
 * Structural definition for player.
 *
 * Defines schemas, types, or services for player inside the NestJS client.
 */
interface Player {
  id: string;
  name: string;
  x: number;
  y: number;
  velocity: number;
  health: number;
  score: number;
}

/**
 * Structural definition for game session.
 *
 * Defines schemas, types, or services for game session inside the NestJS client.
 */
interface GameSession {
  id: string;
  map: string;
  players: Player[];
  tickCount: number;
}

@Injectable()
/**
 * Service class managing game session generator service operations.
 *
 * Defines schemas, types, or services for game session generator service inside the NestJS client.
 */
export class GameSessionGeneratorService implements OnModuleInit {
  private readonly logger = new Logger('GameSessionGenerator');
  private sessions: GameSession[] = [];

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {
    // Initialize standard game sessions
    this.sessions = [
      {
        id: 'lobby-neon-city',
        map: 'Neon City Arena',
        players: [
          {
            id: 'usr-1',
            name: 'CyberStriker',
            x: 120.0,
            y: 350.5,
            velocity: 4.5,
            health: 100,
            score: 0,
          },
          {
            id: 'usr-2',
            name: 'PixelNinja',
            x: 230.1,
            y: 110.2,
            velocity: 5.0,
            health: 85,
            score: 10,
          },
          {
            id: 'usr-3',
            name: 'VoidWalker',
            x: 450.4,
            y: 220.8,
            velocity: 3.8,
            health: 100,
            score: 5,
          },
        ],
        tickCount: 0,
      },
      {
        id: 'lobby-outer-rim',
        map: 'Asteroid Belt Station',
        players: [
          {
            id: 'usr-4',
            name: 'NovaBlast',
            x: 10.0,
            y: 15.0,
            velocity: 6.2,
            health: 90,
            score: 15,
          },
          {
            id: 'usr-5',
            name: 'ZeroG',
            x: -50.0,
            y: 88.0,
            velocity: 5.5,
            health: 100,
            score: 20,
          },
        ],
        tickCount: 0,
      },
    ];
  }

  /**
   * Executes the standard on module init lifecycle step.
   *
   * Performs client execution steps for on module init.
   */
  onModuleInit() {
    this.logger.log(
      'Starting live multiplayer Game Session Loop (running forever)...',
    );

    // Core game loop
    /**
     * Executes the standard set interval lifecycle step.
     *
     * Performs client execution steps for set interval.
     *
     * @param ( - The ( configuration payload.
     */
    setInterval(() => {
      this.generateGameTicks();
    }, 400);

    // Occasional special matchmaker actions
    /**
     * Executes the standard set interval lifecycle step.
     *
     * Performs client execution steps for set interval.
     *
     * @param ( - The ( configuration payload.
     */
    setInterval(() => {
      this.triggerMatchmakingRequest();
    }, 4000);
  }

  /**
   * Executes the standard generate game ticks lifecycle step.
   *
   * Performs client execution steps for generate game ticks.
   */
  private async generateGameTicks() {
    for (const session of this.sessions) {
      session.tickCount++;

      // Update players position (random movement simulation)
      for (const player of session.players) {
        // Normal movement update
        const deltaX = (Math.random() - 0.5) * 15;
        const deltaY = (Math.random() - 0.5) * 15;
        player.x += deltaX;
        player.y += deltaY;

        // Occasional velocity burst (some could trigger anti-cheat limits!)
        if (Math.random() < 0.05) {
          player.velocity = 12.0 + Math.random() * 20.0; // Dynamic high velocity burst!
        } else {
          player.velocity = 4.0 + Math.random() * 2.0;
        }

        // Simulating health changes and scoring
        if (Math.random() < 0.03) {
          player.health = Math.max(
            0,
            player.health - Math.floor(Math.random() * 20),
          );
          if (player.health === 0) {
            player.health = 100; // Respawn
            player.score = Math.max(0, player.score - 5);
          }
        }
      }

      // 1. Publish standard server session Tick Event
      const tickPayload = {
        sessionId: session.id,
        map: session.map,
        timestamp: Date.now(),
        tick: session.tickCount,
        players: session.players,
      };

      const tickKey = `game.session.${session.id}.tick`;
      this.ch.publish(
        EXCHANGE_GAME,
        tickKey,
        Buffer.from(JSON.stringify(tickPayload)),
        { expiration: '5000' }, // Ticks expire quickly to prevent backlog
      );

      // 2. Occasionally publish Action Event (Fires, Frags, Join, Chat)
      if (Math.random() < 0.3) {
        const actingPlayer =
          session.players[Math.floor(Math.random() * session.players.length)];
        const actionType =
          Math.random() < 0.6 ? 'Shoot' : Math.random() < 0.8 ? 'Jump' : 'Frag';

        const actionPayload = {
          sessionId: session.id,
          timestamp: Date.now(),
          player: { id: actingPlayer.id, name: actingPlayer.name },
          action: actionType,
          coordinates: { x: actingPlayer.x, y: actingPlayer.y },
        };

        if (actionType === 'Frag') {
          actingPlayer.score += 10;
        }

        const actionKey = `game.session.${session.id}.action`;
        this.ch.publish(
          EXCHANGE_GAME,
          actionKey,
          Buffer.from(JSON.stringify(actionPayload)),
        );
      }
    }
  }

  /**
   * Executes the standard trigger matchmaking request lifecycle step.
   *
   * Performs client execution steps for trigger matchmaking request.
   */
  private async triggerMatchmakingRequest() {
    const randomUser = `Player_${Math.floor(Math.random() * 9000 + 1000)}`;
    const mmPayload = {
      timestamp: Date.now(),
      requestId: crypto.randomUUID(),
      player: {
        username: randomUser,
        ping: Math.floor(Math.random() * 80 + 10),
        skillRating: Math.floor(Math.random() * 1500 + 1000),
      },
      preferredRegions: ['us-east', 'eu-west'],
    };

    this.logger.log(`Matching new lobby candidate: ${randomUser} ...`);
    this.ch.publish(
      EXCHANGE_ANALYTICS,
      RK_MATCHMAKING_LOBBY,
      Buffer.from(JSON.stringify(mmPayload)),
    );
  }
}
