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
 * File: amqp.module.ts
 * Description: NestJS AMQP client integration and message broker gateway.
 */

import { Global, Logger, Module, OnModuleDestroy } from '@nestjs/common';
import * as amqp from 'amqplib';
import * as fs from 'fs';
import {
  EXCHANGE_ANALYTICS,
  EXCHANGE_DLX,
  EXCHANGE_GAME,
  EXCHANGE_LEADERBOARD,
  EXCHANGE_MATCHMAKING,
  EXCHANGE_METRICS,
  EXCHANGE_SECURITY,
  QUEUE_ANTI_CHEAT,
  QUEUE_DLQ,
  QUEUE_LEADERBOARD_UPDATES,
  QUEUE_LOBBY_CREATED,
  QUEUE_LOBBY_MAINTENANCE,
  QUEUE_MATCHMAKING,
  QUEUE_METRICS_LOGS,
  QUEUE_SECURITY_BROADCAST,
  QUEUE_SESSION_ACTIONS,
  QUEUE_SESSION_TICKS,
  QUEUE_TELEMETRY,
  RK_ALL_SESSION_EVENTS,
  RK_ANTI_CHEAT_ALERT,
  RK_LOBBY_CREATED,
  RK_MATCHMAKING_LOBBY,
  RK_SESSION_ACTION_PATTERN,
  RK_SESSION_TICK_PATTERN,
} from './constants';

export const AMQP_CONNECTION = 'AMQP_CONNECTION';
export const AMQP_CHANNEL = 'AMQP_CHANNEL';

@Global()
@Module({
  providers: [
    {
      provide: AMQP_CONNECTION,
      useFactory: async () => {
        const logger = new Logger('AmqpModule');

        // ── Cluster node endpoints for failover ──
        const isTls = (process.env.AMQP_URL || '').startsWith('amqps:');
        const clusterNodes = [
          process.env.AMQP_URL ||
            (isTls
              ? 'amqps://guest:guest@127.0.0.1:5675/'
              : 'amqp://guest:guest@127.0.0.1:5672/'),
          isTls
            ? 'amqps://guest:guest@127.0.0.1:5675/'
            : 'amqp://guest:guest@127.0.0.1:5672/',
          isTls
            ? 'amqps://guest:guest@127.0.0.1:5676/'
            : 'amqp://guest:guest@127.0.0.1:5673/',
          isTls
            ? 'amqps://guest:guest@127.0.0.1:5677/'
            : 'amqp://guest:guest@127.0.0.1:5674/',
        ];

        // ── Auto-declare Virtual Hosts via any reachable Management API ──
        const mgmtPorts = [15672, 15673, 15674];
        const authHeader =
          'Basic ' + Buffer.from('guest:guest').toString('base64');

        for (const port of mgmtPorts) {
          let reachable = false;
          for (const vhost of ['gaming', 'security', 'analytics']) {
            try {
              await fetch(
                `http://127.0.0.1:${port}/api/vhosts/${encodeURIComponent(vhost)}`,
                {
                  method: 'PUT',
                  headers: { Authorization: authHeader },
                  signal: AbortSignal.timeout(1000),
                },
              );
              reachable = true;
              logger.log(
                `Declared virtual host: "${vhost}" via mgmt port ${port}`,
              );
            } catch {
              // Try next port
            }
          }
          if (reachable) break;
        }

        // ── Failover connection: try each cluster node ──
        const connectWithFailover = async (): Promise<any> => {
          for (const url of clusterNodes) {
            try {
              const connectOptions: any = {};
              if (url.startsWith('amqps:')) {
                const caPath = process.env.CA_CERT_PATH || 'data/tls/ca.pem';
                if (fs.existsSync(caPath)) {
                  connectOptions.ca = [fs.readFileSync(caPath)];
                  logger.log(
                    `Loaded Root CA from ${caPath} for secure TLS verification`,
                  );
                } else {
                  // Fallback: allow self-signed/untrusted certs in development if CA is not configured
                  connectOptions.rejectUnauthorized = false;
                  logger.warn(
                    `CA cert not found at ${caPath}, using rejectUnauthorized=false fallback`,
                  );
                }
              }

              const conn = await amqp.connect(url, connectOptions);
              logger.log(`Connected to AMQP cluster node at ${url}`);

              // Auto-reconnect on unexpected close
              conn.on('close', () => {
                logger.warn(
                  `AMQP connection to ${url} closed, reconnecting in 2s...`,
                );
                /**
                 * Executes the standard set timeout lifecycle step.
                 *
                 * Performs client execution steps for set timeout.
                 *
                 * @param ( - The ( configuration payload.
                 */
                setTimeout(() => {
                  /**
                   * Executes the standard connect with failover lifecycle step.
                   *
                   * Performs client execution steps for connect with failover.
                   */
                  connectWithFailover().catch((err) => {
                    logger.error(`Reconnect failed: ${err.message}`);
                  });
                }, 2000);
              });

              conn.on('error', (err) => {
                logger.error(`AMQP connection error: ${err.message}`);
              });

              return conn;
            } catch (err) {
              logger.warn(
                `Failed to connect to ${url}: ${err.message}, trying next node...`,
              );
            }
          }
          throw new Error(
            'All AMQP cluster nodes unreachable: ' + clusterNodes.join(', '),
          );
        };

        return connectWithFailover();
      },
    },
    {
      provide: AMQP_CHANNEL,
      useFactory: async (conn: amqp.ChannelModel) => {
        const logger = new Logger('AmqpModule');
        const ch = await conn.createChannel();
        await ch.prefetch(100); // High prefetch for game telemetry processing

        // ── Dead Letter Exchange ──────────────────────
        await ch.assertExchange(EXCHANGE_DLX, 'direct', { durable: true });
        await ch.assertQueue(QUEUE_DLQ, { durable: true });
        await ch.bindQueue(QUEUE_DLQ, EXCHANGE_DLX, 'dead');

        const queueArgs = {
          'x-dead-letter-exchange': EXCHANGE_DLX,
          'x-dead-letter-routing-key': 'dead',
        };

        // ── Game Event Exchange (Topic) ───────────────
        await ch.assertExchange(EXCHANGE_GAME, 'topic', { durable: true });

        await ch.assertQueue(QUEUE_SESSION_TICKS, {
          durable: true,
          arguments: queueArgs,
        });
        await ch.assertQueue(QUEUE_SESSION_ACTIONS, {
          durable: true,
          arguments: queueArgs,
        });
        await ch.assertQueue(QUEUE_TELEMETRY, {
          durable: true,
          arguments: queueArgs,
        });

        await ch.bindQueue(
          QUEUE_SESSION_TICKS,
          EXCHANGE_GAME,
          RK_SESSION_TICK_PATTERN,
        );
        await ch.bindQueue(
          QUEUE_SESSION_ACTIONS,
          EXCHANGE_GAME,
          RK_SESSION_ACTION_PATTERN,
        );
        await ch.bindQueue(
          QUEUE_TELEMETRY,
          EXCHANGE_GAME,
          RK_ALL_SESSION_EVENTS,
        );

        // ── Analytics Exchange (Direct) ───────────────
        await ch.assertExchange(EXCHANGE_ANALYTICS, 'direct', {
          durable: true,
        });

        await ch.assertQueue(QUEUE_ANTI_CHEAT, {
          durable: true,
          arguments: queueArgs,
        });
        await ch.assertQueue(QUEUE_MATCHMAKING, {
          durable: true,
          arguments: queueArgs,
        });

        await ch.bindQueue(
          QUEUE_ANTI_CHEAT,
          EXCHANGE_ANALYTICS,
          RK_ANTI_CHEAT_ALERT,
        );
        await ch.bindQueue(
          QUEUE_MATCHMAKING,
          EXCHANGE_ANALYTICS,
          RK_MATCHMAKING_LOBBY,
        );

        // ── Metrics Exchange (Fanout) ─────────────────
        await ch.assertExchange(EXCHANGE_METRICS, 'fanout', { durable: true });
        await ch.assertQueue(QUEUE_METRICS_LOGS, {
          durable: true,
          arguments: queueArgs,
        });
        await ch.bindQueue(QUEUE_METRICS_LOGS, EXCHANGE_METRICS, '');

        // ── Matchmaking Events (Direct) ───────────────
        await ch.assertExchange(EXCHANGE_MATCHMAKING, 'direct', {
          durable: true,
        });
        await ch.assertQueue(QUEUE_LOBBY_CREATED, {
          durable: true,
          arguments: queueArgs,
        });
        await ch.bindQueue(
          QUEUE_LOBBY_CREATED,
          EXCHANGE_MATCHMAKING,
          RK_LOBBY_CREATED,
        );

        // ── Security Broadcast (Fanout) ────────────────
        await ch.assertExchange(EXCHANGE_SECURITY, 'fanout', { durable: true });
        await ch.assertQueue(QUEUE_SECURITY_BROADCAST, {
          durable: true,
          arguments: queueArgs,
        });
        await ch.bindQueue(QUEUE_SECURITY_BROADCAST, EXCHANGE_SECURITY, '');

        // ── Leaderboard Exchange (Topic) ──────────────
        await ch.assertExchange(EXCHANGE_LEADERBOARD, 'topic', {
          durable: true,
        });
        await ch.assertQueue(QUEUE_LEADERBOARD_UPDATES, {
          durable: true,
          arguments: queueArgs,
        });
        await ch.bindQueue(
          QUEUE_LEADERBOARD_UPDATES,
          EXCHANGE_LEADERBOARD,
          'leaderboard.global.*',
        );

        // ── Lobby Maintenance (Direct) ────────────────
        await ch.assertQueue(QUEUE_LOBBY_MAINTENANCE, {
          durable: true,
          arguments: queueArgs,
        });

        await ch.bindQueue(
          QUEUE_LOBBY_MAINTENANCE,
          EXCHANGE_MATCHMAKING,
          'lobby.maintenance',
        );

        logger.log(
          'AMQP Game Topology Declared (7 exchanges, 11 queues, DLX, 3 vhosts)',
        );
        return ch;
      },
      inject: [AMQP_CONNECTION],
    },
  ],
  exports: [AMQP_CONNECTION, AMQP_CHANNEL],
})

/**
 * Service class managing amqp module operations.
 *
 * Defines schemas, types, or services for amqp module inside the NestJS client.
 */
export class AmqpModule implements OnModuleDestroy {
  /**
   * Executes the standard on module destroy lifecycle step.
   *
   * Performs client execution steps for on module destroy.
   */
  async onModuleDestroy() {}
}
