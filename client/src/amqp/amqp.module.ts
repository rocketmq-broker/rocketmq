import { Global, Logger, Module, OnModuleDestroy } from '@nestjs/common';
import * as amqp from 'amqplib';
import {
  AMQP_URL,
  EXCHANGE_GAME,
  EXCHANGE_ANALYTICS,
  EXCHANGE_METRICS,
  EXCHANGE_DLX,
  EXCHANGE_MATCHMAKING,
  EXCHANGE_SECURITY,
  EXCHANGE_LEADERBOARD,
  QUEUE_SESSION_TICKS,
  QUEUE_SESSION_ACTIONS,
  QUEUE_TELEMETRY,
  QUEUE_ANTI_CHEAT,
  QUEUE_MATCHMAKING,
  QUEUE_METRICS_LOGS,
  QUEUE_DLQ,
  QUEUE_LOBBY_CREATED,
  QUEUE_SECURITY_BROADCAST,
  QUEUE_LEADERBOARD_UPDATES,
  QUEUE_LOBBY_MAINTENANCE,
  RK_SESSION_TICK_PATTERN,
  RK_SESSION_ACTION_PATTERN,
  RK_ALL_SESSION_EVENTS,
  RK_ANTI_CHEAT_ALERT,
  RK_MATCHMAKING_LOBBY,
  RK_LOBBY_CREATED,
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

        // ── Auto-declare Virtual Hosts via Management API before connecting ──
        const mgmtUrl = 'http://127.0.0.1:15672/api/vhosts/';
        const authHeader =
          'Basic ' + Buffer.from('guest:guest').toString('base64');

        for (const vhost of ['gaming', 'security', 'analytics']) {
          try {
            await fetch(`${mgmtUrl}${encodeURIComponent(vhost)}`, {
              method: 'PUT',
              headers: { Authorization: authHeader },
            });
            logger.log(`Declared virtual host: "${vhost}"`);
          } catch (err) {
            logger.warn(
              `Failed to auto-create vhost "${vhost}" (is management service up?): ${err.message}`,
            );
          }
        }

        const conn = await amqp.connect(AMQP_URL);
        logger.log(`Connected to AMQP at ${AMQP_URL}`);
        return conn;
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
export class AmqpModule implements OnModuleDestroy {
  async onModuleDestroy() {}
}
