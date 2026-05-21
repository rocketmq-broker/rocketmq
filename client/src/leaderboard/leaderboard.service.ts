import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import {
  QUEUE_SESSION_ACTIONS,
  EXCHANGE_LEADERBOARD,
  RK_LEADERBOARD_GLOBAL,
} from '../amqp/constants';

@Injectable()
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
    setInterval(() => {
      this.broadcastRankings();
    }, 3500);
  }

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
