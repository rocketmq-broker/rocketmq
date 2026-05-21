import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import {
  QUEUE_MATCHMAKING,
  EXCHANGE_MATCHMAKING,
  RK_LOBBY_CREATED,
} from '../amqp/constants';

@Injectable()
export class MatchmakingService implements OnModuleInit {
  private readonly logger = new Logger('MatchmakingService');
  private matchQueue: any[] = [];

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

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
