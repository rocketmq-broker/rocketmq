import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import { QUEUE_ANTI_CHEAT, EXCHANGE_SECURITY } from '../amqp/constants';

@Injectable()
export class CheatBusterEnforcerService implements OnModuleInit {
  private readonly logger = new Logger('CheatBusterEnforcer');
  private bannedUsersList = new Set<string>();

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

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
