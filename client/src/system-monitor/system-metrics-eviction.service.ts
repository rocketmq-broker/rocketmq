import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL } from '../amqp/amqp.module';
import { QUEUE_METRICS_LOGS } from '../amqp/constants';

@Injectable()
export class SystemMetricsEvictionService implements OnModuleInit {
  private readonly logger = new Logger('SystemMetricsEviction');

  constructor(@Inject(AMQP_CHANNEL) private readonly ch: amqp.Channel) {}

  onModuleInit() {
    this.logger.log('Starting System Metrics anomaly & DLQ inspector...');
    this.ch.consume(QUEUE_METRICS_LOGS, (msg) => {
      if (!msg) return;

      try {
        const metrics = JSON.parse(msg.content.toString());
        const cpu = metrics.cpuUtilization || 0;

        if (cpu > 18.0) {
          // Anomaly detected! Let's reject this message so it propagates to the Dead Letter Queue!
          this.logger.warn(
            `⚠️ CPU Utilization Alert (${cpu.toFixed(1)}%). Releasing telemetry payload directly to the Dead Letter Exchange!`,
          );

          // Nack with requeue=false -> forces dead-lettering
          this.ch.nack(msg, false, false);
        } else {
          this.logger.log(`[MONITOR] Telemetry OK (CPU: ${cpu.toFixed(1)}%).`);
          this.ch.ack(msg);
        }
      } catch (err) {
        this.ch.nack(msg, false, false);
      }
    });
  }
}
