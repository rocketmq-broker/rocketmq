import { Module } from '@nestjs/common';
import { LiveAlertService } from './live-alert.service';

@Module({
  providers: [LiveAlertService],
  exports: [LiveAlertService],
})
export class LiveAlertModule {}
