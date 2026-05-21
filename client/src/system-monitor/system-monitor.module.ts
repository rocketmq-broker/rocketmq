import { Module } from '@nestjs/common';
import { SystemMetricsEvictionService } from './system-metrics-eviction.service';

@Module({
  providers: [SystemMetricsEvictionService],
  exports: [SystemMetricsEvictionService],
})
export class SystemMonitorModule {}
