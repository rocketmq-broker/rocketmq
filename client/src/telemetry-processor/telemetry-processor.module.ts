import { Module } from '@nestjs/common';
import { TelemetryProcessorService } from './telemetry-processor.service';

@Module({
  providers: [TelemetryProcessorService],
  exports: [TelemetryProcessorService],
})
export class TelemetryProcessorModule {}
