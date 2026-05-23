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
 * File: system-monitor.module.ts
 * Description: System resource telemetry, JVM/process metric gathering, and cache eviction.
 */

import { Module } from '@nestjs/common';
import { SystemMetricsEvictionService } from './system-metrics-eviction.service';

@Module({
  providers: [SystemMetricsEvictionService],
  exports: [SystemMetricsEvictionService],
})
/**
 * Service class managing system monitor module operations.
 *
 * Defines schemas, types, or services for system monitor module inside the NestJS client.
 */
export class SystemMonitorModule {}
