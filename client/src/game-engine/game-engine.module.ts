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
 * File: game-engine.module.ts
 * Description: Core game session state generator and simulation orchestrator.
 */

import { Module } from '@nestjs/common';
import { GameSessionGeneratorService } from './game-session-generator.service';

@Module({
  providers: [GameSessionGeneratorService],
  exports: [GameSessionGeneratorService],
})
/**
 * Service class managing game engine module operations.
 *
 * Defines schemas, types, or services for game engine module inside the NestJS client.
 */
export class GameEngineModule {}
