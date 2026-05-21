import { Module } from '@nestjs/common';
import { GameSessionGeneratorService } from './game-session-generator.service';

@Module({
  providers: [GameSessionGeneratorService],
  exports: [GameSessionGeneratorService],
})
export class GameEngineModule {}
