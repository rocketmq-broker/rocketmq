import { Module } from '@nestjs/common';
import { CheatBusterEnforcerService } from './cheat-buster-enforcer.service';

@Module({
  providers: [CheatBusterEnforcerService],
  exports: [CheatBusterEnforcerService],
})
export class AntiCheatModule {}
