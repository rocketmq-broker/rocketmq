import { Module } from '@nestjs/common';
import { LobbyMaintenanceService } from './lobby-maintenance.service';

@Module({
  providers: [LobbyMaintenanceService],
  exports: [LobbyMaintenanceService],
})
export class LobbyMaintenanceModule {}
