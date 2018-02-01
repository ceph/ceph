import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ComponentsModule } from './components/components.module';
import { PipesModule } from './pipes/pipes.module';
import { AuthGuardService } from './services/auth-guard.service';
import { AuthStorageService } from './services/auth-storage.service';
import { AuthService } from './services/auth.service';
import { HostService } from './services/host.service';
import { ServicesModule } from './services/services.module';

@NgModule({
  imports: [
    CommonModule,
    PipesModule,
    ComponentsModule,
    ServicesModule
  ],
  exports: [PipesModule, ServicesModule, PipesModule],
  declarations: [],
  providers: [
    AuthService,
    AuthStorageService,
    AuthGuardService,
    HostService
  ]
})
export class SharedModule {}
