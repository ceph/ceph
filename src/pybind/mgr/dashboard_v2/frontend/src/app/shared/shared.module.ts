import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AuthService } from './services/auth.service';
import { AuthStorageService } from './services/auth-storage.service';
import { AuthGuardService } from './services/auth-guard.service';
import { PipesModule } from './pipes/pipes.module';
import { HostService } from './services/host.service';

@NgModule({
  imports: [
    CommonModule,
    PipesModule
  ],
  declarations: [],
  providers: [
    AuthService,
    AuthStorageService,
    AuthGuardService,
    HostService
  ],
  exports: [
    PipesModule
  ]
})
export class SharedModule { }
