import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AuthService } from './services/auth.service';
import { AuthStorageService } from './services/auth-storage.service';
import { AuthGuardService } from './services/auth-guard.service';
import { EmptyComponent } from './empty/empty.component';

@NgModule({
  imports: [
    CommonModule
  ],
  declarations: [EmptyComponent],
  providers: [
    AuthService,
    AuthStorageService,
    AuthGuardService
  ]
})
export class SharedModule { }
