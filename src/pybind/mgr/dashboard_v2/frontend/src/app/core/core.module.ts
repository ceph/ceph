import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { AuthModule } from './auth/auth.module';
import { NavigationModule } from './navigation/navigation.module';

@NgModule({
  imports: [
    CommonModule,
    NavigationModule,
    AuthModule
  ],
  exports: [NavigationModule],
  declarations: []
})
export class CoreModule { }
