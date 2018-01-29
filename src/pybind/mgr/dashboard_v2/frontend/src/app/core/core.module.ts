import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NavigationModule } from './navigation/navigation.module';
import { AuthModule } from './auth/auth.module';

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
