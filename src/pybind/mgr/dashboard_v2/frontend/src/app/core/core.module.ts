import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { CoreRoutingModule } from './core-routing.module';
import { NavigationModule } from './navigation/navigation.module';

@NgModule({
  imports: [
    CommonModule,
    CoreRoutingModule,
    NavigationModule
  ],
  exports: [NavigationModule],
  declarations: []
})
export class CoreModule { }
