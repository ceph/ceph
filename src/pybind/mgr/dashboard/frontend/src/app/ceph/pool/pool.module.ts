import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { ServicesModule } from '../../shared/services/services.module';
import { SharedModule } from '../../shared/shared.module';
import { PoolListComponent } from './pool-list/pool-list.component';

@NgModule({
  imports: [
    CommonModule,
    TabsModule,
    SharedModule,
    ServicesModule
  ],
  exports: [
    PoolListComponent
  ],
  declarations: [
    PoolListComponent
  ]
})
export class PoolModule { }
