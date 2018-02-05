import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { BlockModule } from './block/block.module';
import { ClusterModule } from './cluster/cluster.module';
import { DashboardModule } from './dashboard/dashboard.module';
import { RgwModule } from './rgw/rgw.module';

@NgModule({
  imports: [
    CommonModule,
    ClusterModule,
    DashboardModule,
    RgwModule,
    BlockModule
  ],
  declarations: []
})
export class CephModule { }
