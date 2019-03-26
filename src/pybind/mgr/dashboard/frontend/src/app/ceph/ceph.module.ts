import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { SharedModule } from '../shared/shared.module';
import { CephfsModule } from './cephfs/cephfs.module';
import { ClusterModule } from './cluster/cluster.module';
import { DashboardModule } from './dashboard/dashboard.module';
import { NfsModule } from './nfs/nfs.module';
import { PerformanceCounterModule } from './performance-counter/performance-counter.module';

@NgModule({
  imports: [
    CommonModule,
    ClusterModule,
    DashboardModule,
    PerformanceCounterModule,
    CephfsModule,
    NfsModule,
    SharedModule
  ],
  declarations: []
})
export class CephModule {}
