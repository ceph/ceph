import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { SharedModule } from '../shared/shared.module';
import { BlockModule } from './block/block.module';
import { CephfsModule } from './cephfs/cephfs.module';
import { ClusterModule } from './cluster/cluster.module';
import { DashboardModule } from './dashboard/dashboard.module';
import { NfsModule } from './nfs/nfs.module';
import { PerformanceCounterModule } from './performance-counter/performance-counter.module';
import { PoolModule } from './pool/pool.module';
import { RgwModule } from './rgw/rgw.module';

@NgModule({
  imports: [
    CommonModule,
    ClusterModule,
    DashboardModule,
    RgwModule,
    PerformanceCounterModule,
    BlockModule,
    PoolModule,
    CephfsModule,
    NfsModule,
    SharedModule
  ],
  declarations: []
})
export class CephModule {}
