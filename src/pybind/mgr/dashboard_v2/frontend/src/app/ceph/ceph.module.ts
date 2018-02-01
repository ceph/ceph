import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ClusterModule } from './cluster/cluster.module';
import { DashboardModule } from './dashboard/dashboard.module';

@NgModule({
  imports: [
    CommonModule,
    ClusterModule,
    DashboardModule
  ],
  declarations: []
})
export class CephModule { }
