import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ClusterModule } from './cluster/cluster.module';

@NgModule({
  imports: [
    CommonModule,
    ClusterModule
  ],
  declarations: []
})
export class CephModule { }
