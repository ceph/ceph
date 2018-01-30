import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ClusterModule } from './cluster/cluster.module';

@NgModule({
  imports: [
    CommonModule,
    ClusterModule
  ],
  declarations: []
})
export class CephModule { }
