import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { ComponentsModule } from '../../shared/components/components.module';
import { SharedModule } from '../../shared/shared.module';
import { HostsComponent } from './hosts/hosts.component';
import { MonitorService } from './monitor.service';
import { MonitorComponent } from './monitor/monitor.component';

@NgModule({
  imports: [
    CommonModule,
    ComponentsModule,
    SharedModule,
    RouterModule
  ],
  declarations: [
    HostsComponent,
    MonitorComponent,
  ],
  providers: [
    MonitorService
  ]
})
export class ClusterModule {}
