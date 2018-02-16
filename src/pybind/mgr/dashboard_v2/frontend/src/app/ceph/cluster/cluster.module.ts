import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { ComponentsModule } from '../../shared/components/components.module';
import { SharedModule } from '../../shared/shared.module';
import { ConfigurationComponent } from './configuration/configuration.component';
import { HostsComponent } from './hosts/hosts.component';
import { MonitorService } from './monitor.service';
import { MonitorComponent } from './monitor/monitor.component';

@NgModule({
  imports: [
    CommonModule,
    ComponentsModule,
    SharedModule,
    RouterModule,
    FormsModule
  ],
  declarations: [
    HostsComponent,
    MonitorComponent,
    ConfigurationComponent
  ],
  providers: [
    MonitorService
  ]
})
export class ClusterModule {}
