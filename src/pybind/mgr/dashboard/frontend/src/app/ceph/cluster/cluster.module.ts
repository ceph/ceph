import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { SharedModule } from '../../shared/shared.module';
import { PerformanceCounterModule } from '../performance-counter/performance-counter.module';
import { ConfigurationComponent } from './configuration/configuration.component';
import { HostsComponent } from './hosts/hosts.component';
import { MonitorComponent } from './monitor/monitor.component';
import { OsdDetailsComponent } from './osd/osd-details/osd-details.component';
import { OsdListComponent } from './osd/osd-list/osd-list.component';
import {
  OsdPerformanceHistogramComponent
} from './osd/osd-performance-histogram/osd-performance-histogram.component';

@NgModule({
  entryComponents: [
    OsdDetailsComponent
  ],
  imports: [
    CommonModule,
    PerformanceCounterModule,
    TabsModule.forRoot(),
    SharedModule,
    RouterModule,
    FormsModule
  ],
  declarations: [
    HostsComponent,
    MonitorComponent,
    ConfigurationComponent,
    OsdListComponent,
    OsdDetailsComponent,
    OsdPerformanceHistogramComponent
  ]
})
export class ClusterModule {}
