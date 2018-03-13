import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { ComponentsModule } from '../../shared/components/components.module';
import { SharedModule } from '../../shared/shared.module';
import { PerformanceCounterModule } from '../performance-counter/performance-counter.module';
import { ConfigurationComponent } from './configuration/configuration.component';
import { HostsComponent } from './hosts/hosts.component';
import { MonitorService } from './monitor.service';
import { MonitorComponent } from './monitor/monitor.component';
import { OsdDetailsComponent } from './osd/osd-details/osd-details.component';
import { OsdListComponent } from './osd/osd-list/osd-list.component';
import {
  OsdPerformanceHistogramComponent
} from './osd/osd-performance-histogram/osd-performance-histogram.component';
import { OsdService } from './osd/osd.service';

@NgModule({
  entryComponents: [
    OsdDetailsComponent
  ],
  imports: [
    CommonModule,
    PerformanceCounterModule,
    ComponentsModule,
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
  ],
  providers: [
    MonitorService,
    OsdService
  ]
})
export class ClusterModule {}
