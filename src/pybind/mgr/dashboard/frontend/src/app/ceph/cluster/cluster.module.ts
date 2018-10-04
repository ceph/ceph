import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ModalModule } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { SharedModule } from '../../shared/shared.module';
import { PerformanceCounterModule } from '../performance-counter/performance-counter.module';
import { ConfigurationComponent } from './configuration/configuration.component';
import { HostDetailsComponent } from './hosts/host-details/host-details.component';
import { HostsComponent } from './hosts/hosts.component';
import { MonitorComponent } from './monitor/monitor.component';
import { OsdDetailsComponent } from './osd/osd-details/osd-details.component';
import { OsdFlagsModalComponent } from './osd/osd-flags-modal/osd-flags-modal.component';
import { OsdListComponent } from './osd/osd-list/osd-list.component';
import { OsdPerformanceHistogramComponent } from './osd/osd-performance-histogram/osd-performance-histogram.component';
import { OsdScrubModalComponent } from './osd/osd-scrub-modal/osd-scrub-modal.component';

@NgModule({
  entryComponents: [OsdDetailsComponent, OsdScrubModalComponent, OsdFlagsModalComponent],
  imports: [
    CommonModule,
    PerformanceCounterModule,
    TabsModule.forRoot(),
    SharedModule,
    RouterModule,
    FormsModule,
    ReactiveFormsModule,
    BsDropdownModule.forRoot(),
    ModalModule.forRoot()
  ],
  declarations: [
    HostsComponent,
    MonitorComponent,
    ConfigurationComponent,
    OsdListComponent,
    OsdDetailsComponent,
    OsdPerformanceHistogramComponent,
    OsdScrubModalComponent,
    OsdFlagsModalComponent,
    HostDetailsComponent
  ]
})
export class ClusterModule {}
