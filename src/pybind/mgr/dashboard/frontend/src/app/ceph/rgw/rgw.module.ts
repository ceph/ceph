import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { SharedModule } from '../../shared/shared.module';
import { PerformanceCounterModule } from '../performance-counter/performance-counter.module';
import { RgwDaemonDetailsComponent } from './rgw-daemon-details/rgw-daemon-details.component';
import { RgwDaemonListComponent } from './rgw-daemon-list/rgw-daemon-list.component';

@NgModule({
  entryComponents: [
    RgwDaemonDetailsComponent
  ],
  imports: [
    CommonModule,
    SharedModule,
    PerformanceCounterModule,
    TabsModule.forRoot()
  ],
  exports: [
    RgwDaemonListComponent,
    RgwDaemonDetailsComponent
  ],
  declarations: [
    RgwDaemonListComponent,
    RgwDaemonDetailsComponent
  ]
})
export class RgwModule { }
