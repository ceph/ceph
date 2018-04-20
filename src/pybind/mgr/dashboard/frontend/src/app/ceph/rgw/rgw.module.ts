import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { SharedModule } from '../../shared/shared.module';
import { PerformanceCounterModule } from '../performance-counter/performance-counter.module';
import { RgwBucketDetailsComponent } from './rgw-bucket-details/rgw-bucket-details.component';
import { RgwBucketListComponent } from './rgw-bucket-list/rgw-bucket-list.component';
import { RgwDaemonDetailsComponent } from './rgw-daemon-details/rgw-daemon-details.component';
import { RgwDaemonListComponent } from './rgw-daemon-list/rgw-daemon-list.component';
import { RgwUserDetailsComponent } from './rgw-user-details/rgw-user-details.component';
import { RgwUserListComponent } from './rgw-user-list/rgw-user-list.component';

@NgModule({
  entryComponents: [
    RgwDaemonDetailsComponent,
    RgwBucketDetailsComponent,
    RgwUserDetailsComponent
  ],
  imports: [
    CommonModule,
    SharedModule,
    PerformanceCounterModule,
    TabsModule.forRoot()
  ],
  exports: [
    RgwDaemonListComponent,
    RgwDaemonDetailsComponent,
    RgwBucketListComponent,
    RgwBucketDetailsComponent,
    RgwUserListComponent,
    RgwUserDetailsComponent
  ],
  declarations: [
    RgwDaemonListComponent,
    RgwDaemonDetailsComponent,
    RgwBucketListComponent,
    RgwBucketDetailsComponent,
    RgwUserListComponent,
    RgwUserDetailsComponent
  ]
})
export class RgwModule { }
