import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import {
  AlertModule,
  BsDropdownModule,
  ModalModule,
  TabsModule,
  TooltipModule
} from 'ngx-bootstrap';

import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { PerformanceCounterModule } from '../performance-counter/performance-counter.module';
import { Rgw501Component } from './rgw-501/rgw-501.component';
import { RgwBucketDetailsComponent } from './rgw-bucket-details/rgw-bucket-details.component';
import { RgwBucketFormComponent } from './rgw-bucket-form/rgw-bucket-form.component';
import { RgwBucketListComponent } from './rgw-bucket-list/rgw-bucket-list.component';
import { RgwDaemonDetailsComponent } from './rgw-daemon-details/rgw-daemon-details.component';
import { RgwDaemonListComponent } from './rgw-daemon-list/rgw-daemon-list.component';
import {
  RgwUserCapabilityModalComponent
} from './rgw-user-capability-modal/rgw-user-capability-modal.component';
import { RgwUserDetailsComponent } from './rgw-user-details/rgw-user-details.component';
import { RgwUserFormComponent } from './rgw-user-form/rgw-user-form.component';
import { RgwUserListComponent } from './rgw-user-list/rgw-user-list.component';
import {
  RgwUserS3KeyModalComponent
} from './rgw-user-s3-key-modal/rgw-user-s3-key-modal.component';
import {
  RgwUserSubuserModalComponent
} from './rgw-user-subuser-modal/rgw-user-subuser-modal.component';
import {
  RgwUserSwiftKeyModalComponent
} from './rgw-user-swift-key-modal/rgw-user-swift-key-modal.component';

@NgModule({
  entryComponents: [
    RgwDaemonDetailsComponent,
    RgwBucketDetailsComponent,
    RgwUserDetailsComponent,
    RgwUserSwiftKeyModalComponent,
    RgwUserS3KeyModalComponent,
    RgwUserCapabilityModalComponent,
    RgwUserSubuserModalComponent
  ],
  imports: [
    CommonModule,
    SharedModule,
    AppRoutingModule,
    FormsModule,
    ReactiveFormsModule,
    PerformanceCounterModule,
    AlertModule.forRoot(),
    BsDropdownModule.forRoot(),
    TabsModule.forRoot(),
    TooltipModule.forRoot(),
    ModalModule.forRoot()
  ],
  exports: [
    Rgw501Component,
    RgwDaemonListComponent,
    RgwDaemonDetailsComponent,
    RgwBucketFormComponent,
    RgwBucketListComponent,
    RgwBucketDetailsComponent,
    RgwUserListComponent,
    RgwUserDetailsComponent
  ],
  declarations: [
    Rgw501Component,
    RgwDaemonListComponent,
    RgwDaemonDetailsComponent,
    RgwBucketFormComponent,
    RgwBucketListComponent,
    RgwBucketDetailsComponent,
    RgwUserListComponent,
    RgwUserDetailsComponent,
    RgwBucketFormComponent,
    RgwUserFormComponent,
    RgwUserSwiftKeyModalComponent,
    RgwUserS3KeyModalComponent,
    RgwUserCapabilityModalComponent,
    RgwUserSubuserModalComponent
  ]
})
export class RgwModule { }
