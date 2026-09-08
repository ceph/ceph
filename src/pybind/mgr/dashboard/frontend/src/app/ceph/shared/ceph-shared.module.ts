import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { RouterModule } from '@angular/router';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { PipesModule } from '~/app/shared/pipes/pipes.module';

import { DataTableModule } from '~/app/shared/datatable/datatable.module';
import { SharedModule } from '~/app/shared/shared.module';
import { DeviceListComponent } from './device-list/device-list.component';
import { SmartListComponent } from './smart-list/smart-list.component';
import { HealthChecksComponent } from './health-checks/health-checks.component';
import { InputModule, ModalModule, SelectModule } from 'carbon-components-angular';
import { FeedbackComponent } from './feedback/feedback.component';
import { ReactiveFormsModule } from '@angular/forms';
import { ComponentsModule } from '~/app/shared/components/components.module';

@NgModule({
  imports: [
    CommonModule,
    DataTableModule,
    SharedModule,
    NgbNavModule,
    PipesModule,
    RouterModule,
    ModalModule,
    ReactiveFormsModule,
    InputModule,
    SelectModule,
    ComponentsModule
  ],
  exports: [DeviceListComponent, SmartListComponent, HealthChecksComponent, FeedbackComponent],
  declarations: [DeviceListComponent, SmartListComponent, HealthChecksComponent, FeedbackComponent]
})
export class CephSharedModule {}
