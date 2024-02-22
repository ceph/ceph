import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { PipesModule } from '~/app/shared/pipes/pipes.module';

import { DataTableModule } from '~/app/shared/datatable/datatable.module';
import { SharedModule } from '~/app/shared/shared.module';
import { DeviceListComponent } from './device-list/device-list.component';
import { SmartListComponent } from './smart-list/smart-list.component';
import { HealthChecksComponent } from './health-checks/health-checks.component';

@NgModule({
  imports: [CommonModule, DataTableModule, SharedModule, NgbNavModule, PipesModule],
  exports: [DeviceListComponent, SmartListComponent, HealthChecksComponent],
  declarations: [DeviceListComponent, SmartListComponent, HealthChecksComponent]
})
export class CephSharedModule {}
