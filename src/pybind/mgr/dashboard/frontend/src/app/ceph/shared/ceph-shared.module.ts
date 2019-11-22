import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { DataTableModule } from '../../shared/datatable/datatable.module';
import { SharedModule } from '../../shared/shared.module';
import { DeviceListComponent } from './device-list/device-list.component';

@NgModule({
  imports: [CommonModule, DataTableModule, SharedModule],
  exports: [DeviceListComponent],
  declarations: [DeviceListComponent]
})
export class CephSharedModule {}
