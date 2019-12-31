import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { DataTableModule } from '../../shared/datatable/datatable.module';
import { SharedModule } from '../../shared/shared.module';
import { DeviceListComponent } from './device-list/device-list.component';
import { SmartListComponent } from './smart-list/smart-list.component';

@NgModule({
  imports: [CommonModule, DataTableModule, SharedModule, TabsModule],
  exports: [DeviceListComponent, SmartListComponent],
  declarations: [DeviceListComponent, SmartListComponent]
})
export class CephSharedModule {}
