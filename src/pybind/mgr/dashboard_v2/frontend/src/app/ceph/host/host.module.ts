import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { HostsComponent } from './hosts/hosts.component';
import { SharedModule } from '../../shared/shared.module';
import { ServiceListPipe } from './service-list.pipe';

@NgModule({
  imports: [
    CommonModule,
    SharedModule
  ],
  declarations: [HostsComponent, ServiceListPipe],
})
export class HostModule { }
