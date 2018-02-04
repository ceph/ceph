import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { SharedModule } from '../../shared/shared.module';
import { HostsComponent } from './hosts/hosts.component';
import { ServiceListPipe } from './service-list.pipe';

@NgModule({
  imports: [
    CommonModule,
    SharedModule
  ],
  declarations: [HostsComponent, ServiceListPipe],
})
export class ClusterModule { }
