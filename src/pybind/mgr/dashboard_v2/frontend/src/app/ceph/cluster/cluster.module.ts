import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ComponentsModule } from '../../shared/components/components.module';
import { SharedModule } from '../../shared/shared.module';
import { HostsComponent } from './hosts/hosts.component';
import { ServiceListPipe } from './service-list.pipe';

@NgModule({
  imports: [
    CommonModule,
    ComponentsModule,
    SharedModule
  ],
  declarations: [
    HostsComponent,
    ServiceListPipe
  ],
  providers: [
    ServiceListPipe
  ]
})
export class ClusterModule { }
