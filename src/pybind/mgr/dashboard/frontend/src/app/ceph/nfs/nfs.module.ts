import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbTooltipModule, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { NfsDetailsComponent } from './nfs-details/nfs-details.component';
import { NfsFormClientComponent } from './nfs-form-client/nfs-form-client.component';
import { NfsFormComponent } from './nfs-form/nfs-form.component';
import { NfsListComponent } from './nfs-list/nfs-list.component';
import {
  ButtonModule,
  CheckboxModule,
  GridModule,
  IconModule,
  IconService,
  InputModule,
  LinkModule,
  RadioModule,
  SelectModule,
  TabsModule,
  TagModule
} from 'carbon-components-angular';

import Close from '@carbon/icons/es/close/32';
import { NfsClusterComponent } from './nfs-cluster/nfs-cluster.component';
import { ClusterModule } from '../cluster/cluster.module';
import { NfsClusterResourcePageComponent } from './nfs-cluster-resource-page/nfs-cluster-resource-page.component';
import { NfsClusterResourceSidebarComponent } from './nfs-cluster-resource-sidebar/nfs-cluster-resource-sidebar.component';

@NgModule({
  imports: [
    ReactiveFormsModule,
    RouterModule,
    SharedModule,
    NgbNavModule,
    CommonModule,
    NgbTypeaheadModule,
    NgbTooltipModule,
    GridModule,
    TagModule,
    SelectModule,
    InputModule,
    LinkModule,
    RadioModule,
    CheckboxModule,
    ButtonModule,
    IconModule,
    TabsModule,
    ClusterModule
  ],
  exports: [
    NfsListComponent,
    NfsFormComponent,
    NfsDetailsComponent,
    NfsClusterComponent,
    NfsClusterResourcePageComponent,
    NfsClusterResourceSidebarComponent
  ],
  declarations: [
    NfsListComponent,
    NfsDetailsComponent,
    NfsFormComponent,
    NfsFormClientComponent,
    NfsClusterComponent,
    NfsClusterResourcePageComponent,
    NfsClusterResourceSidebarComponent
  ]
})
export class NfsModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([Close]);
  }
}
