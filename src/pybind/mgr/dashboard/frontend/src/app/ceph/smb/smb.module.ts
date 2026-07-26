import Close from '@carbon/icons/es/close/32';
import { SmbClusterListComponent } from './smb-cluster-list/smb-cluster-list.component';
import { SmbClusterFormComponent } from './smb-cluster-form/smb-cluster-form.component';
import { AppRoutingModule } from '~/app/app-routing.module';
import { DataTableModule } from '~/app/shared/datatable/datatable.module';
import { SmbDomainSettingModalComponent } from './smb-domain-setting-modal/smb-domain-setting-modal.component';
import { SmbClusterResourcePageComponent } from './smb-cluster-resource-page/smb-cluster-resource-page.component';
import { SmbClusterResourceSidebarComponent } from './smb-cluster-resource-sidebar/smb-cluster-resource-sidebar.component';
import { SmbShareListComponent } from './smb-share-list/smb-share-list.component';
import {
  ButtonModule,
  CheckboxModule,
  ComboBoxModule,
  DropdownModule,
  GridModule,
  IconModule,
  IconService,
  InputModule,
  LayoutModule,
  ModalModule,
  NumberModule,
  PlaceholderModule,
  SelectModule,
  TabsModule,
  TagModule,
  FileUploaderModule
} from 'carbon-components-angular';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterModule } from '@angular/router';
import { NgModule } from '@angular/core';
import { SmbShareFormComponent } from './smb-share-form/smb-share-form.component';

import { SmbUsersgroupsListComponent } from './smb-usersgroups-list/smb-usersgroups-list.component';
import { SmbTabsComponent } from './smb-tabs/smb-tabs.component';
import { SmbJoinAuthListComponent } from './smb-join-auth-list/smb-join-auth-list.component';
import { SmbUsersgroupsResourceSidebarComponent } from './smb-usersgroups-resource-sidebar/smb-usersgroups-resource-sidebar.component';
import { SmbJoinAuthFormComponent } from './smb-join-auth-form/smb-join-auth-form.component';
import { SmbUsersgroupsFormComponent } from './smb-usersgroups-form/smb-usersgroups-form.component';
import { SmbOverviewComponent } from './smb-overview/smb-overview.component';
import { SmbUsersgroupsResourcePageComponent } from './smb-usersgroups-resource-page/smb-usersgroups-resource-page.component';

@NgModule({
  imports: [
    RouterModule,
    CommonModule,
    SharedModule,
    AppRoutingModule,
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    DataTableModule,
    GridModule,
    SelectModule,
    TabsModule,
    TagModule,
    FileUploaderModule,
    InputModule,
    CheckboxModule,
    SelectModule,
    DropdownModule,
    ModalModule,
    PlaceholderModule,
    ButtonModule,
    NumberModule,
    LayoutModule,
    ComboBoxModule,
    IconModule
  ],
  exports: [SmbClusterListComponent, SmbClusterFormComponent],
  declarations: [
    SmbClusterListComponent,
    SmbClusterFormComponent,
    SmbDomainSettingModalComponent,
    SmbClusterResourcePageComponent,
    SmbClusterResourceSidebarComponent,
    SmbShareListComponent,
    SmbUsersgroupsListComponent,
    SmbUsersgroupsResourceSidebarComponent,
    SmbTabsComponent,
    SmbJoinAuthListComponent,
    SmbUsersgroupsResourcePageComponent,
    SmbJoinAuthFormComponent,
    SmbUsersgroupsFormComponent,
    SmbShareFormComponent,
    SmbOverviewComponent
  ]
})
export class SmbModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([Close]);
  }
}
