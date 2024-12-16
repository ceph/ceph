import { SmbClusterFormComponent } from './smb-cluster-form/smb-cluster-form.component';
import { AppRoutingModule } from '~/app/app-routing.module';
import { NgChartsModule } from 'ng2-charts';
import { DataTableModule } from '~/app/shared/datatable/datatable.module';
import { SmbDomainSettingModalComponent } from './smb-domain-setting-modal/smb-domain-setting-modal.component';
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
  SelectModule
} from 'carbon-components-angular';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterModule } from '@angular/router';
import { NgModule } from '@angular/core';

import Close from '@carbon/icons/es/close/32';
import { SmbClusterListComponent } from './smb-cluster-list/smb-cluster-list.component';
import { SmbShareListComponent } from './smb-share-list/smb-share-list.component';
import { SmbShareFormComponent } from './smb-share-form/smb-share-form.component';

@NgModule({
  imports: [
    ReactiveFormsModule,
    RouterModule,
    CommonModule,
    SharedModule,
    AppRoutingModule,
    NgChartsModule,
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    DataTableModule,
    GridModule,
    SelectModule,
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
    IconModule,
    CheckboxModule
  ],
  exports: [SmbClusterListComponent, SmbClusterFormComponent],
  declarations: [
    SmbClusterListComponent,
    SmbDomainSettingModalComponent,
    SmbClusterFormComponent,
    SmbShareListComponent,
    SmbShareFormComponent,

  ]
})
export class SmbModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([Close]);
  }
}
