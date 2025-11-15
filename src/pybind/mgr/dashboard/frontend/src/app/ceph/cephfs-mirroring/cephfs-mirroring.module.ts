import Close from '@carbon/icons/es/close/32';
import { AppRoutingModule } from '~/app/app-routing.module';
import { DataTableModule } from '~/app/shared/datatable/datatable.module';

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
import { CephfsMirroringListComponent } from './cephfs-mirroring-list/cephfs-mirroring-list.component';
import { CephfsMirroringDetailListComponent } from './cephfs-mirroring-detail-list/cephfs-mirroring-detail-list.component';

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
  exports: [],
  declarations: [CephfsMirroringListComponent, CephfsMirroringDetailListComponent]
})
export class CephfsMirroringModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([Close]);
  }
}
