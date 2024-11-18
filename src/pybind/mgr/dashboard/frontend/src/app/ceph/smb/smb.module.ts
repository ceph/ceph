import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { NgbDatepickerModule, NgbNavModule, NgbTimepickerModule, NgbTooltipModule, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';

import {
  ButtonModule,
  CheckboxModule,
  ComboBoxModule,
  DatePickerModule,
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
  TimePickerModule
} from 'carbon-components-angular';

import Close from '@carbon/icons/es/close/32';
import { SmbClusterListComponent } from './smb-cluster-list/smb-cluster-list.component';
import { SmbClusterFormComponent } from './smb-cluster-form/smb-cluster-form.component';
import { AppRoutingModule } from '~/app/app-routing.module';
import { NgChartsModule } from 'ng2-charts';
import { DataTableModule } from '~/app/shared/datatable/datatable.module';
import { TreeModule } from '@circlon/angular-tree-component';
import { SmbDomainSettingModalComponent } from './smb-domain-setting-modal/smb-domain-setting-modal.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    AppRoutingModule,
    NgChartsModule,
    TreeModule,
    NgbNavModule,
    FormsModule,
    ReactiveFormsModule,
    NgbTypeaheadModule,
    NgbTooltipModule,
    DataTableModule,
    NgbDatepickerModule,
    NgbTimepickerModule,
    NgbTypeaheadModule,
    GridModule,
    InputModule,
    CheckboxModule,
    SelectModule,
    DropdownModule,
    ModalModule,
    PlaceholderModule,
    DatePickerModule,
    TimePickerModule,
    ButtonModule,
    NumberModule,
    LayoutModule,
    ComboBoxModule,
    IconModule
  ],
  exports: [SmbClusterListComponent,SmbClusterFormComponent],
  declarations: [SmbClusterListComponent,SmbClusterFormComponent, SmbDomainSettingModalComponent]
})
export class SmbModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([Close]);
  }
}
