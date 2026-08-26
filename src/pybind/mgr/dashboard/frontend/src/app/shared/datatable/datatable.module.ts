import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { NgbDropdownModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';

import {
  TableModule,
  ButtonModule,
  IconModule,
  IconService,
  CheckboxModule,
  PaginationModule,
  ThemeModule,
  DialogModule,
  SelectModule,
  TagModule,
  LayerModule,
  InputModule,
  GridModule,
  LayoutModule,
  InlineLoadingModule,
  PopoverModule,
  TooltipModule
} from 'carbon-components-angular';
import AddIcon from '@carbon/icons/es/add/16';
import AddIcon32 from '@carbon/icons/es/add/32';
import FilterIcon from '@carbon/icons/es/filter/16';
import ReloadIcon from '@carbon/icons/es/renew/16';
import DataTableIcon from '@carbon/icons/es/data-table/16';
import CheckIcon from '@carbon/icons/es/checkmark/16';
import CloseIcon from '@carbon/icons/es/close/16';
import MaximizeIcon from '@carbon/icons/es/maximize/16';
import ArrowDown from '@carbon/icons/es/caret--down/16';
import ChevronDwon from '@carbon/icons/es/chevron--down/16';
import CheckMarkIcon from '@carbon/icons/es/checkmark/32';
import CubeIcon from '@carbon/icons/es/cube/32';
import TrashCan16 from '@carbon/icons/es/trash-can/16';
import TrashCan32 from '@carbon/icons/es/trash-can/32';

import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { FormlyModule } from '@ngx-formly/core';
import { FormlyBootstrapModule } from '@ngx-formly/bootstrap';
import { ComponentsModule } from '../components/components.module';
import { DirectivesModule } from '../directives/directives.module';
import { PipesModule } from '../pipes/pipes.module';
import { CRUDTableComponent } from './crud-table/crud-table.component';
import { TableActionsComponent } from './table-actions/table-actions.component';
import { TableKeyValueComponent } from './table-key-value/table-key-value.component';
import { TablePaginationComponent } from './table-pagination/table-pagination.component';
import { TableComponent } from './table/table.component';
import { CrudFormComponent } from '../forms/crud-form/crud-form.component';
import { FormlyArrayTypeComponent } from '../forms/crud-form/formly-array-type/formly-array-type.component';
import { FormlyInputTypeComponent } from '../forms/crud-form/formly-input-type/formly-input-type.component';
import { FormlyObjectTypeComponent } from '../forms/crud-form/formly-object-type/formly-object-type.component';
import { FormlyTextareaTypeComponent } from '../forms/crud-form/formly-textarea-type/formly-textarea-type.component';
import { FormlyInputWrapperComponent } from '../forms/crud-form/formly-input-wrapper/formly-input-wrapper.component';
import { FormlyFileTypeComponent } from '../forms/crud-form/formly-file-type/formly-file-type.component';
import { FormlyFileValueAccessorDirective } from '../forms/crud-form/formly-file-type/formly-file-type-accessor';
import { FormlySelectTypeComponent } from '../forms/crud-form/formly-select-type/formly-select-type.component';
import { FORMLY_CARBON_CONFIG } from '../forms/crud-form/formly-carbon.config';
import { CheckedTableFormComponent } from './checked-table-form/checked-table-form.component';
import { TableDetailDirective } from './directives/table-detail.directive';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    NgbDropdownModule,
    NgbTooltipModule,
    PipesModule,
    ComponentsModule,
    DirectivesModule,
    RouterModule,
    ReactiveFormsModule,
    FormlyModule.forRoot(FORMLY_CARBON_CONFIG),
    FormlyBootstrapModule,
    FormlyModule.forChild(FORMLY_CARBON_CONFIG),
    TableModule,
    ButtonModule,
    IconModule,
    CheckboxModule,
    PaginationModule,
    DialogModule,
    ThemeModule,
    SelectModule,
    TagModule,
    LayerModule,
    InputModule,
    GridModule,
    LayoutModule,
    InlineLoadingModule,
    PopoverModule,
    TooltipModule
  ],
  declarations: [
    TableComponent,
    TableKeyValueComponent,
    TableActionsComponent,
    CRUDTableComponent,
    TablePaginationComponent,
    CrudFormComponent,
    FormlyArrayTypeComponent,
    FormlyInputTypeComponent,
    FormlyObjectTypeComponent,
    FormlyTextareaTypeComponent,
    FormlyInputWrapperComponent,
    FormlyFileTypeComponent,
    FormlyFileValueAccessorDirective,
    FormlySelectTypeComponent,
    CheckedTableFormComponent,
    TableDetailDirective
  ],
  exports: [
    TableComponent,
    TableKeyValueComponent,
    TableActionsComponent,
    CRUDTableComponent,
    TablePaginationComponent,
    CheckedTableFormComponent,
    TableDetailDirective
  ]
})
export class DataTableModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([
      AddIcon,
      FilterIcon,
      ReloadIcon,
      DataTableIcon,
      CheckIcon,
      CloseIcon,
      MaximizeIcon,
      ArrowDown,
      ChevronDwon,
      CheckMarkIcon,
      CubeIcon,
      AddIcon32,
      TrashCan16,
      TrashCan32
    ]);
  }
}
