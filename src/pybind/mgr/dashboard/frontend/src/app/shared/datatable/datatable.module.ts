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
  LayoutModule
} from 'carbon-components-angular';
import AddIcon from '@carbon/icons/es/add/16';
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
import TrashCanIcon from '@carbon/icons/es/trash-can/16';
import LockedIcon from '@carbon/icons/es/locked/16';
import UnlockedIcon from '@carbon/icons/es/unlocked/16';
import ReplicateIcon from '@carbon/icons/es/replicate/16';
import UndoIcon from '@carbon/icons/es/undo/16';
import PlayFilledIcon from '@carbon/icons/es/play--filled/16';
import StopFilledIcon from '@carbon/icons/es/stop--filled/16';
import StethoscopeIcon from '@carbon/icons/es/stethoscope/16';
import SettingsIcon from '@carbon/icons/es/settings/16';
import ScalesIcon from '@carbon/icons/es/scales/16';
import ArrowLeftIcon from '@carbon/icons/es/arrow--left/16';
import ArrowRightIcon from '@carbon/icons/es/arrow--right/16';
import ArrowDownIcon from '@carbon/icons/es/arrow--down/16';
import EraseIcon from '@carbon/icons/es/erase/16';
import UnlinkIcon from '@carbon/icons/es/unlink/16';
import ArrowsHorizontalIcon from '@carbon/icons/es/arrows--horizontal/16';
import ViewIcon from '@carbon/icons/es/view/16';
import CloseFilledIcon from '@carbon/icons/es/close--filled/16';
import DownloadIcon from '@carbon/icons/es/download/16';
import UploadIcon from '@carbon/icons/es/upload/16';
import ToolsIcon from '@carbon/icons/es/tools/16';
import LoginIcon from '@carbon/icons/es/login/16';
import LogoutIcon from '@carbon/icons/es/logout/16';
import FlagIcon from '@carbon/icons/es/flag/16';
import RestartIcon from '@carbon/icons/es/restart/16';
import ListIcon from '@carbon/icons/es/list/16';
import ExportIcon from '@carbon/icons/es/export/16';
import EditIcon from '@carbon/icons/es/edit/16';
import CopyIcon from '@carbon/icons/es/copy/16';
import DocumentAddIcon from '@carbon/icons/es/document--add/16';
import DocumentImportIcon from '@carbon/icons/es/document--import/16';

import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { FormlyModule } from '@ngx-formly/core';
import { FormlyBootstrapModule } from '@ngx-formly/bootstrap';
import { ComponentsModule } from '../components/components.module';
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
    RouterModule,
    ReactiveFormsModule,
    FormlyModule.forRoot({
      types: [
        { name: 'array', component: FormlyArrayTypeComponent },
        { name: 'object', component: FormlyObjectTypeComponent },
        { name: 'input', component: FormlyInputTypeComponent, wrappers: ['input-wrapper'] },
        { name: 'textarea', component: FormlyTextareaTypeComponent, wrappers: ['input-wrapper'] },
        { name: 'file', component: FormlyFileTypeComponent, wrappers: ['input-wrapper'] }
      ],
      validationMessages: [
        { name: 'required', message: 'This field is required' },
        { name: 'json', message: 'This field is not a valid json document' },
        {
          name: 'rgwRoleName',
          message:
            'Role name must contain letters, numbers or the ' +
            'following valid special characters "_+=,.@-]+" (pattern: [0-9a-zA-Z_+=,.@-]+)'
        },
        {
          name: 'rgwRolePath',
          message:
            'Role path must start and finish with a slash "/".' +
            ' (pattern: (\u002F)|(\u002F[\u0021-\u007E]+\u002F))'
        },
        { name: 'file_size', message: 'File size must not exceed 4KiB' },
        {
          name: 'rgwRoleSessionDuration',
          message: 'This field must be a number and should be a value from 1 hour to 12 hour'
        }
      ],
      wrappers: [{ name: 'input-wrapper', component: FormlyInputWrapperComponent }]
    }),
    FormlyBootstrapModule,
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
    LayoutModule
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
    FormlyInputWrapperComponent,
    FormlyFileTypeComponent,
    FormlyFileValueAccessorDirective,
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
      TrashCanIcon,
      LockedIcon,
      UnlockedIcon,
      ReplicateIcon,
      UndoIcon,
      PlayFilledIcon,
      StopFilledIcon,
      StethoscopeIcon,
      SettingsIcon,
      ScalesIcon,
      ArrowLeftIcon,
      ArrowRightIcon,
      ArrowDownIcon,
      EraseIcon,
      UnlinkIcon,
      ArrowsHorizontalIcon,
      ViewIcon,
      CloseFilledIcon,
      DownloadIcon,
      UploadIcon,
      ToolsIcon,
      LoginIcon,
      LogoutIcon,
      FlagIcon,
      RestartIcon,
      ListIcon,
      ExportIcon,
      EditIcon,
      CopyIcon,
      DocumentAddIcon,
      DocumentImportIcon
    ]);
  }
}
