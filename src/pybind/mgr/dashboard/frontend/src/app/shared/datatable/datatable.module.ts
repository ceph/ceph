import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { NgbDropdownModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { NgxPipeFunctionModule } from 'ngx-pipe-function';

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

@NgModule({
  imports: [
    CommonModule,
    NgxDatatableModule,
    NgxPipeFunctionModule,
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
    FormlyBootstrapModule
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
    CheckedTableFormComponent
  ],
  exports: [
    TableComponent,
    NgxDatatableModule,
    TableKeyValueComponent,
    TableActionsComponent,
    CRUDTableComponent,
    TablePaginationComponent,
    CheckedTableFormComponent
  ]
})
export class DataTableModule {}
