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
        { name: 'input', component: FormlyInputTypeComponent }
      ],
      validationMessages: [{ name: 'required', message: 'This field is required' }]
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
    FormlyObjectTypeComponent
  ],
  exports: [
    TableComponent,
    NgxDatatableModule,
    TableKeyValueComponent,
    TableActionsComponent,
    CRUDTableComponent,
    TablePaginationComponent
  ]
})
export class DataTableModule {}
