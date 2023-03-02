import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { FormlyModule } from '@ngx-formly/core';
import { FormlyBootstrapModule } from '@ngx-formly/bootstrap';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { ComponentsModule } from './components/components.module';
import { DataTableModule } from './datatable/datatable.module';
import { DirectivesModule } from './directives/directives.module';
import { PipesModule } from './pipes/pipes.module';
import { AuthGuardService } from './services/auth-guard.service';
import { AuthStorageService } from './services/auth-storage.service';
import { FormatterService } from './services/formatter.service';
import { FormlyArrayTypeComponent } from './forms/crud-form/formly-array-type/formly-array-type.component';
import { FormlyObjectTypeComponent } from './forms/crud-form/formly-object-type/formly-object-type.component';
import { FormlyInputTypeComponent } from './forms/crud-form/formly-input-type/formly-input-type.component';
import { FormlyTextareaTypeComponent } from './forms/crud-form/formly-textarea-type/formly-textarea-type.component';

@NgModule({
  imports: [
    CommonModule,
    PipesModule,
    ComponentsModule,
    DataTableModule,
    DirectivesModule,

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
  declarations: [FormlyTextareaTypeComponent],
  exports: [ComponentsModule, PipesModule, DataTableModule, DirectivesModule],
  providers: [AuthStorageService, AuthGuardService, FormatterService, CssHelper]
})
export class SharedModule {}
