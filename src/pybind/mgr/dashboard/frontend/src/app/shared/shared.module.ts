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
import { FORMLY_CARBON_CONFIG } from './forms/crud-form/formly-carbon.config';
import { BlockUIModule, BlockUIService } from 'ng-block-ui';
import { TilesModule } from 'carbon-components-angular';

@NgModule({
  imports: [
    CommonModule,
    PipesModule,
    ComponentsModule,
    DataTableModule,
    DirectivesModule,

    ReactiveFormsModule,
    FormlyModule.forRoot(FORMLY_CARBON_CONFIG),
    FormlyBootstrapModule,
    FormlyModule.forChild(FORMLY_CARBON_CONFIG),
    BlockUIModule.forRoot()
  ],
  exports: [ComponentsModule, PipesModule, DataTableModule, DirectivesModule, TilesModule],
  providers: [AuthStorageService, AuthGuardService, FormatterService, CssHelper, BlockUIService]
})
export class SharedModule {}
