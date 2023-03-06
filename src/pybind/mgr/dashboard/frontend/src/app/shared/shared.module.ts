import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { Bootstrap4FrameworkModule } from '@ajsf/bootstrap4';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { ComponentsModule } from './components/components.module';
import { DataTableModule } from './datatable/datatable.module';
import { DirectivesModule } from './directives/directives.module';
import { PipesModule } from './pipes/pipes.module';
import { AuthGuardService } from './services/auth-guard.service';
import { AuthStorageService } from './services/auth-storage.service';
import { FormatterService } from './services/formatter.service';

@NgModule({
  imports: [
    CommonModule,
    PipesModule,
    ComponentsModule,
    DataTableModule,
    DirectivesModule,
    Bootstrap4FrameworkModule
  ],
  declarations: [],
  exports: [ComponentsModule, PipesModule, DataTableModule, DirectivesModule],
  providers: [AuthStorageService, AuthGuardService, FormatterService, CssHelper]
})
export class SharedModule {}
