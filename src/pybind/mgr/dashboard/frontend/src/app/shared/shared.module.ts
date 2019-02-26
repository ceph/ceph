import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ApiModule } from './api/api.module';
import { ComponentsModule } from './components/components.module';
import { DataTableModule } from './datatable/datatable.module';
import { DirectivesModule } from './directives/directives.module';
import { PipesModule } from './pipes/pipes.module';
import { AuthGuardService } from './services/auth-guard.service';
import { AuthStorageService } from './services/auth-storage.service';
import { FormatterService } from './services/formatter.service';
import { ServicesModule } from './services/services.module';

@NgModule({
  imports: [
    CommonModule,
    PipesModule,
    ComponentsModule,
    ServicesModule,
    DataTableModule,
    ApiModule,
    DirectivesModule
  ],
  declarations: [],
  exports: [
    ComponentsModule,
    PipesModule,
    ServicesModule,
    DataTableModule,
    ApiModule,
    DirectivesModule
  ],
  providers: [AuthStorageService, AuthGuardService, FormatterService]
})
export class SharedModule {}
