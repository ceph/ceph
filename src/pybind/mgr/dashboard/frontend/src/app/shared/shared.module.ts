import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ApiModule } from './api/api.module';
import { ComponentsModule } from './components/components.module';
import { DataTableModule } from './datatable/datatable.module';
import { DimlessBinaryDirective } from './directives/dimless-binary.directive';
import { PasswordButtonDirective } from './directives/password-button.directive';
import { PipesModule } from './pipes/pipes.module';
import { ServicesModule } from './services/services.module';

@NgModule({
  imports: [
    CommonModule,
    PipesModule,
    ComponentsModule,
    ServicesModule,
    DataTableModule,
    ApiModule
  ],
  declarations: [
    PasswordButtonDirective,
    DimlessBinaryDirective
  ],
  exports: [
    ComponentsModule,
    PipesModule,
    ServicesModule,
    PasswordButtonDirective,
    DimlessBinaryDirective,
    DataTableModule,
    ApiModule
  ]
})
export class SharedModule {}
