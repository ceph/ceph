import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ApiModule } from './api/api.module';
import { ComponentsModule } from './components/components.module';
import { DataTableModule } from './datatable/datatable.module';
import { AutofocusDirective } from './directives/autofocus.directive';
import { Copy2ClipboardButtonDirective } from './directives/copy2clipboard-button.directive';
import { DimlessBinaryDirective } from './directives/dimless-binary.directive';
import { PasswordButtonDirective } from './directives/password-button.directive';
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
    ApiModule
  ],
  declarations: [
    PasswordButtonDirective,
    DimlessBinaryDirective,
    Copy2ClipboardButtonDirective,
    AutofocusDirective
  ],
  exports: [
    ComponentsModule,
    PipesModule,
    ServicesModule,
    PasswordButtonDirective,
    Copy2ClipboardButtonDirective,
    DimlessBinaryDirective,
    DataTableModule,
    ApiModule,
    AutofocusDirective
  ],
  providers: [
    AuthStorageService,
    AuthGuardService,
    FormatterService
  ]
})
export class SharedModule {}
