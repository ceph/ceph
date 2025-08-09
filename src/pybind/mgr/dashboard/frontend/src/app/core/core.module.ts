import { CommonModule } from '@angular/common';
import { NgModule, CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { RouterModule } from '@angular/router';

import { NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import { BlockUIModule } from 'ng-block-ui';

import { ContextComponent } from '~/app/core/context/context.component';
import { ErrorComponent } from './error/error.component';
import { BlankLayoutComponent } from './layouts/blank-layout/blank-layout.component';
import { LoginLayoutComponent } from './layouts/login-layout/login-layout.component';
import { WorkbenchLayoutComponent } from './layouts/workbench-layout/workbench-layout.component';
import { NavigationModule } from './navigation/navigation.module';
import { PlaceholderModule } from 'carbon-components-angular';
import { SharedModule } from '~/app/shared/shared.module';

@NgModule({
  imports: [
    CommonModule,
    NavigationModule,
    NgbDropdownModule,
    RouterModule,
    PlaceholderModule,
    BlockUIModule.forRoot(),
    SharedModule
  ],
  exports: [NavigationModule],
  declarations: [
    ContextComponent,
    WorkbenchLayoutComponent,
    BlankLayoutComponent,
    LoginLayoutComponent,
    ErrorComponent
  ],
  schemas: [CUSTOM_ELEMENTS_SCHEMA]
})
export class CoreModule {}
