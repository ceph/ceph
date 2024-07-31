import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import { BlockUIModule } from 'ng-block-ui';

import { ContextComponent } from '~/app/core/context/context.component';
import { SharedModule } from '~/app/shared/shared.module';
import { ErrorComponent } from './error/error.component';
import { BlankLayoutComponent } from './layouts/blank-layout/blank-layout.component';
import { LoginLayoutComponent } from './layouts/login-layout/login-layout.component';
import { WorkbenchLayoutComponent } from './layouts/workbench-layout/workbench-layout.component';
import { NavigationModule } from './navigation/navigation.module';
import { PlaceholderModule } from 'carbon-components-angular';

@NgModule({
  imports: [
    BlockUIModule.forRoot(),
    CommonModule,
    NavigationModule,
    NgbDropdownModule,
    RouterModule,
    SharedModule,
    PlaceholderModule
  ],
  exports: [NavigationModule],
  declarations: [
    ContextComponent,
    WorkbenchLayoutComponent,
    BlankLayoutComponent,
    LoginLayoutComponent,
    ErrorComponent
  ]
})
export class CoreModule {}
