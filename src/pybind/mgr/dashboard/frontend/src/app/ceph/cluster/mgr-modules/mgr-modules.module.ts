import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { AppRoutingModule } from '~/app/app-routing.module';
import { SharedModule } from '~/app/shared/shared.module';
import { MgrModuleFormComponent } from './mgr-module-form/mgr-module-form.component';
import { MgrModuleListComponent } from './mgr-module-list/mgr-module-list.component';
import { MgrModuleResourcePageComponent } from './mgr-module-resource-page/mgr-module-resource-page.component';
import { MgrModuleResourceSidebarComponent } from './mgr-module-resource-sidebar/mgr-module-resource-sidebar.component';

@NgModule({
  imports: [AppRoutingModule, CommonModule, ReactiveFormsModule, SharedModule, NgbNavModule],
  declarations: [
    MgrModuleListComponent,
    MgrModuleFormComponent,
    MgrModuleResourceSidebarComponent,
    MgrModuleResourcePageComponent
  ]
})
export class MgrModulesModule {}
