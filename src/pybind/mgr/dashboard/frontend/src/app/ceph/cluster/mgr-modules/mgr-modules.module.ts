import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { AppRoutingModule } from '../../../app-routing.module';
import { SharedModule } from '../../../shared/shared.module';
import { MgrModuleDetailsComponent } from './mgr-module-details/mgr-module-details.component';
import { MgrModuleFormComponent } from './mgr-module-form/mgr-module-form.component';
import { MgrModuleListComponent } from './mgr-module-list/mgr-module-list.component';

@NgModule({
  imports: [
    AppRoutingModule,
    CommonModule,
    ReactiveFormsModule,
    SharedModule,
    TabsModule.forRoot()
  ],
  declarations: [MgrModuleListComponent, MgrModuleFormComponent, MgrModuleDetailsComponent]
})
export class MgrModulesModule {}
