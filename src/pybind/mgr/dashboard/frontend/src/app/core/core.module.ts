import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { SharedModule } from '../shared/shared.module';
import { AuthModule } from './auth/auth.module';
import { ForbiddenComponent } from './forbidden/forbidden.component';
import { NavigationModule } from './navigation/navigation.module';
import { NotFoundComponent } from './not-found/not-found.component';

import { MgrModulesModule } from '../ceph/cluster/mgr-modules/mgr-modules.module';
import { DashboardSettingModule } from './dashboard-settings/dashboard-setting.module';

@NgModule({
  imports: [CommonModule, NavigationModule, AuthModule, MgrModulesModule, SharedModule],
  exports: [NavigationModule, DashboardSettingModule],
  declarations: [NotFoundComponent, ForbiddenComponent]
})
export class CoreModule {}
