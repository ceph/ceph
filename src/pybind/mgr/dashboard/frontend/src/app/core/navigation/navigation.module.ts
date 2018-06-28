import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { BsDropdownModule, CollapseModule, PopoverModule, TooltipModule } from 'ngx-bootstrap';

import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { AuthModule } from '../auth/auth.module';
import { AboutComponent } from './about/about.component';
import { DashboardHelpComponent } from './dashboard-help/dashboard-help.component';
import { NavigationComponent } from './navigation/navigation.component';
import { NotificationsComponent } from './notifications/notifications.component';
import { TaskManagerComponent } from './task-manager/task-manager.component';

@NgModule({
  entryComponents: [AboutComponent],
  imports: [
    CommonModule,
    AuthModule,
    CollapseModule.forRoot(),
    BsDropdownModule.forRoot(),
    PopoverModule.forRoot(),
    TooltipModule.forRoot(),
    AppRoutingModule,
    SharedModule,
    RouterModule
  ],
  declarations: [
    AboutComponent,
    NavigationComponent,
    NotificationsComponent,
    TaskManagerComponent,
    DashboardHelpComponent
  ],
  exports: [NavigationComponent]
})
export class NavigationModule {}
