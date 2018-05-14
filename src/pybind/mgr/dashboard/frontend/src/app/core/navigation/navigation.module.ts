import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { BsDropdownModule, CollapseModule, PopoverModule } from 'ngx-bootstrap';

import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { AuthModule } from '../auth/auth.module';
import { NavigationComponent } from './navigation/navigation.component';
import { NotificationsComponent } from './notifications/notifications.component';
import { TaskManagerComponent } from './task-manager/task-manager.component';

@NgModule({
  imports: [
    CommonModule,
    AuthModule,
    CollapseModule.forRoot(),
    BsDropdownModule.forRoot(),
    PopoverModule.forRoot(),
    AppRoutingModule,
    SharedModule,
    RouterModule
  ],
  declarations: [NavigationComponent, NotificationsComponent, TaskManagerComponent],
  exports: [NavigationComponent]
})
export class NavigationModule {}
