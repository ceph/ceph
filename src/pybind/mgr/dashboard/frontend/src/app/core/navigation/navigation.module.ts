import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import { CollapseModule } from 'ngx-bootstrap/collapse';
import { SimplebarAngularModule } from 'simplebar-angular';

import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { AuthModule } from '../auth/auth.module';
import { AboutComponent } from './about/about.component';
import { AdministrationComponent } from './administration/administration.component';
import { BreadcrumbsComponent } from './breadcrumbs/breadcrumbs.component';
import { DashboardHelpComponent } from './dashboard-help/dashboard-help.component';
import { IdentityComponent } from './identity/identity.component';
import { NavigationComponent } from './navigation/navigation.component';
import { NotificationsComponent } from './notifications/notifications.component';

@NgModule({
  imports: [
    CommonModule,
    AuthModule,
    CollapseModule.forRoot(),
    NgbDropdownModule,
    AppRoutingModule,
    SharedModule,
    SimplebarAngularModule,
    RouterModule
  ],
  declarations: [
    AboutComponent,
    BreadcrumbsComponent,
    NavigationComponent,
    NotificationsComponent,
    DashboardHelpComponent,
    AdministrationComponent,
    IdentityComponent
  ],
  exports: [NavigationComponent, BreadcrumbsComponent]
})
export class NavigationModule {}
