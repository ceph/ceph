import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { NgbCollapseModule, NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import { SimplebarAngularModule } from 'simplebar-angular';
import {
  UIShellModule,
  IconService,
  IconModule,
  ThemeModule,
  DialogModule
} from 'carbon-components-angular';

import { AppRoutingModule } from '~/app/app-routing.module';
import { SharedModule } from '~/app/shared/shared.module';
import { AuthModule } from '../auth/auth.module';
import { AboutComponent } from './about/about.component';
import { AdministrationComponent } from './administration/administration.component';
import { ApiDocsComponent } from './api-docs/api-docs.component';
import { BreadcrumbsComponent } from './breadcrumbs/breadcrumbs.component';
import { DashboardHelpComponent } from './dashboard-help/dashboard-help.component';
import { IdentityComponent } from './identity/identity.component';
import { NavigationComponent } from './navigation/navigation.component';
import { NotificationsComponent } from './notifications/notifications.component';

// Icons
import UserFilledIcon from '@carbon/icons/es/user--filled/20';
import SettingsIcon from '@carbon/icons/es/settings/20';
import HelpIcon from '@carbon/icons/es/help/20';
import NotificationIcon from '@carbon/icons/es/notification/20';
import LaunchIcon from '@carbon/icons/es/launch/16';
import DashboardIcon from '@carbon/icons/es/template/16';

@NgModule({
  imports: [
    CommonModule,
    AuthModule,
    NgbCollapseModule,
    NgbDropdownModule,
    AppRoutingModule,
    SharedModule,
    SimplebarAngularModule,
    RouterModule,
    UIShellModule,
    IconModule,
    ThemeModule,
    DialogModule
  ],
  declarations: [
    AboutComponent,
    ApiDocsComponent,
    BreadcrumbsComponent,
    NavigationComponent,
    NotificationsComponent,
    DashboardHelpComponent,
    AdministrationComponent,
    IdentityComponent
  ],
  exports: [NavigationComponent, BreadcrumbsComponent]
})
export class NavigationModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([
      UserFilledIcon,
      SettingsIcon,
      HelpIcon,
      NotificationIcon,
      LaunchIcon,
      DashboardIcon
    ]);
  }
}