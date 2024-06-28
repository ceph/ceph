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
  DialogModule,
  GridModule
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
import DashboardIcon from '@carbon/icons/es/template/20';
import ClusterIcon from '@carbon/icons/es/web-services--cluster/20';
import MultiClusterIcon from '@carbon/icons/es/edge-cluster/20';
import BlockIcon from '@carbon/icons/es/datastore/20';
import ObjectIcon from '@carbon/icons/es/object-storage/20';
import FileIcon from '@carbon/icons/es/file-storage/20';
import ObservabilityIcon from '@carbon/icons/es/observed--hail/20';
import AdminIcon from '@carbon/icons/es/network--admin-control/20';
import LockedIcon from '@carbon/icons/es/locked/16';
import LogoutIcon from '@carbon/icons/es/logout/16';

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
    DialogModule,
    GridModule
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
      DashboardIcon,
      ClusterIcon,
      MultiClusterIcon,
      BlockIcon,
      ObjectIcon,
      FileIcon,
      ObservabilityIcon,
      AdminIcon,
      LockedIcon,
      LogoutIcon
    ]);
  }
}
