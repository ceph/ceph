import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterModule } from '@angular/router';

import { CollapseModule } from 'ngx-bootstrap/collapse';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { PopoverModule } from 'ngx-bootstrap/popover';
import { TooltipModule } from 'ngx-bootstrap/tooltip';
import { SimplebarAngularModule } from 'simplebar-angular';

import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { AuthModule } from '../auth/auth.module';
import { AboutComponent } from './about/about.component';
import { AdministrationComponent } from './administration/administration.component';
import { BreadcrumbsComponent } from './breadcrumbs/breadcrumbs.component';
import { DashboardHelpComponent } from './dashboard-help/dashboard-help.component';
import { IdentityComponent } from './identity/identity.component';
import { MenuItemComponent } from './menu-item/menu-item.component';
import { MenuParentItemComponent } from './menu-parent-item/menu-parent-item.component';
import { MenuComponent } from './menu/menu.component';
import { NavigationComponent } from './navigation/navigation.component';
import { NotificationsComponent } from './notifications/notifications.component';

@NgModule({
  entryComponents: [AboutComponent],
  imports: [
    CommonModule,
    AuthModule,
    BrowserAnimationsModule,
    CollapseModule.forRoot(),
    BsDropdownModule.forRoot(),
    PopoverModule.forRoot(),
    TooltipModule.forRoot(),
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
    IdentityComponent,
    MenuComponent,
    MenuItemComponent,
    MenuParentItemComponent
  ],
  exports: [NavigationComponent, BreadcrumbsComponent]
})
export class NavigationModule {}
