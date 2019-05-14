import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { PopoverModule } from 'ngx-bootstrap/popover';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { ActionLabels, URLVerbs } from '../../shared/constants/app.constants';
import { SharedModule } from '../../shared/shared.module';
import { LoginComponent } from './login/login.component';
import { RoleDetailsComponent } from './role-details/role-details.component';
import { RoleFormComponent } from './role-form/role-form.component';
import { RoleListComponent } from './role-list/role-list.component';
import { SsoNotFoundComponent } from './sso/sso-not-found/sso-not-found.component';
import { UserFormComponent } from './user-form/user-form.component';
import { UserListComponent } from './user-list/user-list.component';
import { UserTabsComponent } from './user-tabs/user-tabs.component';

@NgModule({
  imports: [
    BsDropdownModule.forRoot(),
    CommonModule,
    FormsModule,
    PopoverModule.forRoot(),
    ReactiveFormsModule,
    SharedModule,
    TabsModule.forRoot(),
    RouterModule
  ],
  declarations: [
    LoginComponent,
    RoleDetailsComponent,
    RoleFormComponent,
    RoleListComponent,
    SsoNotFoundComponent,
    UserTabsComponent,
    UserListComponent,
    UserFormComponent
  ]
})
export class AuthModule {}

const routes: Routes = [
  { path: '', redirectTo: 'users', pathMatch: 'full' },
  {
    path: 'users',
    data: { breadcrumbs: 'Users' },
    children: [
      { path: '', component: UserListComponent },
      {
        path: URLVerbs.CREATE,
        component: UserFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:username`,
        component: UserFormComponent,
        data: { breadcrumbs: ActionLabels.EDIT }
      }
    ]
  },
  {
    path: 'roles',
    data: { breadcrumbs: 'Roles' },
    children: [
      { path: '', component: RoleListComponent },
      {
        path: URLVerbs.CREATE,
        component: RoleFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:name`,
        component: RoleFormComponent,
        data: { breadcrumbs: ActionLabels.EDIT }
      }
    ]
  }
];

@NgModule({
  imports: [AuthModule, RouterModule.forChild(routes)]
})
export class RoutedAuthModule {}
