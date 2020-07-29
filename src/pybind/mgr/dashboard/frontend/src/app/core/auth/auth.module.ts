import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

import { NgbNavModule, NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';

import { ActionLabels, URLVerbs } from '../../shared/constants/app.constants';
import { SharedModule } from '../../shared/shared.module';
import { LoginPasswordFormComponent } from './login-password-form/login-password-form.component';
import { LoginComponent } from './login/login.component';
import { RoleDetailsComponent } from './role-details/role-details.component';
import { RoleFormComponent } from './role-form/role-form.component';
import { RoleListComponent } from './role-list/role-list.component';
import { SsoNotFoundComponent } from './sso/sso-not-found/sso-not-found.component';
import { UserFormComponent } from './user-form/user-form.component';
import { UserListComponent } from './user-list/user-list.component';
import { UserPasswordFormComponent } from './user-password-form/user-password-form.component';
import { UserTabsComponent } from './user-tabs/user-tabs.component';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    SharedModule,
    NgbNavModule,
    NgbPopoverModule,
    RouterModule,
    NgBootstrapFormValidationModule
  ],
  declarations: [
    LoginComponent,
    LoginPasswordFormComponent,
    RoleDetailsComponent,
    RoleFormComponent,
    RoleListComponent,
    SsoNotFoundComponent,
    UserTabsComponent,
    UserListComponent,
    UserFormComponent,
    UserPasswordFormComponent
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
