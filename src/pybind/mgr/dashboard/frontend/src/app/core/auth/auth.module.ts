import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

import { NgbModule, NgbNavModule, NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxPipeFunctionModule } from 'ngx-pipe-function';

import { ActionLabels, URLVerbs } from '~/app/shared/constants/app.constants';
import { SharedModule } from '~/app/shared/shared.module';
import { LoginPasswordFormComponent } from './login-password-form/login-password-form.component';
import { LoginComponent } from './login/login.component';
import { RoleDetailsComponent } from './role-details/role-details.component';
import { RoleFormComponent } from './role-form/role-form.component';
import { RoleListComponent } from './role-list/role-list.component';
import { UserFormComponent } from './user-form/user-form.component';
import { UserListComponent } from './user-list/user-list.component';
import { UserPasswordFormComponent } from './user-password-form/user-password-form.component';
import { UserTabsComponent } from './user-tabs/user-tabs.component';

import {
  ButtonModule,
  CheckboxModule,
  DatePickerModule,
  GridModule,
  IconModule,
  IconService,
  InputModule,
  ModalModule,
  NumberModule,
  RadioModule,
  SelectModule,
  UIShellModule,
  TimePickerModule,
  ComboBoxModule
} from 'carbon-components-angular';
// Icons
import ChevronDown from '@carbon/icons/es/chevron--down/16';
import Close from '@carbon/icons/es/close/32';
import AddFilled from '@carbon/icons/es/add--filled/32';
import SubtractFilled from '@carbon/icons/es/subtract--filled/32';
import Reset from '@carbon/icons/es/reset/32';
import EyeIcon from '@carbon/icons/es/view/16';
@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    SharedModule,
    UIShellModule,
    InputModule,
    GridModule,
    ButtonModule,
    IconModule,
    CheckboxModule,
    RadioModule,
    SelectModule,
    NumberModule,
    ModalModule,
    DatePickerModule,
    TimePickerModule,
    NgbNavModule,
    NgbPopoverModule,
    NgxPipeFunctionModule,
    RouterModule,
    NgbModule,
    IconModule,
    GridModule,
    InputModule,
    ComboBoxModule
  ],
  declarations: [
    LoginComponent,
    LoginPasswordFormComponent,
    RoleDetailsComponent,
    RoleFormComponent,
    RoleListComponent,
    UserTabsComponent,
    UserListComponent,
    UserFormComponent,
    UserPasswordFormComponent
  ]
})
export class AuthModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([ChevronDown, Close, AddFilled, SubtractFilled, Reset, EyeIcon]);
  }
}

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
