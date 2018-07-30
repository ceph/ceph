import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { BsDropdownModule, PopoverModule, TabsModule } from 'ngx-bootstrap';

import { SharedModule } from '../../shared/shared.module';
import { LoginComponent } from './login/login.component';
import { LogoutComponent } from './logout/logout.component';
import { RoleDetailsComponent } from './role-details/role-details.component';
import { RoleFormComponent } from './role-form/role-form.component';
import { RoleListComponent } from './role-list/role-list.component';
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
    LogoutComponent,
    RoleDetailsComponent,
    RoleFormComponent,
    RoleListComponent,
    UserTabsComponent,
    UserListComponent,
    UserFormComponent
  ],
  exports: [LogoutComponent]
})
export class AuthModule {}
