import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { SharedModule } from '../shared/shared.module';
import { AuthModule } from './auth/auth.module';
import { NavigationModule } from './navigation/navigation.module';
import { NotFoundComponent } from './not-found/not-found.component';
import { UserInfoComponent } from './user-info/user-info.component';

@NgModule({
  imports: [
    CommonModule,
    NavigationModule,
    AuthModule,
    SharedModule,
    FormsModule,
    ReactiveFormsModule
  ],
  exports: [NavigationModule],
  declarations: [NotFoundComponent, UserInfoComponent]
})
export class CoreModule {}
