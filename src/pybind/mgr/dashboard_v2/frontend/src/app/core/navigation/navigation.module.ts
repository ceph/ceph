import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NavigationComponent } from './navigation/navigation.component';
import { AuthModule } from '../auth/auth.module';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { RouterModule } from '@angular/router';

@NgModule({
  imports: [
    CommonModule,
    AuthModule,
    BsDropdownModule.forRoot(),
    AppRoutingModule,
    SharedModule,
    RouterModule
  ],
  declarations: [NavigationComponent],
  exports: [NavigationComponent]
})
export class NavigationModule {}
