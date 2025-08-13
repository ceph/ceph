import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { SharedModule } from '~/app/shared/shared.module';
import { NotificationsPageComponent } from './notifications-page/notifications-page.component';

import {
  IconModule,
  SearchModule,
  StructuredListModule,
  TagModule
} from 'carbon-components-angular';

@NgModule({
  declarations: [NotificationsPageComponent],
  imports: [
    CommonModule,
    FormsModule,
    NgbModule,
    SharedModule,
    IconModule,
    SearchModule,
    StructuredListModule,
    TagModule
  ],
  exports: [NotificationsPageComponent]
})
export class NotificationsModule {}

const routes: Routes = [
  {
    path: '',
    component: NotificationsPageComponent,
    data: { breadcrumbs: 'Notifications' }
  }
];

@NgModule({
  imports: [NotificationsModule, RouterModule.forChild(routes)]
})
export class RoutedNotificationsModule {}
