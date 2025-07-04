import { NgModule, CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { CommonModule } from '@angular/common';

import {
  UIShellModule,
  ToggleModule,
  TilesModule,
  IconModule,
  ButtonModule,
  TagModule,
  LinkModule,
  GridModule,
  HeaderModule,
  StructuredListModule
} from 'carbon-components-angular';

import { NotificationHeaderComponent } from './notification-header/notification-header.component';
import { NotificationAreaComponent } from './notification-area/notification-area.component';
import { NotificationFooterComponent } from './notification-footer/notification-footer.component';
import { CarbonNotificationPanelComponent } from './carbon-notification-panel/carbon-notification-panel.component';

@NgModule({
  declarations: [
    NotificationHeaderComponent,
    NotificationAreaComponent,
    NotificationFooterComponent,
    CarbonNotificationPanelComponent
  ],
  imports: [
    CommonModule,
    UIShellModule,
    ToggleModule,
    TilesModule,
    IconModule,
    ButtonModule,
    TagModule,
    LinkModule,
    GridModule,
    HeaderModule,
    StructuredListModule
  ],
  exports: [
    CarbonNotificationPanelComponent
  ],
  schemas: [CUSTOM_ELEMENTS_SCHEMA]
})
export class CarbonNotificationModule { } 