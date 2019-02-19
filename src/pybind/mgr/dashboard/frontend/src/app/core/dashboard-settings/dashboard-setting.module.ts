import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { SharedModule } from '../../shared/shared.module';
import { SettingDetailsComponent } from './setting-details/setting-details.component';
import { SettingFormComponent } from './setting-form/setting-form.component';
import { SettingsViewComponent } from './settings-view/settings-view.component';

@NgModule({
  declarations: [SettingDetailsComponent, SettingsViewComponent, SettingFormComponent],
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    TabsModule.forRoot(),
    SharedModule,
    RouterModule
  ],
  exports: [SettingDetailsComponent, SettingFormComponent]
})
export class DashboardSettingModule {}
