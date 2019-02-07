import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';

import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { MgrModulesListComponent } from './mgr-modules-list/mgr-modules-list.component';
import { TelemetryComponent } from './telemetry/telemetry.component';

@NgModule({
  imports: [CommonModule, ReactiveFormsModule, SharedModule, AppRoutingModule],
  declarations: [TelemetryComponent, MgrModulesListComponent]
})
export class MgrModulesModule {}
