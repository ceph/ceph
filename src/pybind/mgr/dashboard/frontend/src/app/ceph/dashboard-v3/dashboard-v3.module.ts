import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbPopoverModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { NgChartsModule } from 'ng2-charts';
import { SimplebarAngularModule } from 'simplebar-angular';

import { SharedModule } from '~/app/shared/shared.module';
import { CephSharedModule } from '../shared/ceph-shared.module';
import { DashboardAreaChartComponent } from './dashboard-area-chart/dashboard-area-chart.component';
import { DashboardPieComponent } from './dashboard-pie/dashboard-pie.component';
import { DashboardTimeSelectorComponent } from './dashboard-time-selector/dashboard-time-selector.component';
import { DashboardV3Component } from './dashboard/dashboard-v3.component';
import { PgSummaryPipe } from './pg-summary.pipe';

@NgModule({
  imports: [
    CephSharedModule,
    CommonModule,
    NgbNavModule,
    SharedModule,
    NgChartsModule,
    RouterModule,
    NgbPopoverModule,
    NgbTooltipModule,
    FormsModule,
    ReactiveFormsModule,
    SimplebarAngularModule
  ],

  declarations: [
    DashboardV3Component,
    DashboardPieComponent,
    PgSummaryPipe,
    DashboardAreaChartComponent,
    DashboardTimeSelectorComponent
  ],

  exports: [DashboardV3Component, DashboardAreaChartComponent, DashboardTimeSelectorComponent]
})
export class DashboardV3Module {}
