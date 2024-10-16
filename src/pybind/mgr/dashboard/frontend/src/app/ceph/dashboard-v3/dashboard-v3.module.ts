import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { provideCharts, withDefaultRegisterables, BaseChartDirective } from 'ng2-charts';
import { SimplebarAngularModule } from 'simplebar-angular';

import { SharedModule } from '~/app/shared/shared.module';
import { CephSharedModule } from '../shared/ceph-shared.module';
import { DashboardAreaChartComponent } from './dashboard-area-chart/dashboard-area-chart.component';
import { DashboardPieComponent } from './dashboard-pie/dashboard-pie.component';
import { DashboardTimeSelectorComponent } from './dashboard-time-selector/dashboard-time-selector.component';
import { DashboardV3Component } from './dashboard/dashboard-v3.component';
import { PgSummaryPipe } from './pg-summary.pipe';
import { ToggletipModule } from 'carbon-components-angular';

@NgModule({
  imports: [
    CephSharedModule,
    CommonModule,
    NgbNavModule,
    SharedModule,
    RouterModule,
    NgbTooltipModule,
    FormsModule,
    ReactiveFormsModule,
    SimplebarAngularModule,
    BaseChartDirective,
    ToggletipModule
  ],
  declarations: [
    DashboardV3Component,
    DashboardPieComponent,
    PgSummaryPipe,
    DashboardAreaChartComponent,
    DashboardTimeSelectorComponent
  ],
  exports: [
    DashboardV3Component,
    DashboardAreaChartComponent,
    DashboardTimeSelectorComponent,
    DashboardPieComponent
  ],
  providers: [provideCharts(withDefaultRegisterables())]
})
export class DashboardV3Module {}
