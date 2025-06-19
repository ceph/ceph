import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { provideCharts, withDefaultRegisterables, BaseChartDirective } from 'ng2-charts';

import { SharedModule } from '~/app/shared/shared.module';
import { DashboardV3Module } from '../dashboard-v3/dashboard-v3.module';
import { CephSharedModule } from '../shared/ceph-shared.module';
import { FeedbackComponent } from '../shared/feedback/feedback.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { HealthPieComponent } from './health-pie/health-pie.component';
import { HealthComponent } from './health/health.component';
import { InfoCardComponent } from './info-card/info-card.component';
import { InfoGroupComponent } from './info-group/info-group.component';
import { MdsDashboardSummaryPipe } from './mds-dashboard-summary.pipe';
import { MgrDashboardSummaryPipe } from './mgr-dashboard-summary.pipe';
import { MonSummaryPipe } from './mon-summary.pipe';
import { osdDashboardSummaryPipe } from './osd-dashboard-summary.pipe';
import { ToggletipModule } from 'carbon-components-angular';

@NgModule({
  imports: [
    CephSharedModule,
    CommonModule,
    NgbNavModule,
    SharedModule,
    RouterModule,
    FormsModule,
    ReactiveFormsModule,
    DashboardV3Module,
    BaseChartDirective,
    ToggletipModule
  ],
  declarations: [
    HealthComponent,
    DashboardComponent,
    MonSummaryPipe,
    osdDashboardSummaryPipe,
    MgrDashboardSummaryPipe,
    MdsDashboardSummaryPipe,
    HealthPieComponent,
    InfoCardComponent,
    InfoGroupComponent,
    FeedbackComponent
  ],
  providers: [provideCharts(withDefaultRegisterables())]
})
export class DashboardModule {}
