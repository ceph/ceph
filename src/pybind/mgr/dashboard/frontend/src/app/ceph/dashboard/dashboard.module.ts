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
import { InputModule, ModalModule, SelectModule, ToggletipModule } from 'carbon-components-angular';

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
    ToggletipModule,
    ModalModule,
    InputModule,
    SelectModule
  ],
  declarations: [DashboardComponent, HealthPieComponent, FeedbackComponent],
  providers: [provideCharts(withDefaultRegisterables())]
})
export class DashboardModule {}
