import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { ChartsModule } from 'ng2-charts';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { SharedModule } from '../../shared/shared.module';
import { DashboardService } from './dashboard.service';
import { DashboardComponent } from './dashboard/dashboard.component';
import { HealthPieComponent } from './health-pie/health-pie.component';
import { HealthComponent } from './health/health.component';
import { LogColorPipe } from './log-color.pipe';
import { MdsSummaryPipe } from './mds-summary.pipe';
import { MgrSummaryPipe } from './mgr-summary.pipe';
import { MonSummaryPipe } from './mon-summary.pipe';
import { OsdSummaryPipe } from './osd-summary.pipe';
import { PgStatusStylePipe } from './pg-status-style.pipe';
import { PgStatusPipe } from './pg-status.pipe';

@NgModule({
  imports: [CommonModule, TabsModule.forRoot(), SharedModule, ChartsModule, RouterModule],
  declarations: [
    HealthComponent,
    DashboardComponent,
    MonSummaryPipe,
    OsdSummaryPipe,
    LogColorPipe,
    MgrSummaryPipe,
    PgStatusPipe,
    MdsSummaryPipe,
    PgStatusStylePipe,
    HealthPieComponent
  ],
  providers: [DashboardService]
})
export class DashboardModule {}
