import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ChartsModule } from 'ng2-charts';
import { PopoverModule } from 'ngx-bootstrap/popover';

import { SharedModule } from '../../shared/shared.module';
import { CephSharedModule } from '../shared/ceph-shared.module';
import { DashboardComponent } from './dashboard/dashboard.component';
import { HealthPieComponent } from './health-pie/health-pie.component';
import { HealthComponent } from './health/health.component';
import { InfoCardComponent } from './info-card/info-card.component';
import { InfoGroupComponent } from './info-group/info-group.component';
import { MdsSummaryPipe } from './mds-summary.pipe';
import { MgrSummaryPipe } from './mgr-summary.pipe';
import { MonSummaryPipe } from './mon-summary.pipe';
import { OsdSummaryPipe } from './osd-summary.pipe';

@NgModule({
  imports: [
    CephSharedModule,
    CommonModule,
    NgbNavModule,
    SharedModule,
    ChartsModule,
    RouterModule,
    PopoverModule.forRoot()
  ],

  declarations: [
    HealthComponent,
    DashboardComponent,
    MonSummaryPipe,
    OsdSummaryPipe,
    MgrSummaryPipe,
    MdsSummaryPipe,
    HealthPieComponent,
    InfoCardComponent,
    InfoGroupComponent
  ]
})
export class DashboardModule {}
