import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import { ChartsModule } from 'ng2-charts';
import { SimplebarAngularModule } from 'simplebar-angular';

import { SharedModule } from '~/app/shared/shared.module';
import { CephSharedModule } from '../shared/ceph-shared.module';
import { CardComponent } from './card/card.component';
import { DashboardPieComponent } from './dashboard-pie/dashboard-pie.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { CardRowComponent } from './card-row/card-row.component';
import { PgSummaryPipe } from './pg-summary.pipe';

@NgModule({
  imports: [
    CephSharedModule,
    CommonModule,
    NgbNavModule,
    SharedModule,
    ChartsModule,
    RouterModule,
    NgbPopoverModule,
    FormsModule,
    ReactiveFormsModule,
    SimplebarAngularModule
  ],

  declarations: [
    DashboardComponent,
    CardComponent,
    DashboardPieComponent,
    DashboardPieComponent,
    CardRowComponent,
    PgSummaryPipe
  ]
})
export class NewDashboardModule {}
