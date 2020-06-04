import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { TreeModule } from 'angular-tree-component';
import { ChartsModule } from 'ng2-charts';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';

import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { CephfsChartComponent } from './cephfs-chart/cephfs-chart.component';
import { CephfsClientsComponent } from './cephfs-clients/cephfs-clients.component';
import { CephfsDetailComponent } from './cephfs-detail/cephfs-detail.component';
import { CephfsDirectoriesComponent } from './cephfs-directories/cephfs-directories.component';
import { CephfsListComponent } from './cephfs-list/cephfs-list.component';
import { CephfsTabsComponent } from './cephfs-tabs/cephfs-tabs.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    AppRoutingModule,
    ChartsModule,
    TreeModule.forRoot(),
    ProgressbarModule.forRoot(),
    NgbNavModule
  ],
  declarations: [
    CephfsDetailComponent,
    CephfsClientsComponent,
    CephfsChartComponent,
    CephfsListComponent,
    CephfsTabsComponent,
    CephfsDirectoriesComponent
  ]
})
export class CephfsModule {}
