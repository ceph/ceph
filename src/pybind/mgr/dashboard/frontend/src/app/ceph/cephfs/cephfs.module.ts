import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { CephfsChartComponent } from './cephfs-chart/cephfs-chart.component';
import { CephfsDetailComponent } from './cephfs-detail/cephfs-detail.component';
import { CephfsListComponent } from './cephfs-list/cephfs-list.component';
import { ClientsComponent } from './clients/clients.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    AppRoutingModule,
    ChartsModule,
    ProgressbarModule.forRoot(),
    TabsModule.forRoot()
  ],
  declarations: [CephfsDetailComponent, ClientsComponent, CephfsChartComponent, CephfsListComponent]
})
export class CephfsModule {}
