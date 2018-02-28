import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';

import { AppRoutingModule } from '../../app-routing.module';
import { SharedModule } from '../../shared/shared.module';
import { CephfsChartComponent } from './cephfs-chart/cephfs-chart.component';
import { CephfsService } from './cephfs.service';
import { CephfsComponent } from './cephfs/cephfs.component';
import { ClientsComponent } from './clients/clients.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    AppRoutingModule,
    ChartsModule,
    ProgressbarModule.forRoot()
  ],
  declarations: [CephfsComponent, ClientsComponent, CephfsChartComponent],
  providers: [CephfsService]
})
export class CephfsModule {}
