import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { TreeModule } from '@circlon/angular-tree-component';
import { NgbNavModule, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import { ChartsModule } from 'ng2-charts';

import { AppRoutingModule } from '~/app/app-routing.module';
import { SharedModule } from '~/app/shared/shared.module';
import { CephfsChartComponent } from './cephfs-chart/cephfs-chart.component';
import { CephfsClientsComponent } from './cephfs-clients/cephfs-clients.component';
import { CephfsDetailComponent } from './cephfs-detail/cephfs-detail.component';
import { CephfsDirectoriesComponent } from './cephfs-directories/cephfs-directories.component';
import { CephfsVolumeFormComponent } from './cephfs-form/cephfs-form.component';
import { CephfsListComponent } from './cephfs-list/cephfs-list.component';
import { CephfsTabsComponent } from './cephfs-tabs/cephfs-tabs.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    AppRoutingModule,
    ChartsModule,
    TreeModule,
    NgbNavModule,
    FormsModule,
    ReactiveFormsModule,
    NgbTypeaheadModule
  ],
  declarations: [
    CephfsDetailComponent,
    CephfsClientsComponent,
    CephfsChartComponent,
    CephfsListComponent,
    CephfsTabsComponent,
    CephfsVolumeFormComponent,
    CephfsDirectoriesComponent
  ]
})
export class CephfsModule {}
