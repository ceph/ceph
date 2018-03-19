import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { IscsiComponent } from './ceph/block/iscsi/iscsi.component';
import { MirroringComponent } from './ceph/block/mirroring/mirroring.component';
import { PoolDetailComponent } from './ceph/block/pool-detail/pool-detail.component';
import { CephfsComponent } from './ceph/cephfs/cephfs/cephfs.component';
import { ClientsComponent } from './ceph/cephfs/clients/clients.component';
import { ConfigurationComponent } from './ceph/cluster/configuration/configuration.component';
import { HostsComponent } from './ceph/cluster/hosts/hosts.component';
import { MonitorComponent } from './ceph/cluster/monitor/monitor.component';
import { OsdListComponent } from './ceph/cluster/osd/osd-list/osd-list.component';
import { DashboardComponent } from './ceph/dashboard/dashboard/dashboard.component';
import {
  PerformanceCounterComponent
} from './ceph/performance-counter/performance-counter/performance-counter.component';
import { RgwDaemonListComponent } from './ceph/rgw/rgw-daemon-list/rgw-daemon-list.component';
import { LoginComponent } from './core/auth/login/login.component';
import { NotFoundComponent } from './core/not-found/not-found.component';
import { AuthGuardService } from './shared/services/auth-guard.service';

const routes: Routes = [
  { path: '', redirectTo: 'dashboard', pathMatch: 'full' },
  { path: 'dashboard', component: DashboardComponent, canActivate: [AuthGuardService] },
  { path: 'hosts', component: HostsComponent, canActivate: [AuthGuardService] },
  { path: 'login', component: LoginComponent },
  { path: 'hosts', component: HostsComponent, canActivate: [AuthGuardService] },
  {
    path: 'rgw',
    component: RgwDaemonListComponent,
    canActivate: [AuthGuardService]
  },
  { path: 'block/iscsi', component: IscsiComponent, canActivate: [AuthGuardService] },
  { path: 'block/pool/:name', component: PoolDetailComponent, canActivate: [AuthGuardService] },
  {
    path: 'perf_counters/:type/:id',
    component: PerformanceCounterComponent,
    canActivate: [AuthGuardService]
  },
  { path: 'monitor', component: MonitorComponent, canActivate: [AuthGuardService] },
  { path: 'cephfs/:id/clients', component: ClientsComponent, canActivate: [AuthGuardService] },
  { path: 'cephfs/:id', component: CephfsComponent, canActivate: [AuthGuardService] },
  { path: 'configuration', component: ConfigurationComponent, canActivate: [AuthGuardService] },
  { path: 'mirroring', component: MirroringComponent, canActivate: [AuthGuardService] },
  { path: '404', component: NotFoundComponent },
  { path: 'osd', component: OsdListComponent, canActivate: [AuthGuardService] },
  { path: '**', redirectTo: '/404'}
];

@NgModule({
  imports: [RouterModule.forRoot(routes, { useHash: true })],
  exports: [RouterModule]
})
export class AppRoutingModule { }
