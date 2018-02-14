import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { PoolDetailComponent } from './ceph/block/pool-detail/pool-detail.component';
import { HostsComponent } from './ceph/cluster/hosts/hosts.component';
import { MonitorComponent } from './ceph/cluster/monitor/monitor.component';
import { DashboardComponent } from './ceph/dashboard/dashboard/dashboard.component';
import {
  PerformanceCounterComponent
} from './ceph/performance-counter/performance-counter/performance-counter.component';
import { RgwDaemonListComponent } from './ceph/rgw/rgw-daemon-list/rgw-daemon-list.component';
import { LoginComponent } from './core/auth/login/login.component';
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
  { path: 'block/pool/:name', component: PoolDetailComponent, canActivate: [AuthGuardService] },
  {
    path: 'perf_counters/:type/:id',
    component: PerformanceCounterComponent,
    canActivate: [AuthGuardService]
  },
  { path: 'monitor', component: MonitorComponent, canActivate: [AuthGuardService] }
];

@NgModule({
  imports: [RouterModule.forRoot(routes, { useHash: true })],
  exports: [RouterModule]
})
export class AppRoutingModule { }
