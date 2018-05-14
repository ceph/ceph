import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { IscsiComponent } from './ceph/block/iscsi/iscsi.component';
import { MirroringComponent } from './ceph/block/mirroring/mirroring.component';
import { RbdFormComponent } from './ceph/block/rbd-form/rbd-form.component';
import { RbdListComponent } from './ceph/block/rbd-list/rbd-list.component';
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
import { PoolListComponent } from './ceph/pool/pool-list/pool-list.component';
import { Rgw501Component } from './ceph/rgw/rgw-501/rgw-501.component';
import { RgwBucketFormComponent } from './ceph/rgw/rgw-bucket-form/rgw-bucket-form.component';
import { RgwBucketListComponent } from './ceph/rgw/rgw-bucket-list/rgw-bucket-list.component';
import { RgwDaemonListComponent } from './ceph/rgw/rgw-daemon-list/rgw-daemon-list.component';
import { RgwUserFormComponent } from './ceph/rgw/rgw-user-form/rgw-user-form.component';
import { RgwUserListComponent } from './ceph/rgw/rgw-user-list/rgw-user-list.component';
import { LoginComponent } from './core/auth/login/login.component';
import { NotFoundComponent } from './core/not-found/not-found.component';
import { AuthGuardService } from './shared/services/auth-guard.service';
import { ModuleStatusGuardService } from './shared/services/module-status-guard.service';

const routes: Routes = [
  { path: '', redirectTo: 'dashboard', pathMatch: 'full' },
  { path: 'dashboard', component: DashboardComponent, canActivate: [AuthGuardService] },
  { path: 'hosts', component: HostsComponent, canActivate: [AuthGuardService] },
  { path: 'login', component: LoginComponent },
  { path: 'hosts', component: HostsComponent, canActivate: [AuthGuardService] },
  { path: 'rgw/501/:message', component: Rgw501Component, canActivate: [AuthGuardService] },
  {
    path: 'rgw',
    canActivateChild: [ModuleStatusGuardService],
    data: {
      moduleStatusGuardConfig: {
        apiPath: 'rgw',
        redirectTo: 'rgw/501'
      }
    },
    children: [
      {
        path: 'daemon',
        component: RgwDaemonListComponent,
        canActivate: [AuthGuardService]
      },
      {
        path: 'user',
        component: RgwUserListComponent,
        canActivate: [AuthGuardService]
      },
      {
        path: 'user/add',
        component: RgwUserFormComponent,
        canActivate: [AuthGuardService]
      },
      {
        path: 'user/edit/:uid',
        component: RgwUserFormComponent,
        canActivate: [AuthGuardService]
      },
      {
        path: 'bucket',
        component: RgwBucketListComponent,
        canActivate: [AuthGuardService]
      },
      {
        path: 'bucket/add',
        component: RgwBucketFormComponent,
        canActivate: [AuthGuardService]
      },
      {
        path: 'bucket/edit/:bucket',
        component: RgwBucketFormComponent,
        canActivate: [AuthGuardService]
      }
    ]
  },
  { path: 'block/iscsi', component: IscsiComponent, canActivate: [AuthGuardService] },
  { path: 'block/rbd', component: RbdListComponent, canActivate: [AuthGuardService] },
  { path: 'rbd/add', component: RbdFormComponent, canActivate: [AuthGuardService] },
  { path: 'rbd/edit/:pool/:name', component: RbdFormComponent, canActivate: [AuthGuardService] },
  { path: 'pool', component: PoolListComponent, canActivate: [AuthGuardService] },
  {
    path: 'rbd/clone/:pool/:name/:snap',
    component: RbdFormComponent,
    canActivate: [AuthGuardService]
  },
  {
    path: 'rbd/copy/:pool/:name',
    component: RbdFormComponent,
    canActivate: [AuthGuardService]
  },
  {
    path: 'rbd/copy/:pool/:name/:snap',
    component: RbdFormComponent,
    canActivate: [AuthGuardService]
  },
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
