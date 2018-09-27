import { NgModule } from '@angular/core';
import { ActivatedRouteSnapshot, RouterModule, Routes } from '@angular/router';

import { IscsiComponent } from './ceph/block/iscsi/iscsi.component';
import { MirroringComponent } from './ceph/block/mirroring/mirroring.component';
import { RbdFormComponent } from './ceph/block/rbd-form/rbd-form.component';
import { RbdImagesComponent } from './ceph/block/rbd-images/rbd-images.component';
import { CephfsListComponent } from './ceph/cephfs/cephfs-list/cephfs-list.component';
import { ConfigurationComponent } from './ceph/cluster/configuration/configuration.component';
import { HostsComponent } from './ceph/cluster/hosts/hosts.component';
import { MonitorComponent } from './ceph/cluster/monitor/monitor.component';
import { OsdListComponent } from './ceph/cluster/osd/osd-list/osd-list.component';
import { DashboardComponent } from './ceph/dashboard/dashboard/dashboard.component';
import { PerformanceCounterComponent } from './ceph/performance-counter/performance-counter/performance-counter.component';
import { PoolListComponent } from './ceph/pool/pool-list/pool-list.component';
import { Rgw501Component } from './ceph/rgw/rgw-501/rgw-501.component';
import { RgwBucketFormComponent } from './ceph/rgw/rgw-bucket-form/rgw-bucket-form.component';
import { RgwBucketListComponent } from './ceph/rgw/rgw-bucket-list/rgw-bucket-list.component';
import { RgwDaemonListComponent } from './ceph/rgw/rgw-daemon-list/rgw-daemon-list.component';
import { RgwUserFormComponent } from './ceph/rgw/rgw-user-form/rgw-user-form.component';
import { RgwUserListComponent } from './ceph/rgw/rgw-user-list/rgw-user-list.component';
import { LoginComponent } from './core/auth/login/login.component';
import { RoleFormComponent } from './core/auth/role-form/role-form.component';
import { RoleListComponent } from './core/auth/role-list/role-list.component';
import { UserFormComponent } from './core/auth/user-form/user-form.component';
import { UserListComponent } from './core/auth/user-list/user-list.component';
import { ForbiddenComponent } from './core/forbidden/forbidden.component';
import { NotFoundComponent } from './core/not-found/not-found.component';
import { BreadcrumbsResolver, IBreadcrumb } from './shared/models/breadcrumbs';
import { AuthGuardService } from './shared/services/auth-guard.service';
import { ModuleStatusGuardService } from './shared/services/module-status-guard.service';

export class PerformanceCounterBreadcrumbsResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot) {
    const result: IBreadcrumb[] = [];

    const fromPath = route.queryParams.fromLink || null;
    let fromText = '';
    switch (fromPath) {
      case '/monitor':
        fromText = 'Monitors';
        break;
      case '/hosts':
        fromText = 'Hosts';
        break;
    }
    result.push({ text: 'Cluster', path: null });
    result.push({ text: fromText, path: fromPath });
    result.push({ text: 'Performance Counters', path: '' });

    return result;
  }
}

const routes: Routes = [
  // Dashboard
  { path: '', redirectTo: 'dashboard', pathMatch: 'full' },
  { path: 'dashboard', component: DashboardComponent, canActivate: [AuthGuardService] },
  // Cluster
  {
    path: 'hosts',
    component: HostsComponent,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Cluster/Hosts' }
  },
  {
    path: 'monitor',
    component: MonitorComponent,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Cluster/Monitors' }
  },
  {
    path: 'osd',
    component: OsdListComponent,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Cluster/OSDs' }
  },
  {
    path: 'configuration',
    component: ConfigurationComponent,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Cluster/Configuration Documentation' }
  },
  {
    path: 'perf_counters/:type/:id',
    component: PerformanceCounterComponent,
    canActivate: [AuthGuardService],
    data: {
      breadcrumbs: PerformanceCounterBreadcrumbsResolver
    }
  },
  // Pools
  {
    path: 'pool',
    component: PoolListComponent,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Pools' }
  },
  // Block
  {
    path: 'block',
    canActivateChild: [AuthGuardService],
    canActivate: [AuthGuardService],
    data: { breadcrumbs: true, text: 'Block', path: null },
    children: [
      {
        path: 'rbd',
        data: { breadcrumbs: 'Images' },
        children: [
          { path: '', component: RbdImagesComponent },
          { path: 'add', component: RbdFormComponent, data: { breadcrumbs: 'Add' } },
          { path: 'edit/:pool/:name', component: RbdFormComponent, data: { breadcrumbs: 'Edit' } },
          {
            path: 'clone/:pool/:name/:snap',
            component: RbdFormComponent,
            data: { breadcrumbs: 'Clone' }
          },
          { path: 'copy/:pool/:name', component: RbdFormComponent, data: { breadcrumbs: 'Copy' } },
          {
            path: 'copy/:pool/:name/:snap',
            component: RbdFormComponent,
            data: { breadcrumbs: 'Copy' }
          }
        ]
      },
      {
        path: 'mirroring',
        component: MirroringComponent,
        data: { breadcrumbs: 'Mirroring' }
      },
      { path: 'iscsi', component: IscsiComponent, data: { breadcrumbs: 'iSCSI' } }
    ]
  },
  // Filesystems
  {
    path: 'cephfs',
    component: CephfsListComponent,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Filesystems' }
  },
  // Object Gateway
  {
    path: 'rgw/501/:message',
    component: Rgw501Component,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Object Gateway' }
  },
  {
    path: 'rgw',
    canActivateChild: [ModuleStatusGuardService, AuthGuardService],
    data: {
      moduleStatusGuardConfig: {
        apiPath: 'rgw',
        redirectTo: 'rgw/501'
      },
      breadcrumbs: true,
      text: 'Object Gateway',
      path: null
    },
    children: [
      { path: 'daemon', component: RgwDaemonListComponent, data: { breadcrumbs: 'Daemons' } },
      {
        path: 'user',
        data: { breadcrumbs: 'Users' },
        children: [
          { path: '', component: RgwUserListComponent },
          { path: 'add', component: RgwUserFormComponent, data: { breadcrumbs: 'Add' } },
          { path: 'edit/:uid', component: RgwUserFormComponent, data: { breadcrumbs: 'Edit' } }
        ]
      },
      {
        path: 'bucket',
        data: { breadcrumbs: 'Buckets' },
        children: [
          { path: '', component: RgwBucketListComponent },
          { path: 'add', component: RgwBucketFormComponent, data: { breadcrumbs: 'Add' } },
          { path: 'edit/:bucket', component: RgwBucketFormComponent, data: { breadcrumbs: 'Edit' } }
        ]
      }
    ]
  },
  // Dashboard Settings
  {
    path: 'user-management',
    canActivate: [AuthGuardService],
    canActivateChild: [AuthGuardService],
    data: { breadcrumbs: 'User management', path: null },
    children: [
      {
        path: '',
        redirectTo: 'users',
        pathMatch: 'full'
      },
      {
        path: 'users',
        data: { breadcrumbs: 'Users' },
        children: [
          { path: '', component: UserListComponent },
          { path: 'add', component: UserFormComponent, data: { breadcrumbs: 'Add' } },
          { path: 'edit/:username', component: UserFormComponent, data: { breadcrumbs: 'Edit' } }
        ]
      },
      {
        path: 'roles',
        data: { breadcrumbs: 'Roles' },
        children: [
          { path: '', component: RoleListComponent },
          { path: 'add', component: RoleFormComponent, data: { breadcrumbs: 'Add' } },
          { path: 'edit/:name', component: RoleFormComponent, data: { breadcrumbs: 'Edit' } }
        ]
      }
    ]
  },
  // System
  { path: 'login', component: LoginComponent },
  { path: '403', component: ForbiddenComponent },
  { path: '404', component: NotFoundComponent },
  { path: '**', redirectTo: '/404' }
];

@NgModule({
  imports: [RouterModule.forRoot(routes, { useHash: true })],
  exports: [RouterModule],
  providers: [PerformanceCounterBreadcrumbsResolver]
})
export class AppRoutingModule {}
