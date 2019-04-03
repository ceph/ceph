import { NgModule } from '@angular/core';
import { ActivatedRouteSnapshot, PreloadAllModules, RouterModule, Routes } from '@angular/router';

import * as _ from 'lodash';

import { CephfsListComponent } from './ceph/cephfs/cephfs-list/cephfs-list.component';
import { ConfigurationFormComponent } from './ceph/cluster/configuration/configuration-form/configuration-form.component';
import { ConfigurationComponent } from './ceph/cluster/configuration/configuration.component';
import { CrushmapComponent } from './ceph/cluster/crushmap/crushmap.component';
import { HostsComponent } from './ceph/cluster/hosts/hosts.component';
import { LogsComponent } from './ceph/cluster/logs/logs.component';
import { MgrModuleFormComponent } from './ceph/cluster/mgr-modules/mgr-module-form/mgr-module-form.component';
import { MgrModuleListComponent } from './ceph/cluster/mgr-modules/mgr-module-list/mgr-module-list.component';
import { MonitorComponent } from './ceph/cluster/monitor/monitor.component';
import { OsdListComponent } from './ceph/cluster/osd/osd-list/osd-list.component';
import { PrometheusListComponent } from './ceph/cluster/prometheus/prometheus-list/prometheus-list.component';
import { DashboardComponent } from './ceph/dashboard/dashboard/dashboard.component';
import { Nfs501Component } from './ceph/nfs/nfs-501/nfs-501.component';
import { NfsFormComponent } from './ceph/nfs/nfs-form/nfs-form.component';
import { NfsListComponent } from './ceph/nfs/nfs-list/nfs-list.component';
import { PerformanceCounterComponent } from './ceph/performance-counter/performance-counter/performance-counter.component';
import { LoginComponent } from './core/auth/login/login.component';
import { SsoNotFoundComponent } from './core/auth/sso/sso-not-found/sso-not-found.component';
import { ForbiddenComponent } from './core/forbidden/forbidden.component';
import { NotFoundComponent } from './core/not-found/not-found.component';
import { BreadcrumbsResolver, IBreadcrumb } from './shared/models/breadcrumbs';
import { AuthGuardService } from './shared/services/auth-guard.service';
import { FeatureTogglesGuardService } from './shared/services/feature-toggles-guard.service';
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

export class StartCaseBreadcrumbsResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot) {
    const path = route.params.name;
    const text = _.startCase(path);
    return [{ text: text, path: path }];
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
    canActivate: [AuthGuardService],
    canActivateChild: [AuthGuardService],
    data: { breadcrumbs: 'Cluster/OSDs' },
    children: [{ path: '', component: OsdListComponent }]
  },
  {
    path: 'configuration',
    data: { breadcrumbs: 'Cluster/Configuration' },
    children: [
      { path: '', component: ConfigurationComponent },
      {
        path: 'edit/:name',
        component: ConfigurationFormComponent,
        data: { breadcrumbs: 'Edit' }
      }
    ]
  },
  {
    path: 'crush-map',
    component: CrushmapComponent,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Cluster/CRUSH map' }
  },
  {
    path: 'logs',
    component: LogsComponent,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Cluster/Logs' }
  },
  {
    path: 'alerts',
    component: PrometheusListComponent,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'Cluster/Alerts' }
  },
  {
    path: 'perf_counters/:type/:id',
    component: PerformanceCounterComponent,
    canActivate: [AuthGuardService],
    data: {
      breadcrumbs: PerformanceCounterBreadcrumbsResolver
    }
  },
  // Mgr modules
  {
    path: 'mgr-modules',
    canActivate: [AuthGuardService],
    canActivateChild: [AuthGuardService],
    data: { breadcrumbs: 'Cluster/Manager modules' },
    children: [
      {
        path: '',
        component: MgrModuleListComponent
      },
      {
        path: 'edit/:name',
        component: MgrModuleFormComponent,
        data: {
          breadcrumbs: StartCaseBreadcrumbsResolver
        }
      }
    ]
  },
  // Pools
  {
    path: 'pool',
    canActivate: [AuthGuardService],
    canActivateChild: [AuthGuardService],
    data: { breadcrumbs: 'Pools' },
    loadChildren: './ceph/pool/pool.module#RoutedPoolModule'
  },
  // Block
  {
    path: 'block',
    canActivateChild: [AuthGuardService],
    canActivate: [AuthGuardService],
    data: { breadcrumbs: true, text: 'Block', path: null },
    loadChildren: './ceph/block/block.module#RoutedBlockModule'
  },
  // Filesystems
  {
    path: 'cephfs',
    component: CephfsListComponent,
    canActivate: [FeatureTogglesGuardService, AuthGuardService],
    data: { breadcrumbs: 'Filesystems' }
  },
  // Object Gateway
  {
    path: 'rgw',
    canActivateChild: [FeatureTogglesGuardService, ModuleStatusGuardService, AuthGuardService],
    data: {
      moduleStatusGuardConfig: {
        apiPath: 'rgw',
        redirectTo: 'rgw/501'
      },
      breadcrumbs: true,
      text: 'Object Gateway',
      path: null
    },
    loadChildren: './ceph/rgw/rgw.module#RoutedRgwModule'
  },
  // Dashboard Settings
  {
    path: 'user-management',
    canActivate: [AuthGuardService],
    canActivateChild: [AuthGuardService],
    data: { breadcrumbs: 'User management', path: null },
    loadChildren: './core/auth/auth.module#RoutedAuthModule'
  },
  // NFS
  {
    path: 'nfs/501/:message',
    component: Nfs501Component,
    canActivate: [AuthGuardService],
    data: { breadcrumbs: 'NFS' }
  },
  {
    path: 'nfs',
    canActivate: [AuthGuardService],
    canActivateChild: [AuthGuardService, ModuleStatusGuardService],
    data: {
      moduleStatusGuardConfig: {
        apiPath: 'nfs-ganesha',
        redirectTo: 'nfs/501'
      },
      breadcrumbs: 'NFS'
    },
    children: [
      { path: '', component: NfsListComponent },
      { path: 'add', component: NfsFormComponent, data: { breadcrumbs: 'Add' } },
      {
        path: 'edit/:cluster_id/:export_id',
        component: NfsFormComponent,
        data: { breadcrumbs: 'Edit' }
      }
    ]
  },
  // Single Sign-On (SSO)
  { path: 'sso/404', component: SsoNotFoundComponent },
  // System
  { path: 'login', component: LoginComponent },
  { path: 'logout', children: [] },
  { path: '403', component: ForbiddenComponent },
  { path: '404', component: NotFoundComponent },
  { path: '**', redirectTo: '/404' }
];

@NgModule({
  imports: [
    RouterModule.forRoot(routes, {
      useHash: true,
      preloadingStrategy: PreloadAllModules
    })
  ],
  exports: [RouterModule],
  providers: [StartCaseBreadcrumbsResolver, PerformanceCounterBreadcrumbsResolver]
})
export class AppRoutingModule {}
