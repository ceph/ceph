import { Injectable, NgModule } from '@angular/core';
import { ActivatedRouteSnapshot, PreloadAllModules, RouterModule, Routes } from '@angular/router';

import * as _ from 'lodash';

import { CephfsListComponent } from './ceph/cephfs/cephfs-list/cephfs-list.component';
import { ConfigurationFormComponent } from './ceph/cluster/configuration/configuration-form/configuration-form.component';
import { ConfigurationComponent } from './ceph/cluster/configuration/configuration.component';
import { CrushmapComponent } from './ceph/cluster/crushmap/crushmap.component';
import { HostFormComponent } from './ceph/cluster/hosts/host-form/host-form.component';
import { HostsComponent } from './ceph/cluster/hosts/hosts.component';
import { InventoryComponent } from './ceph/cluster/inventory/inventory.component';
import { LogsComponent } from './ceph/cluster/logs/logs.component';
import { MgrModuleFormComponent } from './ceph/cluster/mgr-modules/mgr-module-form/mgr-module-form.component';
import { MgrModuleListComponent } from './ceph/cluster/mgr-modules/mgr-module-list/mgr-module-list.component';
import { MonitorComponent } from './ceph/cluster/monitor/monitor.component';
import { OsdFormComponent } from './ceph/cluster/osd/osd-form/osd-form.component';
import { OsdListComponent } from './ceph/cluster/osd/osd-list/osd-list.component';
import { ActiveAlertListComponent } from './ceph/cluster/prometheus/active-alert-list/active-alert-list.component';
import { RulesListComponent } from './ceph/cluster/prometheus/rules-list/rules-list.component';
import { SilenceFormComponent } from './ceph/cluster/prometheus/silence-form/silence-form.component';
import { SilenceListComponent } from './ceph/cluster/prometheus/silence-list/silence-list.component';
import { ServicesComponent } from './ceph/cluster/services/services.component';
import { TelemetryComponent } from './ceph/cluster/telemetry/telemetry.component';
import { DashboardComponent } from './ceph/dashboard/dashboard/dashboard.component';
import { Nfs501Component } from './ceph/nfs/nfs-501/nfs-501.component';
import { NfsFormComponent } from './ceph/nfs/nfs-form/nfs-form.component';
import { NfsListComponent } from './ceph/nfs/nfs-list/nfs-list.component';
import { PerformanceCounterComponent } from './ceph/performance-counter/performance-counter/performance-counter.component';
import { LoginPasswordFormComponent } from './core/auth/login-password-form/login-password-form.component';
import { LoginComponent } from './core/auth/login/login.component';
import { SsoNotFoundComponent } from './core/auth/sso/sso-not-found/sso-not-found.component';
import { UserPasswordFormComponent } from './core/auth/user-password-form/user-password-form.component';
import { ForbiddenComponent } from './core/forbidden/forbidden.component';
import { BlankLayoutComponent } from './core/layouts/blank-layout/blank-layout.component';
import { LoginLayoutComponent } from './core/layouts/login-layout/login-layout.component';
import { WorkbenchLayoutComponent } from './core/layouts/workbench-layout/workbench-layout.component';
import { NotFoundComponent } from './core/not-found/not-found.component';
import { ActionLabels, URLVerbs } from './shared/constants/app.constants';
import { BreadcrumbsResolver, IBreadcrumb } from './shared/models/breadcrumbs';
import { AuthGuardService } from './shared/services/auth-guard.service';
import { ChangePasswordGuardService } from './shared/services/change-password-guard.service';
import { FeatureTogglesGuardService } from './shared/services/feature-toggles-guard.service';
import { ModuleStatusGuardService } from './shared/services/module-status-guard.service';
import { NoSsoGuardService } from './shared/services/no-sso-guard.service';

@Injectable()
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

@Injectable()
export class StartCaseBreadcrumbsResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot) {
    const path = route.params.name;
    const text = _.startCase(path);
    return [{ text: `${text}/Edit`, path: path }];
  }
}

const routes: Routes = [
  // Dashboard
  { path: '', redirectTo: 'dashboard', pathMatch: 'full' },
  {
    path: '',
    component: WorkbenchLayoutComponent,
    canActivate: [AuthGuardService, ChangePasswordGuardService],
    canActivateChild: [AuthGuardService, ChangePasswordGuardService],
    children: [
      { path: 'dashboard', component: DashboardComponent },
      // Cluster
      {
        path: 'hosts',
        data: { breadcrumbs: 'Cluster/Hosts' },
        children: [
          { path: '', component: HostsComponent },
          {
            path: URLVerbs.CREATE,
            component: HostFormComponent,
            data: { breadcrumbs: ActionLabels.CREATE }
          }
        ]
      },
      {
        path: 'monitor',
        component: MonitorComponent,
        data: { breadcrumbs: 'Cluster/Monitors' }
      },
      {
        path: 'services',
        component: ServicesComponent,
        data: { breadcrumbs: 'Cluster/Services' }
      },
      {
        path: 'inventory',
        component: InventoryComponent,
        data: { breadcrumbs: 'Cluster/Inventory' }
      },
      {
        path: 'osd',
        data: { breadcrumbs: 'Cluster/OSDs' },
        children: [
          { path: '', component: OsdListComponent },
          {
            path: URLVerbs.CREATE,
            component: OsdFormComponent,
            data: { breadcrumbs: ActionLabels.CREATE }
          }
        ]
      },
      {
        path: 'configuration',
        data: { breadcrumbs: 'Cluster/Configuration' },
        children: [
          { path: '', component: ConfigurationComponent },
          {
            path: 'edit/:name',
            component: ConfigurationFormComponent,
            data: { breadcrumbs: ActionLabels.EDIT }
          }
        ]
      },
      {
        path: 'crush-map',
        component: CrushmapComponent,
        data: { breadcrumbs: 'Cluster/CRUSH map' }
      },
      {
        path: 'logs',
        component: LogsComponent,
        data: { breadcrumbs: 'Cluster/Logs' }
      },
      {
        path: 'telemetry',
        component: TelemetryComponent,
        data: { breadcrumbs: 'Telemetry configuration' }
      },
      {
        path: 'monitoring',
        data: { breadcrumbs: 'Cluster/Monitoring' },
        children: [
          { path: '', redirectTo: 'active-alerts', pathMatch: 'full' },
          {
            path: 'active-alerts',
            data: { breadcrumbs: 'Active Alerts' },
            component: ActiveAlertListComponent
          },
          {
            path: 'alerts',
            data: { breadcrumbs: 'Alerts' },
            component: RulesListComponent
          },
          {
            path: 'silences',
            data: { breadcrumbs: 'Silences' },
            children: [
              {
                path: '',
                component: SilenceListComponent
              },
              {
                path: URLVerbs.CREATE,
                component: SilenceFormComponent,
                data: { breadcrumbs: `${ActionLabels.CREATE} Silence` }
              },
              {
                path: `${URLVerbs.CREATE}/:id`,
                component: SilenceFormComponent,
                data: { breadcrumbs: ActionLabels.CREATE }
              },
              {
                path: `${URLVerbs.EDIT}/:id`,
                component: SilenceFormComponent,
                data: { breadcrumbs: ActionLabels.EDIT }
              },
              {
                path: `${URLVerbs.RECREATE}/:id`,
                component: SilenceFormComponent,
                data: { breadcrumbs: ActionLabels.RECREATE }
              }
            ]
          }
        ]
      },
      {
        path: 'perf_counters/:type/:id',
        component: PerformanceCounterComponent,
        data: {
          breadcrumbs: PerformanceCounterBreadcrumbsResolver
        }
      },
      // Mgr modules
      {
        path: 'mgr-modules',
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
        data: { breadcrumbs: 'Pools' },
        loadChildren: () => import('./ceph/pool/pool.module').then((m) => m.RoutedPoolModule)
      },
      // Block
      {
        path: 'block',
        data: { breadcrumbs: true, text: 'Block', path: null },
        loadChildren: () => import('./ceph/block/block.module').then((m) => m.RoutedBlockModule)
      },
      // Filesystems
      {
        path: 'cephfs',
        component: CephfsListComponent,
        canActivate: [FeatureTogglesGuardService],
        data: { breadcrumbs: 'Filesystems' }
      },
      // Object Gateway
      {
        path: 'rgw',
        canActivateChild: [FeatureTogglesGuardService, ModuleStatusGuardService],
        data: {
          moduleStatusGuardConfig: {
            apiPath: 'rgw',
            redirectTo: 'rgw/501'
          },
          breadcrumbs: true,
          text: 'Object Gateway',
          path: null
        },
        loadChildren: () => import('./ceph/rgw/rgw.module').then((m) => m.RoutedRgwModule)
      },
      // User/Role Management
      {
        path: 'user-management',
        data: { breadcrumbs: 'User management', path: null },
        loadChildren: () => import('./core/auth/auth.module').then((m) => m.RoutedAuthModule)
      },
      // User Profile
      {
        path: 'user-profile',
        data: { breadcrumbs: 'User profile', path: null },
        children: [
          {
            path: URLVerbs.EDIT,
            component: UserPasswordFormComponent,
            canActivate: [NoSsoGuardService],
            data: { breadcrumbs: ActionLabels.EDIT }
          }
        ]
      },
      // NFS
      {
        path: 'nfs/501/:message',
        component: Nfs501Component,
        data: { breadcrumbs: 'NFS' }
      },
      {
        path: 'nfs',
        canActivateChild: [FeatureTogglesGuardService, ModuleStatusGuardService],
        data: {
          moduleStatusGuardConfig: {
            apiPath: 'nfs-ganesha',
            redirectTo: 'nfs/501'
          },
          breadcrumbs: 'NFS'
        },
        children: [
          { path: '', component: NfsListComponent },
          {
            path: URLVerbs.CREATE,
            component: NfsFormComponent,
            data: { breadcrumbs: ActionLabels.CREATE }
          },
          {
            path: `${URLVerbs.EDIT}/:cluster_id/:export_id`,
            component: NfsFormComponent,
            data: { breadcrumbs: ActionLabels.EDIT }
          }
        ]
      }
    ]
  },
  {
    path: '',
    component: LoginLayoutComponent,
    children: [
      { path: 'login', component: LoginComponent },
      {
        path: 'login-change-password',
        component: LoginPasswordFormComponent,
        canActivate: [NoSsoGuardService]
      }
    ]
  },
  {
    path: '',
    component: BlankLayoutComponent,
    children: [
      // Single Sign-On (SSO)
      { path: 'sso/404', component: SsoNotFoundComponent },
      // System
      { path: '403', component: ForbiddenComponent },
      { path: '404', component: NotFoundComponent },
      { path: '**', redirectTo: '/404' }
    ]
  }
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
