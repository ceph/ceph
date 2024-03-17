import { Injectable, NgModule } from '@angular/core';
import { ActivatedRouteSnapshot, PreloadAllModules, RouterModule, Routes } from '@angular/router';

import _ from 'lodash';

import { CephfsListComponent } from './ceph/cephfs/cephfs-list/cephfs-list.component';
import { ConfigurationFormComponent } from './ceph/cluster/configuration/configuration-form/configuration-form.component';
import { ConfigurationComponent } from './ceph/cluster/configuration/configuration.component';
import { CreateClusterComponent } from './ceph/cluster/create-cluster/create-cluster.component';
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
import { ServiceFormComponent } from './ceph/cluster/services/service-form/service-form.component';
import { ServicesComponent } from './ceph/cluster/services/services.component';
import { TelemetryComponent } from './ceph/cluster/telemetry/telemetry.component';
import { DashboardComponent } from './ceph/dashboard/dashboard/dashboard.component';
import { NfsFormComponent } from './ceph/nfs/nfs-form/nfs-form.component';
import { NfsListComponent } from './ceph/nfs/nfs-list/nfs-list.component';
import { PerformanceCounterComponent } from './ceph/performance-counter/performance-counter/performance-counter.component';
import { LoginPasswordFormComponent } from './core/auth/login-password-form/login-password-form.component';
import { LoginComponent } from './core/auth/login/login.component';
import { UserPasswordFormComponent } from './core/auth/user-password-form/user-password-form.component';
import { ErrorComponent } from './core/error/error.component';
import { BlankLayoutComponent } from './core/layouts/blank-layout/blank-layout.component';
import { LoginLayoutComponent } from './core/layouts/login-layout/login-layout.component';
import { WorkbenchLayoutComponent } from './core/layouts/workbench-layout/workbench-layout.component';
import { ApiDocsComponent } from './core/navigation/api-docs/api-docs.component';
import { ActionLabels, URLVerbs } from './shared/constants/app.constants';
import { CrudFormComponent } from './shared/forms/crud-form/crud-form.component';
import { CRUDTableComponent } from './shared/datatable/crud-table/crud-table.component';
import { BreadcrumbsResolver, IBreadcrumb } from './shared/models/breadcrumbs';
import { AuthGuardService } from './shared/services/auth-guard.service';
import { ChangePasswordGuardService } from './shared/services/change-password-guard.service';
import { FeatureTogglesGuardService } from './shared/services/feature-toggles-guard.service';
import { ModuleStatusGuardService } from './shared/services/module-status-guard.service';
import { NoSsoGuardService } from './shared/services/no-sso-guard.service';
import { UpgradeComponent } from './ceph/cluster/upgrade/upgrade.component';
import { CephfsVolumeFormComponent } from './ceph/cephfs/cephfs-form/cephfs-form.component';
import { UpgradeProgressComponent } from './ceph/cluster/upgrade/upgrade-progress/upgrade-progress.component';
import { MultiClusterComponent } from './ceph/cluster/multi-cluster/multi-cluster.component';
import { MultiClusterListComponent } from './ceph/cluster/multi-cluster/multi-cluster-list/multi-cluster-list.component';
import { FeedbackListComponent } from './ceph/shared/feedback/feedback-list/feedback-list.component';

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
  { path: 'api-docs', component: ApiDocsComponent },
  {
    path: '',
    component: WorkbenchLayoutComponent,
    canActivate: [AuthGuardService, ChangePasswordGuardService],
    canActivateChild: [AuthGuardService, ChangePasswordGuardService],
    children: [
      { path: 'dashboard', component: DashboardComponent },
      { path: 'error', component: ErrorComponent },

      // Cluster
      {
        path: 'expand-cluster',
        component: CreateClusterComponent,
        canActivate: [ModuleStatusGuardService],
        data: {
          moduleStatusGuardConfig: {
            uiApiPath: 'orchestrator',
            redirectTo: 'dashboard',
            backend: 'cephadm'
          },
          breadcrumbs: 'Expand Cluster'
        }
      },
      {
        path: 'hosts',
        component: HostsComponent,
        data: { breadcrumbs: 'Cluster/Hosts' },
        children: [
          {
            path: URLVerbs.ADD,
            component: HostFormComponent,
            outlet: 'modal'
          }
        ]
      },
      {
        path: 'ceph-users',
        component: CRUDTableComponent,
        data: {
          breadcrumbs: 'Administration/Ceph Users',
          resource: 'api.cluster.user@1.0'
        }
      },
      {
        path: 'cluster/user/create',
        component: CrudFormComponent,
        data: {
          breadcrumbs: 'Administration/Ceph Users/Create',
          resource: 'api.cluster.user@1.0'
        }
      },
      {
        path: 'cluster/user/import',
        component: CrudFormComponent,
        data: {
          breadcrumbs: 'Administration/Ceph Users/Import',
          resource: 'api.cluster.user@1.0'
        }
      },
      {
        path: 'cluster/user/edit',
        component: CrudFormComponent,
        data: {
          breadcrumbs: 'Administration/Ceph Users/Edit',
          resource: 'api.cluster.user@1.0'
        }
      },
      {
        path: 'monitor',
        component: MonitorComponent,
        data: { breadcrumbs: 'Cluster/Monitors' }
      },
      {
        path: 'services',
        component: ServicesComponent,
        canActivate: [ModuleStatusGuardService],
        data: {
          moduleStatusGuardConfig: {
            uiApiPath: 'orchestrator',
            redirectTo: 'error',
            section: 'orch',
            section_info: 'Orchestrator',
            header: 'Orchestrator is not available'
          },
          breadcrumbs: 'Administration/Services'
        },
        children: [
          {
            path: URLVerbs.CREATE,
            component: ServiceFormComponent,
            outlet: 'modal'
          },
          {
            path: `${URLVerbs.EDIT}/:type/:name`,
            component: ServiceFormComponent,
            outlet: 'modal'
          }
        ]
      },
      {
        path: 'multi-cluster',
        children: [
          {
            path: 'overview',
            component: MultiClusterComponent
          },
          {
            path: 'manage-clusters',
            component: MultiClusterListComponent,
            data: {
              breadcrumbs: 'Multi-Cluster/Manage Clusters'
            }
          }
        ]
      },
      {
        path: 'inventory',
        canActivate: [ModuleStatusGuardService],
        component: InventoryComponent,
        data: {
          moduleStatusGuardConfig: {
            uiApiPath: 'orchestrator',
            redirectTo: 'error',
            section: 'orch',
            section_info: 'Orchestrator',
            header: 'Orchestrator is not available'
          },
          breadcrumbs: 'Cluster/Physical Disks'
        }
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
        data: { breadcrumbs: 'Administration/Configuration' },
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
        data: { breadcrumbs: 'Observability/Logs' }
      },
      {
        path: 'telemetry',
        component: TelemetryComponent,
        data: { breadcrumbs: 'Telemetry configuration' }
      },
      {
        path: 'monitoring',
        data: { breadcrumbs: 'Observability/Alerts' },
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
        path: 'upgrade',
        canActivate: [ModuleStatusGuardService],
        data: {
          moduleStatusGuardConfig: {
            uiApiPath: 'orchestrator',
            redirectTo: 'error',
            backend: 'cephadm',
            section: 'orch',
            section_info: 'Orchestrator',
            header: 'Orchestrator is not available'
          },
          breadcrumbs: 'Administration/Upgrade'
        },
        children: [
          {
            path: '',
            component: UpgradeComponent
          },
          {
            path: 'progress',
            component: UpgradeProgressComponent,
            data: { breadcrumbs: 'Progress' }
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
        data: { breadcrumbs: 'Administrator/Manager Modules' },
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
      {
        path: 'feedback',
        data: { breadcrumbs: 'Administrator/Feedback' },
        children: [
          {
            path: '',
            component: FeedbackListComponent
          }
        ]
      },
      // Pools
      {
        path: 'pool',
        data: { breadcrumbs: 'Cluster/Pools' },
        loadChildren: () => import('./ceph/pool/pool.module').then((m) => m.RoutedPoolModule)
      },
      // Block
      {
        path: 'block',
        data: { breadcrumbs: true, text: 'Block', path: null },
        loadChildren: () => import('./ceph/block/block.module').then((m) => m.RoutedBlockModule)
      },
      // File Systems
      {
        path: 'cephfs',
        canActivate: [FeatureTogglesGuardService],
        data: { breadcrumbs: 'File/File Systems' },
        children: [
          { path: '', component: CephfsListComponent },
          {
            path: URLVerbs.CREATE,
            component: CephfsVolumeFormComponent,
            data: { breadcrumbs: ActionLabels.CREATE }
          },
          {
            path: `${URLVerbs.EDIT}/:id`,
            component: CephfsVolumeFormComponent,
            data: { breadcrumbs: ActionLabels.EDIT }
          }
        ]
      },
      // Object Gateway
      {
        path: 'rgw',
        canActivate: [FeatureTogglesGuardService, ModuleStatusGuardService],
        data: {
          moduleStatusGuardConfig: {
            uiApiPath: 'rgw',
            redirectTo: 'error',
            section: 'rgw',
            section_info: 'Object Gateway',
            header: 'The Object Gateway Service is not configured'
          },
          breadcrumbs: true,
          text: 'Object',
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
        path: 'nfs',
        canActivateChild: [FeatureTogglesGuardService, ModuleStatusGuardService],
        data: {
          moduleStatusGuardConfig: {
            uiApiPath: 'nfs-ganesha',
            redirectTo: 'error',
            section: 'nfs-ganesha',
            section_info: 'NFS GANESHA',
            header: 'NFS-Ganesha is not configured'
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
    children: [{ path: '**', redirectTo: '/error' }]
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
