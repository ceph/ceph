import { Route, Routes } from '@angular/router';

import { ActionLabels, URLVerbs } from '~/app/shared/constants/app.constants';
import { FeatureTogglesGuardService } from '~/app/shared/services/feature-toggles-guard.service';
import { ModuleStatusGuardService } from '~/app/shared/services/module-status-guard.service';
import { NfsClusterComponent } from './nfs-cluster/nfs-cluster.component';
import { NfsClusterResourceBreadcrumbResolver } from './nfs-cluster-resource-page/nfs-cluster-resource-breadcrumb.resolver';
import { NfsClusterResourcePageComponent } from './nfs-cluster-resource-page/nfs-cluster-resource-page.component';
import { NfsClusterResourceSidebarComponent } from './nfs-cluster-resource-sidebar/nfs-cluster-resource-sidebar.component';
import { NfsFormComponent } from './nfs-form/nfs-form.component';

export const nfsClusterChildRoutes: Routes = [
  { path: '', component: NfsClusterComponent },
  {
    path: ':cluster_id',
    component: NfsClusterResourceSidebarComponent,
    data: {
      breadcrumbs: NfsClusterResourceBreadcrumbResolver,
      showBreadcrumbsLayout: false
    },
    children: [
      { path: '', redirectTo: 'overview', pathMatch: 'full' },
      {
        path: 'overview',
        component: NfsClusterResourcePageComponent,
        data: { breadcrumbs: 'Overview', section: 'overview' }
      }
    ]
  }
];

/**
 * Parent NFS route for cephfs (/cephfs/nfs) or rgw (/rgw/nfs).
 * Pass isRgw=true when used under the RGW route tree; this omits the
 * CephFS-specific CREATE path variant that carries fs_name/subvolume_group.
 */
export function nfsParentRoute(isRgw: boolean, breadcrumbs: string): Route {
  const children: Routes = [
    { path: '', redirectTo: 'cluster', pathMatch: 'full' },
    {
      path: 'cluster',
      data: { breadcrumbs: 'Service clusters' },
      children: nfsClusterChildRoutes
    },
    ...(!isRgw
      ? [
          {
            path: `${URLVerbs.CREATE}/:fs_name/:subvolume_group`,
            component: NfsFormComponent,
            data: { breadcrumbs: ActionLabels.CREATE }
          }
        ]
      : []),
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
  ];

  return {
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
      breadcrumbs
    },
    children
  };
}
