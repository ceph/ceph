import { Route, Routes } from '@angular/router';

import { ActionLabels, URLVerbs } from '~/app/shared/constants/app.constants';
import { ModuleStatusGuardService } from '~/app/shared/services/module-status-guard.service';
import { SmbClusterFormComponent } from './smb-cluster-form/smb-cluster-form.component';
import { SmbClusterListComponent } from './smb-cluster-list/smb-cluster-list.component';
import { SmbClusterResourceBreadcrumbResolver } from './smb-cluster-resource-page/smb-cluster-resource-breadcrumb.resolver';
import { SmbClusterResourcePageComponent } from './smb-cluster-resource-page/smb-cluster-resource-page.component';
import { SmbClusterResourceSidebarComponent } from './smb-cluster-resource-sidebar/smb-cluster-resource-sidebar.component';
import { SmbJoinAuthFormComponent } from './smb-join-auth-form/smb-join-auth-form.component';
import { SmbJoinAuthListComponent } from './smb-join-auth-list/smb-join-auth-list.component';
import { SmbOverviewComponent } from './smb-overview/smb-overview.component';
import { SmbShareFormComponent } from './smb-share-form/smb-share-form.component';
import { SmbUsersgroupsFormComponent } from './smb-usersgroups-form/smb-usersgroups-form.component';
import { SmbUsersgroupsListComponent } from './smb-usersgroups-list/smb-usersgroups-list.component';
import { SmbUsergroupsResourceBreadcrumbResolver } from './smb-usersgroups-resource-page/smb-usersgroups-resource-breadcrumb.resolver';
import { SmbUsersgroupsResourcePageComponent } from './smb-usersgroups-resource-page/smb-usersgroups-resource-page.component';
import { SmbUsersgroupsResourceSidebarComponent } from './smb-usersgroups-resource-sidebar/smb-usersgroups-resource-sidebar.component';
import {
  SMB_CEPHFS_CONTEXT,
  SMB_CONTEXT,
  SMB_RGW_CONTEXT
} from '~/app/shared/constants/smb.constant';

export const smbChildRoutes: Routes = [
  { path: '', component: SmbClusterListComponent },
  {
    path: 'cluster',
    data: { breadcrumbs: 'Clusters' },
    children: [
      { path: '', component: SmbClusterListComponent },
      {
        path: `${URLVerbs.CREATE}`,
        component: SmbClusterFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:cluster_id`,
        component: SmbClusterFormComponent,
        data: { breadcrumbs: ActionLabels.EDIT, editing: true }
      },
      {
        path: ':cluster_id',
        component: SmbClusterResourceSidebarComponent,
        data: {
          breadcrumbs: SmbClusterResourceBreadcrumbResolver,
          showBreadcrumbsLayout: false
        },
        children: [
          { path: '', redirectTo: 'overview', pathMatch: 'full' },
          {
            path: 'overview',
            component: SmbClusterResourcePageComponent,
            data: { breadcrumbs: 'Overview', section: 'overview' }
          },
          {
            path: `${URLVerbs.CREATE}`,
            component: SmbShareFormComponent,
            data: { breadcrumbs: ActionLabels.CREATE }
          },
          {
            path: `${URLVerbs.EDIT}/:shareId`,
            component: SmbShareFormComponent,
            data: { breadcrumbs: ActionLabels.EDIT, editing: true }
          }
        ]
      }
    ]
  },
  {
    path: 'active-directory',
    data: { breadcrumbs: 'Active Directory' },
    children: [
      { path: '', component: SmbJoinAuthListComponent },
      {
        path: `${URLVerbs.CREATE}`,
        component: SmbJoinAuthFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:authId`,
        component: SmbJoinAuthFormComponent,
        data: { breadcrumbs: ActionLabels.EDIT, editing: true }
      }
    ]
  },
  {
    path: 'standalone',
    data: { breadcrumbs: 'Standalone' },
    children: [
      { path: '', component: SmbUsersgroupsListComponent },
      {
        path: `${URLVerbs.CREATE}`,
        component: SmbUsersgroupsFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:usersGroupsId`,
        component: SmbUsersgroupsFormComponent,
        data: { editing: true }
      },
      {
        path: ':users_groups_id',
        component: SmbUsersgroupsResourceSidebarComponent,
        data: {
          breadcrumbs: SmbUsergroupsResourceBreadcrumbResolver,
          showBreadcrumbsLayout: false
        },
        children: [
          { path: '', redirectTo: 'overview', pathMatch: 'full' },
          {
            path: 'overview',
            component: SmbUsersgroupsResourcePageComponent,
            data: { breadcrumbs: 'Overview', section: 'overview' }
          }
        ]
      }
    ]
  },
  {
    path: 'overview',
    component: SmbOverviewComponent,
    data: { breadcrumbs: 'Overview' }
  }
];

/**
 * Parent SMB route for cephfs (/cephfs/smb) or rgw (/rgw/smb).
 * Child components inject SMB_CONTEXT (no route-data / URL parsing).
 */
export function smbParentRoute(isRgw: boolean, breadcrumbs: string): Route {
  const ctx = isRgw ? SMB_RGW_CONTEXT : SMB_CEPHFS_CONTEXT;
  return {
    path: 'smb',
    canActivate: [ModuleStatusGuardService],
    data: {
      breadcrumbs,
      moduleStatusGuardConfig: {
        uiApiPath: 'smb',
        redirectTo: 'error',
        header: 'SMB module is not enabled',
        module_name: 'smb',
        navigate_to: ctx.basePath
      }
    },
    providers: [{ provide: SMB_CONTEXT, useValue: ctx }],
    children: smbChildRoutes
  };
}
