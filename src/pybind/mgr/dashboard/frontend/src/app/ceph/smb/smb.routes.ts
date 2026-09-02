import { Routes } from '@angular/router';

import { ActionLabels, URLVerbs } from '~/app/shared/constants/app.constants';
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
        data: { breadcrumbs: ActionLabels.EDIT }
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
        data: { breadcrumbs: ActionLabels.EDIT }
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
        component: SmbUsersgroupsFormComponent
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
  },
  {
    path: `share/${URLVerbs.CREATE}/:clusterId`,
    component: SmbShareFormComponent,
    data: { breadcrumbs: ActionLabels.CREATE }
  },
  {
    path: `share/${URLVerbs.EDIT}/:clusterId/:shareId`,
    component: SmbShareFormComponent,
    data: { breadcrumbs: ActionLabels.EDIT }
  }
];
