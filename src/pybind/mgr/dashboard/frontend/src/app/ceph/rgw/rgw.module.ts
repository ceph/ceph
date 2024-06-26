import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

import { NgbNavModule, NgbPopoverModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxPipeFunctionModule } from 'ngx-pipe-function';

import { ActionLabels, URLVerbs } from '~/app/shared/constants/app.constants';
import { CRUDTableComponent } from '~/app/shared/datatable/crud-table/crud-table.component';
import { FeatureTogglesGuardService } from '~/app/shared/services/feature-toggles-guard.service';
import { ModuleStatusGuardService } from '~/app/shared/services/module-status-guard.service';

import { SharedModule } from '~/app/shared/shared.module';
import { PerformanceCounterModule } from '../performance-counter/performance-counter.module';
import { RgwBucketDetailsComponent } from './rgw-bucket-details/rgw-bucket-details.component';
import { RgwBucketFormComponent } from './rgw-bucket-form/rgw-bucket-form.component';
import { RgwBucketListComponent } from './rgw-bucket-list/rgw-bucket-list.component';
import { RgwConfigModalComponent } from './rgw-config-modal/rgw-config-modal.component';
import { RgwDaemonDetailsComponent } from './rgw-daemon-details/rgw-daemon-details.component';
import { RgwDaemonListComponent } from './rgw-daemon-list/rgw-daemon-list.component';
import { RgwUserCapabilityModalComponent } from './rgw-user-capability-modal/rgw-user-capability-modal.component';
import { RgwUserDetailsComponent } from './rgw-user-details/rgw-user-details.component';
import { RgwUserFormComponent } from './rgw-user-form/rgw-user-form.component';
import { RgwUserListComponent } from './rgw-user-list/rgw-user-list.component';
import { RgwUserS3KeyModalComponent } from './rgw-user-s3-key-modal/rgw-user-s3-key-modal.component';
import { RgwUserSubuserModalComponent } from './rgw-user-subuser-modal/rgw-user-subuser-modal.component';
import { RgwUserSwiftKeyModalComponent } from './rgw-user-swift-key-modal/rgw-user-swift-key-modal.component';
import { RgwUserTabsComponent } from './rgw-user-tabs/rgw-user-tabs.component';
import { RgwMultisiteDetailsComponent } from './rgw-multisite-details/rgw-multisite-details.component';
import { TreeModule } from '@circlon/angular-tree-component';
import { DataTableModule } from '~/app/shared/datatable/datatable.module';
import { RgwMultisiteRealmFormComponent } from './rgw-multisite-realm-form/rgw-multisite-realm-form.component';
import { RgwMultisiteZonegroupFormComponent } from './rgw-multisite-zonegroup-form/rgw-multisite-zonegroup-form.component';
import { RgwMultisiteZoneFormComponent } from './rgw-multisite-zone-form/rgw-multisite-zone-form.component';
import { CrudFormComponent } from '~/app/shared/forms/crud-form/crud-form.component';
import { RgwMultisiteZoneDeletionFormComponent } from './models/rgw-multisite-zone-deletion-form/rgw-multisite-zone-deletion-form.component';
import { RgwMultisiteZonegroupDeletionFormComponent } from './models/rgw-multisite-zonegroup-deletion-form/rgw-multisite-zonegroup-deletion-form.component';
import { RgwSystemUserComponent } from './rgw-system-user/rgw-system-user.component';
import { RgwMultisiteMigrateComponent } from './rgw-multisite-migrate/rgw-multisite-migrate.component';
import { RgwMultisiteImportComponent } from './rgw-multisite-import/rgw-multisite-import.component';
import { RgwMultisiteExportComponent } from './rgw-multisite-export/rgw-multisite-export.component';
import { CreateRgwServiceEntitiesComponent } from './create-rgw-service-entities/create-rgw-service-entities.component';
import { RgwOverviewDashboardComponent } from './rgw-overview-dashboard/rgw-overview-dashboard.component';
import { DashboardV3Module } from '../dashboard-v3/dashboard-v3.module';
import { RgwSyncPrimaryZoneComponent } from './rgw-sync-primary-zone/rgw-sync-primary-zone.component';
import { RgwSyncMetadataInfoComponent } from './rgw-sync-metadata-info/rgw-sync-metadata-info.component';
import { RgwSyncDataInfoComponent } from './rgw-sync-data-info/rgw-sync-data-info.component';
import { BucketTagModalComponent } from './bucket-tag-modal/bucket-tag-modal.component';
import { NfsListComponent } from '../nfs/nfs-list/nfs-list.component';
import { NfsFormComponent } from '../nfs/nfs-form/nfs-form.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    FormsModule,
    ReactiveFormsModule.withConfig({ callSetDisabledState: 'whenDisabledForLegacyCode' }),
    PerformanceCounterModule,
    NgbNavModule,
    RouterModule,
    NgbTooltipModule,
    NgbPopoverModule,
    NgxPipeFunctionModule,
    TreeModule,
    DataTableModule,
    DashboardV3Module
  ],
  exports: [
    RgwDaemonListComponent,
    RgwDaemonDetailsComponent,
    RgwBucketFormComponent,
    RgwBucketListComponent,
    RgwBucketDetailsComponent,
    RgwUserListComponent,
    RgwUserDetailsComponent
  ],
  declarations: [
    RgwDaemonListComponent,
    RgwDaemonDetailsComponent,
    RgwBucketFormComponent,
    RgwBucketListComponent,
    RgwBucketDetailsComponent,
    RgwUserListComponent,
    RgwUserDetailsComponent,
    RgwBucketFormComponent,
    RgwUserFormComponent,
    RgwUserSwiftKeyModalComponent,
    RgwUserS3KeyModalComponent,
    RgwUserCapabilityModalComponent,
    RgwUserSubuserModalComponent,
    RgwConfigModalComponent,
    RgwUserTabsComponent,
    RgwMultisiteDetailsComponent,
    RgwMultisiteRealmFormComponent,
    RgwMultisiteZonegroupFormComponent,
    RgwMultisiteZoneFormComponent,
    RgwMultisiteZoneDeletionFormComponent,
    RgwMultisiteZonegroupDeletionFormComponent,
    RgwSystemUserComponent,
    RgwMultisiteMigrateComponent,
    RgwMultisiteImportComponent,
    RgwMultisiteExportComponent,
    CreateRgwServiceEntitiesComponent,
    RgwOverviewDashboardComponent,
    RgwSyncPrimaryZoneComponent,
    RgwSyncMetadataInfoComponent,
    RgwSyncDataInfoComponent,
    BucketTagModalComponent
  ]
})
export class RgwModule {}

const routes: Routes = [
  {
    path: '',
    redirectTo: 'rbd',
    pathMatch: 'full' // Required for a clean reload on daemon selection.
  },
  { path: 'daemon', component: RgwDaemonListComponent, data: { breadcrumbs: 'Gateways' } },
  {
    path: 'user',
    data: { breadcrumbs: 'Users' },
    children: [
      { path: '', component: RgwUserListComponent },
      {
        path: URLVerbs.CREATE,
        component: RgwUserFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:uid`,
        component: RgwUserFormComponent,
        data: { breadcrumbs: ActionLabels.EDIT }
      }
    ]
  },
  {
    path: 'roles',
    data: {
      breadcrumbs: 'Roles',
      resource: 'api.rgw.roles@1.0',
      tabs: [
        {
          name: 'Users',
          url: '/rgw/user'
        },
        {
          name: 'Roles',
          url: '/rgw/roles'
        }
      ]
    },
    children: [
      {
        path: '',
        component: CRUDTableComponent
      },
      {
        path: URLVerbs.CREATE,
        component: CrudFormComponent,
        data: {
          breadcrumbs: ActionLabels.CREATE
        }
      },
      {
        path: URLVerbs.EDIT,
        component: CrudFormComponent,
        data: {
          breadcrumbs: ActionLabels.EDIT
        }
      }
    ]
  },
  {
    path: 'bucket',
    data: { breadcrumbs: 'Buckets' },
    children: [
      { path: '', component: RgwBucketListComponent },
      {
        path: URLVerbs.CREATE,
        component: RgwBucketFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:bid`,
        component: RgwBucketFormComponent,
        data: { breadcrumbs: ActionLabels.EDIT }
      }
    ]
  },
  {
    path: 'overview',
    data: { breadcrumbs: 'Overview' },
    children: [{ path: '', component: RgwOverviewDashboardComponent }]
  },
  {
    path: 'multisite',
    data: { breadcrumbs: 'Multi-site' },
    children: [{ path: '', component: RgwMultisiteDetailsComponent }]
  },
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
];

@NgModule({
  imports: [RgwModule, RouterModule.forChild(routes)]
})
export class RoutedRgwModule {}
