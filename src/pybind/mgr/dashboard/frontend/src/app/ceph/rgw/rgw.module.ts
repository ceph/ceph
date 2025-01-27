import { CommonModule, TitleCasePipe } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

import {
  NgbNavModule,
  NgbPopoverModule,
  NgbProgressbar,
  NgbTooltipModule,
  NgbTypeaheadModule
} from '@ng-bootstrap/ng-bootstrap';
import { PipesModule } from '~/app/shared/pipes/pipes.module';

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
import { RgwMultisiteSyncPolicyComponent } from './rgw-multisite-sync-policy/rgw-multisite-sync-policy.component';
import { RgwMultisiteSyncPolicyFormComponent } from './rgw-multisite-sync-policy-form/rgw-multisite-sync-policy-form.component';
import { RgwConfigurationPageComponent } from './rgw-configuration-page/rgw-configuration-page.component';
import { RgwConfigDetailsComponent } from './rgw-config-details/rgw-config-details.component';
import { RgwMultisiteWizardComponent } from './rgw-multisite-wizard/rgw-multisite-wizard.component';
import { RgwMultisiteSyncPolicyDetailsComponent } from './rgw-multisite-sync-policy-details/rgw-multisite-sync-policy-details.component';
import { RgwMultisiteSyncFlowModalComponent } from './rgw-multisite-sync-flow-modal/rgw-multisite-sync-flow-modal.component';
import { RgwMultisiteSyncPipeModalComponent } from './rgw-multisite-sync-pipe-modal/rgw-multisite-sync-pipe-modal.component';
import { RgwMultisiteTabsComponent } from './rgw-multisite-tabs/rgw-multisite-tabs.component';
import { RgwStorageClassListComponent } from './rgw-storage-class-list/rgw-storage-class-list.component';

import {
  ButtonModule,
  GridModule,
  IconModule,
  LoadingModule,
  ModalModule,
  ProgressIndicatorModule,
  CodeSnippetModule,
  InputModule,
  CheckboxModule,
  TreeviewModule,
  RadioModule,
  SelectModule,
  NumberModule,
  TabsModule,
  AccordionModule,
  TagModule,
  TooltipModule,
  ComboBoxModule
} from 'carbon-components-angular';
import { CephSharedModule } from '../shared/ceph-shared.module';
import { RgwUserAccountsComponent } from './rgw-user-accounts/rgw-user-accounts.component';
import { RgwUserAccountsFormComponent } from './rgw-user-accounts-form/rgw-user-accounts-form.component';
import { RgwUserAccountsDetailsComponent } from './rgw-user-accounts-details/rgw-user-accounts-details.component';
import { RgwStorageClassDetailsComponent } from './rgw-storage-class-details/rgw-storage-class-details.component';
import { RgwStorageClassFormComponent } from './rgw-storage-class-form/rgw-storage-class-form.component';
import { RgwBucketTieringFormComponent } from './rgw-bucket-tiering-form/rgw-bucket-tiering-form.component';
import { RgwBucketLifecycleListComponent } from './rgw-bucket-lifecycle-list/rgw-bucket-lifecycle-list.component';
import { RgwRateLimitComponent } from './rgw-rate-limit/rgw-rate-limit.component';
import { RgwRateLimitDetailsComponent } from './rgw-rate-limit-details/rgw-rate-limit-details.component';

@NgModule({
  imports: [
    CommonModule,
    CephSharedModule,
    SharedModule,
    FormsModule,
    ReactiveFormsModule.withConfig({ callSetDisabledState: 'whenDisabledForLegacyCode' }),
    PerformanceCounterModule,
    NgbNavModule,
    RouterModule,
    NgbTooltipModule,
    NgbPopoverModule,
    PipesModule,
    TreeviewModule,
    DataTableModule,
    DashboardV3Module,
    NgbTypeaheadModule,
    ModalModule,
    GridModule,
    ProgressIndicatorModule,
    CodeSnippetModule,
    ButtonModule,
    LoadingModule,
    IconModule,
    NgbProgressbar,
    InputModule,
    AccordionModule,
    CheckboxModule,
    SelectModule,
    NumberModule,
    TabsModule,
    RadioModule,
    TagModule,
    TooltipModule,
    ComboBoxModule
  ],
  exports: [
    RgwDaemonListComponent,
    RgwDaemonDetailsComponent,
    RgwBucketFormComponent,
    RgwBucketListComponent,
    RgwBucketDetailsComponent,
    RgwUserListComponent,
    RgwUserDetailsComponent,
    RgwStorageClassListComponent
  ],
  declarations: [
    RgwRateLimitComponent,
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
    BucketTagModalComponent,
    RgwMultisiteSyncPolicyComponent,
    RgwMultisiteSyncPolicyFormComponent,
    RgwConfigDetailsComponent,
    RgwConfigurationPageComponent,
    RgwMultisiteWizardComponent,
    RgwMultisiteSyncPolicyDetailsComponent,
    RgwMultisiteSyncFlowModalComponent,
    RgwMultisiteSyncPipeModalComponent,
    RgwMultisiteTabsComponent,
    RgwUserAccountsComponent,
    RgwUserAccountsFormComponent,
    RgwUserAccountsDetailsComponent,
    RgwStorageClassListComponent,
    RgwStorageClassDetailsComponent,
    RgwStorageClassFormComponent,
    RgwBucketTieringFormComponent,
    RgwBucketLifecycleListComponent,
    RgwRateLimitDetailsComponent
  ],
  providers: [TitleCasePipe]
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
    path: 'accounts',
    data: { breadcrumbs: 'Accounts' },
    children: [
      { path: '', component: RgwUserAccountsComponent },
      {
        path: URLVerbs.CREATE,
        component: RgwUserAccountsFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:id`,
        component: RgwUserAccountsFormComponent,
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
          name: 'Accounts',
          url: '/rgw/accounts'
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
    children: [
      { path: '', redirectTo: 'configuration', pathMatch: 'full' },
      {
        path: 'configuration',
        component: RgwMultisiteDetailsComponent,
        data: { breadcrumbs: 'Configuration' },
        children: [
          {
            path: 'setup-multisite-replication',
            component: RgwMultisiteWizardComponent,
            outlet: 'modal'
          }
        ]
      },
      {
        path: 'sync-policy',
        component: RgwMultisiteSyncPolicyComponent,
        data: { breadcrumbs: 'Sync-policy' },
        children: [
          {
            path: `${URLVerbs.CREATE}`,
            component: RgwMultisiteSyncPolicyFormComponent,
            outlet: 'modal'
          },
          {
            path: `${URLVerbs.EDIT}/:groupName`,
            component: RgwMultisiteSyncPolicyFormComponent,
            outlet: 'modal'
          },
          {
            path: `${URLVerbs.EDIT}/:groupName/:bucketName`,
            component: RgwMultisiteSyncPolicyFormComponent,
            outlet: 'modal'
          }
        ]
      }
    ]
  },
  {
    path: 'tiering',
    data: { breadcrumbs: 'Tiering' },
    children: [
      { path: '', component: RgwStorageClassListComponent },
      {
        path: URLVerbs.CREATE,
        component: RgwStorageClassFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:zonegroup_name/:placement_target/:storage_class`,
        component: RgwStorageClassFormComponent,
        data: { breadcrumbs: ActionLabels.EDIT }
      }
    ]
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
  },
  {
    path: 'configuration',
    data: { breadcrumbs: 'Configuration' },
    children: [{ path: '', component: RgwConfigurationPageComponent }]
  }
];

@NgModule({
  imports: [RgwModule, RouterModule.forChild(routes)]
})
export class RoutedRgwModule {}
