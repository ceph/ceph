import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

import { TreeModule } from '@circlon/angular-tree-component';
import { NgbNavModule, NgbPopoverModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxPipeFunctionModule } from 'ngx-pipe-function';

import { ActionLabels, URLVerbs } from '~/app/shared/constants/app.constants';
import { FeatureTogglesGuardService } from '~/app/shared/services/feature-toggles-guard.service';
import { ModuleStatusGuardService } from '~/app/shared/services/module-status-guard.service';
import { SharedModule } from '~/app/shared/shared.module';
import { IscsiSettingComponent } from './iscsi-setting/iscsi-setting.component';
import { IscsiTabsComponent } from './iscsi-tabs/iscsi-tabs.component';
import { IscsiTargetDetailsComponent } from './iscsi-target-details/iscsi-target-details.component';
import { IscsiTargetDiscoveryModalComponent } from './iscsi-target-discovery-modal/iscsi-target-discovery-modal.component';
import { IscsiTargetFormComponent } from './iscsi-target-form/iscsi-target-form.component';
import { IscsiTargetImageSettingsModalComponent } from './iscsi-target-image-settings-modal/iscsi-target-image-settings-modal.component';
import { IscsiTargetIqnSettingsModalComponent } from './iscsi-target-iqn-settings-modal/iscsi-target-iqn-settings-modal.component';
import { IscsiTargetListComponent } from './iscsi-target-list/iscsi-target-list.component';
import { IscsiComponent } from './iscsi/iscsi.component';
import { MirroringModule } from './mirroring/mirroring.module';
import { OverviewComponent as RbdMirroringComponent } from './mirroring/overview/overview.component';
import { PoolEditModeModalComponent } from './mirroring/pool-edit-mode-modal/pool-edit-mode-modal.component';
import { RbdConfigurationFormComponent } from './rbd-configuration-form/rbd-configuration-form.component';
import { RbdConfigurationListComponent } from './rbd-configuration-list/rbd-configuration-list.component';
import { RbdDetailsComponent } from './rbd-details/rbd-details.component';
import { RbdFormComponent } from './rbd-form/rbd-form.component';
import { RbdListComponent } from './rbd-list/rbd-list.component';
import { RbdNamespaceFormModalComponent } from './rbd-namespace-form/rbd-namespace-form-modal.component';
import { RbdNamespaceListComponent } from './rbd-namespace-list/rbd-namespace-list.component';
import { RbdPerformanceComponent } from './rbd-performance/rbd-performance.component';
import { RbdSnapshotFormModalComponent } from './rbd-snapshot-form/rbd-snapshot-form-modal.component';
import { RbdSnapshotListComponent } from './rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdTabsComponent } from './rbd-tabs/rbd-tabs.component';
import { RbdTrashListComponent } from './rbd-trash-list/rbd-trash-list.component';
import { RbdTrashMoveModalComponent } from './rbd-trash-move-modal/rbd-trash-move-modal.component';
import { RbdTrashPurgeModalComponent } from './rbd-trash-purge-modal/rbd-trash-purge-modal.component';
import { RbdTrashRestoreModalComponent } from './rbd-trash-restore-modal/rbd-trash-restore-modal.component';

@NgModule({
  imports: [
    CommonModule,
    MirroringModule,
    FormsModule,
    ReactiveFormsModule,
    NgbNavModule,
    NgbPopoverModule,
    NgbTooltipModule,
    NgxPipeFunctionModule,
    SharedModule,
    RouterModule,
    TreeModule
  ],
  declarations: [
    RbdListComponent,
    IscsiComponent,
    IscsiSettingComponent,
    IscsiTabsComponent,
    IscsiTargetListComponent,
    RbdDetailsComponent,
    RbdFormComponent,
    RbdNamespaceFormModalComponent,
    RbdNamespaceListComponent,
    RbdSnapshotListComponent,
    RbdSnapshotFormModalComponent,
    RbdTrashListComponent,
    RbdTrashMoveModalComponent,
    RbdTrashRestoreModalComponent,
    RbdTrashPurgeModalComponent,
    IscsiTargetDetailsComponent,
    IscsiTargetFormComponent,
    IscsiTargetImageSettingsModalComponent,
    IscsiTargetIqnSettingsModalComponent,
    IscsiTargetDiscoveryModalComponent,
    RbdConfigurationListComponent,
    RbdConfigurationFormComponent,
    RbdTabsComponent,
    RbdPerformanceComponent
  ],
  exports: [RbdConfigurationListComponent, RbdConfigurationFormComponent]
})
export class BlockModule {}

/* The following breakdown is needed to allow importing block.module without
    the routes (e.g.: this module is imported by pool.module for RBD QoS
    components)
*/
const routes: Routes = [
  { path: '', redirectTo: 'rbd', pathMatch: 'full' },
  {
    path: 'rbd',
    canActivate: [FeatureTogglesGuardService, ModuleStatusGuardService],
    data: {
      moduleStatusGuardConfig: {
        uiApiPath: 'block/rbd',
        redirectTo: 'error',
        header: $localize`Block Pool is not configured`,
        button_name: $localize`Configure Default pool`,
        button_route: '/pool/create',
        component: 'Default Pool',
        uiConfig: true
      },
      breadcrumbs: 'Images'
    },
    children: [
      { path: '', component: RbdListComponent },
      {
        path: 'namespaces',
        component: RbdNamespaceListComponent,
        data: { breadcrumbs: 'Namespaces' }
      },
      {
        path: 'trash',
        component: RbdTrashListComponent,
        data: { breadcrumbs: 'Trash' }
      },
      {
        path: 'performance',
        component: RbdPerformanceComponent,
        data: { breadcrumbs: 'Overall Performance' }
      },
      {
        path: URLVerbs.CREATE,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:image_spec`,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.EDIT }
      },
      {
        path: `${URLVerbs.CLONE}/:image_spec/:snap`,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.CLONE }
      },
      {
        path: `${URLVerbs.COPY}/:image_spec`,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.COPY }
      },
      {
        path: `${URLVerbs.COPY}/:image_spec/:snap`,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.COPY }
      }
    ]
  },
  {
    path: 'mirroring',
    component: RbdMirroringComponent,
    canActivate: [FeatureTogglesGuardService, ModuleStatusGuardService],
    data: {
      moduleStatusGuardConfig: {
        uiApiPath: 'block/mirroring',
        redirectTo: 'error',
        header: $localize`Block Mirroring is not configured`,
        button_name: $localize`Configure Block Mirroring`,
        button_title: $localize`This will create \"rbd-mirror\" service and a replicated Block pool`,
        component: 'Block Mirroring',
        uiConfig: true
      },
      breadcrumbs: 'Mirroring'
    },
    children: [
      {
        path: `${URLVerbs.EDIT}/:pool_name`,
        component: PoolEditModeModalComponent,
        outlet: 'modal'
      }
    ]
  },
  // iSCSI
  {
    path: 'iscsi',
    canActivate: [FeatureTogglesGuardService],
    data: { breadcrumbs: 'iSCSI' },
    children: [
      { path: '', redirectTo: 'overview', pathMatch: 'full' },
      { path: 'overview', component: IscsiComponent, data: { breadcrumbs: 'Overview' } },
      {
        path: 'targets',
        data: { breadcrumbs: 'Targets' },
        children: [
          { path: '', component: IscsiTargetListComponent },
          {
            path: URLVerbs.CREATE,
            component: IscsiTargetFormComponent,
            data: { breadcrumbs: ActionLabels.CREATE }
          },
          {
            path: `${URLVerbs.EDIT}/:target_iqn`,
            component: IscsiTargetFormComponent,
            data: { breadcrumbs: ActionLabels.EDIT }
          }
        ]
      }
    ]
  }
];

@NgModule({
  imports: [BlockModule, RouterModule.forChild(routes)]
})
export class RoutedBlockModule {}
