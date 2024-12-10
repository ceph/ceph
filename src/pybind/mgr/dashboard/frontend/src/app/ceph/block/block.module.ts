import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

import { NgbNavModule, NgbPopoverModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { PipesModule } from '~/app/shared/pipes/pipes.module';

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
import { NvmeofGatewayComponent } from './nvmeof-gateway/nvmeof-gateway.component';
import { NvmeofSubsystemsComponent } from './nvmeof-subsystems/nvmeof-subsystems.component';
import { NvmeofSubsystemsDetailsComponent } from './nvmeof-subsystems-details/nvmeof-subsystems-details.component';
import { NvmeofTabsComponent } from './nvmeof-tabs/nvmeof-tabs.component';
import { NvmeofSubsystemsFormComponent } from './nvmeof-subsystems-form/nvmeof-subsystems-form.component';
import { NvmeofListenersFormComponent } from './nvmeof-listeners-form/nvmeof-listeners-form.component';
import { NvmeofListenersListComponent } from './nvmeof-listeners-list/nvmeof-listeners-list.component';
import { NvmeofNamespacesListComponent } from './nvmeof-namespaces-list/nvmeof-namespaces-list.component';
import { NvmeofNamespacesFormComponent } from './nvmeof-namespaces-form/nvmeof-namespaces-form.component';
import { NvmeofInitiatorsListComponent } from './nvmeof-initiators-list/nvmeof-initiators-list.component';
import { NvmeofInitiatorsFormComponent } from './nvmeof-initiators-form/nvmeof-initiators-form.component';

import {
  ButtonModule,
  CheckboxModule,
  ComboBoxModule,
  DatePickerModule,
  GridModule,
  IconModule,
  IconService,
  InputModule,
  ModalModule,
  NumberModule,
  RadioModule,
  SelectModule,
  UIShellModule,
  TreeviewModule
} from 'carbon-components-angular';

// Icons
import ChevronDown from '@carbon/icons/es/chevron--down/16';
import Close from '@carbon/icons/es/close/32';
import AddFilled from '@carbon/icons/es/add--filled/32';
import SubtractFilled from '@carbon/icons/es/subtract--filled/32';
import Reset from '@carbon/icons/es/reset/32';

@NgModule({
  imports: [
    CommonModule,
    MirroringModule,
    FormsModule,
    ReactiveFormsModule,
    NgbNavModule,
    NgbPopoverModule,
    NgbTooltipModule,
    PipesModule,
    SharedModule,
    RouterModule,
    TreeviewModule,
    UIShellModule,
    InputModule,
    GridModule,
    ButtonModule,
    IconModule,
    CheckboxModule,
    RadioModule,
    SelectModule,
    NumberModule,
    ModalModule,
    DatePickerModule,
    ComboBoxModule
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
    RbdPerformanceComponent,
    NvmeofGatewayComponent,
    NvmeofSubsystemsComponent,
    NvmeofSubsystemsDetailsComponent,
    NvmeofTabsComponent,
    NvmeofSubsystemsFormComponent,
    NvmeofListenersFormComponent,
    NvmeofListenersListComponent,
    NvmeofNamespacesListComponent,
    NvmeofNamespacesFormComponent,
    NvmeofInitiatorsListComponent,
    NvmeofInitiatorsFormComponent
  ],
  exports: [RbdConfigurationListComponent, RbdConfigurationFormComponent]
})
export class BlockModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([ChevronDown, Close, AddFilled, SubtractFilled, Reset]);
  }
}

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
  },
  // NVMe/TCP
  {
    path: 'nvmeof',
    canActivate: [ModuleStatusGuardService],
    data: {
      breadcrumbs: true,
      text: 'NVMe/TCP',
      path: 'nvmeof',
      disableSplit: true,
      moduleStatusGuardConfig: {
        uiApiPath: 'nvmeof',
        redirectTo: 'error',
        header: $localize`NVMe/TCP Gateway not configured`,
        button_name: $localize`Configure NVMe/TCP`,
        button_route: ['/services', { outlets: { modal: ['create', 'nvmeof'] } }],
        uiConfig: false
      }
    },
    children: [
      { path: '', redirectTo: 'subsystems', pathMatch: 'full' },
      {
        path: 'subsystems',
        component: NvmeofSubsystemsComponent,
        data: { breadcrumbs: 'Subsystems' },
        children: [
          // subsystems
          { path: '', component: NvmeofSubsystemsComponent },
          {
            path: URLVerbs.CREATE,
            component: NvmeofSubsystemsFormComponent,
            outlet: 'modal'
          },
          // listeners
          {
            path: `${URLVerbs.CREATE}/:subsystem_nqn/listener`,
            component: NvmeofListenersFormComponent,
            outlet: 'modal'
          },
          // namespaces
          {
            path: `${URLVerbs.CREATE}/:subsystem_nqn/namespace`,
            component: NvmeofNamespacesFormComponent,
            outlet: 'modal'
          },
          {
            path: `${URLVerbs.EDIT}/:subsystem_nqn/namespace/:nsid`,
            component: NvmeofNamespacesFormComponent,
            outlet: 'modal'
          },
          // initiators
          {
            path: `${URLVerbs.ADD}/:subsystem_nqn/initiator`,
            component: NvmeofInitiatorsFormComponent,
            outlet: 'modal'
          }
        ]
      },
      { path: 'gateways', component: NvmeofGatewayComponent, data: { breadcrumbs: 'Gateways' } }
    ]
  }
];

@NgModule({
  imports: [BlockModule, RouterModule.forChild(routes)]
})
export class RoutedBlockModule {}
