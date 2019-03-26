import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

import { TreeModule } from 'ng2-tree';
import { BsDatepickerModule } from 'ngx-bootstrap/datepicker';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ModalModule } from 'ngx-bootstrap/modal';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { TooltipModule } from 'ngx-bootstrap/tooltip';

import { ActionLabels, URLVerbs } from '../../shared/constants/app.constants';
import { FeatureTogglesGuardService } from '../../shared/services/feature-toggles-guard.service';
import { SharedModule } from '../../shared/shared.module';
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
import { RbdConfigurationFormComponent } from './rbd-configuration-form/rbd-configuration-form.component';
import { RbdConfigurationListComponent } from './rbd-configuration-list/rbd-configuration-list.component';
import { RbdDetailsComponent } from './rbd-details/rbd-details.component';
import { RbdFormComponent } from './rbd-form/rbd-form.component';
import { RbdImagesComponent } from './rbd-images/rbd-images.component';
import { RbdListComponent } from './rbd-list/rbd-list.component';
import { RbdSnapshotFormComponent } from './rbd-snapshot-form/rbd-snapshot-form.component';
import { RbdSnapshotListComponent } from './rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdTrashListComponent } from './rbd-trash-list/rbd-trash-list.component';
import { RbdTrashMoveModalComponent } from './rbd-trash-move-modal/rbd-trash-move-modal.component';
import { RbdTrashPurgeModalComponent } from './rbd-trash-purge-modal/rbd-trash-purge-modal.component';
import { RbdTrashRestoreModalComponent } from './rbd-trash-restore-modal/rbd-trash-restore-modal.component';

@NgModule({
  entryComponents: [
    RbdDetailsComponent,
    RbdSnapshotFormComponent,
    RbdTrashMoveModalComponent,
    RbdTrashRestoreModalComponent,
    RbdTrashPurgeModalComponent,
    IscsiTargetDetailsComponent,
    IscsiTargetImageSettingsModalComponent,
    IscsiTargetIqnSettingsModalComponent,
    IscsiTargetDiscoveryModalComponent
  ],
  imports: [
    CommonModule,
    MirroringModule,
    FormsModule,
    ReactiveFormsModule,
    TabsModule.forRoot(),
    ProgressbarModule.forRoot(),
    BsDropdownModule.forRoot(),
    BsDatepickerModule.forRoot(),
    TooltipModule.forRoot(),
    ModalModule.forRoot(),
    SharedModule,
    RouterModule,
    TreeModule
  ],
  declarations: [
    RbdListComponent,
    IscsiComponent,
    IscsiTabsComponent,
    IscsiTargetListComponent,
    RbdDetailsComponent,
    RbdFormComponent,
    RbdSnapshotListComponent,
    RbdSnapshotFormComponent,
    RbdTrashListComponent,
    RbdTrashMoveModalComponent,
    RbdImagesComponent,
    RbdTrashRestoreModalComponent,
    RbdTrashPurgeModalComponent,
    IscsiTargetDetailsComponent,
    IscsiTargetFormComponent,
    IscsiTargetImageSettingsModalComponent,
    IscsiTargetIqnSettingsModalComponent,
    IscsiTargetDiscoveryModalComponent,
    RbdConfigurationListComponent,
    RbdConfigurationFormComponent
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
    canActivate: [FeatureTogglesGuardService],
    data: { breadcrumbs: 'Images' },
    children: [
      { path: '', component: RbdImagesComponent },
      {
        path: URLVerbs.CREATE,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.CREATE }
      },
      {
        path: `${URLVerbs.EDIT}/:pool/:name`,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.EDIT }
      },
      {
        path: `${URLVerbs.CLONE}/:pool/:name/:snap`,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.CLONE }
      },
      {
        path: `${URLVerbs.COPY}/:pool/:name`,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.COPY }
      },
      {
        path: `${URLVerbs.COPY}/:pool/:name/:snap`,
        component: RbdFormComponent,
        data: { breadcrumbs: ActionLabels.COPY }
      }
    ]
  },
  {
    path: 'mirroring',
    component: RbdMirroringComponent,
    canActivate: [FeatureTogglesGuardService],
    data: { breadcrumbs: 'Mirroring' }
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
            path: URLVerbs.ADD,
            component: IscsiTargetFormComponent,
            data: { breadcrumbs: ActionLabels.ADD }
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
