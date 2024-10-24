import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';
import {
  TabsModule
} from 'carbon-components-angular';

import { NgbNavModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabels, URLVerbs } from '~/app/shared/constants/app.constants';
import { SharedModule } from '~/app/shared/shared.module';
import { BlockModule } from '../block/block.module';
import { CephSharedModule } from '../shared/ceph-shared.module';
import { CrushRuleFormModalComponent } from './crush-rule-form-modal/crush-rule-form-modal.component';
import { ErasureCodeProfileFormModalComponent } from './erasure-code-profile-form/erasure-code-profile-form-modal.component';
import { PoolDetailsComponent } from './pool-details/pool-details.component';
import { PoolFormComponent } from './pool-form/pool-form.component';
import { PoolListComponent } from './pool-list/pool-list.component';

@NgModule({
  imports: [
    CephSharedModule,
    CommonModule,
    NgbNavModule,
    SharedModule,
    RouterModule,
    ReactiveFormsModule,
    NgbTooltipModule,
    BlockModule,
    TabsModule
  ],
  exports: [PoolListComponent, PoolFormComponent],
  declarations: [
    PoolListComponent,
    PoolFormComponent,
    ErasureCodeProfileFormModalComponent,
    CrushRuleFormModalComponent,
    PoolDetailsComponent
  ]
})
export class PoolModule {}

const routes: Routes = [
  { path: '', component: PoolListComponent },
  {
    path: URLVerbs.CREATE,
    component: PoolFormComponent,
    data: { breadcrumbs: ActionLabels.CREATE }
  },
  {
    path: `${URLVerbs.EDIT}/:name`,
    component: PoolFormComponent,
    data: { breadcrumbs: ActionLabels.EDIT }
  }
];

@NgModule({
  imports: [PoolModule, RouterModule.forChild(routes)]
})
export class RoutedPoolModule {}
