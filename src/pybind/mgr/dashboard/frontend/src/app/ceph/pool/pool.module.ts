import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { PopoverModule } from 'ngx-bootstrap/popover';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { TooltipModule } from 'ngx-bootstrap/tooltip';

import { ActionLabels, URLVerbs } from '../../shared/constants/app.constants';
import { ServicesModule } from '../../shared/services/services.module';
import { SharedModule } from '../../shared/shared.module';
import { BlockModule } from '../block/block.module';
import { CephSharedModule } from '../shared/ceph-shared.module';
import { ErasureCodeProfileFormComponent } from './erasure-code-profile-form/erasure-code-profile-form.component';
import { PoolDetailsComponent } from './pool-details/pool-details.component';
import { PoolFormComponent } from './pool-form/pool-form.component';
import { PoolListComponent } from './pool-list/pool-list.component';

@NgModule({
  imports: [
    CephSharedModule,
    CommonModule,
    TabsModule,
    PopoverModule.forRoot(),
    SharedModule,
    RouterModule,
    ReactiveFormsModule,
    BsDropdownModule,
    ServicesModule,
    TooltipModule.forRoot(),
    BlockModule
  ],
  exports: [PoolListComponent, PoolFormComponent],
  declarations: [
    PoolListComponent,
    PoolFormComponent,
    ErasureCodeProfileFormComponent,
    PoolDetailsComponent
  ],
  entryComponents: [ErasureCodeProfileFormComponent]
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
