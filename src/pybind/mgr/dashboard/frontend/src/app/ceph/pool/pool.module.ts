import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';

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
import {
  IconModule,
  InputModule,
  CheckboxModule,
  RadioModule,
  SelectModule,
  NumberModule,
  TabsModule,
  AccordionModule,
  TagModule,
  TooltipModule,
  ComboBoxModule,
  ToggletipModule,
  IconService,
  LayoutModule,
  SkeletonModule,
  ModalModule,
  ButtonModule,
  GridModule,
  DropdownModule
} from 'carbon-components-angular';
import HelpIcon from '@carbon/icons/es/help/16';
import UnlockedIcon from '@carbon/icons/es/unlocked/16';
import LockedIcon from '@carbon/icons/es/locked/16';
import EditIcon from '@carbon/icons/es/edit/16';
import ScalesIcon from '@carbon/icons/es/scales/20';
import UserIcon from '@carbon/icons/es/user/16';
import CubeIcon from '@carbon/icons/es/cube/20';
import ShareIcon from '@carbon/icons/es/share/16';
import ViewIcon from '@carbon/icons/es/view/16';
import PasswordIcon from '@carbon/icons/es/password/16';
import ArrowDownIcon from '@carbon/icons/es/arrow--down/16';
import ProgressBarRoundIcon from '@carbon/icons/es/progress-bar--round/32';
import ToolsIcon from '@carbon/icons/es/tools/32';
import ParentChild from '@carbon/icons/es/parent-child/20';
import UserAccessLocked from '@carbon/icons/es/user--access-locked/16';

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
    IconModule,
    InputModule,
    AccordionModule,
    CheckboxModule,
    NumberModule,
    TabsModule,
    TagModule,
    TooltipModule,
    ComboBoxModule,
    ToggletipModule,
    RadioModule,
    SelectModule,
    LayoutModule,
    SkeletonModule,
    ModalModule,
    ButtonModule,
    GridModule,
    DropdownModule
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
export class PoolModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([
      HelpIcon,
      UnlockedIcon,
      LockedIcon,
      EditIcon,
      ScalesIcon,
      CubeIcon,
      UserIcon,
      ShareIcon,
      ViewIcon,
      PasswordIcon,
      ArrowDownIcon,
      ProgressBarRoundIcon,
      ToolsIcon,
      ParentChild,
      UserAccessLocked,
      LockedIcon,
      UnlockedIcon
    ]);
  }
}

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
