import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbTooltipModule, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';

import {
  ButtonModule,
  GridModule,
  IconModule,
  IconService,
  InputModule,
  SelectModule
} from 'carbon-components-angular';

import Close from '@carbon/icons/es/close/32';
import { SmbClusterListComponent } from './smb-cluster-list/smb-cluster-list.component';
import { SmbUsersgroupsListComponent } from './smb-usersgroups-list/smb-usersgroups-list.component';
import { SmbTabsComponent } from './smb-tabs/smb-tabs.component';
import { SmbJoinAuthListComponent } from './smb-join-auth-list/smb-join-auth-list.component';
import { SmbJoinAuthFormComponent } from './smb-join-auth-form/smb-join-auth-form.component';
import { SmbUsersgroupsFormComponent } from './smb-usersgroups-form/smb-usersgroups-form.component';

@NgModule({
  imports: [
    ReactiveFormsModule,
    RouterModule,
    SharedModule,
    NgbNavModule,
    CommonModule,
    NgbTypeaheadModule,
    NgbTooltipModule,
    GridModule,
    SelectModule,
    InputModule,
    ButtonModule,
    IconModule
  ],
  exports: [SmbClusterListComponent],
  declarations: [
    SmbClusterListComponent,
    SmbUsersgroupsListComponent,
    SmbTabsComponent,
    SmbJoinAuthListComponent,
    SmbJoinAuthFormComponent,
    SmbUsersgroupsFormComponent
  ]
})
export class SmbModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([Close]);
  }
}
