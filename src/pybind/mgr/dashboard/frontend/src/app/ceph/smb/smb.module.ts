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
  declarations: [SmbClusterListComponent]
})
export class SmbModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([Close]);
  }
}
