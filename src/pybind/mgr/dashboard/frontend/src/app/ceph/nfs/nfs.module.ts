import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbTooltipModule, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { NfsDetailsComponent } from './nfs-details/nfs-details.component';
import { NfsFormClientComponent } from './nfs-form-client/nfs-form-client.component';
import { NfsFormComponent } from './nfs-form/nfs-form.component';
import { NfsListComponent } from './nfs-list/nfs-list.component';
import {
  ButtonModule,
  CheckboxModule,
  GridModule,
  IconModule,
  IconService,
  InputModule,
  RadioModule,
  SelectModule
} from 'carbon-components-angular';

import Close from '@carbon/icons/es/close/32';

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
    RadioModule,
    CheckboxModule,
    ButtonModule,
    IconModule
  ],
  exports: [NfsListComponent, NfsFormComponent, NfsDetailsComponent],
  declarations: [NfsListComponent, NfsDetailsComponent, NfsFormComponent, NfsFormClientComponent]
})
export class NfsModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([Close]);
  }
}
