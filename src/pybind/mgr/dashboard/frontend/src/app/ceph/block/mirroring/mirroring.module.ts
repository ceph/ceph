import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbProgressbarModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { BootstrapCreateModalComponent } from './bootstrap-create-modal/bootstrap-create-modal.component';
import { BootstrapImportModalComponent } from './bootstrap-import-modal/bootstrap-import-modal.component';
import { DaemonListComponent } from './daemon-list/daemon-list.component';
import { ImageListComponent } from './image-list/image-list.component';
import { MirrorHealthColorPipe } from './mirror-health-color.pipe';
import { OverviewComponent } from './overview/overview.component';
import { PoolEditModeModalComponent } from './pool-edit-mode-modal/pool-edit-mode-modal.component';
import { PoolEditPeerModalComponent } from './pool-edit-peer-modal/pool-edit-peer-modal.component';
import { PoolListComponent } from './pool-list/pool-list.component';
import {
  ButtonModule,
  CheckboxModule,
  GridModule,
  IconModule,
  IconService,
  InputModule,
  ModalModule,
  SelectModule
} from 'carbon-components-angular';

// Icons
import EditIcon from '@carbon/icons/es/edit/32';
import CheckMarkIcon from '@carbon/icons/es/checkmark/32';
import ResetIcon from '@carbon/icons/es/reset/32';
import DocumentAddIcon from '@carbon/icons/es/document--add/16';
import DocumentImportIcon from '@carbon/icons/es/document--import/16';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    NgbNavModule,
    RouterModule,
    FormsModule,
    ReactiveFormsModule,
    NgbProgressbarModule,
    NgbTooltipModule,
    ModalModule,
    InputModule,
    CheckboxModule,
    SelectModule,
    GridModule,
    ButtonModule,
    IconModule
  ],
  declarations: [
    BootstrapCreateModalComponent,
    BootstrapImportModalComponent,
    DaemonListComponent,
    ImageListComponent,
    OverviewComponent,
    PoolEditModeModalComponent,
    PoolEditPeerModalComponent,
    PoolListComponent,
    MirrorHealthColorPipe
  ],
  exports: [OverviewComponent]
})
export class MirroringModule {
  constructor(private iconService: IconService) {
    this.iconService.registerAll([
      EditIcon,
      CheckMarkIcon,
      ResetIcon,
      DocumentAddIcon,
      DocumentImportIcon
    ]);
  }
}
