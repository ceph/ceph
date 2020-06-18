import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbProgressbarModule } from '@ng-bootstrap/ng-bootstrap';
import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';
import { ModalModule } from 'ngx-bootstrap/modal';

import { SharedModule } from '../../../shared/shared.module';
import { BootstrapCreateModalComponent } from './bootstrap-create-modal/bootstrap-create-modal.component';
import { BootstrapImportModalComponent } from './bootstrap-import-modal/bootstrap-import-modal.component';
import { DaemonListComponent } from './daemon-list/daemon-list.component';
import { EditSiteNameModalComponent } from './edit-site-name-modal/edit-site-name-modal.component';
import { ImageListComponent } from './image-list/image-list.component';
import { MirrorHealthColorPipe } from './mirror-health-color.pipe';
import { OverviewComponent } from './overview/overview.component';
import { PoolEditModeModalComponent } from './pool-edit-mode-modal/pool-edit-mode-modal.component';
import { PoolEditPeerModalComponent } from './pool-edit-peer-modal/pool-edit-peer-modal.component';
import { PoolListComponent } from './pool-list/pool-list.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    NgbNavModule,
    RouterModule,
    FormsModule,
    ReactiveFormsModule,
    NgbProgressbarModule,
    ModalModule.forRoot(),
    NgBootstrapFormValidationModule
  ],
  declarations: [
    BootstrapCreateModalComponent,
    BootstrapImportModalComponent,
    DaemonListComponent,
    EditSiteNameModalComponent,
    ImageListComponent,
    OverviewComponent,
    PoolEditModeModalComponent,
    PoolEditPeerModalComponent,
    PoolListComponent,
    MirrorHealthColorPipe
  ],
  exports: [OverviewComponent]
})
export class MirroringModule {}
