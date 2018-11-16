import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { TreeModule } from 'ng2-tree';
import { AlertModule } from 'ngx-bootstrap/alert';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ModalModule } from 'ngx-bootstrap/modal';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { TooltipModule } from 'ngx-bootstrap/tooltip';

import { SharedModule } from '../../../shared/shared.module';

import { DaemonListComponent } from './daemon-list/daemon-list.component';
import { ImageListComponent } from './image-list/image-list.component';
import { MirrorHealthColorPipe } from './mirror-health-color.pipe';
import { OverviewComponent } from './overview/overview.component';
import { PoolEditModeModalComponent } from './pool-edit-mode-modal/pool-edit-mode-modal.component';
import { PoolEditPeerModalComponent } from './pool-edit-peer-modal/pool-edit-peer-modal.component';
import { PoolListComponent } from './pool-list/pool-list.component';

@NgModule({
  entryComponents: [OverviewComponent, PoolEditModeModalComponent, PoolEditPeerModalComponent],
  imports: [
    CommonModule,
    TabsModule.forRoot(),
    SharedModule,
    RouterModule,
    FormsModule,
    ReactiveFormsModule,
    ProgressbarModule.forRoot(),
    BsDropdownModule.forRoot(),
    ModalModule.forRoot(),
    AlertModule.forRoot(),
    TooltipModule.forRoot(),
    TreeModule
  ],
  declarations: [
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
export class MirroringModule {}
