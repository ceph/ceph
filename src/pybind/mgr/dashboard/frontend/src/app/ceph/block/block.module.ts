import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { BsDropdownModule, ModalModule, TabsModule, TooltipModule } from 'ngx-bootstrap';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';

import { SharedModule } from '../../shared/shared.module';
import {
  FlattenConfirmationModalComponent
} from './flatten-confirmation-modal/flatten-confimation-modal.component';
import { IscsiComponent } from './iscsi/iscsi.component';
import { MirrorHealthColorPipe } from './mirror-health-color.pipe';
import { MirroringComponent } from './mirroring/mirroring.component';
import { RbdDetailsComponent } from './rbd-details/rbd-details.component';
import { RbdFormComponent } from './rbd-form/rbd-form.component';
import { RbdListComponent } from './rbd-list/rbd-list.component';
import { RbdSnapshotFormComponent } from './rbd-snapshot-form/rbd-snapshot-form.component';
import { RbdSnapshotListComponent } from './rbd-snapshot-list/rbd-snapshot-list.component';
import {
  RollbackConfirmationModalComponent
} from './rollback-confirmation-modal/rollback-confimation-modal.component';

@NgModule({
  entryComponents: [
    RbdDetailsComponent,
    RbdSnapshotFormComponent,
    RollbackConfirmationModalComponent,
    FlattenConfirmationModalComponent
  ],
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    TabsModule.forRoot(),
    ProgressbarModule.forRoot(),
    BsDropdownModule.forRoot(),
    TooltipModule.forRoot(),
    ModalModule.forRoot(),
    SharedModule,
    RouterModule
  ],
  declarations: [
    RbdListComponent,
    IscsiComponent,
    MirroringComponent,
    MirrorHealthColorPipe,
    RbdDetailsComponent,
    RbdFormComponent,
    RbdSnapshotListComponent,
    RbdSnapshotFormComponent,
    RollbackConfirmationModalComponent,
    FlattenConfirmationModalComponent
  ]
})
export class BlockModule { }
