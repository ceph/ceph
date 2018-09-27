import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { BsDropdownModule, ModalModule, TabsModule, TooltipModule } from 'ngx-bootstrap';
import { BsDatepickerModule } from 'ngx-bootstrap/datepicker';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';

import { SharedModule } from '../../shared/shared.module';
import { IscsiComponent } from './iscsi/iscsi.component';
import { MirrorHealthColorPipe } from './mirror-health-color.pipe';
import { MirroringComponent } from './mirroring/mirroring.component';
import { RbdDetailsComponent } from './rbd-details/rbd-details.component';
import { RbdFormComponent } from './rbd-form/rbd-form.component';
import { RbdImagesComponent } from './rbd-images/rbd-images.component';
import { RbdListComponent } from './rbd-list/rbd-list.component';
import { RbdSnapshotFormComponent } from './rbd-snapshot-form/rbd-snapshot-form.component';
import { RbdSnapshotListComponent } from './rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdTrashListComponent } from './rbd-trash-list/rbd-trash-list.component';
import { RbdTrashMoveModalComponent } from './rbd-trash-move-modal/rbd-trash-move-modal.component';
import { RbdTrashPurgeModalComponent } from './rbd-trash-purge-modal/rbd-trash-purge-modal.component';
import { RbdTrashRestoreModalComponent } from './rbd-trash-restore-modal/rbd-trash-restore-modal.component';

@NgModule({
  entryComponents: [
    RbdDetailsComponent,
    RbdSnapshotFormComponent,
    RbdTrashMoveModalComponent,
    RbdTrashRestoreModalComponent,
    RbdTrashPurgeModalComponent
  ],
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    TabsModule.forRoot(),
    ProgressbarModule.forRoot(),
    BsDropdownModule.forRoot(),
    BsDatepickerModule.forRoot(),
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
    RbdTrashListComponent,
    RbdTrashMoveModalComponent,
    RbdImagesComponent,
    RbdTrashRestoreModalComponent,
    RbdTrashPurgeModalComponent
  ]
})
export class BlockModule {}
