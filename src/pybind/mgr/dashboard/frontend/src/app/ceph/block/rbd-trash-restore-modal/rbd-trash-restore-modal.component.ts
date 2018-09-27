import { Component, OnInit } from '@angular/core';

import { BsModalRef } from 'ngx-bootstrap';

import { RbdService } from '../../../shared/api/rbd.service';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';

@Component({
  selector: 'cd-rbd-trash-restore-modal',
  templateUrl: './rbd-trash-restore-modal.component.html',
  styleUrls: ['./rbd-trash-restore-modal.component.scss']
})
export class RbdTrashRestoreModalComponent implements OnInit {
  metaType: string;
  poolName: string;
  imageName: string;
  imageId: string;
  executingTasks: ExecutingTask[];

  restoreForm: CdFormGroup;

  constructor(
    private rbdService: RbdService,
    public modalRef: BsModalRef,
    private fb: CdFormBuilder,
    private taskWrapper: TaskWrapperService
  ) {}

  ngOnInit() {
    this.restoreForm = this.fb.group({
      name: this.imageName
    });
  }

  restore() {
    const name = this.restoreForm.getValue('name');

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('rbd/trash/restore', {
          pool_name: this.poolName,
          image_id: this.imageId,
          image_name: this.imageName,
          new_image_name: name
        }),
        call: this.rbdService.restoreTrash(this.poolName, this.imageId, name)
      })
      .subscribe(
        undefined,
        () => {
          this.restoreForm.setErrors({ cdSubmitButton: true });
        },
        () => {
          this.modalRef.hide();
        }
      );
  }
}
