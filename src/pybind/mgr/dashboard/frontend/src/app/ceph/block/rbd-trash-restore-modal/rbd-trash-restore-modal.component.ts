import { Component, OnInit } from '@angular/core';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { RbdService } from '../../../shared/api/rbd.service';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { ImageSpec } from '../../../shared/models/image-spec';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';

@Component({
  selector: 'cd-rbd-trash-restore-modal',
  templateUrl: './rbd-trash-restore-modal.component.html',
  styleUrls: ['./rbd-trash-restore-modal.component.scss']
})
export class RbdTrashRestoreModalComponent implements OnInit {
  poolName: string;
  namespace: string;
  imageName: string;
  imageSpec: string;
  imageId: string;
  executingTasks: ExecutingTask[];

  restoreForm: CdFormGroup;

  constructor(
    private rbdService: RbdService,
    public activeModal: NgbActiveModal,
    private fb: CdFormBuilder,
    private taskWrapper: TaskWrapperService
  ) {}

  ngOnInit() {
    this.imageSpec = new ImageSpec(this.poolName, this.namespace, this.imageName).toString();
    this.restoreForm = this.fb.group({
      name: this.imageName
    });
  }

  restore() {
    const name = this.restoreForm.getValue('name');
    const imageSpec = new ImageSpec(this.poolName, this.namespace, this.imageId);

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('rbd/trash/restore', {
          image_id_spec: imageSpec.toString(),
          new_image_name: name
        }),
        call: this.rbdService.restoreTrash(imageSpec, name)
      })
      .subscribe({
        error: () => {
          this.restoreForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.activeModal.close();
        }
      });
  }
}
