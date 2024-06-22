import { Component, Inject, OnInit, Optional } from '@angular/core';

import { BaseModal } from 'carbon-components-angular';

import { RbdService } from '~/app/shared/api/rbd.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ImageSpec } from '~/app/shared/models/image-spec';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-rbd-trash-restore-modal',
  templateUrl: './rbd-trash-restore-modal.component.html',
  styleUrls: ['./rbd-trash-restore-modal.component.scss']
})
export class RbdTrashRestoreModalComponent extends BaseModal implements OnInit {
  executingTasks: ExecutingTask[];

  restoreForm: CdFormGroup;

  constructor(
    private rbdService: RbdService,
    public actionLabels: ActionLabelsI18n,
    private fb: CdFormBuilder,
    private taskWrapper: TaskWrapperService,

    @Inject('poolName') public poolName: string,
    @Inject('namespace') public namespace: string,
    @Inject('imageName') public imageName: string,
    @Inject('imageId') public imageId: string,
    @Optional() @Inject('imageSpec') public imageSpec = ''
  ) {
    super();
  }

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
          this.closeModal();
        }
      });
  }
}
