import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { Observable, Subject } from 'rxjs';
import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';

import { RbdService } from '~/app/shared/api/rbd.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ImageSpec } from '~/app/shared/models/image-spec';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskManagerService } from '~/app/shared/services/task-manager.service';

@Component({
  selector: 'cd-rbd-snapshot-form-modal',
  templateUrl: './rbd-snapshot-form-modal.component.html',
  styleUrls: ['./rbd-snapshot-form-modal.component.scss']
})
export class RbdSnapshotFormModalComponent implements OnInit {
  poolName: string;
  namespace: string;
  imageName: string;
  snapName: string;
  mirroring: string;

  snapshotForm: CdFormGroup;

  editing = false;
  action: string;
  resource: string;

  public onSubmit: Subject<string> = new Subject();

  peerConfigured$: Observable<any>;

  constructor(
    public activeModal: NgbActiveModal,
    private rbdService: RbdService,
    private taskManagerService: TaskManagerService,
    private notificationService: NotificationService,
    private actionLabels: ActionLabelsI18n,
    private rbdMirrorService: RbdMirroringService
  ) {
    this.action = this.actionLabels.CREATE;
    this.resource = $localize`RBD Snapshot`;
    this.createForm();
  }

  createForm() {
    this.snapshotForm = new CdFormGroup({
      snapshotName: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      mirrorImageSnapshot: new UntypedFormControl(false, {})
    });
  }

  ngOnInit(): void {
    this.peerConfigured$ = this.rbdMirrorService.getPeerForPool(this.poolName);
  }

  setSnapName(snapName: string) {
    this.snapName = snapName;
    this.snapshotForm.get('snapshotName').setValue(snapName);
  }

  onMirrorCheckBoxChange() {
    if (this.snapshotForm.getValue('mirrorImageSnapshot') === true) {
      this.snapshotForm.get('snapshotName').setValue('');
      this.snapshotForm.get('snapshotName').clearValidators();
    } else {
      this.snapshotForm.get('snapshotName').setValue(this.snapName);
      this.snapshotForm.get('snapshotName').setValidators([Validators.required]);
      this.snapshotForm.get('snapshotName').updateValueAndValidity();
    }
  }

  /**
   * Set the 'editing' flag. If set to TRUE, the modal dialog is in
   * 'Edit' mode, otherwise in 'Create' mode.
   * @param {boolean} editing
   */
  setEditing(editing: boolean = true) {
    this.editing = editing;
    this.action = this.editing ? this.actionLabels.RENAME : this.actionLabels.CREATE;
  }

  editAction() {
    const snapshotName = this.snapshotForm.getValue('snapshotName');
    const imageSpec = new ImageSpec(this.poolName, this.namespace, this.imageName);
    const finishedTask = new FinishedTask();
    finishedTask.name = 'rbd/snap/edit';
    finishedTask.metadata = {
      image_spec: imageSpec.toString(),
      snapshot_name: snapshotName
    };
    this.rbdService
      .renameSnapshot(imageSpec, this.snapName, snapshotName)
      .toPromise()
      .then(() => {
        this.taskManagerService.subscribe(
          finishedTask.name,
          finishedTask.metadata,
          (asyncFinishedTask: FinishedTask) => {
            this.notificationService.notifyTask(asyncFinishedTask);
          }
        );
        this.activeModal.close();
        this.onSubmit.next(this.snapName);
      })
      .catch(() => {
        this.snapshotForm.setErrors({ cdSubmitButton: true });
      });
  }

  createAction() {
    const snapshotName = this.snapshotForm.getValue('snapshotName');
    const mirrorImageSnapshot = this.snapshotForm.getValue('mirrorImageSnapshot');
    const imageSpec = new ImageSpec(this.poolName, this.namespace, this.imageName);
    const finishedTask = new FinishedTask();
    finishedTask.name = 'rbd/snap/create';
    finishedTask.metadata = {
      image_spec: imageSpec.toString(),
      snapshot_name: snapshotName
    };
    this.rbdService
      .createSnapshot(imageSpec, snapshotName, mirrorImageSnapshot)
      .toPromise()
      .then(() => {
        this.taskManagerService.subscribe(
          finishedTask.name,
          finishedTask.metadata,
          (asyncFinishedTask: FinishedTask) => {
            this.notificationService.notifyTask(asyncFinishedTask);
          }
        );
        this.activeModal.close();
        this.onSubmit.next(snapshotName);
      })
      .catch(() => {
        this.snapshotForm.setErrors({ cdSubmitButton: true });
      });
  }

  submit() {
    if (this.editing) {
      this.editAction();
    } else {
      this.createAction();
    }
  }
}
