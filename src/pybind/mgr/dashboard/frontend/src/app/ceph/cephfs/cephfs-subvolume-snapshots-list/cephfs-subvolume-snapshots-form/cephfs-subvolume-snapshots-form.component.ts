import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import moment from 'moment';
import { Observable } from 'rxjs';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { CephfsSubvolume } from '~/app/shared/models/cephfs-subvolume.model';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-cephfs-subvolume-snapshots-form',
  templateUrl: './cephfs-subvolume-snapshots-form.component.html',
  styleUrls: ['./cephfs-subvolume-snapshots-form.component.scss']
})
export class CephfsSubvolumeSnapshotsFormComponent extends CdForm implements OnInit {
  fsName: string;
  subVolumeName: string;
  subVolumeGroupName: string;
  subVolumeGroups: string[];

  isEdit = false;

  snapshotForm: CdFormGroup;

  action: string;
  resource: string;

  subVolumes$: Observable<CephfsSubvolume[]>;

  constructor(
    public activeModal: NgbActiveModal,
    private actionLabels: ActionLabelsI18n,
    private taskWrapper: TaskWrapperService,
    private cephFsSubvolumeService: CephfsSubvolumeService
  ) {
    super();
    this.resource = $localize`snapshot`;
    this.action = this.actionLabels.CREATE;
  }

  ngOnInit(): void {
    this.createForm();

    this.subVolumes$ = this.cephFsSubvolumeService.get(this.fsName, this.subVolumeGroupName, false);
    this.loadingReady();
  }

  createForm() {
    this.snapshotForm = new CdFormGroup({
      snapshotName: new FormControl(moment().toISOString(true), {
        validators: [Validators.required],
        asyncValidators: [
          CdValidators.unique(
            this.cephFsSubvolumeService.snapshotExists,
            this.cephFsSubvolumeService,
            null,
            null,
            this.fsName,
            this.subVolumeName,
            this.subVolumeGroupName
          )
        ]
      }),
      volumeName: new FormControl({ value: this.fsName, disabled: true }),
      subVolumeName: new FormControl(this.subVolumeName),
      subvolumeGroupName: new FormControl(this.subVolumeGroupName)
    });
  }

  onSelectionChange(groupName: string) {
    this.subVolumeGroupName = groupName;
    this.subVolumes$ = this.cephFsSubvolumeService.get(this.fsName, this.subVolumeGroupName, false);
    this.subVolumes$.subscribe((subVolumes) => {
      this.subVolumeName = subVolumes[0].name;
      this.snapshotForm.get('subVolumeName').setValue(this.subVolumeName);

      this.resetValidators();
    });
  }

  resetValidators(subVolumeName?: string) {
    this.subVolumeName = subVolumeName;
    this.snapshotForm
      .get('snapshotName')
      .setAsyncValidators(
        CdValidators.unique(
          this.cephFsSubvolumeService.snapshotExists,
          this.cephFsSubvolumeService,
          null,
          null,
          this.fsName,
          this.subVolumeName,
          this.subVolumeGroupName
        )
      );
    this.snapshotForm.get('snapshotName').updateValueAndValidity();
  }

  submit() {
    const snapshotName = this.snapshotForm.getValue('snapshotName');
    const subVolumeName = this.snapshotForm.getValue('subVolumeName');
    const subVolumeGroupName = this.snapshotForm.getValue('subvolumeGroupName');
    const volumeName = this.snapshotForm.getValue('volumeName');

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('cephfs/subvolume/snapshot/' + URLVerbs.CREATE, {
          snapshotName: snapshotName
        }),
        call: this.cephFsSubvolumeService.createSnapshot(
          volumeName,
          snapshotName,
          subVolumeName,
          subVolumeGroupName
        )
      })
      .subscribe({
        error: () => this.snapshotForm.setErrors({ cdSubmitButton: true }),
        complete: () => this.activeModal.close()
      });
  }
}
