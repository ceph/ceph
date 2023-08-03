import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { Pool } from '../../pool/pool';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import _ from 'lodash';
import { CdValidators } from '~/app/shared/forms/cd-validators';

@Component({
  selector: 'cd-cephfs-subvolume-form',
  templateUrl: './cephfs-subvolume-form.component.html',
  styleUrls: ['./cephfs-subvolume-form.component.scss']
})
export class CephfsSubvolumeFormComponent implements OnInit {
  fsName: string;
  pools: Pool[];

  subvolumeForm: CdFormGroup;

  action: string;
  resource: string;

  dataPools: Pool[];

  columns: CdTableColumn[];
  scopePermissions: Array<any> = [];
  scopes: string[] = ['owner', 'group', 'others'];

  constructor(
    public activeModal: NgbActiveModal,
    private actionLabels: ActionLabelsI18n,
    private taskWrapper: TaskWrapperService,
    private cephFsSubvolumeService: CephfsSubvolumeService,
    private formatter: FormatterService
  ) {
    this.action = this.actionLabels.CREATE;
    this.resource = $localize`Subvolume`;
  }

  ngOnInit(): void {
    this.columns = [
      {
        prop: 'scope',
        name: $localize`All`,
        flexGrow: 0.5
      },
      {
        prop: 'read',
        name: $localize`Read`,
        flexGrow: 0.5,
        cellClass: 'text-center'
      },
      {
        prop: 'write',
        name: $localize`Write`,
        flexGrow: 0.5,
        cellClass: 'text-center'
      },
      {
        prop: 'execute',
        name: $localize`Execute`,
        flexGrow: 0.5,
        cellClass: 'text-center'
      }
    ];

    this.dataPools = this.pools.filter((pool) => pool.type === 'data');
    this.createForm();
  }

  createForm() {
    this.subvolumeForm = new CdFormGroup({
      volumeName: new FormControl({ value: this.fsName, disabled: true }),
      subvolumeName: new FormControl('', {
        validators: [Validators.required],
        asyncValidators: [
          CdValidators.unique(
            this.cephFsSubvolumeService.exists,
            this.cephFsSubvolumeService,
            null,
            null,
            this.fsName
          )
        ]
      }),
      pool: new FormControl(this.dataPools[0]?.pool, {
        validators: [Validators.required]
      }),
      size: new FormControl(null, {
        updateOn: 'blur'
      }),
      uid: new FormControl(null),
      gid: new FormControl(null),
      mode: new FormControl({}),
      isolatedNamespace: new FormControl(false)
    });
  }

  submit() {
    const subVolumeName = this.subvolumeForm.getValue('subvolumeName');
    const pool = this.subvolumeForm.getValue('pool');
    const size = this.formatter.toBytes(this.subvolumeForm.getValue('size'));
    const uid = this.subvolumeForm.getValue('uid');
    const gid = this.subvolumeForm.getValue('gid');
    const mode = this.formatter.toOctalPermission(this.subvolumeForm.getValue('mode'));
    const isolatedNamespace = this.subvolumeForm.getValue('isolatedNamespace');
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('cephfs/subvolume/' + URLVerbs.CREATE, {
          subVolumeName: subVolumeName
        }),
        call: this.cephFsSubvolumeService.create(
          this.fsName,
          subVolumeName,
          pool,
          size,
          uid,
          gid,
          mode,
          isolatedNamespace
        )
      })
      .subscribe({
        error: () => {
          this.subvolumeForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.activeModal.close();
        }
      });
  }
}
