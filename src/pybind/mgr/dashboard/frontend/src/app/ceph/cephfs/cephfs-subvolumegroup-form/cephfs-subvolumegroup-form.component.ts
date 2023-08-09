import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
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
  selector: 'cd-cephfs-subvolumegroup-form',
  templateUrl: './cephfs-subvolumegroup-form.component.html',
  styleUrls: ['./cephfs-subvolumegroup-form.component.scss']
})
export class CephfsSubvolumegroupFormComponent implements OnInit {
  fsName: string;
  pools: Pool[];

  subvolumegroupForm: CdFormGroup;

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
    private cephfsSubvolumeGroupService: CephfsSubvolumeGroupService,
    private formatter: FormatterService
  ) {
    this.action = this.actionLabels.CREATE;
    this.resource = $localize`subvolume group`;
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
    this.subvolumegroupForm = new CdFormGroup({
      volumeName: new FormControl({ value: this.fsName, disabled: true }),
      subvolumegroupName: new FormControl('', {
        validators: [Validators.required],
        asyncValidators: [
          CdValidators.unique(
            this.cephfsSubvolumeGroupService.exists,
            this.cephfsSubvolumeGroupService,
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
      mode: new FormControl({})
    });
  }

  submit() {
    const subvolumegroupName = this.subvolumegroupForm.getValue('subvolumegroupName');
    const pool = this.subvolumegroupForm.getValue('pool');
    const size = this.formatter.toBytes(this.subvolumegroupForm.getValue('size'));
    const uid = this.subvolumegroupForm.getValue('uid');
    const gid = this.subvolumegroupForm.getValue('gid');
    const mode = this.formatter.toOctalPermission(this.subvolumegroupForm.getValue('mode'));
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('cephfs/subvolume/group/' + URLVerbs.CREATE, {
          subvolumegroupName: subvolumegroupName
        }),
        call: this.cephfsSubvolumeGroupService.create(
          this.fsName,
          subvolumegroupName,
          pool,
          size,
          uid,
          gid,
          mode
        )
      })
      .subscribe({
        error: () => {
          this.subvolumegroupForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.activeModal.close();
        }
      });
  }
}
